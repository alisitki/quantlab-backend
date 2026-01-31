/**
 * QuantLab Replay Engine v1.2 — Main Engine
 * 
 * Orchestrates meta loading, validation, and cursor-based streaming replay.
 * Uses ts_event + seq for deterministic ordering (see ORDERING_CONTRACT.js).
 * 
 * Features:
 *   - Cursor-based resume capability
 *   - Clock integration for timing control
 *   - Deterministic event ordering
 */

import { loadMeta, loadMultiMeta } from './MetaLoader.js';
import { validateAll } from './SchemaValidator.js';
import { ParquetReader } from './ParquetReader.js';
import { createCursor, encodeCursor, decodeCursor } from './CursorCodec.js';
import { pageCache, filesCache } from './ReplayCache.js';
import { replayMetrics } from '../../services/replayd/metrics.js';
import { ReplayAggregator } from './ReplayAggregator.js';
import AsapClock from './clock/AsapClock.js';
import crypto from 'node:crypto';

/** Default batch size for streaming */
const DEFAULT_BATCH_SIZE = 10_000;

/**
 * Deterministic event replay engine for compact datasets.
 * Streams rows from a single or multiple parquet + meta.json pairs.
 * Uses cursor-based pagination for large dataset safety.
 */
export class ReplayEngine {
  /** @type {string|string[]} */
  #parquetPath;
  /** @type {string|string[]} */
  #metaPath;
  /** @type {import('./types.js').MetaData|null} */
  #meta = null;
  /** @type {ParquetReader|null} */
  #reader = null;
  /** @type {boolean} */
  #validated = false;
  /** @type {string} */
  #partitionId;
  /** @type {Object} */
  #identity;

  /**
   * @param {string|{parquet: string|string[], meta: string|string[]}} input - Paths or config object
   * @param {string} [metaPath] - Optional meta path
   * @param {Object} [identity] - Optional identity: { stream, date, symbol }
   */
  constructor(input, metaPath, identity = {}) {
    if (typeof input === 'object' && input.parquet && input.meta) {
      this.#parquetPath = input.parquet;
      this.#metaPath = input.meta;
      this.#identity = metaPath || {}; // Handle case where 2nd arg is identity
    } else {
      this.#parquetPath = input;
      this.#metaPath = metaPath;
      this.#identity = identity;
    }
    this.#partitionId = crypto.createHash('md5').update(JSON.stringify(this.#parquetPath)).digest('hex');
  }

  /**
   * Load and validate metadata + parquet schema
   */
  async validate() {
    if (this.#validated) return this.#meta;

    // Load meta.json with identity for proper caching
    if (Array.isArray(this.#metaPath)) {
      this.#meta = await loadMultiMeta(this.#metaPath, this.#identity);
    } else {
      this.#meta = await loadMeta(this.#metaPath, this.#identity);
    }

    this.#reader = new ParquetReader(this.#parquetPath);
    await this.#reader.init();
    const actualRows = await this.#reader.getRowCount();
    validateAll(this.#meta, actualRows);

    this.#validated = true;
    return this.#meta;
  }

  // ... getMeta same ...

  /**
   * Stream replay events as an AsyncGenerator using cursor-based pagination.
   */
  async *replay(opts = {}) {
    const { 
      startTs, 
      endTs, 
      batchSize = DEFAULT_BATCH_SIZE,
      cursor: cursorBase64,
      clock = new AsapClock()
    } = opts;

    await this.validate();

    let cursor = cursorBase64 ? decodeCursor(cursorBase64) : null;
    let rowsEmitted = 0;
    let batchesProcessed = 0;
    let cacheHits = 0;
    const startTime = Date.now();
    let lastCursor = null;
    let isFirstEvent = true;

    // Determinism fingerprint: hash of (ts_event + seq) for all emitted events
    const fingerprintHash = crypto.createHash('sha256');

    const aggregator = opts.aggregate ? new ReplayAggregator(opts.aggregate) : null;

    while (true) {
      const cursorKey = cursor ? `${cursor.ts_event}:${cursor.seq}` : 'start';
      // Production page key includes manifest_id
      const pageKey = `page:${this.#partitionId}:${cursorKey}:${batchSize}:${this.#meta.schema_version}:${this.#meta.manifest_id}:${startTs || 'none'}:${endTs || 'none'}`;
      
      const batchStartNs = process.hrtime.bigint();
      let batch = pageCache.get(pageKey);
      if (batch) {
        cacheHits++;
        replayMetrics.cacheHitsTotal++;
      } else {
        batch = await this.#reader.queryBatchCursor(batchSize, cursor, startTs, endTs);
        if (batch.length > 0) pageCache.set(pageKey, batch);
      }

      if (batch.length === 0) break;

      for (const row of batch) {
        if (isFirstEvent) {
          if (typeof clock.init === 'function') clock.init(row.ts_event);
          isFirstEvent = false;
        }

        if (typeof clock.wait === 'function') await clock.wait(row.ts_event);

        // Update fingerprint for determinism
        fingerprintHash.update(`${row.ts_event}:${row.seq}`);

        if (aggregator) {
          for await (const aggRow of aggregator.process(row)) {
            yield aggRow;
            rowsEmitted++;
            lastCursor = createCursor(aggRow);
          }
        } else {
          yield row;
          rowsEmitted++;
          lastCursor = createCursor(row);
        }
        cursor = createCursor(row); // Update reader position
      }

      batchesProcessed++;
      replayMetrics.replayEngineCyclesTotal++;
      replayMetrics.replayProcessingLatencyMs = Number(process.hrtime.bigint() - batchStartNs) / 1e6;
      if (batch.length < batchSize) break;
    }

    // Flush aggregator
    if (aggregator) {
      for await (const aggRow of aggregator.flush()) {
        yield aggRow;
        rowsEmitted++;
        lastCursor = createCursor(aggRow);
      }
    }

    if (typeof clock.onEnd === 'function') clock.onEnd();

    return {
      rowsEmitted,
      batchesProcessed,
      cacheHits,
      fingerprint: fingerprintHash.digest('hex'), // Determinism check
      elapsedMs: Date.now() - startTime,
      lastCursor,
      lastCursorEncoded: lastCursor ? encodeCursor(lastCursor) : null
    };
  }

  /**
   * Close reader and release resources
   */
  async close() {
    if (this.#reader) {
      await this.#reader.close();
      this.#reader = null;
    }
    this.#validated = false;
  }
}
