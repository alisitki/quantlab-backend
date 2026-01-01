/**
 * QuantLab Replay Engine v1.1 — Main Engine
 * Orchestrates meta loading, validation, and cursor-based streaming replay.
 * Uses ts_event + seq for deterministic ordering.
 */

import { loadMeta } from './MetaLoader.js';
import { validateAll } from './SchemaValidator.js';
import { ParquetReader } from './ParquetReader.js';

/** Default batch size for streaming */
const DEFAULT_BATCH_SIZE = 10_000;

/**
 * Deterministic event replay engine for compact datasets.
 * Streams rows from a single parquet + meta.json pair.
 * Uses cursor-based pagination for large dataset safety.
 */
export class ReplayEngine {
  /** @type {string} */
  #parquetPath;
  /** @type {string} */
  #metaPath;
  /** @type {import('./types.js').MetaData|null} */
  #meta = null;
  /** @type {ParquetReader|null} */
  #reader = null;
  /** @type {boolean} */
  #validated = false;

  /**
   * @param {string} parquetPath - Absolute path to data.parquet
   * @param {string} metaPath - Absolute path to meta.json
   */
  constructor(parquetPath, metaPath) {
    this.#parquetPath = parquetPath;
    this.#metaPath = metaPath;
  }

  /**
   * Load and validate metadata + parquet schema
   * @returns {Promise<import('./types.js').MetaData>}
   */
  async validate() {
    if (this.#validated) return this.#meta;

    // Load meta.json
    this.#meta = await loadMeta(this.#metaPath);

    // Initialize parquet reader
    this.#reader = new ParquetReader(this.#parquetPath);
    await this.#reader.init();

    // Get actual row count and validate
    const actualRows = await this.#reader.getRowCount();
    validateAll(this.#meta, actualRows);

    this.#validated = true;
    return this.#meta;
  }

  /**
   * Get metadata (loads if not already loaded)
   * @returns {Promise<import('./types.js').MetaData>}
   */
  async getMeta() {
    if (!this.#meta) {
      await this.validate();
    }
    return this.#meta;
  }

  /**
   * Stream replay events as an AsyncGenerator using cursor-based pagination.
   * Order: ts_event ASC, seq ASC (deterministic)
   * @param {import('./types.js').ReplayOptions} [opts={}]
   * @yields {import('./types.js').Row}
   * @returns {AsyncGenerator<import('./types.js').Row, import('./types.js').ReplayStats>}
   */
  async *replay(opts = {}) {
    const { startTs, endTs, batchSize = DEFAULT_BATCH_SIZE } = opts;

    // Ensure validation is complete
    await this.validate();

    // Cursor state: null for first batch
    let cursor = null;
    let rowsEmitted = 0;
    let batchesProcessed = 0;
    const startTime = Date.now();

    while (true) {
      const batch = await this.#reader.queryBatchCursor(batchSize, cursor, startTs, endTs);

      if (batch.length === 0) break;

      for (const row of batch) {
        yield row;
        rowsEmitted++;

        // Update cursor after each row
        cursor = {
          ts_event: row.ts_event,
          seq: row.seq
        };
      }

      batchesProcessed++;

      // If batch is smaller than limit, we've reached the end
      if (batch.length < batchSize) break;
    }

    return {
      rowsEmitted,
      batchesProcessed,
      elapsedMs: Date.now() - startTime
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
