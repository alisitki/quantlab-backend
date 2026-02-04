/**
 * FeatureOrchestrator — deterministic feature extraction via ReplayEngine.
 */

import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import parquet from 'parquetjs-lite';
import { ReplayEngine } from '../../core/replay/ReplayEngine.js';
import { buildDatasetPaths } from '../replayd/config.js';
import { canonicalStringify } from '../../core/strategy/state/StateSerializer.js';
import {
  FEATURE_MANIFEST_SCHEMA_VERSION
} from './constants.js';
import {
  FeatureSetV1,
  FEATURE_COLUMNS,
  FEATURE_SET_ID,
  FEATURE_SET_VERSION
} from './extractors/FeatureSetV1.js';

export class FeatureOrchestrator {
  constructor({ jobStore, config }) {
    this.jobStore = jobStore;
    this.config = config;
    this.runningJobs = new Set();
  }

  normalizeJob(input) {
    const streams = Array.isArray(input.streams) ? [...input.streams] : [];
    const symbols = Array.isArray(input.symbols) ? [...input.symbols] : [];
    streams.sort();
    symbols.sort();

    return {
      date_range: {
        start: input.date_range?.[0],
        end: input.date_range?.[1]
      },
      streams,
      symbols,
      feature_set_id: input.feature_set_id || FEATURE_SET_ID,
      feature_set_version: input.feature_set_version || FEATURE_SET_VERSION,
      label_horizon_sec: Number(input.label_horizon_sec || 10),
      seed: input.seed || null
    };
  }

  computeJobId(normalizedJob) {
    const payload = canonicalStringify(normalizedJob);
    return createHash('sha256').update(payload).digest('hex');
  }

  buildPartitions(job) {
    const dates = this.#dateRange(job.date_range.start, job.date_range.end);
    const partitions = [];
    for (const date of dates) {
      for (const stream of job.streams) {
        for (const symbol of job.symbols) {
          partitions.push({ date, stream, symbol });
        }
      }
    }
    return partitions;
  }

  async submit(jobInput) {
    const normalized = this.normalizeJob(jobInput);
    const jobId = this.computeJobId(normalized);

    const existing = await this.jobStore.get(jobId);
    if (existing) {
      return existing;
    }

    const job = {
      job_id: jobId,
      state: 'pending',
      date_range: [normalized.date_range.start, normalized.date_range.end],
      streams: normalized.streams,
      symbols: normalized.symbols,
      feature_set_id: normalized.feature_set_id,
      feature_set_version: normalized.feature_set_version,
      label_horizon_sec: normalized.label_horizon_sec,
      seed: normalized.seed,
      partitions: this.buildPartitions({ date_range: normalized.date_range, streams: normalized.streams, symbols: normalized.symbols })
    };

    await this.jobStore.save(job);

    this.run(jobId).catch(() => {});

    return job;
  }

  async run(jobId) {
    if (this.runningJobs.has(jobId)) return;
    this.runningJobs.add(jobId);

    try {
      const job = await this.jobStore.get(jobId);
      if (!job) return;

      job.state = 'running';
      await this.jobStore.save(job);

      console.log(`[FeatureOrchestrator] job_id=${jobId} component=featurexd action=job_start partitions=${job.partitions.length}`);

      const result = await this.#runJob(job);

      job.state = result ? 'completed' : 'failed';
      job.result = result;
      await this.jobStore.save(job);

      console.log(`[FeatureOrchestrator] job_id=${jobId} component=featurexd action=job_complete state=${job.state} row_count=${result?.row_count ?? 0}`);
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[FeatureOrchestrator] job_id=${jobId} component=featurexd action=job_failed error=${msg}`);
      const job = await this.jobStore.get(jobId);
      if (job) {
        job.state = 'failed';
        await this.jobStore.save(job);
      }
    } finally {
      this.runningJobs.delete(jobId);
    }
  }

  async #runJob(job) {
    const outDir = path.join(this.config.datasetsDir, job.job_id);
    await fs.mkdir(outDir, { recursive: true });

    const schema = this.#buildParquetSchema();
    const parquetPath = path.join(outDir, 'features.parquet');

    const writer = await parquet.ParquetWriter.openFile(schema, parquetPath);

    const fingerprintHash = createHash('sha256');
    let rowCount = 0;

    for (const partition of job.partitions) {
      const partitionCount = await this.#processPartition(partition, writer, fingerprintHash, job);
      if (!partitionCount) {
        console.log(`[FeatureOrchestrator] job_id=${job.job_id} component=featurexd action=partition_skipped date=${partition.date} stream=${partition.stream} symbol=${partition.symbol}`);
      } else {
        rowCount += partitionCount;
      }
    }

    await writer.close();

    rowCount = writer.metadata?.rowCount || rowCount;
    const fingerprint = `sha256:${fingerprintHash.digest('hex')}`;

    const manifest = this.#buildManifest(job, rowCount, fingerprint);
    const manifestPath = path.join(outDir, 'dataset_manifest.json');
    await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));

    return {
      output_dir: outDir,
      parquet_path: parquetPath,
      manifest_path: manifestPath,
      row_count: rowCount,
      fingerprint
    };
  }

  async #processPartition(partition, writer, fingerprintHash, job) {
    const { date, stream, symbol } = partition;

    const paths = buildDatasetPaths(stream, symbol, date);
    const replay = new ReplayEngine({ parquet: paths.parquet, meta: paths.meta }, { stream, date, symbol });

    let featureSet = new FeatureSetV1();
    let rowsEmitted = 0;

    try {
      for await (const event of replay.replay({ batchSize: this.config.batchSize })) {
        const features = featureSet.onEvent(event);
        if (!features) continue;

        const row = this.#buildRow(event, features);
        await writer.appendRow(row);
        this.#updateFingerprint(fingerprintHash, row, job);
        rowsEmitted += 1;
      }
    } catch (err) {
      console.error(`[FeatureOrchestrator] job_id=${job.job_id} component=featurexd action=partition_error error=${err.message} date=${date} stream=${stream} symbol=${symbol}`);
      return 0;
    } finally {
      await replay.close();
    }

    return rowsEmitted;
  }

  #buildRow(event, features) {
    const tsEvent = this.#toBigInt(event.ts_event);
    const seq = this.#toBigInt(event.seq ?? 0);

    const row = {
      ts_event: tsEvent,
      seq,
      ...features
    };

    return row;
  }

  #buildParquetSchema() {
    const fields = {
      ts_event: { type: 'INT64' },
      seq: { type: 'INT64' }
    };

    for (const name of FEATURE_COLUMNS) {
      fields[name] = { type: 'DOUBLE' };
    }

    return new parquet.ParquetSchema(fields);
  }

  #buildManifest(job, rowCount, fingerprint) {
    return {
      schema_version: FEATURE_MANIFEST_SCHEMA_VERSION,
      job_id: job.job_id,
      feature_set_id: job.feature_set_id,
      feature_set_version: job.feature_set_version,
      label_horizon_sec: job.label_horizon_sec,
      ordering: ['ts_event', 'seq'],
      features: [...FEATURE_COLUMNS],
      symbols: [...job.symbols],
      date_range: [...job.date_range],
      row_count: rowCount,
      fingerprint
    };
  }

  #updateFingerprint(hash, row) {
    const parts = [String(row.ts_event), String(row.seq)];
    for (const name of FEATURE_COLUMNS) {
      const value = row[name];
      parts.push(value === null || value === undefined ? 'null' : String(value));
    }
    hash.update(parts.join('|'));
  }

  #dateRange(start, end) {
    const dates = [];
    const startDate = this.#parseDate(start);
    const endDate = this.#parseDate(end);
    if (!startDate || !endDate) return dates;

    let current = new Date(Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate()));
    const last = new Date(Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate()));

    while (current <= last) {
      dates.push(current.toISOString().slice(0, 10));
      current.setUTCDate(current.getUTCDate() + 1);
    }
    return dates;
  }

  #parseDate(value) {
    if (!value || typeof value !== 'string') return null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
    const date = new Date(value + 'T00:00:00Z');
    if (Number.isNaN(date.getTime())) return null;
    return date;
  }

  #toBigInt(value) {
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return BigInt(Math.trunc(value));
    if (typeof value === 'string' && value.trim() !== '') {
      try {
        return BigInt(value.trim());
      } catch {
        return BigInt(0);
      }
    }
    return BigInt(0);
  }
}
