/**
 * LabelOrchestrator — deterministic labeling from feature datasets.
 */

import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import parquet from 'parquetjs-lite';
import { LABEL_MANIFEST_SCHEMA_VERSION } from './constants.js';
import { LABEL_SET_ID, LABEL_SET_VERSION, buildReturnsLabels } from './labelsets/ReturnsV1.js';

export class LabelOrchestrator {
  constructor({ jobStore, config }) {
    this.jobStore = jobStore;
    this.config = config;
    this.runningJobs = new Set();
  }

  normalizeJob(input) {
    return {
      feature_job_id: input.feature_job_id,
      label_set_id: input.label_set_id || LABEL_SET_ID,
      label_set_version: input.label_set_version || LABEL_SET_VERSION,
      label_horizon_sec: Number(input.label_horizon_sec || 10),
      seed: input.seed || null
    };
  }

  computeJobId(normalizedJob) {
    const payload = this.#canonicalStringify(normalizedJob);
    return createHash('sha256').update(payload).digest('hex');
  }

  async submit(jobInput) {
    const normalized = this.normalizeJob(jobInput);
    const jobId = this.computeJobId(normalized);

    const existing = await this.jobStore.get(jobId);
    if (existing) return existing;

    const job = {
      job_id: jobId,
      state: 'pending',
      feature_job_id: normalized.feature_job_id,
      label_set_id: normalized.label_set_id,
      label_set_version: normalized.label_set_version,
      label_horizon_sec: normalized.label_horizon_sec,
      seed: normalized.seed
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
      console.log(`[LabelOrchestrator] job_id=${jobId} component=labeld action=job_start feature_job_id=${job.feature_job_id}`);

      const result = await this.#runJob(job);

      job.state = result ? 'completed' : 'failed';
      job.result = result;
      await this.jobStore.save(job);

      console.log(`[LabelOrchestrator] job_id=${jobId} component=labeld action=job_complete state=${job.state} row_count=${result?.row_count ?? 0}`);
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[LabelOrchestrator] job_id=${jobId} component=labeld action=job_failed error=${msg}`);
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
    const featureDir = path.join(this.config.featureDatasetsDir, job.feature_job_id);
    const featurePath = path.join(featureDir, 'features.parquet');

    const featureRows = await this.#readFeatures(featurePath);
    if (!featureRows || featureRows.length === 0) {
      console.log(`[LabelOrchestrator] job_id=${job.job_id} component=labeld action=job_skipped reason=missing_features path=${featurePath}`);
      return null;
    }

    this.#validateOrdering(featureRows, job.job_id);

    const horizonMs = Math.trunc(job.label_horizon_sec * 1000);
    const labels = buildReturnsLabels(featureRows, horizonMs);

    const outDir = path.join(this.config.datasetsDir, job.job_id);
    await fs.mkdir(outDir, { recursive: true });

    const schema = this.#buildParquetSchema();
    const outPath = path.join(outDir, 'labels.parquet');
    const writer = await parquet.ParquetWriter.openFile(schema, outPath);

    const fingerprintHash = createHash('sha256');
    for (let i = 0; i < featureRows.length; i++) {
      const row = featureRows[i];
      const label = labels[i];
      const outRow = {
        ts_event: row.ts_event,
        seq: row.seq,
        label_future_return: label.label_future_return,
        label_direction: label.label_direction
      };
      await writer.appendRow(outRow);
      this.#updateFingerprint(fingerprintHash, outRow);
    }

    await writer.close();

    const rowCount = writer.metadata?.rowCount || featureRows.length;
    const fingerprint = `sha256:${fingerprintHash.digest('hex')}`;

    const manifest = this.#buildManifest(job, rowCount, fingerprint);
    const manifestPath = path.join(outDir, 'label_manifest.json');
    await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));

    return {
      output_dir: outDir,
      labels_path: outPath,
      manifest_path: manifestPath,
      row_count: rowCount,
      fingerprint
    };
  }

  async #readFeatures(featurePath) {
    try {
      const reader = await parquet.ParquetReader.openFile(featurePath);
      const cursor = reader.getCursor();
      const rows = [];

      let record;
      while ((record = await cursor.next())) {
        const ts = this.#toBigInt(record.ts_event);
        const seq = this.#toBigInt(record.seq ?? 0);
        const mid = Number(record.f_mid_price);
        rows.push({
          ts_event: ts,
          seq,
          f_mid_price: Number.isFinite(mid) ? mid : NaN
        });
      }

      await reader.close();
      return rows;
    } catch {
      return null;
    }
  }

  #buildParquetSchema() {
    return new parquet.ParquetSchema({
      ts_event: { type: 'INT64' },
      seq: { type: 'INT64' },
      label_future_return: { type: 'DOUBLE', optional: true },
      label_direction: { type: 'INT32', optional: true }
    });
  }

  #buildManifest(job, rowCount, fingerprint) {
    return {
      schema_version: LABEL_MANIFEST_SCHEMA_VERSION,
      job_id: job.job_id,
      feature_job_id: job.feature_job_id,
      label_set_id: job.label_set_id,
      label_set_version: job.label_set_version,
      label_horizon_sec: job.label_horizon_sec,
      ordering: 'inherits features.parquet',
      row_count: rowCount,
      fingerprint
    };
  }

  #updateFingerprint(hash, row) {
    const parts = [String(row.ts_event), String(row.seq)];
    parts.push(row.label_future_return === null || row.label_future_return === undefined ? 'null' : String(row.label_future_return));
    parts.push(row.label_direction === null || row.label_direction === undefined ? 'null' : String(row.label_direction));
    hash.update(parts.join('|'));
  }

  #validateOrdering(rows, jobId) {
    let lastTs = null;
    let lastSeq = null;

    for (const row of rows) {
      const ts = row.ts_event;
      const seq = row.seq;
      if (lastTs !== null) {
        const backward = ts < lastTs || (ts === lastTs && seq <= lastSeq);
        if (backward) {
          const msg = `ORDERING_ERROR: ts_event/seq drift detected last=${lastTs}:${lastSeq} new=${ts}:${seq}`;
          console.error(`[LabelOrchestrator] job_id=${jobId} component=labeld action=ordering_error error=${msg}`);
          throw new Error(msg);
        }
      }
      lastTs = ts;
      lastSeq = seq;
    }
  }

  #canonicalStringify(obj) {
    if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
    if (Array.isArray(obj)) return '[' + obj.map((item) => this.#canonicalStringify(item)).join(',') + ']';

    const keys = Object.keys(obj).sort();
    return '{' + keys.map((k) => `"${k}":${this.#canonicalStringify(obj[k])}`).join(',') + '}';
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
