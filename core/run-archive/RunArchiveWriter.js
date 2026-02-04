/**
 * QuantLab Replay Run Archive Writer
 * Writes deterministic replay run outputs to S3.
 */

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { canonicalStringify } from '../strategy/state/StateSerializer.js';

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function ensureString(value, name) {
  if (!value || typeof value !== 'string') {
    throw new Error(`RUN_ARCHIVE_CONFIG_ERROR: ${name} is required`);
  }
  return value;
}

function toIsoFromNs(ns) {
  if (ns === null || ns === undefined) return null;
  const ts = BigInt(ns);
  const ms = Number(ts / 1_000_000n);
  return new Date(ms).toISOString();
}

export class RunArchiveWriter {
  /** @type {S3Client|null} */
  #s3;
  /** @type {string} */
  #bucket;
  /** @type {boolean} */
  #enabled;

  constructor({ enabled, bucket, endpoint, accessKey, secretKey }) {
    this.#enabled = enabled;
    this.#bucket = bucket;

    if (!this.#enabled) {
      this.#s3 = null;
      return;
    }

    this.#s3 = new S3Client({
      endpoint,
      region: 'auto',
      credentials: {
        accessKeyId: accessKey,
        secretAccessKey: secretKey
      },
      forcePathStyle: true
    });
  }

  static fromEnv() {
    const enabled = envBool(process.env.RUN_ARCHIVE_ENABLED || '0');
    if (!enabled) {
      return new RunArchiveWriter({
        enabled: false,
        bucket: '',
        endpoint: '',
        accessKey: '',
        secretKey: ''
      });
    }

    const bucket = ensureString(process.env.RUN_ARCHIVE_S3_BUCKET, 'RUN_ARCHIVE_S3_BUCKET');
    const endpoint = ensureString(process.env.RUN_ARCHIVE_S3_ENDPOINT, 'RUN_ARCHIVE_S3_ENDPOINT');
    const accessKey = ensureString(process.env.RUN_ARCHIVE_S3_ACCESS_KEY, 'RUN_ARCHIVE_S3_ACCESS_KEY');
    const secretKey = ensureString(process.env.RUN_ARCHIVE_S3_SECRET_KEY, 'RUN_ARCHIVE_S3_SECRET_KEY');

    return new RunArchiveWriter({
      enabled,
      bucket,
      endpoint,
      accessKey,
      secretKey
    });
  }

  /**
   * @param {Object} run
   * @param {string} run.replay_run_id
   * @param {string} run.seed
   * @param {string} run.manifest_id
   * @param {string|string[]} run.parquet_path
   * @param {bigint|null} run.first_ts_event
   * @param {bigint|null} run.last_ts_event
   * @param {string} run.stop_reason
   * @param {Array<Object>} run.decisions
   * @param {Object} run.stats
   * @returns {Promise<void>}
   */
  async write(run) {
    if (!this.#enabled) return;

    const prefix = `replay_runs/replay_run_id=${run.replay_run_id}`;

    const manifest = {
      replay_run_id: run.replay_run_id,
      seed: run.seed,
      manifest_id: run.manifest_id,
      parquet_path: run.parquet_path,
      started_at: toIsoFromNs(run.first_ts_event),
      finished_at: toIsoFromNs(run.last_ts_event),
      stop_reason: run.stop_reason
    };

    const decisionsLines = run.decisions.map((d) => {
      return canonicalStringify({
        replay_run_id: d.replay_run_id,
        cursor: d.cursor,
        ts_event: d.ts_event,
        decision: d.decision
      });
    });
    const decisionsBody = decisionsLines.length > 0
      ? decisionsLines.join('\n') + '\n'
      : '';

    const stats = {
      emitted_event_count: run.stats.emitted_event_count,
      decision_count: run.stats.decision_count,
      duration_ms: run.stats.duration_ms
    };

    await this.#putObject(`${prefix}/manifest.json`, canonicalStringify(manifest), 'application/json');
    await this.#putObject(`${prefix}/decisions.jsonl`, decisionsBody, 'application/x-ndjson');
    await this.#putObject(`${prefix}/stats.json`, canonicalStringify(stats), 'application/json');
  }

  async #putObject(key, body, contentType) {
    const cmd = new PutObjectCommand({
      Bucket: this.#bucket,
      Key: key,
      Body: body,
      ContentType: contentType
    });
    await this.#s3.send(cmd);
  }
}

export default RunArchiveWriter;
