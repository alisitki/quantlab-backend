/**
 * QuantLab Replay Run Archive Reader (S3, read-only)
 */

import { S3Client, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function ensureString(value, name) {
  if (!value || typeof value !== 'string') {
    throw new Error(`RUN_ARCHIVE_CONFIG_ERROR: ${name} is required`);
  }
  return value;
}

function notFoundError(message) {
  const err = new Error(message);
  err.code = 'RUN_NOT_FOUND';
  return err;
}

function s3ReadError(message) {
  const err = new Error(message);
  err.code = 'S3_READ_ERROR';
  return err;
}

function archiveDisabledError() {
  const err = new Error('ARCHIVE_DISABLED: RUN_ARCHIVE_ENABLED=0');
  err.code = 'ARCHIVE_DISABLED';
  return err;
}

function invalidCursorError(message) {
  const err = new Error(message);
  err.code = 'INVALID_CURSOR';
  return err;
}

async function streamToString(body) {
  const chunks = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf-8');
}

export class RunArchiveReader {
  /** @type {S3Client} */
  #s3;
  /** @type {string} */
  #bucket;

  constructor({ bucket, endpoint, accessKey, secretKey }) {
    this.#bucket = bucket;
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

  #emitMetrics(payload) {
    try {
      console.log(JSON.stringify(payload));
    } catch {
      // Ignore metrics failures
    }
  }

  static fromEnv() {
    const enabled = envBool(process.env.RUN_ARCHIVE_ENABLED || '0');
    if (!enabled) throw archiveDisabledError();

    const bucket = ensureString(process.env.RUN_ARCHIVE_S3_BUCKET, 'RUN_ARCHIVE_S3_BUCKET');
    const endpoint = ensureString(process.env.RUN_ARCHIVE_S3_ENDPOINT, 'RUN_ARCHIVE_S3_ENDPOINT');
    const accessKey = ensureString(process.env.RUN_ARCHIVE_S3_ACCESS_KEY, 'RUN_ARCHIVE_S3_ACCESS_KEY');
    const secretKey = ensureString(process.env.RUN_ARCHIVE_S3_SECRET_KEY, 'RUN_ARCHIVE_S3_SECRET_KEY');

    return new RunArchiveReader({ bucket, endpoint, accessKey, secretKey });
  }

  async #getJson(key) {
    const start = Date.now();
    try {
      const res = await this.#s3.send(new GetObjectCommand({ Bucket: this.#bucket, Key: key }));
      const text = await streamToString(res.Body);
      const parsed = JSON.parse(text);
      this.#emitMetrics({
        metric: 'archive_read_latency_ms',
        value: Date.now() - start
      });
      return parsed;
    } catch (err) {
      this.#emitMetrics({
        metric: 'archive_read_errors_total',
        value: 1
      });
      const code = err?.$metadata?.httpStatusCode;
      if (code === 404 || err?.name === 'NoSuchKey') {
        throw notFoundError(`RUN_NOT_FOUND: ${key}`);
      }
      throw s3ReadError(`S3_READ_ERROR: ${err.message || String(err)}`);
    }
  }

  async #getText(key) {
    const start = Date.now();
    try {
      const res = await this.#s3.send(new GetObjectCommand({ Bucket: this.#bucket, Key: key }));
      const text = await streamToString(res.Body);
      this.#emitMetrics({
        metric: 'archive_read_latency_ms',
        value: Date.now() - start
      });
      return text;
    } catch (err) {
      this.#emitMetrics({
        metric: 'archive_read_errors_total',
        value: 1
      });
      const code = err?.$metadata?.httpStatusCode;
      if (code === 404 || err?.name === 'NoSuchKey') {
        throw notFoundError(`RUN_NOT_FOUND: ${key}`);
      }
      throw s3ReadError(`S3_READ_ERROR: ${err.message || String(err)}`);
    }
  }

  #encodeCursor(replayRunId, lineIndex) {
    const payload = {
      v: 1,
      replay_run_id: replayRunId,
      line_index: lineIndex
    };
    const json = JSON.stringify(payload);
    return Buffer.from(json, 'utf-8').toString('base64');
  }

  #decodeCursor(cursor, replayRunId) {
    if (!cursor || typeof cursor !== 'string') {
      throw invalidCursorError('INVALID_CURSOR: cursor must be a base64 string');
    }

    let parsed;
    try {
      const json = Buffer.from(cursor, 'base64').toString('utf-8');
      parsed = JSON.parse(json);
    } catch (err) {
      throw invalidCursorError('INVALID_CURSOR: decode failed');
    }

    if (parsed.v !== 1) {
      throw invalidCursorError('INVALID_CURSOR: version mismatch');
    }
    if (parsed.replay_run_id !== replayRunId) {
      throw invalidCursorError('INVALID_CURSOR: replay_run_id mismatch');
    }
    if (!Number.isInteger(parsed.line_index) || parsed.line_index < 0) {
      throw invalidCursorError('INVALID_CURSOR: invalid line_index');
    }

    return parsed;
  }

  /**
   * List runs sorted by started_at DESC.
   * Cursor is replay_run_id of last item from previous page.
   */
  async listRuns({ limit = 50, cursor = null } = {}) {
    const start = Date.now();
    let runs = null;

    try {
      const index = await this.#getJson('replay_runs/_index.json');
      if (Array.isArray(index)) {
        runs = index;
      }
    } catch (err) {
      // Fallback to scan
      runs = null;
    }

    if (!runs) {
      const prefix = 'replay_runs/';
      let continuation = undefined;
      const prefixes = [];

      do {
        const res = await this.#s3.send(new ListObjectsV2Command({
          Bucket: this.#bucket,
          Prefix: prefix,
          Delimiter: '/',
          ContinuationToken: continuation
        }));

        if (res.CommonPrefixes) {
          for (const p of res.CommonPrefixes) {
            if (p.Prefix) prefixes.push(p.Prefix);
          }
        }

        continuation = res.IsTruncated ? res.NextContinuationToken : undefined;
      } while (continuation);

      runs = [];
      for (const p of prefixes) {
        const replayRunId = p.replace('replay_runs/replay_run_id=', '').replace(/\/$/, '');
        const manifest = await this.getManifest(replayRunId);
        const stats = await this.getStats(replayRunId);
        runs.push({
          replay_run_id: replayRunId,
          started_at: manifest.started_at,
          finished_at: manifest.finished_at,
          stop_reason: manifest.stop_reason,
          decision_count: stats.decision_count
        });
      }
    }

    runs.sort((a, b) => {
      const ta = a.started_at ? Date.parse(a.started_at) : 0;
      const tb = b.started_at ? Date.parse(b.started_at) : 0;
      if (tb !== ta) return tb - ta;
      if (a.replay_run_id < b.replay_run_id) return -1;
      if (a.replay_run_id > b.replay_run_id) return 1;
      return 0;
    });

    let startIndex = 0;
    if (cursor) {
      const idx = runs.findIndex(r => r.replay_run_id === cursor);
      if (idx >= 0) startIndex = idx + 1;
    }

    const result = runs.slice(startIndex, startIndex + limit);
    this.#emitMetrics({
      metric: 'archive_read_latency_ms',
      value: Date.now() - start
    });
    return result;
  }

  async getManifest(replayRunId) {
    const key = `replay_runs/replay_run_id=${replayRunId}/manifest.json`;
    return this.#getJson(key);
  }

  async getStats(replayRunId) {
    const key = `replay_runs/replay_run_id=${replayRunId}/stats.json`;
    return this.#getJson(key);
  }

  async getDecisionsCursor(replayRunId, { cursor = null, limit } = {}) {
    const start = Date.now();
    const key = `replay_runs/replay_run_id=${replayRunId}/decisions.jsonl`;
    const text = await this.#getText(key);
    const lines = text.split('\n').filter(l => l.trim().length > 0);

    let startIndex = 0;
    if (cursor) {
      const decoded = this.#decodeCursor(cursor, replayRunId);
      startIndex = decoded.line_index;
    }

    if (startIndex > lines.length) {
      throw invalidCursorError('INVALID_CURSOR: line_index out of range');
    }

    const endIndex = Number.isFinite(limit) ? startIndex + limit : lines.length;
    const items = lines.slice(startIndex, endIndex).map((line) => JSON.parse(line));

    const nextCursor = endIndex < lines.length
      ? this.#encodeCursor(replayRunId, endIndex)
      : null;

    this.#emitMetrics({
      metric: 'decisions_read_lines',
      value: items.length
    });
    this.#emitMetrics({
      metric: 'archive_read_latency_ms',
      value: Date.now() - start
    });
    return { items, next_cursor: nextCursor };
  }

  async getDecisions(replayRunId, { limit, offset } = {}) {
    const safeOffset = Number.isFinite(offset) ? offset : 0;
    const cursor = this.#encodeCursor(replayRunId, safeOffset);
    const result = await this.getDecisionsCursor(replayRunId, { cursor, limit });
    return result.items;
  }
}

export default RunArchiveReader;
