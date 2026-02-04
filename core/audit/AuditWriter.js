/**
 * Audit Writer (append-only)
 */

import { mkdir, rename, open } from 'node:fs/promises';
import { join } from 'node:path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import crypto from 'node:crypto';

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function pad2(n) {
  return n.toString().padStart(2, '0');
}

function dateKeyFromNs(tsNs) {
  const ms = Number(tsNs / 1_000_000n);
  const d = new Date(ms);
  return `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;
}

export class AuditWriter {
  #enabled;
  #enabledS3;
  #bucket;
  #s3;
  #spoolDir;
  #errorCount = 0;

  constructor({ enabled, enabledS3, bucket, endpoint, accessKey, secretKey, spoolDir }) {
    this.#enabled = enabled;
    this.#enabledS3 = enabledS3;
    this.#bucket = bucket;
    this.#spoolDir = spoolDir;
    this.#s3 = enabledS3
      ? new S3Client({
          endpoint,
          region: 'auto',
          credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
          forcePathStyle: true
        })
      : null;
  }

  static fromEnv() {
    const enabled = envBool(process.env.AUDIT_ENABLED ?? '1');
    const enabledS3 = envBool(process.env.RUN_ARCHIVE_ENABLED || '0');
    const spoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';

    if (!enabled) {
      return new AuditWriter({
        enabled,
        enabledS3: false,
        bucket: '',
        endpoint: '',
        accessKey: '',
        secretKey: '',
        spoolDir
      });
    }

    if (!enabledS3) {
      return new AuditWriter({
        enabled,
        enabledS3: false,
        bucket: '',
        endpoint: '',
        accessKey: '',
        secretKey: '',
        spoolDir
      });
    }

    return new AuditWriter({
      enabled,
      enabledS3,
      bucket: process.env.RUN_ARCHIVE_S3_BUCKET || '',
      endpoint: process.env.RUN_ARCHIVE_S3_ENDPOINT || '',
      accessKey: process.env.RUN_ARCHIVE_S3_ACCESS_KEY || '',
      secretKey: process.env.RUN_ARCHIVE_S3_SECRET_KEY || '',
      spoolDir
    });
  }

  get errorCount() { return this.#errorCount; }

  async write(event) {
    if (!this.#enabled) return;
    const tsNs = event.ts || BigInt(Date.now()) * 1_000_000n;
    const dateKey = dateKeyFromNs(tsNs);
    const auditId = event.audit_id || crypto.randomUUID();
    const payload = {
      audit_id: auditId,
      ts: tsNs.toString(),
      actor: event.actor,
      action: event.action,
      target_type: event.target_type,
      target_id: event.target_id,
      reason: event.reason ?? null,
      metadata: event.metadata || {}
    };

    const line = JSON.stringify(payload) + '\n';
    const dir = join(this.#spoolDir, `date=${dateKey}`);
    await mkdir(dir, { recursive: true });
    const baseName = `part-${tsNs.toString()}-${auditId}.jsonl`;
    const tmpPath = join(dir, `${baseName}.tmp`);
    const finalPath = join(dir, baseName);

    const fh = await open(tmpPath, 'w');
    try {
      await fh.writeFile(line);
      await fh.sync();
    } finally {
      await fh.close();
    }

    await rename(tmpPath, finalPath);

    if (this.#enabledS3 && this.#s3 && this.#bucket) {
      const key = `audit/date=${dateKey}/${baseName}`;
      try {
        await this.#s3.send(new PutObjectCommand({
          Bucket: this.#bucket,
          Key: key,
          Body: line,
          ContentType: 'application/x-ndjson'
        }));
      } catch (err) {
        this.#errorCount += 1;
        try {
          console.error(`[AUDIT] upload_failed error=${err.message || String(err)}`);
        } catch {
          // ignore
        }
      }
    }
  }
}

export const auditWriter = AuditWriter.fromEnv();

export async function emitAudit(event) {
  try {
    await auditWriter.write(event);
  } catch (err) {
    try {
      console.error(`[AUDIT] write_failed error=${err.message || String(err)}`);
    } catch {
      // ignore
    }
  }
}

export default auditWriter;
