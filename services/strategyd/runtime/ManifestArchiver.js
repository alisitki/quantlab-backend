/**
 * ManifestArchiver — gzip + upload run manifest to object storage.
 * Fire-and-forget; never throws to caller.
 */

import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const gzipAsync = promisify(gzip);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

let S3Client = null;
let PutObjectCommand = null;
try {
  const require = createRequire(import.meta.url);
  const sdkPath = path.resolve(__dirname, '../../../core/node_modules/@aws-sdk/client-s3');
  ({ S3Client, PutObjectCommand } = require(sdkPath));
} catch (err) {
  S3Client = null;
  PutObjectCommand = null;
}

export class ManifestArchiver {
  constructor() {
    this.enabled = process.env.RUN_ARCHIVE_ENABLED === '1';
    if (!this.enabled) return;

    if (!S3Client || !PutObjectCommand) {
      this.enabled = false;
      console.warn('[ManifestArchiver] component=strategyd action=disabled reason=missing_sdk');
      return;
    }

    this.bucket = process.env.RUN_ARCHIVE_S3_BUCKET;
    this.endpoint = process.env.RUN_ARCHIVE_S3_ENDPOINT;
    const accessKeyId = process.env.RUN_ARCHIVE_S3_ACCESS_KEY;
    const secretAccessKey = process.env.RUN_ARCHIVE_S3_SECRET_KEY;

    if (!this.bucket || !this.endpoint || !accessKeyId || !secretAccessKey) {
      this.enabled = false;
      console.warn('[ManifestArchiver] component=strategyd action=disabled reason=missing_env');
      return;
    }

    this.client = new S3Client({
      endpoint: this.endpoint,
      forcePathStyle: true,
      credentials: { accessKeyId, secretAccessKey }
    });
  }

  async archive(manifestPath, manifest) {
    if (!this.enabled) return;
    if (!manifest || !manifest.run_id) return;

    const runId = manifest.run_id;
    const key = this.#buildKey(manifest);

    let body;
    try {
      body = await gzipAsync(JSON.stringify(manifest));
    } catch (err) {
      const msg = err?.message || 'gzip_failed';
      console.error(`[ManifestArchiver] run_id=${runId} component=strategyd action=archive_error error=${msg}`);
      return;
    }

    const deadline = Date.now() + 10000;
    for (let attempt = 1; attempt <= 2; attempt++) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;

      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), remaining);
      try {
        await this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: body,
            ContentType: 'application/json',
            ContentEncoding: 'gzip'
          }),
          { abortSignal: controller.signal }
        );
        clearTimeout(timer);
        console.log(`[ManifestArchiver] run_id=${runId} component=strategyd action=uploaded key=${key}`);
        return;
      } catch (err) {
        clearTimeout(timer);
        if (attempt >= 2 || Date.now() >= deadline) {
          const msg = err?.name === 'AbortError' ? 'timeout' : (err?.message || 'upload_failed');
          console.error(`[ManifestArchiver] run_id=${runId} component=strategyd action=archive_error error=${msg}`);
          return;
        }
      }
    }

    console.error(`[ManifestArchiver] run_id=${runId} component=strategyd action=archive_error error=timeout`);
  }

  #buildKey(manifest) {
    const date = this.#extractDate(manifest);
    const yyyy = String(date.getUTCFullYear());
    const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(date.getUTCDate()).padStart(2, '0');
    return `strategyd/${yyyy}/${mm}/${dd}/${manifest.run_id}.json.gz`;
  }

  #extractDate(manifest) {
    const value = manifest.started_at || manifest.ended_at;
    const date = value ? new Date(value) : new Date();
    if (Number.isNaN(date.getTime())) return new Date();
    return date;
  }
}
