#!/usr/bin/env node
/**
 * Run Archive Retention Runner (dry-run by default)
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';
import { S3Client, ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';

import { RunArchiveReader } from '../core/run-archive/RunArchiveReader.js';
import { RetentionPolicy } from '../core/run-archive/RetentionPolicy.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const envPath = resolve(__dirname, '../core/.env');
if (existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

function envRequired(name) {
  const v = process.env[name];
  if (!v) throw new Error(`ENV_MISSING: ${name}`);
  return v;
}

function parseCliArgs() {
  const { values } = parseArgs({
    options: {
      apply: { type: 'boolean', default: false },
      'max-delete': { type: 'string', default: '100' }
    },
    allowPositionals: false
  });
  return values;
}

async function getRunKeys(s3, bucket, replayRunId) {
  const prefix = `replay_runs/replay_run_id=${replayRunId}/`;
  let continuation = undefined;
  const keys = [];
  let totalSize = 0;

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuation
    }));

    if (res.Contents) {
      for (const obj of res.Contents) {
        if (obj.Key) {
          keys.push(obj.Key);
          totalSize += obj.Size || 0;
        }
      }
    }

    continuation = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuation);

  return { keys, totalSize };
}

async function deleteKeys(s3, bucket, keys) {
  if (keys.length === 0) return { deleted: 0, errors: 0 };

  const res = await s3.send(new DeleteObjectsCommand({
    Bucket: bucket,
    Delete: {
      Objects: keys.map(k => ({ Key: k })),
      Quiet: true
    }
  }));

  const deleted = res.Deleted ? res.Deleted.length : 0;
  const errors = res.Errors ? res.Errors.length : 0;
  return { deleted, errors };
}

async function main() {
  const args = parseCliArgs();
  const apply = !!args.apply;
  const maxDelete = parseInt(args['max-delete'], 10);

  const scanStart = Date.now();

  const policy = RetentionPolicy.fromEnv();
  if (!policy.enabled) {
    console.log(JSON.stringify({
      event: 'retention_disabled',
      scanned_runs: 0,
      expired_candidates: 0,
      deleted_runs: 0,
      errors: 0
    }));
    return;
  }

  const bucket = envRequired('RUN_ARCHIVE_S3_BUCKET');
  const endpoint = envRequired('RUN_ARCHIVE_S3_ENDPOINT');
  const accessKey = envRequired('RUN_ARCHIVE_S3_ACCESS_KEY');
  const secretKey = envRequired('RUN_ARCHIVE_S3_SECRET_KEY');

  const s3 = new S3Client({
    endpoint,
    region: 'auto',
    credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
    forcePathStyle: true
  });

  const reader = RunArchiveReader.fromEnv();
  const runs = await reader.listRuns({ limit: 100000 });

  const now = new Date();
  let scannedRuns = 0;
  let expiredCandidates = 0;
  let deletedRuns = 0;
  let errors = 0;
  let totalCandidateBytes = 0;

  const candidates = [];
  for (const run of runs) {
    scannedRuns++;
    if (policy.isExpired(run.finished_at, now)) {
      candidates.push(run);
    }
  }

  for (const run of candidates) {
    expiredCandidates++;
    try {
      const { keys, totalSize } = await getRunKeys(s3, bucket, run.replay_run_id);
      totalCandidateBytes += totalSize;

      if (!apply) continue;
      if (deletedRuns >= maxDelete) break;

      const { deleted, errors: delErrors } = await deleteKeys(s3, bucket, keys);
      if (deleted > 0) deletedRuns++;
      if (delErrors > 0) errors += delErrors;
    } catch (err) {
      errors++;
    }
  }

  console.log(JSON.stringify({
    event: apply ? 'retention_apply' : 'retention_dry_run',
    scanned_runs: scannedRuns,
    expired_candidates: expiredCandidates,
    deleted_runs: deletedRuns,
    errors,
    total_candidate_bytes: totalCandidateBytes,
    retention_days: policy.retentionDays,
    retention_scan_duration_ms: Date.now() - scanStart,
    retention_deleted_total: deletedRuns,
    retention_errors_total: errors
  }));
}

main().catch((err) => {
  console.error(JSON.stringify({
    event: 'retention_error',
    error: err.message || String(err)
  }));
  process.exit(1);
});
