#!/usr/bin/env node
/**
 * Build Run Archive Index (dry-run by default)
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';
import { S3Client, ListObjectsV2Command, PutObjectCommand } from '@aws-sdk/client-s3';

import { RunArchiveReader } from '../core/run-archive/RunArchiveReader.js';
import { canonicalStringify } from '../core/strategy/state/StateSerializer.js';

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
      apply: { type: 'boolean', default: false }
    },
    allowPositionals: false
  });
  return values;
}

async function listRunPrefixes(s3, bucket) {
  const prefix = 'replay_runs/';
  let continuation = undefined;
  const prefixes = [];

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
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

  return prefixes;
}

async function main() {
  const args = parseCliArgs();
  const apply = !!args.apply;

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
  const prefixes = await listRunPrefixes(s3, bucket);

  const items = [];
  for (const p of prefixes) {
    const replayRunId = p.replace('replay_runs/replay_run_id=', '').replace(/\/$/, '');
    const manifest = await reader.getManifest(replayRunId);
    const stats = await reader.getStats(replayRunId);

    items.push({
      replay_run_id: replayRunId,
      started_at: manifest.started_at,
      finished_at: manifest.finished_at,
      stop_reason: manifest.stop_reason,
      decision_count: stats.decision_count
    });
  }

  items.sort((a, b) => {
    const ta = a.started_at ? Date.parse(a.started_at) : 0;
    const tb = b.started_at ? Date.parse(b.started_at) : 0;
    if (tb !== ta) return tb - ta;
    if (a.replay_run_id < b.replay_run_id) return -1;
    if (a.replay_run_id > b.replay_run_id) return 1;
    return 0;
  });

  const body = canonicalStringify(items);

  if (!apply) {
    console.log(JSON.stringify({
      event: 'index_dry_run',
      runs: items.length
    }));
    return;
  }

  await s3.send(new PutObjectCommand({
    Bucket: bucket,
    Key: 'replay_runs/_index.json',
    Body: body,
    ContentType: 'application/json'
  }));

  console.log(JSON.stringify({
    event: 'index_applied',
    runs: items.length
  }));
}

main().catch((err) => {
  console.error(JSON.stringify({
    event: 'index_error',
    error: err.message || String(err)
  }));
  process.exit(1);
});
