#!/usr/bin/env node
/**
 * Read audit events from S3 (filter by date/run/action)
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

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
      date: { type: 'string' },
      run: { type: 'string' },
      action: { type: 'string' }
    },
    allowPositionals: false
  });
  return values;
}

async function streamToString(body) {
  const chunks = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf-8');
}

async function main() {
  const args = parseCliArgs();
  if (!args.date) {
    console.error('Usage: node tools/read-audit.js --date YYYYMMDD [--run <id>] [--action <action>]');
    process.exit(1);
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

  const prefix = `audit/date=${args.date}/`;
  let continuation;
  const keys = [];

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuation
    }));
    if (res.Contents) {
      for (const obj of res.Contents) {
        if (obj.Key) keys.push(obj.Key);
      }
    }
    continuation = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuation);

  const out = [];
  for (const key of keys) {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const text = await streamToString(res.Body);
    const lines = text.split('\n').filter(Boolean);
    for (const line of lines) {
      const ev = JSON.parse(line);
      if (args.run && ev.metadata?.live_run_id !== args.run) continue;
      if (args.action && ev.action !== args.action) continue;
      out.push(ev);
    }
  }

  console.log(JSON.stringify(out, null, 2));
}

main().catch((err) => {
  console.error('ERROR', err.message || String(err));
  process.exit(1);
});
