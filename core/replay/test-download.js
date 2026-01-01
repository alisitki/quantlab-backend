#!/usr/bin/env node
/**
 * Test helper: Download sample data from S3 and run replay example
 */

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { writeFile, mkdir } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import dotenv from 'dotenv';

dotenv.config();

const TEST_DIR = '/tmp/replay-test';

const s3 = new S3Client({
  endpoint: process.env.S3_COMPACT_ENDPOINT || process.env.S3_ENDPOINT,
  region: 'auto',
  credentials: {
    accessKeyId: process.env.S3_COMPACT_ACCESS_KEY || process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_COMPACT_SECRET_KEY || process.env.S3_SECRET_KEY
  },
  forcePathStyle: true
});

const BUCKET = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';

async function findFirstCompactDataset() {
  console.log(`[S3] Scanning ${BUCKET}...`);
  const cmd = new ListObjectsV2Command({ Bucket: BUCKET, MaxKeys: 20 });
  const res = await s3.send(cmd);
  
  if (!res.Contents || res.Contents.length === 0) {
    throw new Error('No objects found in quantlab-compact bucket');
  }

  // Find first data.parquet
  const parquetKey = res.Contents.find(obj => obj.Key.endsWith('data.parquet'))?.Key;
  if (!parquetKey) {
    throw new Error('No data.parquet found');
  }

  // Derive meta.json path
  const prefix = parquetKey.replace('data.parquet', '');
  const metaKey = prefix + 'meta.json';

  console.log(`[S3] Found: ${parquetKey}`);
  return { parquetKey, metaKey };
}

async function downloadFile(key, localPath) {
  console.log(`[DOWNLOAD] ${key} -> ${localPath}`);
  const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  const res = await s3.send(cmd);
  await pipeline(Readable.fromWeb(res.Body.transformToWebStream()), createWriteStream(localPath));
}

async function main() {
  await mkdir(TEST_DIR, { recursive: true });

  const { parquetKey, metaKey } = await findFirstCompactDataset();
  
  const parquetPath = `${TEST_DIR}/data.parquet`;
  const metaPath = `${TEST_DIR}/meta.json`;

  await downloadFile(parquetKey, parquetPath);
  await downloadFile(metaKey, metaPath);

  console.log('\n[READY] Test data downloaded');
  console.log(`  Parquet: ${parquetPath}`);
  console.log(`  Meta:    ${metaPath}`);
  console.log('\nRun: node replay/example.js /tmp/replay-test/data.parquet /tmp/replay-test/meta.json\n');
}

main().catch(err => {
  console.error('[ERROR]', err.message);
  process.exit(1);
});
