#!/usr/bin/env node
/**
 * Download multi-day data from S3 for Sprint-2
 * Temporary tool - downloads consecutive days of a symbol
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { writeFile, mkdir } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import dotenv from 'dotenv';

dotenv.config();

const s3 = new S3Client({
  endpoint: process.env.S3_COMPACT_ENDPOINT,
  region: 'auto',
  credentials: {
    accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
    secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
  },
  forcePathStyle: true
});

const BUCKET = 'quantlab-compact';

async function downloadFile(key, localPath) {
  const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  const res = await s3.send(cmd);
  await pipeline(Readable.from(res.Body), createWriteStream(localPath));
  console.log(`  ✓ ${key} → ${localPath}`);
}

async function downloadMultiDay(symbol, dates, outputDir) {
  await mkdir(outputDir, { recursive: true });

  for (const date of dates) {
    const prefix = `exchange=binance/stream=bbo/symbol=${symbol}/date=${date}`;
    const parquetKey = `${prefix}/data.parquet`;
    const metaKey = `${prefix}/meta.json`;

    try {
      await downloadFile(parquetKey, `${outputDir}/${symbol}_${date}.parquet`);
      await downloadFile(metaKey, `${outputDir}/${symbol}_${date}_meta.json`);
    } catch (err) {
      console.error(`  ✗ Failed to download ${date}: ${err.message}`);
    }
  }
}

// Sprint-2: Download 7 consecutive days of ADA/USDT (20260108-20260114)
const symbol = 'adausdt';
const dates = [
  '20260108',  // Known to exist (GOOD quality, 3.27M rows)
  '20260109',
  '20260110',
  '20260111',
  '20260112',
  '20260113',
  '20260114'
];

const outputDir = 'data/sprint2';

console.log(`Downloading ${dates.length} days of ${symbol.toUpperCase()}...`);
downloadMultiDay(symbol, dates, outputDir)
  .then(() => console.log('\n✓ Download complete'))
  .catch(err => {
    console.error('\n✗ Download failed:', err);
    process.exit(1);
  });
