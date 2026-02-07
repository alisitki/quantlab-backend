#!/usr/bin/env node
/**
 * List available S3 compact data by symbol and date
 * Temporary tool for Sprint-2 data discovery
 */

import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
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

async function listData() {
  console.log(`Listing ${BUCKET}...`);
  const cmd = new ListObjectsV2Command({
    Bucket: BUCKET,
    MaxKeys: 300
  });
  const res = await s3.send(cmd);

  const bySymbol = {};
  for (const obj of res.Contents || []) {
    const match = obj.Key.match(/symbol=(\w+)\/date=(\d+)/);
    if (match && obj.Key.endsWith('data.parquet')) {
      const [, symbol, date] = match;
      if (!bySymbol[symbol]) bySymbol[symbol] = [];
      bySymbol[symbol].push(date);
    }
  }

  console.log('\nAvailable data:');
  for (const [symbol, dates] of Object.entries(bySymbol).sort()) {
    const sorted = dates.sort();
    console.log(`  ${symbol.toUpperCase()}: ${sorted.length} days`);
    console.log(`    Range: ${sorted[0]} to ${sorted[sorted.length-1]}`);
    console.log(`    Dates: ${sorted.join(', ')}`);
  }
}

listData().catch(console.error);
