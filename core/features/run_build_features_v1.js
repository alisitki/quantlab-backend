#!/usr/bin/env node

/**
 * run_build_features_v1.js
 * CLI entry for building deterministic ML datasets (Contract v1).
 */

import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import pkg from 'parquetjs-lite';
const { ParquetWriter, ParquetSchema } = pkg;
import { parquetReadObjects } from 'hyparquet';
import { decompress } from 'fzstd';
import { writeFile, mkdir, rm, readFile } from 'node:fs/promises';
import { createWriteStream, createReadStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';
import crypto from 'node:crypto';
import dotenv from 'dotenv';
import { FeatureBuilderV1 } from './FeatureBuilderV1.js';

dotenv.config();

const argv = process.argv.slice(2);
const args = {};
for (let i = 0; i < argv.length; i += 2) {
  const key = argv[i].replace('--', '');
  args[key] = argv[i + 1];
}

const { exchange, stream, symbol, date } = args;

if (!exchange || !stream || !symbol || !date) {
  console.error('Usage: node run_build_features_v1.js --exchange <ex> --stream <str> --symbol <sym> --date <YYYYMMDD>');
  process.exit(1);
}

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
const INPUT_PREFIX = `exchange=${exchange}/stream=${stream}/symbol=${symbol}/date=${date}/`;
const OUTPUT_PREFIX = `features/featureset=v1/exchange=${exchange}/stream=${stream}/symbol=${symbol}/date=${date}/`;

const TMP_DIR = `/tmp/quantlab-features-${crypto.randomBytes(4).toString('hex')}`;

const SCHEMA = new ParquetSchema({
  ts_event: { type: 'INT64' },
  f_mid: { type: 'DOUBLE' },
  f_spread: { type: 'DOUBLE' },
  f_spread_bps: { type: 'DOUBLE' },
  f_imbalance: { type: 'DOUBLE' },
  f_microprice: { type: 'DOUBLE' },
  f_ret_1s: { type: 'DOUBLE' },
  f_ret_5s: { type: 'DOUBLE' },
  f_ret_10s: { type: 'DOUBLE' },
  f_ret_30s: { type: 'DOUBLE' },
  f_vol_10s: { type: 'DOUBLE' },
  label_dir_10s: { type: 'INT32' }
});

async function main() {
  try {
    await mkdir(TMP_DIR, { recursive: true });
    console.log(`[INIT] Temp dir: ${TMP_DIR}`);

    // 1. Download data.parquet and meta.json from curated
    const inputParquetLocal = path.join(TMP_DIR, 'input.parquet');
    console.log(`[S3] Downloading curated data: ${INPUT_PREFIX}data.parquet`);
    
    try {
      await downloadS3File(`${INPUT_PREFIX}data.parquet`, inputParquetLocal);
    } catch (err) {
      console.error(`[ERROR] Failed to download input parquet: ${err.message}`);
      process.exit(1);
    }

    // 2. Read rows from parquet using hyparquet + fzstd
    console.log(`[PARQUET] Reading rows from ${inputParquetLocal}...`);
    const data = await readFile(inputParquetLocal);
    const asyncBuffer = {
        byteLength: data.byteLength,
        slice: async (start, end) => data.buffer.slice(data.byteOffset + start, data.byteOffset + end)
    };
    
    let rows;
    try {
      rows = await parquetReadObjects({ 
          file: asyncBuffer,
          compressors: {
              ZSTD: (input) => decompress(input)
          }
      });
    } catch (err) {
      console.error(`[ERROR] hyparquet failed: ${err.message}`);
      throw err;
    }
    console.log(`[PARQUET] Loaded ${rows.length} rows.`);

    // 3. Process features
    console.log(`[FEATURES] Building featureset v1...`);
    const builder = new FeatureBuilderV1();
    const featuredRows = builder.process(rows);
    console.log(`[FEATURES] Generated ${featuredRows.length} featured rows.`);

    if (featuredRows.length === 0) {
      console.warn(`[WARN] No featured rows generated (insufficient data?).`);
      return;
    }

    // 4. Write output parquet locally
    const outputParquetLocal = path.join(TMP_DIR, 'data.parquet');
    console.log(`[PARQUET] Writing featured rows to ${outputParquetLocal}...`);
    const writer = await ParquetWriter.openFile(SCHEMA, outputParquetLocal);
    for (const row of featuredRows) {
      await writer.appendRow(row);
    }
    await writer.close();

    // 5. Generate meta.json
    const tsMin = featuredRows[0].ts_event;
    const tsMax = featuredRows[featuredRows.length - 1].ts_event;
    const columns = Object.keys(SCHEMA.fields);
    
    const configObj = {
      featureset_version: "v1",
      label_horizon_sec: 10,
      symbol,
      date,
      columns,
      formulas_version: "1.0.0"
    };

    const configHash = crypto.createHash('sha256')
      .update(JSON.stringify(configObj))
      .digest('hex');

    const meta = {
      ...configObj,
      rows: featuredRows.length,
      ts_min: Number(tsMin),
      ts_max: Number(tsMax),
      config_hash: configHash
    };
    const metaLocal = path.join(TMP_DIR, 'meta.json');
    await writeFile(metaLocal, JSON.stringify(meta, null, 2));

    // 6. Upload to S3
    console.log(`[S3] Uploading to ${OUTPUT_PREFIX}...`);
    await uploadS3File(outputParquetLocal, `${OUTPUT_PREFIX}data.parquet`);
    await uploadS3File(metaLocal, `${OUTPUT_PREFIX}meta.json`);

    console.log(`[SUCCESS] Features built and uploaded.`);
    console.log(`  Output: s3://${BUCKET}/${OUTPUT_PREFIX}`);

  } catch (err) {
    console.error(`[FATAL] ${err.message || err.stack || err}`);
  } finally {
    console.log(`[CLEANUP] Removing ${TMP_DIR}`);
    await rm(TMP_DIR, { recursive: true, force: true });
  }
}

async function downloadS3File(key, localPath) {
  const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  const res = await s3.send(cmd);
  await pipeline(Readable.fromWeb(res.Body.transformToWebStream()), createWriteStream(localPath));
}

async function uploadS3File(localPath, key) {
  const fileContent = await readFile(localPath);
  const cmd = new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: fileContent,
    ContentType: key.endsWith('.json') ? 'application/json' : 'application/octet-stream'
  });
  await s3.send(cmd);
}

main();
