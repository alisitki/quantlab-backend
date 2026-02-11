set -euo pipefail
node - <<'NODE'
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { mkdir } from 'node:fs/promises';
import { createWriteStream, existsSync } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import dotenv from 'dotenv';

dotenv.config();

const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
const s3 = new S3Client({
  endpoint: process.env.S3_COMPACT_ENDPOINT,
  region: 'auto',
  credentials: {
    accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
    secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
  },
  forcePathStyle: true
});

async function download(key, outPath) {
  if (existsSync(outPath)) {
    console.log(`SKIP\t${key}\t->\t${outPath}`);
    return;
  }
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  await pipeline(Readable.from(res.Body), createWriteStream(outPath));
  console.log(`OK\t${key}\t->\t${outPath}`);
}

const symbol = 'adausdt';
const dates = ['20260116','20260117','20260118'];

for (const date of dates) {
  const dir = `data/curated/exchange=binance/stream=bbo/symbol=${symbol}/date=${date}`;
  await mkdir(dir, { recursive: true });
  await download(`exchange=binance/stream=bbo/symbol=${symbol}/date=${date}/data.parquet`, `${dir}/data.parquet`);
  await download(`exchange=binance/stream=bbo/symbol=${symbol}/date=${date}/meta.json`, `${dir}/meta.json`);
}
NODE
