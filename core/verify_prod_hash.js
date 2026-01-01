import 'dotenv/config';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import crypto from 'crypto';

const s3 = new S3Client({
  endpoint: process.env.S3_COMPACT_ENDPOINT,
  region: process.env.S3_COMPACT_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
    secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
  },
  forcePathStyle: true
});

const prefix = 'models/production/btcusdt/';
const res = await s3.send(new ListObjectsV2Command({ Bucket: 'quantlab-artifacts', Prefix: prefix }));

if (!res.Contents || res.Contents.length === 0) {
  console.log('PRODUCTION_HASH: empty');
} else {
  const manifest = res.Contents
    .sort((a, b) => a.Key.localeCompare(b.Key))
    .map(c => `${c.Key}:${c.Size}:${c.ETag}`)
    .join('|');
  
  const hash = crypto.createHash('sha256').update(manifest).digest('hex');
  console.log('PRODUCTION_HASH:', hash);
  console.log('File count:', res.Contents.length);
  res.Contents.forEach(c => console.log('-', c.Key));
}
