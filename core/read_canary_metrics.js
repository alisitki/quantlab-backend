import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import dotenv from 'dotenv';
import path from 'path';

// Explicitly load .env from api root
dotenv.config({ path: path.join(process.cwd(), '.env') });

const config = {
    endpoint: process.env.S3_COMPACT_ENDPOINT || process.env.S3_ENDPOINT,
    region: 'auto', // Try 'auto' or 'us-east-1'
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
};

console.log('--- Config Debug ---');
console.log('Endpoint:', config.endpoint);
console.log('AccessKeyId:', config.credentials.accessKeyId ? config.credentials.accessKeyId.substring(0, 5) + '...' : 'MISSING');
console.log('Bucket:', process.env.S3_ARTIFACTS_BUCKET || 'quantlab-artifacts');
console.log('--------------------');

const client = new S3Client(config);

async function readMetrics() {
    const bucket = process.env.S3_ARTIFACTS_BUCKET || 'quantlab-artifacts';
    const key = 'ml-artifacts/job-btcusdt-20251229-c9341eb80c7d510b/metrics.json';

    console.log(`Attempting to read s3://${bucket}/${key}`);

    try {
        const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
        const res = await client.send(cmd);
        const body = await res.Body.transformToString();
        console.log('\n--- METRICS.JSON CONTENT ---');
        console.log(body);
        console.log('----------------------------\n');
    } catch (err) {
        console.error('FAILED to read metrics.json:', err.message);
        if (err.name === 'InvalidAccessKeyId') {
             console.error('Check if S3_COMPACT_ACCESS_KEY in .env matches the one expected by the server.');
        }
    }
}

readMetrics();
