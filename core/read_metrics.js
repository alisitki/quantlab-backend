import 'dotenv/config';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from './scheduler/config.js';

const client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
});

const key = 'ml-artifacts/job-btcusdt-20251229-c9341eb80c7d510b/metrics.json';
const bucket = 'quantlab-artifacts';

async function main() {
    try {
        const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
        const data = await client.send(cmd);
        const str = await data.Body.transformToString();
        console.log(str);
    } catch (err) {
        console.error(err);
    }
}
main();
