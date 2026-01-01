import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from './scheduler/config.js';
import 'dotenv/config';

async function readFile(key) {
  const client = new S3Client({
    endpoint: SCHEDULER_CONFIG.s3.artifactEndpoint,
    region: process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  console.log('Access Key:', process.env.S3_COMPACT_ACCESS_KEY ? (process.env.S3_COMPACT_ACCESS_KEY.substring(0, 4) + '...') : 'UNDEFINED');

  const bucket = SCHEDULER_CONFIG.s3.artifactBucket;
  console.log(`Reading s3://${bucket}/${key}`);

  try {
    const response = await client.send(new GetObjectCommand({
      Bucket: bucket,
      Key: key
    }));
    
    const body = await response.Body.transformToString();
    console.log('--- CONTENT START ---');
    console.log(body);
    console.log('--- CONTENT END ---');
  } catch (err) {
    console.error('Error reading file:', err);
  }
}

const key = process.argv[2];
readFile(key);
