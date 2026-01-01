import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from './scheduler/config.js';
import 'dotenv/config';

async function listProduction(symbol) {
  const client = new S3Client({
    endpoint: SCHEDULER_CONFIG.s3.artifactEndpoint,
    region: process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY,
      secretAccessKey: process.env.S3_SECRET_KEY
    },
    forcePathStyle: true
  });

const prefix = process.argv[2] || `${SCHEDULER_CONFIG.s3.productionPrefix}/btcusdt/`;
  console.log(`Listing S3 prefix: s3://${SCHEDULER_CONFIG.s3.artifactBucket}/${prefix}`);

  try {
    const listObjects = async (p, token) => {
        const cmd = new ListObjectsV2Command({
            Bucket: SCHEDULER_CONFIG.s3.artifactBucket,
            Prefix: p,
            ContinuationToken: token
        });
        const res = await client.send(cmd);
        if (res.Contents) {
            res.Contents.forEach(obj => {
                console.log(`- ${obj.Key} (Size: ${obj.Size}, LastModified: ${obj.LastModified})`);
            });
        }
        if (res.IsTruncated) {
            await listObjects(p, res.NextContinuationToken);
        }
    };
    await listObjects(prefix);
  } catch (err) {
    console.error('Error listing S3 objects:', err);
  }
}

listProduction();
