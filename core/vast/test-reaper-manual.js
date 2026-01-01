import 'dotenv/config';
import { S3Client, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';
import { execSync } from 'child_process';

async function runManualTest() {
  console.log('--- Orphan Reaper Manual Simulation ---');

  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  const fakeId = 999999;
  const leaseKey = `ml-leases/${fakeId}.json`;
  const bucket = SCHEDULER_CONFIG.s3.artifactBucket;

  console.log(`1. Creating fake stale lease for instance ${fakeId}...`);
  const staleLease = JSON.stringify({
    instanceId: fakeId,
    lastHeartbeatAt: new Date(Date.now() - 20 * 60 * 1000).toISOString(), // 20m ago
    createdAt: new Date(Date.now() - 30 * 60 * 1000).toISOString()
  });

  await s3Client.send(new PutObjectCommand({
    Bucket: bucket,
    Key: leaseKey,
    Body: staleLease
  }));

  console.log('2. Running reaper in dry-run mode (Simulated)...');
  console.log('NOTE: Since instance ${fakeId} does not exist on Vast, we can only verify logic via log if we had a real instance.');
  console.log('Actually, let\'s just show the reaper output for a real run (Found 0).');
  
  try {
    const output = execSync('node vast/reap_orphans.js --dry-run', { encoding: 'utf8' });
    console.log(output);
  } finally {
    console.log('3. Cleaning up fake lease...');
    await s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: leaseKey }));
  }

  console.log('\n✅ Manual Logic Simulation Complete.');
}

runManualTest().catch(err => {
  console.error(err);
  process.exit(1);
});
