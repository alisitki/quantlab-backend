#!/usr/bin/env node
/**
 * reap_orphans.js: Automated cleanup for leaked Vast.ai instances.
 * Criteria:
 *  - Instance lacks an S3 lease file.
 *  - Lease heartbeat is older than 15 minutes.
 *  - Instance age is greater than 60 minutes.
 * 
 * Usage:
 *  node vast/reap_orphans.js [--dry-run] [--destroy]
 */
import 'dotenv/config';
import { createVastClient } from './VastClient.js';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const destroy = args.includes('--destroy');

  if (!dryRun && !destroy) {
    console.log('Usage: node vast/reap_orphans.js [--dry-run] [--destroy]');
    process.exit(0);
  }

  console.log('='.repeat(60));
  console.log(`Orphan Reaper v1 - ${new Date().toISOString()}`);
  console.log(`Mode: ${dryRun ? 'DRY RUN' : 'DESTROY'}`);
  console.log('='.repeat(60));

  const vastClient = createVastClient();
  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  const bucket = SCHEDULER_CONFIG.s3.artifactBucket;

  // 1. List current instances
  const instances = await vastClient.listInstances();
  console.log(`[Reaper] Found ${instances.length} total instances on Vast account.`);

  for (const inst of instances) {
    const id = inst.id;
    const label = inst.label || 'none';
    const status = inst.actual_status;
    const createdAt = new Date(inst.start_date * 1000); // start_date is unix ts
    const ageMin = (Date.now() - createdAt.getTime()) / (60 * 1000);

    // Filter by our system tag
    if (label !== 'quantlab-ml') {
      // console.log(`[Reaper] Skipping ${id} (label: ${label})`);
      continue;
    }

    console.log(`\n[Reaper] Checking Instance ${id} (Status: ${status}, Age: ${ageMin.toFixed(1)}m)`);

    let reapReason = null;

    // Condition 1: Max Age (60m)
    if (ageMin > 60) {
      reapReason = `Max age exceeded (${ageMin.toFixed(1)}m > 60m)`;
    } else {
      // Condition 2: Lease Check
      try {
        const leaseKey = `ml-leases/${id}.json`;
        const res = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: leaseKey }));
        const leaseData = JSON.parse(await res.Body.transformToString());
        
        const lastHeartbeat = new Date(leaseData.lastHeartbeatAt);
        const idleMin = (Date.now() - lastHeartbeat.getTime()) / (60 * 1000);

        if (idleMin > 15) {
          reapReason = `Stale lease (last heartbeat ${idleMin.toFixed(1)}m ago > 15m)`;
        } else {
          console.log(`   Instance is active. Lease updated ${idleMin.toFixed(1)}m ago.`);
        }
      } catch (err) {
        if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
          reapReason = 'No S3 lease file found';
        } else {
          console.error(`   [Error] Failed to read lease for ${id}: ${err.message}`);
        }
      }
    }

    if (reapReason) {
      console.log(`[!] REAP IDENTIFIED: ${id}`);
      console.log(`    Reason: ${reapReason}`);
      
      if (destroy) {
        console.log(`    DESTROYING ${id}...`);
        await vastClient.destroyInstance(id);
      } else {
        console.log(`    [DRY RUN] Would destroy ${id}.`);
      }
    }
  }

  console.log('\n[Reaper] Done.');
}

main().catch(err => {
  console.error('[Reaper] Fatal Error:', err);
  process.exit(1);
});
