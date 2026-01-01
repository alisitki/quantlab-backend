#!/usr/bin/env node
/**
 * run_daily_ml.js: Main entry point for daily ML training orchestration.
 * 
 * Usage:
 *   node scheduler/run_daily_ml.js [options]
 * 
 * Options:
 *   --symbol <symbol>     Train specific symbol (default: all from config)
 *   --date <YYYYMMDD>     Train on specific date (default: yesterday)
 *   --dry-run             Generate JobSpecs without launching GPU
 *   --skip-promotion      Skip model promotion step
 *   --help                Show this help message
 */
import 'dotenv/config';
import { JobSpecGenerator } from './JobSpecGenerator.js';
import { SCHEDULER_CONFIG } from './config.js';
import { createVastClient } from '../vast/VastClient.js';
import { RemoteJobRunner } from '../vast/RemoteJobRunner.js';
import { Promoter } from '../promotion/Promoter.js';
import { S3Client, HeadObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { execSync } from 'child_process';

// Global state for emergency cleanup
let activeInstanceId = null;
let heartbeatInterval = null;
let vastClientRef = null;
let s3ClientRef = null;

/**
 * Emergency cleanup function for process signals.
 */
async function emergencyCleanup() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
  if (activeInstanceId && vastClientRef) {
    const id = activeInstanceId;
    activeInstanceId = null; // Prevent re-entry
    console.error(`\n[Lifecycle] EMERGENCY CLEANUP: Destroying instance ${id}...`);
    try {
      // Best effort deletion of lease and instance
      if (s3ClientRef) {
        const leaseKey = `ml-leases/${id}.json`;
        await s3ClientRef.send(new DeleteObjectCommand({ 
          Bucket: SCHEDULER_CONFIG.s3.artifactBucket, 
          Key: leaseKey 
        })).catch(() => {});
      }
      await vastClientRef.destroyInstance(id);
    } catch (e) {
      console.error(`[Lifecycle] Emergency cleanup failed: ${e.message}`);
    }
  }
}

// Process signals
process.on('SIGINT', async () => {
  await emergencyCleanup();
  process.exit(130);
});
process.on('SIGTERM', async () => {
  await emergencyCleanup();
  process.exit(143);
});
process.on('uncaughtException', async (err) => {
  console.error('[FATAL] Uncaught Exception:', err);
  await emergencyCleanup();
  process.exit(1);
});
process.on('unhandledRejection', async (reason) => {
  console.error('[FATAL] Unhandled Rejection:', reason);
  await emergencyCleanup();
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv.slice(2));
  
  if (args.help) {
    printHelp();
    return;
  }
  
  console.log('='.repeat(60));
  console.log('ML Daily Training Orchestrator v1');
  console.log('='.repeat(60));
  console.log(`Start Time: ${new Date().toISOString()}`);
  console.log(`Mode: ${args.dryRun ? 'DRY RUN' : 'LIVE'}`);
  console.log('');
  
  // Determine symbols to train
  const symbols = args.symbol 
    ? [args.symbol] 
    : SCHEDULER_CONFIG.defaultSymbols;
  
  // Determine date
  const date = args.date || JobSpecGenerator.getYesterdayDate();
  
  // Determine promotion and canary status
  const promoteMode = args.promote || process.env.PROMOTE_MODE || SCHEDULER_CONFIG.s3.promoteMode || 'off';
  
  // Canary detection: explicit flag OR env OR --live test run
  const isCanary = args.canary === 'true' || args.canary === true || 
                   process.env.RUN_MODE === 'canary' ||
                   args.live === true || args.live === 'true';

  console.log(`Symbols: ${symbols.join(', ')}`);
  console.log(`Date: ${date}`);
  console.log(`Promote Mode: ${promoteMode}${isCanary ? ' (CANARY/LIVE: non-prod)' : ''}`);
  console.log(`Ensure Features: ${args.ensureFeatures ? 'ON' : 'OFF'}`);
  console.log('');
  
  // Initialize S3 client for existence checks
  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });
  
  // Generate JobSpecs for all symbols
  const jobSpecs = JobSpecGenerator.generateBatch({ 
    symbols, 
    date,
    modelOverrides: {
      featureset: args.featureset || 'v1'
    }
  });
  
  console.log('--- Generated Job Specs ---');
  for (const spec of jobSpecs) {
    console.log(`  ${spec.jobId} (hash: ${spec.configHash.substring(0, 12)}...)`);
  }
  console.log('');
  
  // Process features existence and optional building
  const validatedJobSpecs = [];
  const results = {
    successful: [],
    failed: [],
    promoted: [],
    dry_pass: [],
    rejected: [],
    off: []
  };

  for (const jobSpec of jobSpecs) {
    const featurePath = jobSpec.dataset.featurePath;
    const bucket = featurePath.replace('s3://', '').split('/')[0];
    const key = featurePath.replace(`s3://${bucket}/`, '');
    
    console.log(`[Scheduler] Checking features: ${featurePath}...`);
    let featuresExist = false;
    try {
      await s3Client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      featuresExist = true;
      console.log(`[Scheduler] Features found.`);
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        console.log(`[Scheduler] Features missing.`);
      } else {
        console.warn(`[Scheduler] S3 check failed: ${err.message}`);
      }
    }

    if (!featuresExist) {
      if (args.ensureFeatures) {
        console.log(`[Scheduler] --ensure-features is ON. Invoking FeatureBuilder...`);
        try {
          const { exchange, stream, symbol, dateRange } = jobSpec.dataset;
          const date = dateRange.date;
          const buildCmd = `node features/run_build_features_v1.js --exchange ${exchange} --stream ${stream} --symbol ${symbol} --date ${date}`;
          console.log(`[Scheduler] Executing: ${buildCmd}`);
          
          if (!args.dryRun) {
            execSync(buildCmd, { stdio: 'inherit', cwd: process.cwd() });
            console.log(`[Scheduler] FeatureBuilder success.`);
            featuresExist = true;
          } else {
            console.log(`[DRY RUN] Would execute FeatureBuilder.`);
            featuresExist = true; // Assume success for dry-run flow
          }
        } catch (err) {
          console.error(`[ERROR] FeatureBuilder failed: ${err.message}`);
          results.failed.push({
            jobId: jobSpec.jobId,
            symbol: jobSpec.dataset.symbol,
            error: `FeatureBuilder failed: ${err.message}`
          });
          continue;
        }
      } else {
        console.error(`[ERROR] Features parquet missing; rerun with --ensure-features or build manually.`);
        results.failed.push({
          jobId: jobSpec.jobId,
          symbol: jobSpec.dataset.symbol,
          error: "features parquet missing; rerun with --ensure-features"
        });
        continue;
      }
    }

    if (featuresExist) {
      validatedJobSpecs.push(jobSpec);
    }
  }

  if (args.dryRun) {
    console.log('');
    console.log('[DRY RUN] Summary of jobs to be executed:');
    for (const spec of validatedJobSpecs) {
      console.log(JSON.stringify(spec, null, 2));
    }
    if (results.failed.length > 0) {
      console.log('\n[DRY RUN] The following jobs would FAIL due to missing features:');
      for (const fail of results.failed) {
        console.log(`  - ${fail.jobId}: ${fail.error}`);
      }
    }
    console.log('\n[DRY RUN] No GPU instance launched.');
    return;
  }
  
  if (validatedJobSpecs.length === 0 && jobSpecs.length > 0) {
    console.error('[FATAL] No jobs remaining after feature validation.');
    process.exit(1);
  }

  // Initialize clients
  const vastClient = createVastClient();
  const promoter = new Promoter();
  
  vastClientRef = vastClient;
  s3ClientRef = s3Client;

  /**
   * Update lease heartbeat on S3.
   */
  async function updateLease(id, symbol, jobId, createdAt) {
    const leaseKey = `ml-leases/${id}.json`;
    const payload = JSON.stringify({
      instanceId: id,
      symbol,
      jobId,
      lastHeartbeatAt: new Date().toISOString(),
      createdAt
    });
    try {
      await s3Client.send(new PutObjectCommand({
        Bucket: SCHEDULER_CONFIG.s3.artifactBucket,
        Key: leaseKey,
        Body: payload,
        ContentType: 'application/json'
      }));
    } catch (e) {
      console.warn(`[Lease] Heartbeat failed for ${id}: ${e.message}`);
    }
  }

  /**
   * Delete lease from S3.
   */
  async function deleteLease(id) {
    const leaseKey = `ml-leases/${id}.json`;
    try {
      await s3Client.send(new DeleteObjectCommand({
        Bucket: SCHEDULER_CONFIG.s3.artifactBucket,
        Key: leaseKey
      }));
    } catch (e) {
      console.warn(`[Lease] Deletion failed for ${id}: ${e.message}`);
    }
  }

  // Process each job
  for (const jobSpec of validatedJobSpecs) {
    let instanceId = null;
    
    try {
      console.log('');
      console.log(`${'='.repeat(60)}`);
      console.log(`Processing: ${jobSpec.jobId}`);
      console.log(`${'='.repeat(60)}`);
      
      // 1. Search for GPU offers
      const offers = await vastClient.searchOffers({
        minGpuMemory: SCHEDULER_CONFIG.gpu.minGpuMemory,
        maxHourlyCost: SCHEDULER_CONFIG.gpu.maxHourlyCost,
        minDiskSpace: SCHEDULER_CONFIG.gpu.minDiskSpace,
        preferredTypes: SCHEDULER_CONFIG.gpu.preferredTypes
      });
      
      if (offers.length === 0) {
        throw new Error('No suitable GPU offers found');
      }
      
      // 2. Try multiple offers with retry logic
      let instance = null;
      let attempt = 0;
      const MAX_RETRIES = 20;
      const MAX_SSH_FAILED_OFFERS = 10; // SSH Zero-Patience: max offers to try
      
      let jobResult = null;
      const badOfferIds = new Set(); // Track offers that failed SSH
      let sshFailedOfferCount = 0;

      // Loop through offers until success or max retries
      for (const offer of offers) {
        // Skip offers that already failed SSH
        if (badOfferIds.has(offer.id)) {
          continue;
        }

        attempt++;
        if (attempt > MAX_RETRIES) {
          console.error(`[Scheduler] Max retries (${MAX_RETRIES}) reached. Giving up.`);
          break;
        }

        // SSH Zero-Patience: cost bound on bad offers
        if (sshFailedOfferCount >= MAX_SSH_FAILED_OFFERS) {
          console.error(`[Scheduler] Max SSH-failed offers (${MAX_SSH_FAILED_OFFERS}) reached. Giving up.`);
          break;
        }

        // Avoid Vast API 429 by adding a delay
        await new Promise(resolve => setTimeout(resolve, 3000));

        console.log(`Trying GPU offer ${attempt}/${offers.length}: ${offer.gpu_name} @ $${offer.dph_total}/hr (Offer ID: ${offer.id})`);
        
        try {
          // Attempt creation
          instance = await vastClient.createInstance(offer.id, {
            image: 'ubuntu:22.04',
            diskSpace: SCHEDULER_CONFIG.gpu.minDiskSpace,
            onstart: 'apt-get update && apt-get install -y curl && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs'
          });
          
          instanceId = instance.instanceId;
          activeInstanceId = instanceId;
          const createdAt = new Date().toISOString();

          // 2.5 Start Lease Heartbeat (S3)
          console.log(`[Lease] Starting heartbeat for instance ${instanceId}...`);
          heartbeatInterval = setInterval(() => {
            updateLease(instanceId, jobSpec.dataset.symbol, jobSpec.jobId, createdAt);
          }, 30000);
          await updateLease(instanceId, jobSpec.dataset.symbol, jobSpec.jobId, createdAt);

          console.log(`Successfully created instance ${instanceId} from offer ${offer.id}`);

          // 3. Wait for ready state (API status)
          try {
             await vastClient.waitUntilReady(instanceId, 300000); // 5 min max for API
             console.log(`[Scheduler] Instance ${instanceId} is ready (Vast API).`);
             
             // 4. Wait for SSH readiness (Stage 1-3) & Execute job
             const sshInfo = await vastClient.getSshInfo(instanceId);
             const runner = new RemoteJobRunner(sshInfo, vastClient, instanceId);
             
             jobResult = await runner.executeJob(jobSpec);
             
             // If we reached here, job completed successfully
             break; 
             
          } catch (execErr) {
             const errMsg = execErr.message || '';
             
             // SSH Zero-Patience: Track SSH failures separately
             const isSshHardFail = 
                errMsg.includes('SSH_HARD_TIMEOUT') ||
                errMsg.includes('SSH_KEX_FATAL');

             if (isSshHardFail) {
                console.warn(`[WARNING] SSH HARD FAIL on offer ${offer.id}: ${errMsg}`);
                badOfferIds.add(offer.id);
                sshFailedOfferCount++;
                // Instance already destroyed by RemoteJobRunner
                instanceId = null;
                instance = null;
                continue; // Try next offer
             }
             
             // Check for other retryable conditions
             const isRetryable = 
                errMsg.includes('SSH_NOT_READY_TIMEOUT') ||
                errMsg.includes('ECONNREFUSED') ||
                errMsg.includes('ETIMEDOUT') ||
                errMsg.includes('API Error') ||
                errMsg.includes('Request timeout') ||
                errMsg.includes('failed to inject CDI devices') || 
                errMsg.includes('OCI runtime create failed');

             if (isRetryable) {
                console.warn(`[WARNING] Attempt ${attempt} failed with retryable error: ${errMsg}`);
                console.warn(`[WARNING] Destroying instance ${instanceId} and trying next offer...`);
                
                try { await vastClient.destroyInstance(instanceId); } catch(e) {}
                instanceId = null;
                instance = null;
                continue; // Try next offer
             } else {
                console.error(`[FATAL] Job failed with non-retryable error: ${errMsg}`);
                try { await vastClient.destroyInstance(instanceId); } catch(e) {}
                throw execErr;
             }
          }

        } catch (err) {
          const errMsg = err.message || '';
          if (errMsg.includes('API Error') || errMsg.includes('creation failed') || errMsg.includes('Request timeout')) {
            console.warn(`[WARNING] Offer ${offer.id} creation failed: ${errMsg}`);
            continue;
          }
          throw err; 
        }
      }

      if (!jobResult) {
        throw new Error(`All ${Math.min(offers.length, MAX_RETRIES)} attempts failed to provision a working GPU and complete the job (SSH failed offers: ${sshFailedOfferCount})`);
      }
      
      // 5. Record success
      results.successful.push({
        jobId: jobSpec.jobId,
        symbol: jobSpec.dataset.symbol,
        runtimeMs: jobResult.runtimeMs,
        metrics: jobResult.metrics
      });
      
      // 6. Model evaluation & promotion
      const promoResult = await promoter.evaluate(
        jobSpec.dataset.symbol,
        jobResult.metrics,
        jobSpec.jobId,
        { mode: promoteMode, canary: isCanary },
        jobSpec  // Pass jobSpec for decision config generation
      );
      
      if (promoResult.decision === 'promoted') {
        results.promoted.push(promoResult);
      } else if (promoResult.decision === 'dry_pass') {
        results.dry_pass.push(promoResult);
      } else if (promoResult.decision === 'rejected') {
        results.rejected.push(promoResult);
      } else {
        results.off.push(promoResult);
      }
      
    } catch (err) {
      console.error(`[ERROR] Job ${jobSpec.jobId} failed:`, err.message);
      results.failed.push({
        jobId: jobSpec.jobId,
        symbol: jobSpec.dataset.symbol,
        error: err.message
      });
    } finally {
      // Clear heartbeat
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
      }

      // ALWAYS destroy instance
      if (instanceId) {
        try {
          // Delete lease first
          await deleteLease(instanceId);
          await vastClient.destroyInstance(instanceId);
        } catch (destroyErr) {
          console.error(`[WARNING] Failed to destroy instance ${instanceId}:`, destroyErr.message);
        }
        activeInstanceId = null;
      }
    }
  }
  
  // Print summary
  console.log('');
  console.log('='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`Total Jobs: ${jobSpecs.length}`);
  console.log(`Successful: ${results.successful.length}`);
  console.log(`Failed: ${results.failed.length}`);
  console.log(`Promoted: ${results.promoted.length}`);
  console.log(`Dry Pass: ${results.dry_pass.length}`);
  console.log(`Rejected: ${results.rejected.length}`);
  console.log(`Promo Off: ${results.off.length}`);
  console.log('');
  
  if (results.failed.length > 0) {
    console.log('Failed Jobs:');
    for (const fail of results.failed) {
      console.log(`  - ${fail.jobId}: ${fail.error}`);
    }
  }
  
  if (results.promoted.length > 0) {
    console.log('Promoted Models:');
    for (const promo of results.promoted) {
      console.log(`  - ${promo.symbol}: ${promo.reason}`);
    }
  }
  
  if (results.dry_pass.length > 0) {
    console.log('Dry Pass (Ready but not promoted):');
    for (const dp of results.dry_pass) {
      console.log(`  - ${dp.symbol}: ${dp.reason}`);
    }
  }
  
  console.log('');
  console.log(`End Time: ${new Date().toISOString()}`);
  
  // Exit with error if any jobs failed
  if (results.failed.length > 0) {
    process.exit(1);
  }
}

function parseArgs(args) {
  const result = {
    symbol: null,
    date: null,
    promote: null,
    canary: false,
    live: false,
    dryRun: false,
    ensureFeatures: false,
    featureset: 'v1',
    help: false
  };
  
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--symbol':
        result.symbol = args[++i];
        break;
      case '--date':
        result.date = args[++i];
        break;
      case '--promote':
        result.promote = args[++i];
        break;
      case '--canary':
        const canaryVal = args[++i];
        result.canary = canaryVal === 'true' || canaryVal === true;
        break;
      case '--live':
        result.live = true;
        break;
      case '--dry-run':
        result.dryRun = true;
        break;
      case '--ensure-features':
        result.ensureFeatures = true;
        break;
      case '--featureset':
        result.featureset = args[++i];
        break;
      case '--help':
        result.help = true;
        break;
    }
  }
  
  return result;
}

function printHelp() {
  console.log(`
ML Daily Training Orchestrator

Usage:
  node scheduler/run_daily_ml.js [options]

Options:
  --symbol <symbol>     Train specific symbol (default: all from config)
  --date <YYYYMMDD>     Train on specific date (default: yesterday)
  --promote <mode>      Promotion mode: off|dry|auto (default: off)
  --canary <bool>       Mark as canary run (blocks auto promotion)
  --live                Launch remote GPU but treat as non-prod/canary
  --ensure-features     Build features if missing (default: false)
  --featureset <v1>     Feature set version (default: v1)
  --dry-run             Generate JobSpecs without launching GPU
  --help                Show this help message

Promotion Guard v2:
  - 'off' (default): No metrics comparison, no promotion.
  - 'dry': Calculates decision but never writes to production artifacts.
  - 'auto': Promotes to production prefix only if NOT in canary/live-test mode.
  - Note: --live or --canary will automatically downgrade 'auto' -> 'dry'.

Examples:
  # Dry run for btcusdt
  node scheduler/run_daily_ml.js --dry-run --symbol btcusdt

  # Full daily run with auto promotion
  node scheduler/run_daily_ml.js --promote auto

  # Canary run (auto is downgraded to dry)
  node scheduler/run_daily_ml.js --promote auto --canary true
`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
