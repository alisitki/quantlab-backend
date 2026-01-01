#!/usr/bin/env node
/**
 * run_daily_prod.js: Unified Production Daily ML Orchestrator.
 * 
 * Supports both DRY and LIVE modes with identical execution paths.
 * Differences are config/flag based only.
 * 
 * Usage:
 *   node scheduler/run_daily_prod.js --mode dry|live --symbol btcusdt [options]
 * 
 * Modes:
 *   DRY:  --promote dry, hash must be unchanged
 *   LIVE: --promote auto, hash may change if promoted
 */
import 'dotenv/config';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { execSync } from 'child_process';
import fs from 'fs';
import crypto from 'crypto';
import { SCHEDULER_CONFIG } from './config.js';
import { appendEntry } from './AuditLogger.js';

async function main() {
  const args = parseArgs(process.argv.slice(2));
  
  // Validate required args
  if (!args.symbol) {
    console.error('Usage: node scheduler/run_daily_prod.js --mode dry|live --symbol btcusdt [--date 20251229] [--date-from --date-to] [--ensure-features]');
    process.exit(1);
  }
  
  if (!['dry', 'live'].includes(args.mode)) {
    console.error('Error: --mode must be "dry" or "live"');
    process.exit(1);
  }
  
  const isDryMode = args.mode === 'dry';
  const promoteFlag = isDryMode ? 'dry' : 'auto';
  const canaryFlag = 'false'; // Always false for prod runs
  
  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  const artifactBucket = SCHEDULER_CONFIG.s3.artifactBucket;
  const prodPrefix = `models/production/${args.symbol}/`;

  // Date setup
  let dates = [];
  if (args.dateFrom && args.dateTo) {
    dates = getDatesInRange(args.dateFrom, args.dateTo);
  } else if (args.date) {
    dates = [args.date];
  } else {
    dates = [getYesterday()];
  }

  console.log('='.repeat(60));
  console.log(`Daily Prod Orchestrator (${args.mode.toUpperCase()}) - ${new Date().toISOString()}`);
  console.log(`Symbol: ${args.symbol}`);
  console.log(`Dates: ${dates.join(', ')}`);
  console.log(`Promote: ${promoteFlag}, Canary: ${canaryFlag}`);
  console.log('='.repeat(60));

  const summary = {
    symbol: args.symbol,
    mode: args.mode.toUpperCase(),
    startedAt: new Date().toISOString(),
    runs: []
  };

  for (const date of dates) {
    console.log(`\n>>> STARTING ${args.mode.toUpperCase()} RUN FOR DATE: ${date}`);
    
    // 1. Capture production hash BEFORE
    const hashBefore = await getProductionHash(s3Client, artifactBucket, prodPrefix);
    console.log(`[Safety] Initial Prod Hash: ${hashBefore}`);

    // 2. Build command with mode-specific flags
    const env = { ...process.env, PSEUDO_PROBA: '0' };
    const flags = [
      `--symbol ${args.symbol}`,
      `--date ${date}`,
      `--canary ${canaryFlag}`,
      `--promote ${promoteFlag}`,
      '--live', // Always use --live to launch GPU
      args.ensureFeatures ? '--ensure-features' : ''
    ].filter(Boolean).join(' ');

    const cmd = `node scheduler/run_daily_ml.js ${flags}`;
    console.log(`[Exec] ${cmd}`);

    const runResult = { 
      date, 
      mode: args.mode.toUpperCase(),
      status: 'SUCCESS',
      trainingError: null
    };
    
    let jobId = null;
    let metrics = null;

    try {
      // 3. Execute training job
      execSync(cmd, { stdio: 'inherit', env });
      
      // 4. Post-run: fetch metrics
      const metricsResult = await findNewestMetrics(s3Client, artifactBucket, args.symbol, date);
      jobId = metricsResult?.jobId;
      metrics = metricsResult?.content;
      runResult.jobId = jobId;
      runResult.metrics = metrics;

      // 5. Orphan check (always dry-run to report)
      console.log('[Safety] Running Orphan Reaper (Dry Run)...');
      const reaperOutput = execSync('node vast/reap_orphans.js --dry-run', { encoding: 'utf8' });
      const orphanCountMatch = reaperOutput.match(/Found (\d+) total instances/);
      runResult.totalInstancesUnderReaper = orphanCountMatch ? parseInt(orphanCountMatch[1]) : 0;
      console.log(reaperOutput);

      // 6. Capture production hash AFTER
      const hashAfter = await getProductionHash(s3Client, artifactBucket, prodPrefix);
      runResult.hashBefore = hashBefore;
      runResult.hashAfter = hashAfter;
      runResult.hashChanged = hashBefore !== hashAfter;
      
      console.log(`[Safety] Final Prod Hash: ${hashAfter} -> ${runResult.hashChanged ? 'CHANGED' : 'UNCHANGED'}`);

      // 7. Safety check: DRY mode must not change hash
      if (isDryMode && runResult.hashChanged) {
        throw new Error('SAFETY VIOLATION: Production hash changed during DRY run!');
      }
      
      runResult.safetyPass = true;

    } catch (err) {
      console.error(`[CRITICAL] Run failed for ${date}:`, err.message);
      runResult.status = 'FAILED';
      runResult.trainingError = err.message;
      runResult.safetyPass = false;
    }

    // 8. Append to audit log
    appendEntry(date, {
      symbol: args.symbol,
      mode: args.mode.toUpperCase(),
      job_id: jobId,
      training_status: runResult.status,
      training_error: runResult.trainingError,
      promotion_decision: runResult.status === 'SUCCESS' ? { evaluated: true } : null,
      prod_hash_before: runResult.hashBefore,
      prod_hash_after: runResult.hashAfter,
      hash_changed: runResult.hashChanged
    });

    summary.runs.push(runResult);
  }

  // Final reporting
  summary.endedAt = new Date().toISOString();
  if (!fs.existsSync('reports')) fs.mkdirSync('reports');
  
  const reportPath = dates.length > 1 
    ? `reports/daily_backfill_${args.mode}_summary.json`
    : `reports/daily_${dates[0]}_${args.mode}.json`;
    
  fs.writeFileSync(reportPath, JSON.stringify(summary, null, 2));
  console.log(`\n[Report] Saved to ${reportPath}`);
  
  const allSafe = summary.runs.every(r => r.safetyPass !== false);
  const allSuccess = summary.runs.every(r => r.status === 'SUCCESS');
  
  console.log(`\n[Summary] DONE. Success: ${allSuccess}, Safety: ${allSafe}`);
  if (!allSafe || !allSuccess) process.exit(1);
}

/**
 * Helpers
 */

function parseArgs(args) {
  const result = { mode: 'dry' }; // Default to dry for safety
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--mode') result.mode = args[++i];
    if (args[i] === '--symbol') result.symbol = args[++i];
    if (args[i] === '--date') result.date = args[++i];
    if (args[i] === '--date-from') result.dateFrom = args[++i];
    if (args[i] === '--date-to') result.dateTo = args[++i];
    if (args[i] === '--ensure-features') result.ensureFeatures = true;
  }
  return result;
}

function getYesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

function getDatesInRange(from, to) {
  const dates = [];
  let current = new Date(`${from.slice(0, 4)}-${from.slice(4, 6)}-${from.slice(6, 8)}`);
  const end = new Date(`${to.slice(0, 4)}-${to.slice(4, 6)}-${to.slice(6, 8)}`);
  
  while (current <= end) {
    dates.push(current.toISOString().split('T')[0].replace(/-/g, ''));
    current.setDate(current.getDate() + 1);
  }
  return dates;
}

async function getProductionHash(s3Client, bucket, prefix) {
  try {
    const res = await s3Client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
    if (!res.Contents || res.Contents.length === 0) return 'empty';
    const manifest = res.Contents
      .sort((a, b) => a.Key.localeCompare(b.Key))
      .map(c => `${c.Key}:${c.Size}:${c.ETag}`)
      .join('|');
    return crypto.createHash('sha256').update(manifest).digest('hex');
  } catch (e) {
    return 'error';
  }
}

async function findNewestMetrics(s3Client, bucket, symbol, date) {
  const prefix = `ml-artifacts/`;
  try {
    const res = await s3Client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
    const matches = (res.Contents || [])
      .filter(c => c.Key.includes(`job-${symbol}-${date}`) && c.Key.endsWith('metrics.json'))
      .sort((a, b) => b.LastModified - a.LastModified);

    if (matches.length > 0) {
      const gRes = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: matches[0].Key }));
      const content = JSON.parse(await gRes.Body.transformToString());
      const jobId = matches[0].Key.split('/')[1];
      return { jobId, content };
    }
  } catch (e) {}
  return null;
}

main().catch(err => {
  console.error('[Fatal]', err);
  process.exit(1);
});
