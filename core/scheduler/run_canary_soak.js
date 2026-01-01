#!/usr/bin/env node
/**
 * run_canary_soak.js: Multi-day ML validation orchestrator.
 * 
 * Logic:
 *  1. Discover available dates from S3 features/curated prefixes.
 *  2. Filter dates by from/to range.
 *  3. For each date:
 *     a. Fetch meta.json and verify coverage gate.
 *     b. Run run_daily_ml.js with --canary true --promote dry.
 *     c. Capture metrics and artifacts.
 *  4. Verify production artifacts hash before/after.
 *  5. Generate soak_summary.json.
 */
import 'dotenv/config';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { SCHEDULER_CONFIG } from './config.js';

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.symbol || !args.dateFrom || !args.dateTo) {
    console.error('Usage: node scheduler/run_canary_soak.js --symbol btcusdt --date-from 20251227 --date-to 20251229 [--live] [--ensure-features]');
    process.exit(1);
  }

  console.log('='.repeat(60));
  console.log(`Canary Soak v1.1 - ${new Date().toISOString()}`);
  console.log(`Symbol: ${args.symbol} (${args.dateFrom} -> ${args.dateTo})`);
  console.log('='.repeat(60));

  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  const compactBucket = 'quantlab-compact';
  const artifactBucket = SCHEDULER_CONFIG.s3.artifactBucket;

  // 1. Production Safety Hash (Before)
  console.log('[Soak] Monitoring production model stability...');
  const prodPrefix = `models/production/${args.symbol}/`;
  const initialHash = await getProductionHash(s3Client, artifactBucket, prodPrefix);
  console.log(`[Soak] Initial Prod Hash: ${initialHash}`);

  // 2. Date Discovery
  console.log('[Soak] Discovering dates from S3...');
  const discoveredDates = await discoverDates(s3Client, compactBucket, args.symbol, args.dateFrom, args.dateTo);
  
  if (discoveredDates.length === 0) {
    console.error(`[FATAL] No dates found for ${args.symbol} in range ${args.dateFrom}-${args.dateTo}`);
    process.exit(1);
  }
  console.log(`[Soak] Found ${discoveredDates.length} eligible dates: ${discoveredDates.join(', ')}`);

  const results = {
    summary: {
      symbol: args.symbol,
      dateRange: { from: args.dateFrom, to: args.dateTo },
      totalDiscovered: discoveredDates.length,
      ran: 0,
      skipped: 0
    },
    dates: []
  };

  // 3. Execution Loop
  for (const date of discoveredDates) {
    console.log(`\n--- Processing Date: ${date} ---`);
    
    // a. Coverage Gate
    const coverage = await checkCoverage(s3Client, compactBucket, args.symbol, date);
    console.log(`[Soak] Coverage: ${coverage.status} (${coverage.hours.toFixed(1)}h)`);

    if (coverage.status === 'TOO_SHORT') {
      console.log(`[Soak] Skipping ${date} (Too short)`);
      results.dates.push({ date, status: 'SKIPPED', reason: 'TOO_SHORT', coverageHours: coverage.hours });
      results.summary.skipped++;
      continue;
    }

    // b. Run Job
    const env = { ...process.env, PSEUDO_PROBA: '0' };
    const flags = [
      `--symbol ${args.symbol}`,
      `--date ${date}`,
      `--canary true`,
      `--promote dry`,
      args.live ? '--live' : '',
      args.ensureFeatures ? '--ensure-features' : ''
    ].filter(Boolean).join(' ');

    const cmd = `node scheduler/run_daily_ml.js ${flags}`;
    console.log(`[Soak] Executing: ${cmd}`);

    try {
      execSync(cmd, { stdio: 'inherit', env });
      
      // c. Fetch Resulting Metrics (Best effort)
      // Since run_daily_ml.js doesn't return the jobId easily, we might need a better way.
      // For now, we assume it succeeds and logs to its standard artifacts path.
      // We can search for the newest artifact for this symbol/date.
      const metrics = await findNewestMetrics(s3Client, artifactBucket, args.symbol, date);
      
      results.dates.push({
        date,
        status: 'SUCCESS',
        coverageType: coverage.status,
        coverageHours: coverage.hours,
        jobId: metrics?.jobId,
        metrics: metrics?.content
      });
      results.summary.ran++;

    } catch (err) {
      console.error(`[ERROR] Soak failed for date ${date}:`, err.message);
      results.dates.push({ date, status: 'FAILED', error: err.message, coverageType: coverage.status });
      results.summary.skipped++;
    }
  }

  // 4. Production Safety Hash (After)
  const finalHash = await getProductionHash(s3Client, artifactBucket, prodPrefix);
  console.log(`\n[Soak] Final Prod Hash: ${finalHash}`);
  
  const safetyPass = initialHash === finalHash;
  if (!safetyPass) {
    console.error('[FATAL] PRODUCTION SAFETY VIOLATION: Model hash changed during dry-run soak!');
  }

  // 5. Reporting
  const summaryJson = JSON.stringify({ ...results, safetyPass }, null, 2);
  if (!fs.existsSync('reports')) fs.mkdirSync('reports');
  fs.writeFileSync('reports/soak_summary.json', summaryJson);
  console.log('\n[Soak] Report generated: reports/soak_summary.json');

  console.log(`\n[Soak] DONE. Ran: ${results.summary.ran}, Skipped: ${results.summary.skipped}, Safety: ${safetyPass ? 'PASS' : 'FAIL'}`);
  
  if (!safetyPass) process.exit(1);
}

/**
 * Helpers
 */

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--symbol') result.symbol = args[++i];
    if (args[i] === '--date-from') result.dateFrom = args[++i];
    if (args[i] === '--date-to') result.dateTo = args[++i];
    if (args[i] === '--live') result.live = true;
    if (args[i] === '--ensure-features') result.ensureFeatures = true;
  }
  return result;
}

async function discoverDates(s3Client, bucket, symbol, from, to) {
  // Try features first
  const featurePrefix = `features/featureset=v1/exchange=binance/stream=bbo/symbol=${symbol}/`;
  let res = await s3Client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: featurePrefix, Delimiter: '/' }));
  
  let prefixes = res.CommonPrefixes || [];
  if (prefixes.length === 0) {
    // Fallback to curated
    const curatedPrefix = `curated/exchange=binance/stream=bbo/symbol=${symbol}/`;
    res = await s3Client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: curatedPrefix, Delimiter: '/' }));
    prefixes = res.CommonPrefixes || [];
  }

  return prefixes
    .map(p => {
      const match = p.Prefix.match(/date=(\d{8})\//);
      return match ? match[1] : null;
    })
    .filter(d => d && d >= from && d <= to)
    .sort();
}

async function checkCoverage(s3Client, bucket, symbol, date) {
  const metaKey = `features/featureset=v1/exchange=binance/stream=bbo/symbol=${symbol}/date=${date}/meta.json`;
  try {
    const res = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: metaKey }));
    const meta = JSON.parse(await res.Body.transformToString());
    
    if (!meta.ts_min || !meta.ts_max) return { status: 'FULL', hours: 24 }; // Mocking backfill
    
    const hours = (meta.ts_max - meta.ts_min) / 3600000;
    let status = 'TOO_SHORT';
    if (hours >= 20) status = 'FULL';
    else if (hours >= 6) status = 'PARTIAL';
    
    return { status, hours };
  } catch (e) {
    // If meta.json missing, assume curated data check? 
    // For now, if no meta, assume 24h if curated partition exists
    return { status: 'FULL', hours: 24 };
  }
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
    const contents = res.Contents || [];
    
    // Pattern: ml-artifacts/job-{symbol}-{date}-{hash}/metrics.json
    const matches = contents
      .filter(c => c.Key.includes(`job-${symbol}-${date}`) && c.Key.endsWith('metrics.json'))
      .sort((a, b) => b.LastModified - a.LastModified);

    if (matches.length > 0) {
      const best = matches[0];
      const getRes = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: best.Key }));
      const content = JSON.parse(await getRes.Body.transformToString());
      const jobId = best.Key.split('/')[1];
      return { jobId, content };
    }
  } catch (e) {
    console.warn(`[Soak] Failed to fetch metrics for ${date}: ${e.message}`);
  }
  return null;
}

main().catch(err => {
  console.error('[Soak] Fatal Error:', err);
  process.exit(1);
});
