#!/usr/bin/env node
/**
 * cleanup_retention.js: Safe retention and cleanup for QuantLab artifacts.
 * 
 * Logic (STRICT COMPLIANCE):
 *   1. Scans daily_runs/ to build a job status map (Single Source of Truth).
 *   2. Scans S3 (quantlab-artifacts) with FULL PAGINATION and ASC sorting.
 *   3. Enforces EXPLICIT ALLOWLIST (S3 Prefix & Local path.resolve).
 *   4. Enforces ACTIVE JOB GUARD (Skip if job in daily_runs within 24h).
 *   5. Enforces PROMOTED GUARD (hash_changed === true AND status === SUCCESS).
 *   6. Outputs a deletion plan and plan JSON.
 *   7. Executes deletions with --apply.
 */
import 'dotenv/config';
import { S3Client, ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import path from 'path';

const PLAN_DIR = 'cleanup';
const AUDIT_DIR = 'daily_runs';
const LOGS_DIR = 'logs';

// S3 Artifacts Bucket & Credentials
const ARTIFACT_BUCKET = process.env.S3_ARTIFACTS_BUCKET || 'quantlab-artifacts';
const S3_OPTS = {
  endpoint: process.env.S3_ARTIFACTS_ENDPOINT || process.env.S3_ENDPOINT,
  region: process.env.S3_ARTIFACTS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.S3_ARTIFACTS_ACCESS_KEY || process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_ARTIFACTS_SECRET_KEY || process.env.S3_SECRET_KEY
  },
  forcePathStyle: true
};

const CATEGORIES = {
  S3_PROMOTED: { ttlDays: Infinity, minTTL: Infinity, label: 'Promoted ML Jobs (S3)' },
  S3_REJECTED: { ttlDays: 30, minTTL: 7, label: 'Rejected ML Jobs (S3)' },
  S3_FAILED:   { ttlDays: 7, minTTL: 7, label: 'Failed ML Jobs (S3)' },
  S3_STALE_LEASE: { ttlDays: 1, minTTL: 0, label: 'Stale Leases (S3)' },
  LOCAL_CRON:  { ttlDays: 30, minTTL: 1, label: 'Local Cron Logs' },
  LOCAL_ALERTS: { ttlDays: 90, minTTL: 1, label: 'Local Alerts' }
};

function parseArgs(args) {
  const result = { dryRun: true, olderThanDays: null, verbose: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--apply') result.dryRun = false;
    if (args[i] === '--verbose') result.verbose = true;
    if (args[i] === '--older-than-days') result.olderThanDays = parseInt(args[++i], 10);
  }
  return result;
}

const GLOBAL_ARGS = parseArgs(process.argv.slice(2));

async function getJobStatusMap() {
  const map = new Map();
  if (!fs.existsSync(AUDIT_DIR)) return map;
  const files = fs.readdirSync(AUDIT_DIR).filter(f => f.endsWith('.json'));
  for (const file of files) {
    try {
      const content = fs.readFileSync(path.join(AUDIT_DIR, file), 'utf8');
      content.trim().split('\n').filter(l => l.length > 0).forEach(line => {
        try {
          const entry = JSON.parse(line);
          if (entry.job_id) {
            map.set(entry.job_id, {
              status: entry.training_status,
              promoted: entry.training_status === 'SUCCESS' && entry.hash_changed === true,
              timestamp: new Date(entry.timestamp).getTime()
            });
          }
        } catch (e) {}
      });
    } catch (e) {}
  }
  return map;
}

async function scanS3(s3Client, bucket, prefix) {
  let contents = [];
  let token = null;
  let pages = 0;
  try {
    do {
      pages++;
      const res = await s3Client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token }));
      if (res.Contents) contents.push(...res.Contents);
      token = res.IsTruncated ? res.NextContinuationToken : null;
    } while (token);
    console.log(`[S3] Listed ${prefix}: objects=${contents.length} pages=${pages}`);
    return contents.sort((a, b) => a.Key.localeCompare(b.Key));
  } catch (e) {
    console.error(`[S3] Failed to scan ${bucket}/${prefix}: ${e.message}`);
    return [];
  }
}

function isOlderThan(lastModified, days) {
  const diff = Date.now() - new Date(lastModified).getTime();
  return diff > (days * 24 * 60 * 60 * 1000);
}

function isWithinLast24h(timestamp) {
  return (Date.now() - timestamp) < (24 * 60 * 60 * 1000);
}

async function main() {
  const s3Artifacts = new S3Client(S3_OPTS);
  console.log(`[Cleanup] Mode: ${GLOBAL_ARGS.dryRun ? 'DRY-RUN' : 'APPLY'}`);
  if (!fs.existsSync(PLAN_DIR)) fs.mkdirSync(PLAN_DIR);

  const jobMap = await getJobStatusMap();
  const candidates = [];
  const now = new Date();

  // 1. S3 SCAN: ml-artifacts/
  const artifactEntries = await scanS3(s3Artifacts, ARTIFACT_BUCKET, 'ml-artifacts/');
  const jobDirs = new Map();
  for (const item of artifactEntries) {
    if (!item.Key.startsWith('ml-artifacts/')) { console.error(`[BLOCK] S3 Key violation: ${item.Key}`); continue; }
    const jobId = item.Key.split('/')[1];
    if (!jobId) continue;
    if (!jobDirs.has(jobId)) jobDirs.set(jobId, { keys: [], lastModified: item.LastModified });
    jobDirs.get(jobId).keys.push(item.Key);
    if (item.LastModified > jobDirs.get(jobId).lastModified) jobDirs.get(jobId).lastModified = item.LastModified;
  }

  for (const [jobId, info] of jobDirs) {
    const status = jobMap.get(jobId);
    if (status && isWithinLast24h(status.timestamp)) continue;
    if (!status && !isOlderThan(info.lastModified, 1)) continue;

    let category = status ? (status.promoted ? 'S3_PROMOTED' : (status.status === 'SUCCESS' ? 'S3_REJECTED' : 'S3_FAILED')) : 'S3_FAILED';
    const config = CATEGORIES[category];
    if (!config || config.ttlDays === Infinity) continue;
    const ttl = GLOBAL_ARGS.olderThanDays !== null ? Math.max(GLOBAL_ARGS.olderThanDays, config.minTTL) : config.ttlDays;
    if (isOlderThan(info.lastModified, ttl)) {
      candidates.push({ type: 'S3', category, label: config.label, keys: info.keys, bucket: ARTIFACT_BUCKET, job_id: jobId });
    }
  }

  // 2. S3 SCAN: ml-leases/
  const leaseEntries = await scanS3(s3Artifacts, ARTIFACT_BUCKET, 'ml-leases/');
  for (const lease of leaseEntries) {
    if (!lease.Key.startsWith('ml-leases/')) { console.error(`[BLOCK] S3 Key violation: ${lease.Key}`); continue; }
    const ttl = GLOBAL_ARGS.olderThanDays !== null ? Math.max(GLOBAL_ARGS.olderThanDays, 0) : CATEGORIES.S3_STALE_LEASE.ttlDays;
    if (isOlderThan(lease.LastModified, ttl)) {
      candidates.push({ type: 'S3_KEY', category: 'S3_STALE_LEASE', label: CATEGORIES.S3_STALE_LEASE.label, keys: [lease.Key], bucket: ARTIFACT_BUCKET });
    }
  }

  // 3. LOCAL SCAN: logs/ (path traversal hardening)
  const resolvedLogsDir = path.resolve(LOGS_DIR);
  const base = resolvedLogsDir.endsWith(path.sep) ? resolvedLogsDir : resolvedLogsDir + path.sep;
  
  if (fs.existsSync(LOGS_DIR)) {
    for (const file of fs.readdirSync(LOGS_DIR)) {
      const fullPath = path.join(LOGS_DIR, file);
      const resolvedPath = path.resolve(fullPath);
      
      if (!resolvedPath.startsWith(base)) {
        console.error(`[BLOCK] Local path violation: ${fullPath} (resolved: ${resolvedPath})`);
        continue;
      }
      
      const stats = fs.statSync(fullPath);
      let cat = file.includes('cron_daily.log') ? 'LOCAL_CRON' : (file.includes('alerts.jsonl') ? 'LOCAL_ALERTS' : null);
      if (cat) {
        const config = CATEGORIES[cat];
        const ttl = GLOBAL_ARGS.olderThanDays !== null ? Math.max(GLOBAL_ARGS.olderThanDays, config.minTTL) : config.ttlDays;
        if (isOlderThan(stats.mtime, ttl)) candidates.push({ type: 'LOCAL', category: cat, label: config.label, path: fullPath });
      }
    }
  }

  if (candidates.length === 0) { console.log('[Cleanup] No candidates found.'); process.exit(0); }
  const summary = new Map();
  candidates.forEach(c => summary.set(c.label, (summary.get(c.label) || 0) + (c.keys ? c.keys.length : 1)));
  console.log('\nDELETION PLAN:\n' + '-'.repeat(40));
  for (const [lbl, count] of summary) console.log(`${lbl.padEnd(25)}: ${count} item(s)`);
  console.log('-'.repeat(40));

  fs.writeFileSync(path.join(PLAN_DIR, `plan_${now.toISOString().replace(/:/g, '-')}.json`), JSON.stringify({ timestamp: now.toISOString(), mode: GLOBAL_ARGS.dryRun ? 'DRY-RUN' : 'APPLY', candidates }, null, 2));
  if (GLOBAL_ARGS.dryRun) { console.log('[Cleanup] DRY RUN: No actions taken.'); process.exit(0); }

  let hasError = false;
  console.log('[Cleanup] EXECUTING DELETIONS...');
  for (const c of candidates) {
    try {
      if (c.type === 'S3' || c.type === 'S3_KEY') {
        for (let i = 0; i < c.keys.length; i += 1000) await s3Artifacts.send(new DeleteObjectsCommand({ Bucket: c.bucket, Delete: { Objects: c.keys.slice(i, i + 1000).map(k => ({ Key: k })) } }));
      } else if (c.type === 'LOCAL') fs.unlinkSync(c.path);
    } catch (e) { console.error(`[ERROR] Failed to delete: ${e.message}`); hasError = true; }
  }
  process.exit(hasError ? 1 : 0);
}

main().catch(err => {
  console.error('[Fatal Error] ' + err.message);
  process.exit(GLOBAL_ARGS.dryRun ? 0 : 1);
});
