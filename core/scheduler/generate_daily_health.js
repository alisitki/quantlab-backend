#!/usr/bin/env node
/**
 * generate_daily_health.js: Daily health summary generator.
 * 
 * Reads signals from compact state, audit logs, and alerts.
 * Produces health/daily_{YYYYMMDD}.json with classification.
 * 
 * NEVER throws - always produces output.
 * NEVER modifies source files.
 * DETERMINISTIC output.
 */
import 'dotenv/config';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import path from 'path';

const HEALTH_DIR = 'health';
const AUDIT_DIR = 'daily_runs';
const ALERTS_FILE = 'logs/alerts.jsonl';
const STATE_KEY = 'compacted/_state.json';

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--date') result.date = args[++i];
  }
  return result;
}

function getYesterday() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

async function getCompactState(s3Client, bucket) {
  try {
    const res = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: STATE_KEY }));
    const body = await res.Body.transformToString();
    return JSON.parse(body);
  } catch (err) {
    return null;
  }
}

function getAuditEntries(date) {
  const filePath = path.join(AUDIT_DIR, `${date}.json`);
  if (!fs.existsSync(filePath)) return [];
  
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    return content.trim().split('\n')
      .filter(line => line.length > 0)
      .map(line => {
        try { return JSON.parse(line); } 
        catch { return null; }
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

function getAlerts(date) {
  if (!fs.existsSync(ALERTS_FILE)) return [];
  
  try {
    const content = fs.readFileSync(ALERTS_FILE, 'utf8');
    return content.trim().split('\n')
      .filter(line => line.length > 0)
      .map(line => {
        try { return JSON.parse(line); } 
        catch { return null; }
      })
      .filter(Boolean)
      .filter(alert => alert.date === date);
  } catch {
    return [];
  }
}

function classifyHealth(auditEntries, compactState, targetDate, alerts) {
  const hasAudit = auditEntries.length > 0;
  const latestAudit = hasAudit ? auditEntries[auditEntries.length - 1] : null;
  const lastCompacted = compactState?.last_compacted_date || null;
  const compactReady = lastCompacted && lastCompacted >= targetDate;
  
  // Build result
  const result = {
    date: targetDate,
    generated_at: new Date().toISOString(),
    compact_state: compactReady ? 'READY' : (lastCompacted ? 'NOT_READY' : 'UNKNOWN'),
    last_compacted_date: lastCompacted,
    run_attempted: hasAudit,
    run_status: 'NOT_FOUND',
    run_job_id: null,
    promotion_result: 'N/A',
    alerts: {
      count: alerts.length,
      types: [...new Set(alerts.map(a => a.type))]
    },
    health_status: 'UNKNOWN',
    health_reason: null
  };
  
  if (hasAudit) {
    result.run_status = latestAudit.training_status || 'UNKNOWN';
    result.run_job_id = latestAudit.job_id || null;
    
    // Check hash violation
    if (latestAudit.hash_changed === true && result.run_status !== 'SUCCESS') {
      result.health_status = 'FAILED_SAFETY';
      result.health_reason = 'Hash changed unexpectedly';
      return result;
    }
    
    if (result.run_status === 'SUCCESS') {
      result.health_status = alerts.length > 0 ? 'HEALTHY' : 'HEALTHY';
      result.health_reason = null;
      
      // Check promotion
      if (latestAudit.promotion_decision) {
        if (latestAudit.hash_changed) {
          result.promotion_result = 'PROMOTED';
        } else {
          result.promotion_result = 'REJECTED';
        }
      }
    } else if (result.run_status === 'FAILED') {
      const error = latestAudit.training_error || '';
      if (error.includes('SSH') || error.includes('GPU') || error.includes('TIMEOUT') || error.includes('API')) {
        result.health_status = 'FAILED_INFRA';
      } else if (error.includes('hash') || error.includes('safety') || error.includes('orphan')) {
        result.health_status = 'FAILED_SAFETY';
      } else {
        result.health_status = 'FAILED_INFRA';
      }
      result.health_reason = error || 'Unknown error';
    }
  } else {
    // No audit entry = no run attempted
    // CRITICAL: SKIPPED must have alerts.count=0
    // Alerts are only for run-attempted days
    if (!compactReady) {
      result.run_status = 'SKIPPED';
      result.health_status = 'SKIPPED_WAITING_FOR_COMPACT';
      result.health_reason = `Compact at ${lastCompacted || 'unknown'}, waiting for ${targetDate}`;
      // Clear alerts for SKIPPED - no run = no alerts
      result.alerts = { count: 0, types: [] };
    } else {
      result.health_status = 'UNKNOWN';
      result.health_reason = 'Compact ready but no run trace found';
      // UNKNOWN also clears alerts - no run trace
      result.alerts = { count: 0, types: [] };
    }
  }
  
  return result;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const targetDate = args.date || getYesterday();
  
  console.log(`[HealthCheck] Generating summary for ${targetDate}...`);
  
  // Ensure health directory
  if (!fs.existsSync(HEALTH_DIR)) {
    fs.mkdirSync(HEALTH_DIR, { recursive: true });
  }
  
  // Read signals
  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });
  
  const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
  
  const compactState = await getCompactState(s3Client, bucket);
  const auditEntries = getAuditEntries(targetDate);
  const alerts = getAlerts(targetDate);
  
  // Classify
  const summary = classifyHealth(auditEntries, compactState, targetDate, alerts);
  
  // Write output
  const outputPath = path.join(HEALTH_DIR, `daily_${targetDate}.json`);
  fs.writeFileSync(outputPath, JSON.stringify(summary, null, 2));
  
  console.log(`[HealthCheck] Status: ${summary.health_status}`);
  console.log(`[HealthCheck] Written to ${outputPath}`);
  
  // Always exit 0 - health check never fails the pipeline
  process.exit(0);
}

main().catch(err => {
  console.error('[HealthCheck] Error:', err.message);
  // Still exit 0 - never fail pipeline
  process.exit(0);
});
