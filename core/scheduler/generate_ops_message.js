#!/usr/bin/env node
/**
 * generate_ops_message.js: Aggregates Health, Digest, Alerts, and Retention data 
 * into a single deterministic JSON payload for external notifications.
 */
import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';

const HEALTH_DIR = 'health';
const OPS_DIR = 'ops';
const MESSAGE_DIR = path.join(OPS_DIR, 'messages');
const CLEANUP_DIR = 'cleanup';
const LOGS_DIR = 'logs';
const ALERTS_LOG = path.join(LOGS_DIR, 'alerts.jsonl');

/**
 * Returns YYYYMMDD for a Date object
 */
function formatDate(date) {
  return date.toISOString().split('T')[0].replace(/-/g, '');
}

/**
 * Parses CLI arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const result = { date: null, last: null, yesterday: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--date') result.date = args[++i];
    else if (args[i] === '--last') result.last = parseInt(args[++i], 10);
    else if (args[i] === '--yesterday') result.yesterday = true;
  }
  return result;
}

/**
 * Safely reads and parses JSON
 */
function safeReadJSON(filePath, errors) {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (e) {
    errors.push(`Failed to parse ${filePath}: ${e.message}`);
    return null;
  }
}

/**
 * Finds the latest retention plan within the last 36 hours (Deterministic)
 */
function findLatestRetentionPlan(errors) {
  if (!fs.existsSync(CLEANUP_DIR)) return null;
  const now = Date.now();
  const windowMs = 36 * 60 * 60 * 1000;
  
  const plans = [];
  try {
    const files = fs.readdirSync(CLEANUP_DIR).filter(f => f.startsWith('plan_') && f.endsWith('.json'));
    for (const file of files) {
      const fullPath = path.join(CLEANUP_DIR, file);
      const plan = safeReadJSON(fullPath, errors); // Plan contract: record errors
      if (plan && plan.timestamp) {
        const ts = new Date(plan.timestamp).getTime();
        if (ts <= now && ts >= now - windowMs) {
          plans.push({ plan, ts, filename: file });
        }
      }
    }
  } catch (e) {
    errors.push(`Error reading retention plans: ${e.message}`);
  }

  if (plans.length === 0) return null;

  // Deterministic sort: ts DESC, filename DESC
  plans.sort((a, b) => {
    if (b.ts !== a.ts) return b.ts - a.ts;
    return b.filename.localeCompare(a.filename);
  });

  return plans[0].plan;
}

/**
 * Filters alerts by date and returns unique types and IDs
 */
function getAlertSummary(date, errors) {
  const summary = { count: 0, types: [], alert_ids: [] };
  if (!fs.existsSync(ALERTS_LOG)) return summary;
  
  try {
    const content = fs.readFileSync(ALERTS_LOG, 'utf8').trim();
    if (!content) return summary;

    const types = new Set();
    const alertIds = new Set();
    content.split('\n').forEach(line => {
      if (!line.trim()) return;
      try {
        const entry = JSON.parse(line);
        if (entry.date === date) {
          summary.count++;
          if (entry.type) types.add(entry.type);
          if (entry.alert_id) alertIds.add(entry.alert_id);
        }
      } catch (e) {}
    });
    summary.types = Array.from(types).sort();
    summary.alert_ids = Array.from(alertIds).sort();
  } catch (e) {
    errors.push(`Error reading alerts log: ${e.message}`);
  }
  return summary;
}

/**
 * Processes a single date
 */
function processDate(dateString) {
  const errors = [];
  const generatedAt = new Date().toISOString();
  
  // 1. Read Health
  const healthFile = path.join(HEALTH_DIR, `daily_${dateString}.json`);
  const health = safeReadJSON(healthFile, errors);
  if (!health) errors.push(`Health file missing for ${dateString}`);
  
  // 2. Read Digest
  const digestFile = path.join(OPS_DIR, `digest_${dateString}.json`);
  const digest = safeReadJSON(digestFile, errors);
  if (!digest) errors.push(`Ops digest missing for ${dateString}`);
  
  // 3. Alerts
  const alertSummary = getAlertSummary(dateString, errors);
  
  // 4. Retention (uses global latest plan within 36h)
  const retentionPlan = findLatestRetentionPlan(errors);
  const retentionSummary = {
    plan_found: !!retentionPlan,
    plan_timestamp: retentionPlan ? retentionPlan.timestamp : null,
    candidates_total: 0,
    by_label: {}
  };
  
  if (retentionPlan && retentionPlan.candidates) {
    retentionPlan.candidates.forEach(c => {
      const count = (c.keys && c.keys.length) || 1;
      retentionSummary.candidates_total += count;
      retentionSummary.by_label[c.label] = (retentionSummary.by_label[c.label] || 0) + count;
    });
  }

  // 5. Derive Compact State
  let compactState = 'UNKNOWN';
  if (health) {
    if (health.compact_state) compactState = health.compact_state;
    else if (health.compact_ready === true) compactState = 'READY';
    else if (health.compact_ready === false) compactState = 'NOT_READY';
  }

  // 6. Build Payload
  const payload = {
    date: dateString,
    generated_at: generatedAt,
    status: {
      health_status: health ? health.health_status : 'UNKNOWN',
      run_status: health ? health.run_status : 'UNKNOWN',
      promotion_result: health ? health.promotion_result : 'UNKNOWN',
      compact_state: compactState
    },
    alerts: alertSummary,
    retention: retentionSummary,
    summary_lines: [],
    errors: errors
  };

  // 7. Compose Summary Lines
  const statusEmoji = payload.status.health_status === 'HEALTHY' ? '✅' : (payload.status.health_status === 'SKIPPED' ? '⚪' : '❌');
  const digestMsg = digest ? digest.summary : `${statusEmoji} ${payload.status.health_status}: Summary unavailable`;
  payload.summary_lines.push(digestMsg);
  
  if (alertSummary.count > 0) {
    const typeStr = alertSummary.types.length > 0 ? ` (${alertSummary.types.join(', ')})` : '';
    payload.summary_lines.push(`⚠️ ${alertSummary.count} alerts detected${typeStr}`);
  } else {
    payload.summary_lines.push('🔔 No alerts detected');
  }
  
  if (retentionSummary.plan_found) {
    payload.summary_lines.push(`🧹 Retention DRY-RUN: ${retentionSummary.candidates_total} candidates (plan @ ${retentionSummary.plan_timestamp})`);
  } else {
    payload.summary_lines.push('🧹 Retention: No dry-run in last 36h');
  }

  // Drift Check (72h)
  const driftWindowMs = 72 * 60 * 60 * 1000;
  // We check if the latest plan is older than 72h
  // note: findLatestRetentionPlan(36h) might return null, but plan might still exist in 72h.
  // We need a separate wider check for drift alert.
  let latestPlanTs = 0;
  if (fs.existsSync(CLEANUP_DIR)) {
    try {
      const files = fs.readdirSync(CLEANUP_DIR).filter(f => f.startsWith('plan_') && f.endsWith('.json'));
      files.forEach(f => {
        const p = safeReadJSON(path.join(CLEANUP_DIR, f), []);
        if (p && p.timestamp) {
          const t = new Date(p.timestamp).getTime();
          if (t > latestPlanTs) latestPlanTs = t;
        }
      });
    } catch (e) {}
  }

  if (latestPlanTs === 0 || (Date.now() - latestPlanTs) > driftWindowMs) {
    console.error(`[Drift] Retention drift detected! Latest plan: ${latestPlanTs ? new Date(latestPlanTs).toISOString() : 'NEVER'}`);
    try {
      execSync(`node scheduler/alert_hook.js --type RETENTION_DRIFT_DETECTED --date ${dateString}`, { stdio: 'inherit' });
    } catch (e) {
      console.error(`[Drift] Failed to trigger alert: ${e.message}`);
    }
  }

  // 8. Write Output
  if (!fs.existsSync(MESSAGE_DIR)) fs.mkdirSync(MESSAGE_DIR, { recursive: true });
  const outputFile = path.join(MESSAGE_DIR, `message_${dateString}.json`);
  fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2));

  // 9. STDOUT Preview
  console.log(`\n--- [${dateString}] Daily Ops Message Preview ---`);
  payload.summary_lines.forEach(line => console.log(line));
}

/**
 * Main entry point
 */
function main() {
  const opts = parseArgs();
  let dates = [];

  // Yesterday UTC as fallback base
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const yesterdayStr = formatDate(yesterday);

  if (opts.date) {
    // 1. Explicit --date
    dates = [opts.date];
  } else if (opts.last && opts.last > 0) {
    // 2. --last N (precedence over --yesterday)
    const baseDate = new Date();
    baseDate.setUTCDate(baseDate.getUTCDate() - 1); // Start from yesterday
    for (let i = 0; i < opts.last; i++) {
      const d = new Date(baseDate.getTime());
      d.setUTCDate(d.getUTCDate() - i);
      dates.push(formatDate(d));
    }
    dates.reverse(); // ASC order: [yesterday-(N-1), ..., yesterday]
  } else {
    // 3. Default (yesterday)
    dates = [yesterdayStr];
  }

  dates.forEach(date => processDate(date));
}

main();
