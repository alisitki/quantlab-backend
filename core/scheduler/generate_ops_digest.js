#!/usr/bin/env node
/**
 * generate_ops_digest.js: Daily ops digest generator.
 * 
 * Reads health summary and produces human-readable digest.
 * Outputs: ops/digest_{date}.json and ops/digest_{date}.txt
 * 
 * NEVER throws - always produces output.
 * DETERMINISTIC output.
 */
import fs from 'fs';
import path from 'path';

const HEALTH_DIR = 'health';
const OPS_DIR = 'ops';

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

function formatDate(yyyymmdd) {
  return `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;
}

function generateSummaryMessage(health) {
  const status = health.health_status;
  const reason = health.health_reason || '';
  const promo = health.promotion_result || 'N/A';
  
  switch (status) {
    case 'HEALTHY':
      return `✅ ML training completed successfully. Promotion: ${promo}`;
    case 'SKIPPED_WAITING_FOR_COMPACT':
      return `⏭️ Skipped: waiting for compact data`;
    case 'FAILED_INFRA':
      return `❌ Failed: infrastructure error (${reason || 'unknown'})`;
    case 'FAILED_SAFETY':
      return `🚨 Failed: safety violation (${reason})`;
    case 'UNKNOWN':
      return `⚠️ Unknown: no run trace found`;
    default:
      return `❓ Status: ${status}`;
  }
}

function generateTxtDigest(digest) {
  const lines = [
    '══════════════════════════════════════════════════════════════',
    `QUANTLAB DAILY OPS DIGEST — ${formatDate(digest.date)}`,
    '══════════════════════════════════════════════════════════════',
    `Status:     ${digest.health_status}`,
    `Compact:    ${digest.compact_ready ? 'READY' : 'NOT_READY'}`,
    `Run:        ${digest.run_status}`,
    `Job ID:     ${digest.job_id || 'N/A'}`,
    `Promotion:  ${digest.promotion_result}`,
    `Alerts:     ${digest.alert_count}`,
    '',
    digest.summary,
    '══════════════════════════════════════════════════════════════'
  ];
  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const targetDate = args.date || getYesterday();
  
  console.log(`[OpsDigest] Generating digest for ${targetDate}...`);
  
  // Ensure ops directory
  if (!fs.existsSync(OPS_DIR)) {
    fs.mkdirSync(OPS_DIR, { recursive: true });
  }
  
  // Read health summary
  const healthPath = path.join(HEALTH_DIR, `daily_${targetDate}.json`);
  let health;
  
  if (!fs.existsSync(healthPath)) {
    console.log(`[OpsDigest] Health file not found: ${healthPath}`);
    health = {
      date: targetDate,
      health_status: 'UNKNOWN',
      compact_state: 'UNKNOWN',
      run_attempted: false,
      run_status: 'NOT_FOUND',
      run_job_id: null,
      promotion_result: 'N/A',
      alerts: { count: 0, types: [] },
      health_reason: 'Health summary not generated'
    };
  } else {
    try {
      health = JSON.parse(fs.readFileSync(healthPath, 'utf8'));
    } catch (err) {
      console.error(`[OpsDigest] Failed to parse health file: ${err.message}`);
      health = {
        date: targetDate,
        health_status: 'UNKNOWN',
        health_reason: 'Failed to parse health summary'
      };
    }
  }
  
  // Build digest
  const digest = {
    date: targetDate,
    generated_at: new Date().toISOString(),
    health_status: health.health_status,
    compact_ready: health.compact_state === 'READY',
    run_attempted: health.run_attempted || false,
    run_status: health.run_status || 'UNKNOWN',
    job_id: health.run_job_id || null,
    promotion_result: health.promotion_result || 'N/A',
    alert_count: health.alerts?.count || 0,
    summary: generateSummaryMessage(health)
  };
  
  // Write JSON
  const jsonPath = path.join(OPS_DIR, `digest_${targetDate}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(digest, null, 2));
  
  // Write TXT
  const txtPath = path.join(OPS_DIR, `digest_${targetDate}.txt`);
  fs.writeFileSync(txtPath, generateTxtDigest(digest));
  
  console.log(`[OpsDigest] JSON: ${jsonPath}`);
  console.log(`[OpsDigest] TXT:  ${txtPath}`);
  console.log(`[OpsDigest] ${digest.summary}`);
  
  // Always exit 0
  process.exit(0);
}

main().catch(err => {
  console.error('[OpsDigest] Error:', err.message);
  process.exit(0);
});
