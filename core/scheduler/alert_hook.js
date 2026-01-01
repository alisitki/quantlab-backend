import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const ALERT_LOG = 'logs/alerts.jsonl';

/**
 * @typedef {Object} AlertPayload
 * @property {string} alert_id
 * @property {'CRON_FAILURE'|'ORPHAN_DETECTED'|'HASH_VIOLATION'|'AUDIT_MISSING'|'STATE_MISSING'|'RETENTION_RUNNER_FAILURE'|'OPS_DEQUEUE_FAILURE'|'RETENTION_DRIFT_DETECTED'} type
 * @property {string} timestamp
 * @property {string} symbol
 * @property {string} date
 * @property {number|null} exit_code
 * @property {string} message
 * @property {Object} context
 */

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--type') result.type = args[++i];
    if (args[i] === '--exit-code') result.exitCode = parseInt(args[++i], 10);
    if (args[i] === '--symbol') result.symbol = args[++i];
    if (args[i] === '--message') result.message = args[++i];
    if (args[i] === '--date') result.date = args[++i];
    if (args[i] === '--log-path') result.logPath = args[++i];
    if (args[i] === '--job-id') result.jobId = args[++i];
  }
  return result;
}

function getYesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

function buildMessage(type, exitCode) {
  switch (type) {
    case 'CRON_FAILURE': return `Cron job failed with exit code ${exitCode}`;
    case 'ORPHAN_DETECTED': return `Orphan processes detected`;
    case 'HASH_VIOLATION': return `Production binary hash violation detected`;
    case 'AUDIT_MISSING': return `Audit trail entry missing for daily run`;
    case 'STATE_MISSING': return `Expected state artifact missing`;
    case 'RETENTION_RUNNER_FAILURE': return `Retention cleanup runner failed`;
    case 'OPS_DEQUEUE_FAILURE': return `Operations outbox dequeue failed`;
    case 'RETENTION_DRIFT_DETECTED': return `Retention drift detected (process may have stopped)`;
    default: return `Generic alert: ${type}`;
  }
}

function sendAlert(payload) {
  // Ensure logs directory
  const logDir = path.dirname(ALERT_LOG);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  // Append to JSONL file
  const line = JSON.stringify(payload) + '\n';
  fs.appendFileSync(ALERT_LOG, line, 'utf8');
  
  // Print to stderr for visibility
  console.error('============================================================');
  console.error('[ALERT]', payload.type);
  console.error('Alert ID:', payload.alert_id);
  console.error('Message:', payload.message);
  console.error('Symbol:', payload.symbol);
  console.error('Date:', payload.date);
  console.error('Exit Code:', payload.exit_code);
  console.error('Timestamp:', payload.timestamp);
  console.error('============================================================');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  
  if (!args.type) {
    console.error('Usage: node scheduler/alert_hook.js --type TYPE [--exit-code N] [--symbol S] [--message M] [--date YYYYMMDD]');
    process.exit(1);
  }
  
  const timestamp = new Date().toISOString();
  const type = args.type;
  const message = args.message || buildMessage(args.type, args.exitCode);

  // Deterministic alert_id based on type, date (if any), and message
  // Note: we use date instead of timestamp for alert_id to allow deduplication if needed, 
  // but for now timestamp makes it unique per call.
  // Actually, for correlation, we want to find THIS alert in the logs.
  const alertId = crypto.createHash('sha256')
    .update(`${type}:${timestamp}:${message}`)
    .digest('hex')
    .substring(0, 16);

  /** @type {AlertPayload} */
  const payload = {
    alert_id: alertId,
    type: type,
    timestamp: timestamp,
    symbol: args.symbol || 'unknown',
    date: args.date || getYesterday(),
    exit_code: args.exitCode ?? null,
    message: message,
    context: {
      log_path: args.logPath ? path.resolve(args.logPath) : path.resolve('logs/cron_daily.log'),
      job_id: args.jobId || null
    }
  };
  
  sendAlert(payload);
  console.log(`[alert_hook] Alert logged to ${ALERT_LOG}`);
}

main();
