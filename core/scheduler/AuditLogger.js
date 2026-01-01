/**
 * AuditLogger: Append-only JSON log for daily ML runs.
 * 
 * Path pattern: daily_runs/{YYYYMMDD}.json
 * Format: JSONL (one JSON object per line)
 * 
 * Guarantees:
 *   - Append-only (no overwrites, no deletions)
 *   - Safe for concurrent writers (line-based)
 *   - All entries timestamped
 */
import fs from 'fs';
import path from 'path';

const AUDIT_DIR = 'daily_runs';

/**
 * @typedef {Object} AuditEntry
 * @property {string} timestamp - ISO-8601 timestamp
 * @property {string} symbol - Trading symbol
 * @property {string} date - Training date (YYYYMMDD)
 * @property {'DRY'|'LIVE'} mode - Run mode
 * @property {string} job_id - Job identifier
 * @property {'SUCCESS'|'FAILED'} training_status - Training outcome
 * @property {string|null} training_error - Error message if failed
 * @property {Object|null} promotion_decision - PromotionGuard decision
 * @property {string} prod_hash_before - Production hash before run
 * @property {string} prod_hash_after - Production hash after run
 * @property {boolean} hash_changed - Whether hash changed
 */

/**
 * Ensure audit directory exists.
 */
function ensureAuditDir() {
  if (!fs.existsSync(AUDIT_DIR)) {
    fs.mkdirSync(AUDIT_DIR, { recursive: true });
  }
}

/**
 * Get audit file path for a date.
 * @param {string} date - Date in YYYYMMDD format
 * @returns {string} File path
 */
function getAuditFilePath(date) {
  return path.join(AUDIT_DIR, `${date}.json`);
}

/**
 * Append an entry to the audit log.
 * Uses JSONL format - one JSON object per line.
 * 
 * @param {string} date - Date in YYYYMMDD format
 * @param {Partial<AuditEntry>} entry - Audit entry data
 */
export function appendEntry(date, entry) {
  ensureAuditDir();
  
  const filePath = getAuditFilePath(date);
  
  const fullEntry = {
    timestamp: new Date().toISOString(),
    symbol: null,
    date: date,
    mode: null,
    job_id: null,
    training_status: null,
    training_error: null,
    promotion_decision: null,
    prod_hash_before: null,
    prod_hash_after: null,
    hash_changed: null,
    ...entry
  };
  
  // Append as JSONL (newline-delimited JSON)
  const line = JSON.stringify(fullEntry) + '\n';
  fs.appendFileSync(filePath, line, 'utf8');
  
  console.log(`[AuditLogger] Appended entry to ${filePath}`);
}

/**
 * Read all entries for a date.
 * @param {string} date - Date in YYYYMMDD format
 * @returns {AuditEntry[]} Array of audit entries
 */
export function readEntries(date) {
  const filePath = getAuditFilePath(date);
  
  if (!fs.existsSync(filePath)) {
    return [];
  }
  
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.trim().split('\n').filter(l => l.length > 0);
  
  return lines.map(line => {
    try {
      return JSON.parse(line);
    } catch (e) {
      console.warn(`[AuditLogger] Failed to parse line: ${line.substring(0, 50)}...`);
      return null;
    }
  }).filter(Boolean);
}

/**
 * Check if an audit file exists for a date.
 * @param {string} date - Date in YYYYMMDD format
 * @returns {boolean}
 */
export function hasEntries(date) {
  const filePath = getAuditFilePath(date);
  return fs.existsSync(filePath);
}

/**
 * Get count of entries for a date.
 * @param {string} date - Date in YYYYMMDD format
 * @returns {number}
 */
export function getEntryCount(date) {
  return readEntries(date).length;
}

// Export for testing
export const AUDIT_DIRECTORY = AUDIT_DIR;
