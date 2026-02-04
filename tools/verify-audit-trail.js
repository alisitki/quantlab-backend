#!/usr/bin/env node
/**
 * Verify audit trail append-only records
 */

import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';

process.env.AUDIT_SPOOL_DIR = '/tmp/quantlab-audit-test';
process.env.RUN_ARCHIVE_ENABLED = '0';

function dateKey() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

async function main() {
  const { emitAudit } = await import('../core/audit/AuditWriter.js');
  const runId = 'run_audit_test';

  await emitAudit({
    actor: 'system',
    action: 'RUN_START',
    target_type: 'run',
    target_id: runId,
    reason: null,
    metadata: { live_run_id: runId }
  });

  await emitAudit({
    actor: 'system',
    action: 'GUARD_FAIL',
    target_type: 'run',
    target_id: runId,
    reason: 'test_guard',
    metadata: { live_run_id: runId }
  });

  await emitAudit({
    actor: 'system',
    action: 'RUN_STOP',
    target_type: 'run',
    target_id: runId,
    reason: 'MANUAL_STOP',
    metadata: { live_run_id: runId }
  });

  const dir = join('/tmp/quantlab-audit-test', `date=${dateKey()}`);
  const files = await readdir(dir);
  let count = 0;
  for (const f of files) {
    const raw = await readFile(join(dir, f), 'utf-8');
    count += raw.trim().split('\n').filter(Boolean).length;
  }

  if (count < 3) {
    console.error('FAIL: expected 3 audit records');
    process.exit(1);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message || String(err));
  process.exit(1);
});
