#!/usr/bin/env node
/**
 * promote.js — Apply promotion gate to leaderboard rows.
 *
 * Usage:
 *   node promote.js --exp_id <exp_id> [--dry_run true]
 */

import { readFile, appendFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const REGISTRY_DIR = path.resolve('./services/strategyd/registry');
const EXP_DIR = path.resolve('./services/strategyd/experiments');
const CAND_JSONL = path.join(REGISTRY_DIR, 'candidates.jsonl');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

async function readJson(filePath) {
  const raw = await readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function readJsonl(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
  } catch {
    return [];
  }
}

function candidateId(expId, paramsHash, tickFingerprint) {
  return createHash('sha256').update(`${expId}:${paramsHash}:${tickFingerprint}`).digest('hex');
}

function gateCheck(row, validationReport) {
  const reasons = [];
  if (row.validation_status !== 'passed') reasons.push('validation_status');
  if (!row.tick?.pnl_pct) reasons.push('tick_pnl_pct_missing');
  if (row.snapshot?.max_dd === undefined || row.snapshot.max_dd < -0.15) reasons.push('max_dd');
  if (row.snapshot?.trades === undefined || row.snapshot.trades < 20) reasons.push('trades');
  const pnlDiff = row.pnl_diff ?? validationReport?.deltas?.pnl_diff;
  if (pnlDiff === undefined || Math.abs(pnlDiff) > 0.01) reasons.push('pnl_diff');
  return { pass: reasons.length === 0, reasons };
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.exp_id) {
    console.error('Usage: node promote.js --exp_id <exp_id> [--dry_run true]');
    process.exit(1);
  }
  const expId = args.exp_id;
  const dryRun = args.dry_run === 'true';

  await mkdir(REGISTRY_DIR, { recursive: true });

  const leaderboardPath = path.join(EXP_DIR, expId, 'leaderboard.json');
  const leaderboard = await readJson(leaderboardPath);
  const rows = leaderboard.rows || [];

  const existingCandidates = await readJsonl(CAND_JSONL);
  const existingIds = new Set(existingCandidates.map(c => c.candidate_id));

  let candidates_added = 0;
  let rejected_count = 0;
  const rejectReasons = {};

  for (const row of rows) {
    const validationReport = row.validation_report_path
      ? await readJson(row.validation_report_path)
      : null;
    const tickFingerprint = validationReport?.tick?.input_fingerprint;

    const gate = gateCheck(row, validationReport);
    if (!gate.pass) {
      rejected_count++;
      gate.reasons.forEach(r => { rejectReasons[r] = (rejectReasons[r] || 0) + 1; });
      continue;
    }
    if (!tickFingerprint) {
      rejected_count++;
      rejectReasons.tick_fingerprint_missing = (rejectReasons.tick_fingerprint_missing || 0) + 1;
      continue;
    }

    const candidate_id = candidateId(expId, row.params_hash, tickFingerprint);
    if (existingIds.has(candidate_id)) {
      continue;
    }

    const snapshotReport = await readJson(row.report_path);
    const candidate = {
      candidate_id,
      exp_id: expId,
      strategy_id: snapshotReport.strategy_id,
      strategy_version: snapshotReport.strategy_version,
      params_hash: row.params_hash,
      params_short: row.params_short,
      link: {
        report_path: row.report_path,
        validation_report_path: row.validation_report_path
      },
      scores: {
        tick_pnl_pct: row.tick?.pnl_pct,
        max_dd: row.snapshot?.max_dd,
        trades: row.snapshot?.trades
      },
      tick_fingerprint: tickFingerprint
    };

    if (!dryRun) {
      await appendFile(CAND_JSONL, JSON.stringify(candidate) + '\n');
      existingIds.add(candidate_id);
    }
    candidates_added++;
  }

  console.log(JSON.stringify({
    exp_id: expId,
    candidates_added,
    rejected_count,
    reject_reasons: rejectReasons,
    dry_run: dryRun
  }));
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
