#!/usr/bin/env node
/**
 * eval-sweep.js — Parameter sweep runner with leaderboard output.
 *
 * Usage:
 *   node eval-sweep.js --spec /path/to/spec.json [--exp_id my_exp] [--concurrency 2]
 */

import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { run as runOrchestrator } from './EvalOrchestrator.js';

const BASE_DIR = path.resolve('./services/strategyd/experiments');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
  if (Array.isArray(obj)) return '[' + obj.map(stableStringify).join(',') + ']';
  const keys = Object.keys(obj).sort();
  return '{' + keys.map(k => `"${k}":${stableStringify(obj[k])}`).join(',') + '}';
}

function hashSpec(spec) {
  return createHash('sha256').update(stableStringify(spec)).digest('hex').slice(0, 12);
}

function gridKeys(grid) {
  return Object.keys(grid).sort();
}

function cartesian(grid) {
  const keys = gridKeys(grid);
  const combos = [];
  const walk = (idx, acc) => {
    if (idx === keys.length) {
      combos.push({ ...acc });
      return;
    }
    const key = keys[idx];
    const values = grid[key] || [];
    for (const v of values) {
      acc[key] = v;
      walk(idx + 1, acc);
    }
  };
  walk(0, {});
  return { keys, combos };
}

function mapParams(params) {
  const mapped = { ...params };
  if (mapped.ema_fast !== undefined) {
    mapped.fastPeriod = mapped.ema_fast;
  }
  if (mapped.ema_slow !== undefined) {
    mapped.slowPeriod = mapped.ema_slow;
  }
  return mapped;
}

function summarizeParams(params) {
  const keys = Object.keys(params).sort();
  return keys.map(k => `${k}=${params[k]}`).join(';');
}

function scoreRow(row) {
  return row.tick?.pnl_pct ?? row.snapshot?.pnl_pct ?? -Infinity;
}

function sortRows(rows) {
  return rows.sort((a, b) => {
    const scoreA = scoreRow(a);
    const scoreB = scoreRow(b);
    if (scoreA !== scoreB) return scoreB - scoreA;
    const ddA = a.snapshot?.max_dd ?? -Infinity;
    const ddB = b.snapshot?.max_dd ?? -Infinity;
    if (ddA !== ddB) return ddB - ddA;
    const tradesA = a.snapshot?.trades ?? 0;
    const tradesB = b.snapshot?.trades ?? 0;
    if (tradesA !== tradesB) return tradesB - tradesA;
    return a.params_hash.localeCompare(b.params_hash);
  });
}

async function readJson(filePath) {
  const raw = await readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function readJsonIfExists(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function progressEvery(total) {
  const byPercent = Math.max(1, Math.floor(total * 0.05));
  return Math.min(25, byPercent);
}

async function runWithConcurrency(items, concurrency, handler) {
  const results = [];
  let idx = 0;

  async function worker() {
    while (idx < items.length) {
      const current = idx++;
      results[current] = await handler(items[current], current);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.max(1, concurrency); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.spec) {
    console.error('Usage: node eval-sweep.js --spec <spec.json> [--exp_id id] [--concurrency N]');
    process.exit(1);
  }

  const spec = JSON.parse(await readFile(args.spec, 'utf8'));
  const exp_id = args.exp_id || hashSpec(spec);
  const concurrency = Number(args.concurrency || '1');

  const expDir = path.join(BASE_DIR, exp_id);
  await mkdir(expDir, { recursive: true });

  const leaderboardPath = path.join(expDir, 'leaderboard.json');
  const csvPath = path.join(expDir, 'leaderboard.csv');

  const existing = await readJsonIfExists(leaderboardPath);
  const existingRows = existing?.rows || [];
  const existingHashes = new Set(existingRows.map(r => r.params_hash));

  const { combos } = cartesian(spec.grid || {});
  const total_jobs = combos.length;
  const progressStep = progressEvery(total_jobs);
  let done_jobs = 0;
  let skipped_jobs = 0;
  let validated_jobs = 0;

  console.log(`[SWEEP] exp_id=${exp_id} total_jobs=${total_jobs} concurrency=${concurrency}`);

  const rows = [...existingRows];

  const handler = async (combo) => {
    const params = mapParams(combo);
    const paramsJson = stableStringify(params);
    const params_hash = createHash('sha256').update(paramsJson).digest('hex');

    if (existingHashes.has(params_hash)) {
      skipped_jobs++;
      done_jobs++;
      if (done_jobs % progressStep === 0 || done_jobs === total_jobs) {
        console.log(`[SWEEP] exp_id=${exp_id} done_jobs=${done_jobs} skipped_jobs=${skipped_jobs} validated_jobs=${validated_jobs}`);
      }
      return null;
    }

    const runResult = await runOrchestrator({
      strategy: spec.strategy_id,
      symbol: spec.dataset.symbol,
      date: spec.dataset.date,
      params: paramsJson,
      validate_with_tick: spec.validate_with_tick !== false
    });

    const snapshotReport = await readJson(runResult.snapshot_report);
    const validationReport = runResult.validation_report_path
      ? await readJson(runResult.validation_report_path)
      : null;

    const row = {
      rank: null,
      params_hash,
      params_short: summarizeParams(params),
      snapshot: {
        pnl_pct: snapshotReport.results.pnl_pct,
        max_dd: snapshotReport.results.max_drawdown_pct,
        trades: snapshotReport.results.trades_count
      },
      tick: validationReport ? { pnl_pct: validationReport.tick.pnl_pct } : null,
      validation_status: snapshotReport.validation_status,
      pnl_diff: validationReport ? validationReport.deltas.pnl_diff : null,
      report_path: snapshotReport.artifacts.report_path,
      validation_report_path: snapshotReport.artifacts.validation_report_path
    };

    rows.push(row);
    existingHashes.add(params_hash);
    done_jobs++;
    if (validationReport) validated_jobs++;

    if (done_jobs % progressStep === 0 || done_jobs === total_jobs) {
      console.log(`[SWEEP] exp_id=${exp_id} done_jobs=${done_jobs} skipped_jobs=${skipped_jobs} validated_jobs=${validated_jobs}`);
    }
    return row;
  };

  await runWithConcurrency(combos, concurrency, handler);

  const sorted = sortRows(rows);
  sorted.forEach((r, idx) => { r.rank = idx + 1; });

  const validatedFromRows = sorted.filter(r => r.validation_report_path).length;
  const leaderboard = {
    exp_id,
    strategy_id: spec.strategy_id,
    dataset: spec.dataset,
    total_jobs,
    done_jobs,
    skipped_jobs,
    validated_jobs: validatedFromRows,
    rows: sorted
  };

  await writeFile(leaderboardPath, JSON.stringify(leaderboard, null, 2));

  const header = [
    'rank',
    'params_hash',
    'params',
    'snapshot_pnl_pct',
    'snapshot_max_dd',
    'snapshot_trades',
    'tick_pnl_pct',
    'validation_status',
    'pnl_diff',
    'report_path',
    'validation_report_path'
  ];

  const csvLines = [header.join(',')];
  for (const row of sorted) {
    csvLines.push([
      row.rank,
      row.params_hash,
      `"${row.params_short}"`,
      row.snapshot?.pnl_pct ?? '',
      row.snapshot?.max_dd ?? '',
      row.snapshot?.trades ?? '',
      row.tick?.pnl_pct ?? '',
      row.validation_status,
      row.pnl_diff ?? '',
      row.report_path,
      row.validation_report_path || ''
    ].join(','));
  }
  await writeFile(csvPath, csvLines.join('\n'));

  const leaderboardHash = createHash('sha256')
    .update(stableStringify(sorted))
    .digest('hex');
  console.log(`[SWEEP] exp_id=${exp_id} leaderboard_hash=${leaderboardHash}`);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
