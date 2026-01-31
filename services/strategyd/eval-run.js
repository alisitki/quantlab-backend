#!/usr/bin/env node
/**
 * eval-run.js — Strategy Evaluation Runner
 *
 * Usage:
 *   node eval-run.js --mode snapshot --strategy ema_cross --symbol BTCUSDT --date 2026-01-04 --out report.json
 *   node eval-run.js --mode tick --strategy ema_cross --symbol BTCUSDT --date 2026-01-04 --out report.json
 */

import { SSEStrategyRunner } from './runtime/SSEStrategyRunner.js';
import { EvalEngine } from '../../core/backtest/EvalEngine.js';
import { writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
import path from 'node:path';
import { execSync } from 'node:child_process';

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

function requireMode(mode) {
  if (!mode || (mode !== 'snapshot' && mode !== 'tick')) {
    throw new Error('EVAL_ERROR: --mode must be one of: snapshot, tick');
  }
  return mode;
}

function dataModeFor(mode) {
  return mode === 'snapshot' ? 'snapshot_1s' : 'tick_full';
}

function aggregateFor(mode) {
  return mode === 'snapshot' ? '1s' : null;
}

async function fileHashSha256(filePath) {
  try {
    const buf = await readFile(filePath);
    return createHash('sha256').update(buf).digest('hex');
  } catch {
    return null;
  }
}

function getGitCommit(repoRoot) {
  try {
    return execSync('git rev-parse HEAD', { cwd: repoRoot, stdio: ['ignore', 'pipe', 'ignore'] })
      .toString()
      .trim();
  } catch {
    return null;
  }
}

function getGitDirty(repoRoot) {
  try {
    const out = execSync('git status --porcelain', { cwd: repoRoot, stdio: ['ignore', 'pipe', 'ignore'] })
      .toString()
      .trim();
    return out.length > 0;
  } catch {
    return null;
  }
}

async function getReplaydVersion(replaydUrl) {
  try {
    const res = await fetch(`${replaydUrl}/health`);
    if (!res.ok) return null;
    const json = await res.json();
    return json.replay_version || null;
  } catch {
    return null;
  }
}

async function getStrategydVersion() {
  try {
    const filePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'package.json');
    const json = JSON.parse(await readFile(filePath, 'utf8'));
    return json.version || null;
  } catch {
    return null;
  }
}

async function readJsonIfExists(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function resolveStrategyArtifact(strategyId, strategyVersion) {
  if (!strategyId) return null;
  try {
    const registryDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'strategy_registry');
    const indexPath = path.join(registryDir, 'index.json');
    const jsonlPath = path.join(registryDir, 'strategies.jsonl');
    if (!strategyVersion) {
      const index = await readJsonIfExists(indexPath);
      const entry = index?.[strategyId];
      return entry ? { path: entry.artifact_path, sha256: entry.artifact_sha256, version: entry.latest_version } : null;
    }
    const raw = await readFile(jsonlPath, 'utf8');
    const rows = raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
    const match = rows.find(r => r.strategy_id === strategyId && r.strategy_version === strategyVersion);
    return match ? { path: match.artifact_path, sha256: match.artifact_sha256, version: match.strategy_version } : null;
  } catch {
    return null;
  }
}

export async function runEval(params) {
  const mode = requireMode(params.mode);
  const strategy_id = params.strategy || params.strategy_id || 'ema_cross';
  const strategy_version = params.strategy_version || null;
  const symbol = params.symbol || 'BTCUSDT';
  const date = params.date || '2026-01-04';
  const outPath = params.out || `eval_${Date.now()}.json`;
  const runId = params.run_id || `eval_${Date.now()}`;

  const aggregate = aggregateFor(mode);
  const data_mode = dataModeFor(mode);
  const validation_status = params.validation_status || 'not_required';
  const registryArtifact = await resolveStrategyArtifact(strategy_id, strategy_version);
  const strategy_file = params.strategy_file || registryArtifact?.path || null;
  const strategy_artifact_sha256 = params.strategy_file ? await fileHashSha256(params.strategy_file) : (registryArtifact?.sha256 || null);

  const replaydUrl = process.env.REPLAYD_URL || 'http://localhost:3030';

  console.log(`[EVAL] eval_run_id=${runId} mode=${mode} action=start strategy=${strategy_id} symbol=${symbol} date=${date} aggregate=${aggregate || 'none'}`);

  const runner = new SSEStrategyRunner({
    runId,
    replaydUrl,
    replaydToken: process.env.REPLAYD_TOKEN || 'test-secret',
    dataset: 'bbo',
    symbol,
    date,
    aggregate,
    strategyConfig: params.params ? JSON.parse(params.params) : {}
  });

  const startTime = Date.now();
  await runner.start();
  const duration_ms = Date.now() - startTime;

  const snapshot = runner.getSnapshot();
  const runSnap = runner.getRunSnapshot();

  const results = EvalEngine.computeResults(snapshot, 10000); // Fixed 10k initial for v1 eval

  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
  const lockfilePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'package-lock.json');
  const strategyArtifactHash = strategy_file ? await fileHashSha256(strategy_file) : null;

  const report = {
    run_id: runId,
    strategy_id,
    strategy_version: strategy_version || registryArtifact?.version || 'v1.0.0',
    dataset: {
      exchange: 'binance',
      stream: 'bbo',
      symbol,
      date_range: date
    },
    data_mode,
    validation_status,
    params_hash: createHash('sha256').update(params.params || '{}').digest('hex'),
    input_fingerprint: runSnap.last_cursor, // Using last_cursor as proxy for input consumed
    results,
    determinism: {
      state_hash: runSnap.state_hash,
      fills_hash: runSnap.fills_hash
    },
    timings: {
      duration_ms,
      events_processed: runSnap.event_count
    },
    artifacts: {
      report_path: outPath,
      logs_ref: 'strategyd.log',
      validation_report_path: params.validation_report_path || null
    },
    provenance: {
      git_commit: getGitCommit(repoRoot),
      git_dirty: getGitDirty(repoRoot),
      strategyd_version: await getStrategydVersion(),
      replayd_version: await getReplaydVersion(replaydUrl),
      node_version: process.version,
      lockfile_hash: await fileHashSha256(lockfilePath),
      strategy_artifact_path: strategy_file || null,
      strategy_artifact_sha256: strategy_artifact_sha256
    }
  };

  await writeFile(outPath, JSON.stringify(report, null, 2));
  console.log(`[EVAL] eval_run_id=${runId} mode=${mode} action=end pnl_pct=${results.pnl_pct} validation_status=${validation_status}`);
  console.log(`[EVAL] Report written to ${outPath}`);
  console.log(`[EVAL] Results: PnL%=${(results.pnl_pct * 100).toFixed(4)}% Drawdown=${(results.max_drawdown_pct * 100).toFixed(4)}% Trades=${results.trades_count}`);
  return report;
}

const isDirect = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirect) {
  const params = parseArgs(process.argv.slice(2));
  runEval(params).catch(err => {
    console.error('[EVAL] Failed:', err);
    process.exit(1);
  });
}
