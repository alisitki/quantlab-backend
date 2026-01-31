#!/usr/bin/env node
/**
 * runTwin.js — orchestrate twin runs (ML off vs shadow) with identical replay inputs.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ReplayEngine } from '../../../core/replay/index.js';
import { createCursor, encodeCursor } from '../../../core/replay/CursorCodec.js';
import { RuntimeAdapterV2 } from '../runtime/RuntimeAdapterV2.js';
import { ShadowObsBuilder } from '../runtime/ShadowObsBuilder.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const MAX_QUEUE_CAPACITY = Number(process.env.STRATEGYD_MAX_QUEUE_CAPACITY || 2000);
const QUEUE_SOFT_LIMIT = Math.max(10, MAX_QUEUE_CAPACITY - 5);

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true;
    args[name] = value;
    if (value !== true) i++;
  }
  return args;
}

function extractIdentity(parquetPath, metaPath) {
  const combined = `${parquetPath || ''} ${metaPath || ''}`;
  const stream = combined.match(/stream=([^/\\\s]+)/i)?.[1] || null;
  const symbol = combined.match(/symbol=([^/\\\s]+)/i)?.[1] || null;
  const date = combined.match(/date=([^/\\\s]+)/i)?.[1] || null;
  return { stream, symbol, date };
}

async function ensureLocalPath(p) {
  if (!p || typeof p !== 'string') return;
  if (p.startsWith('s3://')) return;
  try {
    await fs.access(p);
  } catch {
    throw new Error(`FILE_NOT_FOUND: ${p}`);
  }
}

async function runOnce({ mode, parquetPath, metaPath, identity, strategyId, seed }) {
  process.env.ML_ACTIVE_ENABLED = '0';
  process.env.ML_ACTIVE_KILL = '0';
  process.env.ML_SHADOW_ENABLED = mode === 'shadow' ? '1' : '0';
  process.env.ML_DECISION_PATH = '';
  process.env.ML_MODEL_PATH = '';
  process.env.ML_TRAINING_JOB_ID = '';

  const runId = `twin_${mode}_${Date.now()}`;
  const config = {
    runId,
    dataset: identity.stream || 'bbo',
    symbol: identity.symbol || 'UNKNOWN',
    date: identity.date || 'UNKNOWN',
    strategyId,
    seed,
    strategyConfig: {
      strategy_id: strategyId
    }
  };

  const adapter = new RuntimeAdapterV2(config);
  await adapter.start();

  console.log(`[TwinRun] run_id=${runId} mode=${mode} action=start parquet=${parquetPath} meta=${metaPath}`);

  const engine = new ReplayEngine({ parquet: parquetPath, meta: metaPath }, identity);
  const startTime = Date.now();
  let rows = 0;

  for await (const row of engine.replay()) {
    const cursor = encodeCursor(createCursor(row));
    adapter.onSseEvent({ ...row, cursor });
    rows += 1;
    while (adapter.getStats().queueSize >= QUEUE_SOFT_LIMIT) {
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  await engine.close();
  adapter.stop('finished');
  await adapter.finalizeManifest();

  const durationMs = Date.now() - startTime;
  const runSnap = adapter.getRunSnapshot();
  const manifest = await adapter.getManifestManager().get(runId);

  console.log(
    `[TwinRun] run_id=${runId} mode=${mode} action=end rows=${rows} duration_ms=${durationMs} state=${runSnap.state_hash?.slice(0, 8)} fills=${runSnap.fills_hash?.slice(0, 8)}`
  );

  return { runId, runSnap, manifest };
}

function assertTwinParity(off, shadow) {
  if (!off?.runSnap || !shadow?.runSnap) {
    throw new Error('RUN_SNAPSHOT_MISSING');
  }
  if (off.runSnap.state_hash !== shadow.runSnap.state_hash) {
    throw new Error(`STATE_HASH_MISMATCH off=${off.runSnap.state_hash} shadow=${shadow.runSnap.state_hash}`);
  }
  if (off.runSnap.fills_hash !== shadow.runSnap.fills_hash) {
    throw new Error(`FILLS_HASH_MISMATCH off=${off.runSnap.fills_hash} shadow=${shadow.runSnap.fills_hash}`);
  }
}

async function buildObs(runId) {
  const builder = new ShadowObsBuilder();
  await builder.buildForRun({ runId });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const parquetPath = args.parquet;
  const metaPath = args.meta;
  const strategyId = args.strategy || args.strategy_id;
  const seed = args.seed || null;
  const mlMode = args.ml_mode || 'shadow';

  if (!parquetPath || !metaPath || !strategyId || !seed) {
    console.error('Usage: node runTwin.js --parquet <path> --meta <path> --strategy <id> --seed <seed> [--ml_mode off|shadow]');
    process.exit(1);
  }
  if (mlMode !== 'off' && mlMode !== 'shadow') {
    console.error('Invalid --ml_mode. Expected: off|shadow');
    process.exit(1);
  }

  await ensureLocalPath(parquetPath);
  await ensureLocalPath(metaPath);

  const identity = extractIdentity(parquetPath, metaPath);
  await fs.mkdir(RUNS_DIR, { recursive: true });

  const offRun = await runOnce({ mode: 'off', parquetPath, metaPath, identity, strategyId, seed });
  const shadowRun = await runOnce({ mode: 'shadow', parquetPath, metaPath, identity, strategyId, seed });

  assertTwinParity(offRun, shadowRun);

  await buildObs(offRun.runId);
  await buildObs(shadowRun.runId);

  console.log(`[TwinRun] action=done off_run_id=${offRun.runId} shadow_run_id=${shadowRun.runId}`);
}

main().catch((err) => {
  console.error(`[TwinRun] action=error error=${err.message}`);
  process.exit(1);
});
