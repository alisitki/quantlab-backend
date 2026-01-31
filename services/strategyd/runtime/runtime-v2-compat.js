#!/usr/bin/env node
/**
 * runtime-v2-compat.js
 *
 * Compares legacy SSEStrategyRunner with RuntimeAdapterV2 for deterministic parity.
 * Requires replayd to be running.
 */

import { SSEStrategyRunner } from './SSEStrategyRunner.js';
import { ManifestManager } from './ManifestManager.js';

const replaydUrl = process.env.REPLAYD_URL || 'http://localhost:3030';
const replaydToken = process.env.REPLAYD_TOKEN || null;
const dataset = process.env.DATASET || 'bbo';
const symbol = process.env.SYMBOL || 'BTCUSDT';
const date = process.env.DATE || '2026-01-04';
const speed = process.env.SPEED || 'asap';
const aggregate = process.env.AGGREGATE || null;
const stopAtEventIndex = Number(process.env.STOP_AT_EVENT_INDEX || 500);

async function runOnce({ useV2 }) {
  const runId = `compat_${useV2 ? 'v2' : 'legacy'}_${Date.now()}`;

  const runner = new SSEStrategyRunner({
    runId,
    replaydUrl,
    replaydToken,
    dataset,
    symbol,
    date,
    speed,
    aggregate,
    stopAtEventIndex,
    strategyConfig: {
      fastPeriod: Number(process.env.FAST_PERIOD) || 9,
      slowPeriod: Number(process.env.SLOW_PERIOD) || 21,
      positionSize: Number(process.env.POSITION_SIZE) || 0.1
    },
    executionConfig: {
      initialCapital: Number(process.env.INITIAL_CAPITAL) || 10000,
      feeRate: Number(process.env.FEE_RATE) || 0.0004
    },
    strategyRuntimeV2: useV2
  });

  const manifestManager = new ManifestManager();
  await manifestManager.init();

  const startedAt = Date.now();
  await runner.start();
  const durationMs = Date.now() - startedAt;

  const runSnap = runner.getRunSnapshot();
  const manifest = await manifestManager.get(runId);

  if (!manifest) {
    throw new Error(`MANIFEST_MISSING: ${runId}`);
  }

  return { runId, runSnap, manifest, durationMs };
}

function assertManifestFields(manifest) {
  if (!manifest.run_id || !manifest.input || !manifest.output) {
    throw new Error('MANIFEST_SHAPE_INVALID');
  }

  const requiredOutput = ['last_cursor', 'event_count', 'fills_count', 'equity_end', 'state_hash', 'fills_hash'];
  for (const field of requiredOutput) {
    if (manifest.output[field] === undefined || manifest.output[field] === null) {
      throw new Error(`MANIFEST_MISSING_FIELD: ${field}`);
    }
  }
}

(async () => {
  console.log(`[COMPAT] start replaydUrl=${replaydUrl} dataset=${dataset} symbol=${symbol} date=${date} stopAtEventIndex=${stopAtEventIndex}`);

  const legacy = await runOnce({ useV2: false });
  const v2 = await runOnce({ useV2: true });

  assertManifestFields(legacy.manifest);
  assertManifestFields(v2.manifest);

  if (legacy.runSnap.event_count !== v2.runSnap.event_count) {
    throw new Error(`EVENT_COUNT_MISMATCH legacy=${legacy.runSnap.event_count} v2=${v2.runSnap.event_count}`);
  }

  console.log(`[COMPAT] legacy run_id=${legacy.runId} events=${legacy.runSnap.event_count} duration_ms=${legacy.durationMs}`);
  console.log(`[COMPAT] v2     run_id=${v2.runId} events=${v2.runSnap.event_count} duration_ms=${v2.durationMs}`);
  console.log('[COMPAT] OK');
})().catch((err) => {
  console.error('[COMPAT] FAILED', err.message);
  process.exit(1);
});
