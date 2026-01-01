#!/usr/bin/env node
/**
 * QuantLab Baseline Strategy v1 — Test Runner
 * 
 * Tests:
 * 1. End-to-end strategy execution with real S3 BBO data
 * 2. Determinism verification (2 runs = identical result)
 * 3. Metrics pipeline consumption
 * 
 * Usage:
 *   node strategy/baseline/test-baseline.js <s3_parquet_glob> <s3_meta_path>
 * 
 * Example:
 *   node strategy/baseline/test-baseline.js \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/*.parquet" \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/meta.json"
 */

import crypto from 'crypto';
import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../Runner.js';
import { ExecutionEngine } from '../../execution/index.js';
import { buildBacktestSummary, printBacktestSummary } from '../../backtest/summary.js';
import { BaselineStrategy } from './BaselineStrategy.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node strategy/baseline/test-baseline.js <s3_parquet_path> <s3_meta_path>');
  console.error('\nExample:');
  console.error('  node strategy/baseline/test-baseline.js \\');
  console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/*.parquet" \\');
  console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/meta.json"');
  process.exit(1);
}

/**
 * Hash execution state for determinism comparison
 */
function hashState(state) {
  const data = JSON.stringify({
    fillCount: state.fills.length,
    equity: state.equity.toFixed(8),
    realizedPnl: state.totalRealizedPnl.toFixed(8),
    fills: state.fills.map(f => ({
      id: f.fillId,
      side: f.side,
      qty: f.qty,
      price: f.fillPrice.toFixed(8)
    }))
  });
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 16);
}

/**
 * Run a single backtest
 */
async function runOnce(label) {
  console.log(`\n--- RUN ${label} ---`);
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new BaselineStrategy({
    symbol: 'btcusdt',
    orderQty: 0.01,
    cooldownEvents: 50,
    momentumThreshold: 0.0001,
    spreadMaxBps: 10
  });

  try {
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 5000,
        parquetPath,
        metaPath,
        executionEngine
      }
    });

    const state = executionEngine.snapshot();
    const hash = hashState(state);
    const strategyStats = strategy.getStats();
    
    console.log(`processed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`signals=${strategyStats.signalCount}`);
    console.log(`trades=${strategyStats.tradeCount}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`pnl=${state.totalRealizedPnl.toFixed(4)}`);
    console.log(`HASH=${hash}`);

    return { hash, state, result, strategyStats };
  } finally {
    await replayEngine.close();
  }
}

async function main() {
  console.log('========================================');
  console.log('  BASELINE STRATEGY v1 TEST');
  console.log('========================================');
  console.log(`DATASET: ${parquetPath}`);

  // Run twice for determinism check
  const run1 = await runOnce('1');
  const run2 = await runOnce('2');

  console.log('\n--- DETERMINISM CHECK ---');
  console.log(`Run1 HASH: ${run1.hash}`);
  console.log(`Run2 HASH: ${run2.hash}`);

  const deterministic = run1.hash === run2.hash;
  if (deterministic) {
    console.log('✓ PASS: Deterministic (identical hashes)');
  } else {
    console.log('✗ FAIL: Non-deterministic (hashes differ)');
    process.exit(1);
  }

  // Validate trades
  console.log('\n--- TRADE VALIDATION ---');
  const tradeCount = run1.state.fills.length;
  if (tradeCount > 0) {
    console.log(`✓ PASS: Non-zero trades (${tradeCount} fills)`);
  } else {
    console.log('✗ FAIL: Zero trades generated');
    process.exit(1);
  }

  // Metrics pipeline
  console.log('\n--- METRICS PIPELINE ---');
  try {
    const summary = buildBacktestSummary(run1.state, { initialCapital: 10000 });
    printBacktestSummary(summary);
    console.log('✓ PASS: Metrics computed successfully');
  } catch (err) {
    console.log(`✗ FAIL: Metrics error - ${err.message}`);
    process.exit(1);
  }

  // Final summary
  console.log('\n========================================');
  console.log('  ALL TESTS PASSED');
  console.log('========================================');
  console.log(`Total events: ${run1.result.stats.processed}`);
  console.log(`Total fills:  ${run1.state.fills.length}`);
  console.log(`Final equity: ${run1.state.equity.toFixed(2)}`);
}

main().catch(err => {
  console.error(`\nFATAL ERROR: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
