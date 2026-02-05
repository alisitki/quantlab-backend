#!/usr/bin/env node
/**
 * StrategyV1 Backtest Runner
 *
 * Tests StrategyV1 with real S3 BBO data and computes performance metrics.
 *
 * Usage:
 *   node core/strategy/v1/tests/test-strategyv1-backtest.js <s3_parquet> <s3_meta>
 *   node core/strategy/v1/tests/test-strategyv1-backtest.js <s3_parquet> <s3_meta> --config quality
 *
 * Example:
 *   node core/strategy/v1/tests/test-strategyv1-backtest.js \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/*.parquet" \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/meta.json"
 */

import crypto from 'crypto';
import { ReplayEngine } from '../../../replay/index.js';
import { runReplayWithStrategy } from '../../Runner.js';
import { ExecutionEngine } from '../../../execution/index.js';
import { buildBacktestSummary, printBacktestSummary } from '../../../backtest/summary.js';
import { StrategyV1 } from '../StrategyV1.js';
import { getConfig } from '../config.js';

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
async function runBacktest(parquetPath, metaPath, configName = 'default') {
  console.log(`\n--- StrategyV1 Backtest (${configName}) ---`);

  // 1. Load config
  const config = getConfig(configName);

  // Override feature report path (use sample report)
  config.featureReportPath = './reports/feature_analysis_full_2026-02-05.json';

  // 2. Initialize engines
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new StrategyV1(config);

  try {
    // 3. Run backtest
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

    // 4. Get state and build metrics
    const state = executionEngine.snapshot();
    const hash = hashState(state);
    const summary = buildBacktestSummary(state, { initialCapital: 10000 });

    // 5. Print results
    console.log(`processed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`pnl=${state.totalRealizedPnl.toFixed(4)}`);
    console.log(`HASH=${hash}`);

    printBacktestSummary(summary);

    // 6. Return for comparison
    return {
      config: configName,
      strategy: 'StrategyV1',
      hash,
      summary,
      state,
      stats: result.stats
    };
  } finally {
    await replayEngine.close();
  }
}

/**
 * Main entry point
 */
async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error('Usage: node test-strategyv1-backtest.js <s3_parquet> <s3_meta> [--config <name>]');
    console.error('\nExample:');
    console.error('  node test-strategyv1-backtest.js \\');
    console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/*.parquet" \\');
    console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/meta.json"');
    console.error('\nWith config preset:');
    console.error('  node test-strategyv1-backtest.js <parquet> <meta> --config quality');
    process.exit(1);
  }

  const parquetPath = args[0];
  const metaPath = args[1];
  let configName = 'default';

  // Parse --config flag
  const configFlagIndex = args.indexOf('--config');
  if (configFlagIndex !== -1 && args[configFlagIndex + 1]) {
    configName = args[configFlagIndex + 1];
  }

  console.log('========================================');
  console.log('  STRATEGYV1 BACKTEST');
  console.log('========================================');
  console.log(`DATASET: ${parquetPath}`);
  console.log(`CONFIG:  ${configName}`);

  // Run backtest
  const result = await runBacktest(parquetPath, metaPath, configName);

  // Validate trades
  console.log('\n--- TRADE VALIDATION ---');
  const tradeCount = result.state.fills.length;
  if (tradeCount > 0) {
    console.log(`✓ PASS: Generated ${tradeCount} fills`);
  } else {
    console.log('⚠ WARNING: Zero trades generated');
  }

  // Final summary
  console.log('\n========================================');
  console.log('  BACKTEST COMPLETE');
  console.log('========================================');
  console.log(`Total events: ${result.stats.processed}`);
  console.log(`Total fills:  ${result.state.fills.length}`);
  console.log(`Final equity: ${result.state.equity.toFixed(2)}`);
  console.log(`Return:       ${result.summary.return_pct >= 0 ? '+' : ''}${result.summary.return_pct.toFixed(2)}%`);
  console.log(`Max DD:       ${result.summary.max_drawdown_pct.toFixed(2)}%`);
  console.log(`State Hash:   ${result.hash}`);
}

main().catch(err => {
  console.error(`\nFATAL ERROR: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
