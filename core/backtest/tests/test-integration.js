#!/usr/bin/env node
/**
 * Test: Backtest Integration
 * Verifies: Replay → Strategy → Execution → Backtest pipeline
 * Runs with real S3 dataset
 */

import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { ExecutionEngine, OrderSide } from '../../execution/index.js';
import { buildBacktestSummary, printBacktestSummary } from '../index.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node backtest/tests/test-integration.js <s3_parquet_path> <s3_meta_path>');
  console.error('Example: node backtest/tests/test-integration.js s3://bucket/data.parquet s3://bucket/meta.json');
  process.exit(1);
}

/**
 * Simple strategy that trades at fixed intervals
 */
class SimpleTestStrategy {
  constructor() {
    this.orderInterval = 100;
    this.orderQty = 0.01;
    this.lastSide = 'SELL'; // Start with BUY
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;
    
    if (ctx.stats.processed > 0 && ctx.stats.processed % this.orderInterval === 0) {
      this.lastSide = this.lastSide === 'BUY' ? 'SELL' : 'BUY';
      
      ctx.placeOrder({
        symbol: event.symbol || 'BTCUSDT',
        side: this.lastSide,
        qty: this.orderQty,
        ts_event: event.ts_event
      });
    }
  }
}

async function main() {
  console.log('=== BACKTEST INTEGRATION TEST ===');
  console.log(`Dataset: ${parquetPath}`);
  console.log(`Meta: ${metaPath}\n`);

  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ 
    initialCapital: 10000,
    recordEquityCurve: true
  });
  const strategy = new SimpleTestStrategy();

  try {
    // Run replay with strategy and execution
    console.log('--- Running Replay + Strategy + Execution ---');
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

    console.log(`Events processed: ${result.stats.processed}`);
    console.log(`Fills: ${executionEngine.getFillCount()}`);

    // Get execution state snapshot
    const snapshot = executionEngine.snapshot();

    // Build backtest summary
    console.log('\n--- Building Backtest Summary ---');
    const summary = buildBacktestSummary(snapshot, { initialCapital: 10000 });
    
    // Print summary
    printBacktestSummary(summary);

    // Verify consistency
    console.log('--- Verification ---');
    
    // 1. Equity end should equal snapshot.equity
    const equityMatch = Math.abs(summary.equity_end - snapshot.equity) < 0.01;
    console.log(`Equity match: ${equityMatch ? '✓' : '✗'} (summary=${summary.equity_end}, snapshot=${snapshot.equity.toFixed(2)})`);

    // 2. Trades count should match fills
    const tradesMatch = summary.trades === snapshot.fills.length;
    console.log(`Trades match: ${tradesMatch ? '✓' : '✗'} (summary=${summary.trades}, fills=${snapshot.fills.length})`);

    // 3. Win rate should be in valid range
    const winRateValid = summary.win_rate >= 0 && summary.win_rate <= 1;
    console.log(`Win rate valid: ${winRateValid ? '✓' : '✗'} (${summary.win_rate})`);

    // 4. Max drawdown should be <= 0
    const ddValid = summary.max_drawdown_pct <= 0;
    console.log(`Max DD valid: ${ddValid ? '✓' : '✗'} (${summary.max_drawdown_pct}%)`);

    // Final result
    const allPass = equityMatch && tradesMatch && winRateValid && ddValid;
    
    if (allPass) {
      console.log('\nRESULT: PASS ✓');
      console.log('Replay → Strategy → Execution → Backtest pipeline verified');
    } else {
      console.log('\nRESULT: FAIL ✗');
      console.log('Some verifications failed');
      process.exit(1);
    }

    // Output final JSON summary
    console.log('\n--- JSON Output ---');
    console.log(JSON.stringify(summary, null, 2));

  } finally {
    await replayEngine.close();
  }
}

main().catch(err => {
  console.error(`RESULT: FAIL (${err.message})`);
  console.error(err.stack);
  process.exit(1);
});
