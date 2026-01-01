#!/usr/bin/env node
/**
 * Test: Single Order Determinism
 * Verifies: Same replay → same fills → same PnL
 */

import crypto from 'crypto';
import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { ExecutionEngine, OrderSide } from '../index.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node execution/tests/test-determinism.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

/**
 * Strategy that places orders at fixed intervals for determinism testing
 */
class DeterministicOrderStrategy {
  constructor() {
    this.orderInterval = 100; // Place order every N events
    this.orderQty = 0.01;
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;
    
    // Place alternating BUY/SELL orders at fixed intervals
    if (ctx.stats.processed > 0 && ctx.stats.processed % this.orderInterval === 0) {
      const side = (ctx.stats.processed / this.orderInterval) % 2 === 1 
        ? OrderSide.BUY 
        : OrderSide.SELL;
      
      ctx.placeOrder({
        symbol: event.symbol || 'BTCUSDT',
        side,
        qty: this.orderQty,
        ts_event: event.ts_event
      });
    }
  }
}

/**
 * Hash the final execution state for comparison
 */
function hashState(state) {
  const data = JSON.stringify({
    fillCount: state.fills.length,
    equity: state.equity.toFixed(8),
    realizedPnl: state.totalRealizedPnl.toFixed(8),
    positions: Object.entries(state.positions).map(([sym, pos]) => ({
      symbol: sym,
      size: pos.size,
      avgEntry: pos.avgEntryPrice.toFixed(8)
    }))
  });
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 16);
}

async function runOnce(label) {
  console.log(`\n--- RUN ${label} ---`);
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new DeterministicOrderStrategy();

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
    
    console.log(`processed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`pnl=${state.totalRealizedPnl.toFixed(4)}`);
    console.log(`HASH=${hash}`);

    return { hash, state };
  } finally {
    await replayEngine.close();
  }
}

async function main() {
  console.log('=== DETERMINISM TEST ===');
  console.log(`DATASET: ${parquetPath}`);

  const run1 = await runOnce('1');
  const run2 = await runOnce('2');

  console.log('\n--- COMPARISON ---');
  console.log(`Run1 HASH: ${run1.hash}`);
  console.log(`Run2 HASH: ${run2.hash}`);

  if (run1.hash === run2.hash) {
    console.log('\nRESULT: PASS ✓');
    console.log('Same replay → same fills → same PnL');
  } else {
    console.log('\nRESULT: FAIL ✗');
    console.log('Determinism violated: hashes differ');
    process.exit(1);
  }
}

main().catch(err => {
  console.error(`RESULT: FAIL (${err.message})`);
  process.exit(1);
});
