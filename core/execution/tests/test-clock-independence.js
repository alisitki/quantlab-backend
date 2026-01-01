#!/usr/bin/env node
/**
 * Test: Clock Independence
 * Verifies: AsapClock vs ScaledClock produce identical execution results
 */

import crypto from 'crypto';
import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { ExecutionEngine, OrderSide } from '../index.js';
import AsapClock from '../../replay/clock/AsapClock.js';
import ScaledClock from '../../replay/clock/ScaledClock.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node execution/tests/test-clock-independence.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

/**
 * Strategy that places deterministic orders
 */
class ClockTestStrategy {
  constructor() {
    this.orderInterval = 100;
    this.orderQty = 0.01;
    this.maxEvents = 1000; // Limit for faster test with scaled clock
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;
    if (ctx.stats.processed >= this.maxEvents) return;
    
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

function hashState(state) {
  const data = JSON.stringify({
    fillCount: state.fills.length,
    equity: state.equity.toFixed(8),
    realizedPnl: state.totalRealizedPnl.toFixed(8)
  });
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 16);
}

async function runWithClock(label, clock) {
  console.log(`\n--- RUN: ${label} ---`);
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new ClockTestStrategy();

  try {
    const startTime = Date.now();
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 500,
        parquetPath,
        metaPath,
        executionEngine,
        clock
      }
    });
    const elapsed = Date.now() - startTime;

    const state = executionEngine.snapshot();
    const hash = hashState(state);
    
    console.log(`processed=${result.stats.processed} (${elapsed}ms)`);
    console.log(`fills=${state.fills.length}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`HASH=${hash}`);

    return { hash, state };
  } finally {
    await replayEngine.close();
  }
}

async function main() {
  console.log('=== CLOCK INDEPENDENCE TEST ===');
  console.log(`DATASET: ${parquetPath}`);

  // Run with ASAP clock (instant)
  const asapResult = await runWithClock('AsapClock', AsapClock);
  
  // Run with ScaledClock (10000x speed - very fast but still uses delay logic)
  const scaledClock = new ScaledClock({ speed: 10000 });
  const scaledResult = await runWithClock('ScaledClock (10000x)', scaledClock);

  console.log('\n--- COMPARISON ---');
  console.log(`AsapClock   HASH: ${asapResult.hash}`);
  console.log(`ScaledClock HASH: ${scaledResult.hash}`);

  if (asapResult.hash === scaledResult.hash) {
    console.log('\nRESULT: PASS ✓');
    console.log('Clock mode does not affect execution results');
  } else {
    console.log('\nRESULT: FAIL ✗');
    console.log('Clock independence violated: hashes differ');
    process.exit(1);
  }
}

main().catch(err => {
  console.error(`RESULT: FAIL (${err.message})`);
  process.exit(1);
});
