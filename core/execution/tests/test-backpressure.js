#!/usr/bin/env node
/**
 * Test: Backpressure Compatibility
 * Verifies: Slow strategy with delays still produces correct fills
 */

import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { ExecutionEngine, OrderSide } from '../index.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node execution/tests/test-backpressure.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

/**
 * Strategy with artificial delays to test backpressure
 */
class SlowOrderStrategy {
  constructor() {
    this.orderInterval = 50;
    this.orderQty = 0.01;
    this.delayMs = 10; // Small delay per order
    this.maxEvents = 500; // Limit for faster test
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;
    if (ctx.stats.processed >= this.maxEvents) return;
    
    if (ctx.stats.processed > 0 && ctx.stats.processed % this.orderInterval === 0) {
      // Simulate slow processing
      await new Promise(resolve => setTimeout(resolve, this.delayMs));
      
      const side = (ctx.stats.processed / this.orderInterval) % 2 === 1 
        ? OrderSide.BUY 
        : OrderSide.SELL;
      
      const fill = ctx.placeOrder({
        symbol: event.symbol || 'BTCUSDT',
        side,
        qty: this.orderQty,
        ts_event: event.ts_event
      });
      
      // Validate fill price matches event price
      const eventPrice = event.price ?? event.close;
      if (Math.abs(fill.fillPrice - eventPrice) > 0.0001) {
        throw new Error(`Fill price mismatch: ${fill.fillPrice} vs ${eventPrice}`);
      }
    }
  }
}

async function main() {
  console.log('=== BACKPRESSURE COMPATIBILITY TEST ===');
  console.log(`DATASET: ${parquetPath}`);

  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new SlowOrderStrategy();

  try {
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 100,
        parquetPath,
        metaPath,
        executionEngine
      }
    });

    const state = executionEngine.snapshot();
    
    console.log(`\nprocessed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`equity=${state.equity.toFixed(4)}`);

    // Validate we got expected number of fills
    const expectedFills = Math.floor(Math.min(result.stats.processed, 500) / 50);
    if (state.fills.length >= expectedFills - 1) {
      console.log('\nRESULT: PASS ✓');
      console.log('Slow strategy with backpressure produces correct fills');
    } else {
      console.log(`\nRESULT: FAIL ✗`);
      console.log(`Expected ~${expectedFills} fills, got ${state.fills.length}`);
      process.exit(1);
    }
  } catch (err) {
    console.error(`\nRESULT: FAIL (${err.message})`);
    process.exit(1);
  } finally {
    await replayEngine.close();
  }
}

main();
