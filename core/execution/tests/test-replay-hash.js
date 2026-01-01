#!/usr/bin/env node
/**
 * Test: Replay Hash Equality
 * Verifies: Multiple runs with different batch sizes produce same final equity hash
 */

import crypto from 'crypto';
import { ReplayEngine } from '../../replay/index.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { ExecutionEngine, OrderSide } from '../index.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node execution/tests/test-replay-hash.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

/**
 * Strategy that places orders at fixed intervals
 */
class HashTestStrategy {
  constructor() {
    this.orderInterval = 100;
    this.orderQty = 0.01;
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;
    
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
 * Hash equity curve for comparison
 */
function hashEquityCurve(state) {
  // Hash based on final state (not full curve for performance)
  const data = JSON.stringify({
    fillCount: state.fills.length,
    finalEquity: state.equity.toFixed(8),
    realizedPnl: state.totalRealizedPnl.toFixed(8),
    unrealizedPnl: state.totalUnrealizedPnl.toFixed(8),
    // Include first and last fills for verification
    firstFill: state.fills[0] ? {
      price: state.fills[0].fillPrice.toFixed(8),
      qty: state.fills[0].qty
    } : null,
    lastFill: state.fills.length > 0 ? {
      price: state.fills[state.fills.length - 1].fillPrice.toFixed(8),
      qty: state.fills[state.fills.length - 1].qty
    } : null
  });
  return crypto.createHash('sha256').update(data).digest('hex');
}

async function runWithBatchSize(batchSize) {
  console.log(`\n--- BATCH SIZE: ${batchSize} ---`);
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ 
    initialCapital: 10000,
    recordEquityCurve: false // Disable for performance
  });
  const strategy = new HashTestStrategy();

  try {
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize,
        parquetPath,
        metaPath,
        executionEngine
      }
    });

    const state = executionEngine.snapshot();
    const hash = hashEquityCurve(state);
    
    console.log(`processed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`HASH=${hash.slice(0, 16)}...`);

    return { hash, processed: result.stats.processed };
  } finally {
    await replayEngine.close();
  }
}

async function main() {
  console.log('=== REPLAY HASH EQUALITY TEST ===');
  console.log(`DATASET: ${parquetPath}`);
  console.log('Testing: Different batch sizes should produce identical results');

  const batchSizes = [1000, 5000, 10000];
  const results = [];

  for (const batchSize of batchSizes) {
    const result = await runWithBatchSize(batchSize);
    results.push({ batchSize, ...result });
  }

  console.log('\n--- HASH COMPARISON ---');
  for (const r of results) {
    console.log(`Batch ${r.batchSize}: ${r.hash.slice(0, 16)}...`);
  }

  // Check all hashes are equal
  const allEqual = results.every(r => r.hash === results[0].hash);
  
  if (allEqual) {
    console.log('\nRESULT: PASS ✓');
    console.log(`All batch sizes produce identical hash: ${results[0].hash.slice(0, 16)}`);
  } else {
    console.log('\nRESULT: FAIL ✗');
    console.log('Batch sizes produce different hashes - determinism violated');
    process.exit(1);
  }
}

main().catch(err => {
  console.error(`RESULT: FAIL (${err.message})`);
  process.exit(1);
});
