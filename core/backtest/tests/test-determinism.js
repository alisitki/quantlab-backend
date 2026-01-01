#!/usr/bin/env node
/**
 * Test: Backtest Determinism
 * Verifies: Same execution snapshot → same summary output (bit-level)
 */

import crypto from 'crypto';
import { buildBacktestSummary } from '../index.js';

/**
 * Create a mock execution state snapshot
 * All data is deterministic - no random or wall-clock
 */
function createMockSnapshot() {
  // Deterministic equity curve (100 points)
  const equityCurve = [];
  let equity = 10000;
  const baseTs = 1704067200000000000n; // Fixed timestamp

  for (let i = 0; i < 100; i++) {
    // Deterministic price changes
    const change = ((i % 7) - 3) * 10 + ((i % 13) - 6) * 5;
    equity += change;
    
    equityCurve.push({
      ts_event: baseTs + BigInt(i * 1000000000),
      equity
    });
  }

  // Deterministic fills
  const fills = [];
  for (let i = 0; i < 20; i++) {
    const isEven = i % 2 === 0;
    fills.push({
      id: `fill_${i + 1}`,
      orderId: `ord_${i + 1}`,
      symbol: 'BTCUSDT',
      side: isEven ? 'BUY' : 'SELL',
      qty: 0.01,
      fillPrice: 42000 + ((i % 10) - 5) * 100,
      fillValue: 0.01 * (42000 + ((i % 10) - 5) * 100),
      fee: 0.01 * (42000 + ((i % 10) - 5) * 100) * 0.0004,
      ts_event: baseTs + BigInt(i * 5000000000)
    });
  }

  return {
    positions: { 
      BTCUSDT: { 
        symbol: 'BTCUSDT', 
        size: 0, 
        avgEntryPrice: 0, 
        realizedPnl: 0,
        currentPrice: 42000
      } 
    },
    fills,
    equityCurve,
    totalRealizedPnl: -8.4, // Fixed value
    totalUnrealizedPnl: 0,
    equity: equityCurve[equityCurve.length - 1].equity,
    maxPositionValue: 420
  };
}

/**
 * Hash a summary object for comparison
 */
function hashSummary(summary) {
  // Convert to deterministic string (bigints become strings)
  const json = JSON.stringify(summary, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString();
    }
    return value;
  });
  return crypto.createHash('sha256').update(json).digest('hex');
}

async function main() {
  console.log('=== BACKTEST DETERMINISM TEST ===\n');

  // Create the same snapshot
  const snapshot = createMockSnapshot();

  // Run 1
  console.log('--- Run 1 ---');
  const summary1 = buildBacktestSummary(snapshot, { initialCapital: 10000 });
  const hash1 = hashSummary(summary1);
  console.log('Summary:', JSON.stringify(summary1, null, 2));
  console.log(`HASH: ${hash1}\n`);

  // Run 2 (identical input)
  console.log('--- Run 2 ---');
  const summary2 = buildBacktestSummary(snapshot, { initialCapital: 10000 });
  const hash2 = hashSummary(summary2);
  console.log('Summary:', JSON.stringify(summary2, null, 2));
  console.log(`HASH: ${hash2}\n`);

  // Compare
  console.log('--- COMPARISON ---');
  console.log(`Run1 HASH: ${hash1}`);
  console.log(`Run2 HASH: ${hash2}`);

  if (hash1 === hash2) {
    console.log('\nRESULT: PASS ✓');
    console.log('Same input → same output (bit-level determinism)');
  } else {
    console.log('\nRESULT: FAIL ✗');
    console.log('Determinism violated: hashes differ');
    process.exit(1);
  }

  // Additional: verify summary values are sane
  console.log('\n--- SANITY CHECKS ---');
  console.log(`equity_start: ${summary1.equity_start} (expected ~10000)`);
  console.log(`equity_end: ${summary1.equity_end}`);
  console.log(`trades: ${summary1.trades} (expected 20)`);
  console.log(`win_rate: ${summary1.win_rate} (expected 0-1)`);

  if (summary1.trades !== 20) {
    console.log('FAIL: trades count mismatch');
    process.exit(1);
  }

  if (summary1.win_rate < 0 || summary1.win_rate > 1) {
    console.log('FAIL: win_rate out of range');
    process.exit(1);
  }

  console.log('\nAll sanity checks passed ✓');
}

main().catch(err => {
  console.error(`RESULT: FAIL (${err.message})`);
  console.error(err.stack);
  process.exit(1);
});
