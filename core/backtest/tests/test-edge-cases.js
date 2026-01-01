#!/usr/bin/env node
/**
 * Test: Backtest Edge Cases
 * Verifies: No trades, flat equity, single trade
 */

import { buildBacktestSummary } from '../index.js';
import { validateEquityCurve, computeReturns } from '../equity.js';
import { maxDrawdown, winRate, avgTradePnl } from '../metrics.js';

const baseTs = 1704067200000000000n;

function assertEquals(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${expected}, got ${actual}`);
  }
}

function assertApprox(actual, expected, epsilon, message) {
  if (Math.abs(actual - expected) > epsilon) {
    throw new Error(`${message}: expected ~${expected}, got ${actual}`);
  }
}

function test(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    return true;
  } catch (err) {
    console.log(`✗ ${name}`);
    console.log(`  Error: ${err.message}`);
    return false;
  }
}

let passed = 0;
let failed = 0;

console.log('=== BACKTEST EDGE CASE TESTS ===\n');

// Test 1: No trades
if (test('No trades - empty fills array', () => {
  const snapshot = {
    positions: {},
    fills: [],
    equityCurve: [
      { ts_event: baseTs, equity: 10000 },
      { ts_event: baseTs + 1000000000n, equity: 10000 }
    ],
    totalRealizedPnl: 0,
    totalUnrealizedPnl: 0,
    equity: 10000,
    maxPositionValue: 0
  };

  const summary = buildBacktestSummary(snapshot);
  
  assertEquals(summary.trades, 0, 'trades');
  assertEquals(summary.win_rate, 0, 'win_rate');
  assertEquals(summary.avg_trade_pnl, 0, 'avg_trade_pnl');
  assertEquals(summary.total_pnl, 0, 'total_pnl');
})) passed++; else failed++;

// Test 2: Flat equity
if (test('Flat equity - no change', () => {
  const equityCurve = [];
  for (let i = 0; i < 10; i++) {
    equityCurve.push({
      ts_event: baseTs + BigInt(i * 1000000000),
      equity: 10000
    });
  }

  const validation = validateEquityCurve(equityCurve);
  assertEquals(validation.valid, true, 'validation.valid');

  const md = maxDrawdown(equityCurve);
  assertEquals(md, 0, 'maxDrawdown');

  const returns = computeReturns(equityCurve);
  assertEquals(returns.length, 9, 'returns.length');
  returns.forEach((r, i) => {
    assertEquals(r.return, 0, `returns[${i}]`);
  });
})) passed++; else failed++;

// Test 3: Single trade
if (test('Single trade', () => {
  const snapshot = {
    positions: { BTCUSDT: { symbol: 'BTCUSDT', size: 0.01, avgEntryPrice: 42000, realizedPnl: 0, currentPrice: 42100 } },
    fills: [{
      id: 'fill_1',
      orderId: 'ord_1',
      symbol: 'BTCUSDT',
      side: 'BUY',
      qty: 0.01,
      fillPrice: 42000,
      fillValue: 420,
      fee: 0.168,
      ts_event: baseTs
    }],
    equityCurve: [
      { ts_event: baseTs, equity: 10000 },
      { ts_event: baseTs + 1000000000n, equity: 10001 }
    ],
    totalRealizedPnl: 0,
    totalUnrealizedPnl: 1,
    equity: 10001,
    maxPositionValue: 420
  };

  const summary = buildBacktestSummary(snapshot);
  
  assertEquals(summary.trades, 1, 'trades');
  assertEquals(summary.total_pnl, 1, 'total_pnl');
})) passed++; else failed++;

// Test 4: Empty equity curve
if (test('Empty equity curve - uses initial capital', () => {
  const snapshot = {
    positions: {},
    fills: [],
    equityCurve: [],
    totalRealizedPnl: 0,
    totalUnrealizedPnl: 0,
    equity: 10000,
    maxPositionValue: 0
  };

  const summary = buildBacktestSummary(snapshot, { initialCapital: 10000 });
  
  assertEquals(summary.equity_start, 10000, 'equity_start');
  assertEquals(summary.equity_end, 10000, 'equity_end');
  assertEquals(summary.total_pnl, 0, 'total_pnl');
})) passed++; else failed++;

// Test 5: Equity curve validation - non-monotonic
if (test('Non-monotonic ts_event validation fails', () => {
  const curve = [
    { ts_event: baseTs + 1000000000n, equity: 10000 },
    { ts_event: baseTs, equity: 10001 } // Earlier timestamp
  ];

  const validation = validateEquityCurve(curve);
  assertEquals(validation.valid, false, 'validation.valid');
  if (!validation.error.includes('before previous')) {
    throw new Error('Expected error about ts_event order');
  }
})) passed++; else failed++;

// Test 6: Non-finite equity value
if (test('Non-finite equity validation fails', () => {
  const curve = [
    { ts_event: baseTs, equity: 10000 },
    { ts_event: baseTs + 1000000000n, equity: NaN }
  ];

  const validation = validateEquityCurve(curve);
  assertEquals(validation.valid, false, 'validation.valid');
  if (!validation.error.includes('not finite')) {
    throw new Error('Expected error about finite equity');
  }
})) passed++; else failed++;

// Test 7: Max drawdown calculation
if (test('Max drawdown calculation', () => {
  const curve = [
    { ts_event: baseTs, equity: 10000 },
    { ts_event: baseTs + 1000000000n, equity: 10500 }, // Peak
    { ts_event: baseTs + 2000000000n, equity: 9975 },  // Trough: -5%
    { ts_event: baseTs + 3000000000n, equity: 10200 }
  ];

  const md = maxDrawdown(curve);
  assertApprox(md, -0.05, 0.001, 'maxDrawdown');
})) passed++; else failed++;

// Test 8: Win rate with round-trips
if (test('Win rate with winning and losing trades', () => {
  const fills = [
    // Round-trip 1: BUY at 100, SELL at 110 = WIN
    { id: 'f1', orderId: 'o1', symbol: 'A', side: 'BUY', qty: 1, fillPrice: 100, fillValue: 100, fee: 0.04, ts_event: baseTs },
    { id: 'f2', orderId: 'o2', symbol: 'A', side: 'SELL', qty: 1, fillPrice: 110, fillValue: 110, fee: 0.044, ts_event: baseTs + 1000000000n },
    // Round-trip 2: BUY at 100, SELL at 95 = LOSS
    { id: 'f3', orderId: 'o3', symbol: 'A', side: 'BUY', qty: 1, fillPrice: 100, fillValue: 100, fee: 0.04, ts_event: baseTs + 2000000000n },
    { id: 'f4', orderId: 'o4', symbol: 'A', side: 'SELL', qty: 1, fillPrice: 95, fillValue: 95, fee: 0.038, ts_event: baseTs + 3000000000n }
  ];

  const wr = winRate(fills);
  assertEquals(wr, 0.5, 'winRate'); // 1 win, 1 loss = 50%
})) passed++; else failed++;

// Summary
console.log(`\n--- RESULTS ---`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);

if (failed > 0) {
  console.log('\nRESULT: FAIL ✗');
  process.exit(1);
} else {
  console.log('\nRESULT: PASS ✓');
}
