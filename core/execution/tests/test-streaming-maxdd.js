/**
 * Unit tests for streaming maxDrawdown calculation
 * Validates O(1) streaming implementation matches legacy array-based calculation
 */

import { ExecutionState } from '../state.js';
import { maxDrawdown } from '../../backtest/metrics.js';

// Test utilities
function assert(condition, message) {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

function assertClose(a, b, tolerance = 0.0001, message = '') {
  const diff = Math.abs(a - b);
  if (diff > tolerance) {
    throw new Error(`Assertion failed: ${message}\n  Expected: ${a}\n  Got: ${b}\n  Diff: ${diff}`);
  }
}

// Test 1: Simple drawdown scenario
function testSimpleDrawdown() {
  console.log('\n[TEST 1] Simple drawdown scenario');

  // Scenario: 10000 -> 12000 (peak) -> 9000 (trough) -> 10500
  // MaxDD = (12000 - 9000) / 12000 = 0.25 = 25%

  const legacy = new ExecutionState(10000, { streamingMaxDD: false });
  const streaming = new ExecutionState(10000, { streamingMaxDD: true });

  // Simulate equity changes (mock getEquity)
  const equityValues = [10000, 11000, 12000, 10000, 9000, 10500];

  for (let i = 0; i < equityValues.length; i++) {
    const ts = BigInt(1000000 + i * 1000);

    // Mock equity by setting realized PnL
    const targetEquity = equityValues[i];
    const pnl = targetEquity - (i === 0 ? 10000 : equityValues[i - 1]);

    // Create a mock position to inject equity
    const pos = legacy.getPosition('TEST');
    pos.realizedPnl = targetEquity - 10000;

    const pos2 = streaming.getPosition('TEST');
    pos2.realizedPnl = targetEquity - 10000;

    legacy.recordEquity(ts);
    streaming.recordEquity(ts);
  }

  // Calculate maxDD from legacy
  const legacySnapshot = legacy.snapshot();
  const legacyMaxDD = maxDrawdown(legacySnapshot.equityCurve);

  // Get maxDD from streaming
  const streamingSnapshot = streaming.snapshot();
  const streamingMaxDD = -streamingSnapshot.maxDrawdown;  // Convert to negative

  console.log(`  Legacy maxDD: ${legacyMaxDD.toFixed(4)} (${(legacyMaxDD * 100).toFixed(2)}%)`);
  console.log(`  Streaming maxDD: ${streamingMaxDD.toFixed(4)} (${(streamingMaxDD * 100).toFixed(2)}%)`);
  console.log(`  Peak equity: ${streamingSnapshot.peakEquity}`);

  assertClose(legacyMaxDD, streamingMaxDD, 0.0001, 'MaxDD should match');
  assert(streamingSnapshot.peakEquity === 12000, 'Peak equity should be 12000');

  console.log('  ✅ PASS');
}

// Test 2: No drawdown (monotonic increase)
function testNoDrawdown() {
  console.log('\n[TEST 2] No drawdown (monotonic increase)');

  const streaming = new ExecutionState(10000, { streamingMaxDD: true });

  // Equity only goes up: 10000 -> 11000 -> 12000 -> 13000
  const equityValues = [10000, 11000, 12000, 13000];

  for (let i = 0; i < equityValues.length; i++) {
    const ts = BigInt(1000000 + i * 1000);
    const pos = streaming.getPosition('TEST');
    pos.realizedPnl = equityValues[i] - 10000;
    streaming.recordEquity(ts);
  }

  const snapshot = streaming.snapshot();

  console.log(`  MaxDD: ${snapshot.maxDrawdown.toFixed(4)}`);
  console.log(`  Peak equity: ${snapshot.peakEquity}`);

  assert(snapshot.maxDrawdown === 0, 'MaxDD should be 0 for monotonic increase');
  assert(snapshot.peakEquity === 13000, 'Peak should be final equity');

  console.log('  ✅ PASS');
}

// Test 3: Multiple drawdowns (find maximum)
function testMultipleDrawdowns() {
  console.log('\n[TEST 3] Multiple drawdowns');

  // Scenario:
  // 10000 -> 12000 (peak1) -> 10800 (DD1 = 10%)
  // -> 14000 (peak2) -> 11200 (DD2 = 20%) <- MAX
  // -> 13000 (recover)

  const legacy = new ExecutionState(10000, { streamingMaxDD: false });
  const streaming = new ExecutionState(10000, { streamingMaxDD: true });

  const equityValues = [10000, 12000, 10800, 14000, 11200, 13000];

  for (let i = 0; i < equityValues.length; i++) {
    const ts = BigInt(1000000 + i * 1000);
    const pos = legacy.getPosition('TEST');
    pos.realizedPnl = equityValues[i] - 10000;
    legacy.recordEquity(ts);

    const pos2 = streaming.getPosition('TEST');
    pos2.realizedPnl = equityValues[i] - 10000;
    streaming.recordEquity(ts);
  }

  const legacySnapshot = legacy.snapshot();
  const streamingSnapshot = streaming.snapshot();

  const legacyMaxDD = maxDrawdown(legacySnapshot.equityCurve);
  const streamingMaxDD = -streamingSnapshot.maxDrawdown;

  console.log(`  Legacy maxDD: ${legacyMaxDD.toFixed(4)} (${(legacyMaxDD * 100).toFixed(2)}%)`);
  console.log(`  Streaming maxDD: ${streamingMaxDD.toFixed(4)} (${(streamingMaxDD * 100).toFixed(2)}%)`);
  console.log(`  Peak equity: ${streamingSnapshot.peakEquity}`);

  assertClose(legacyMaxDD, streamingMaxDD, 0.0001, 'MaxDD should match');
  assert(streamingSnapshot.peakEquity === 14000, 'Peak should be 14000');

  // Expected maxDD = (14000 - 11200) / 14000 = 0.20 = 20%
  assertClose(streamingMaxDD, -0.20, 0.01, 'MaxDD should be ~20%');

  console.log('  ✅ PASS');
}

// Test 4: Backward compatibility - metrics.js
function testBackwardCompatibility() {
  console.log('\n[TEST 4] Backward compatibility - metrics.js');

  // Legacy mode should still work
  const legacy = new ExecutionState(10000, { streamingMaxDD: false });
  const equityValues = [10000, 12000, 9000];

  for (let i = 0; i < equityValues.length; i++) {
    const ts = BigInt(1000000 + i * 1000);
    const pos = legacy.getPosition('TEST');
    pos.realizedPnl = equityValues[i] - 10000;
    legacy.recordEquity(ts);
  }

  const legacySnapshot = legacy.snapshot();
  const maxDD1 = maxDrawdown(legacySnapshot.equityCurve);
  const maxDD2 = maxDrawdown(legacySnapshot);  // Should also work

  console.log(`  maxDrawdown(equityCurve): ${maxDD1.toFixed(4)}`);
  console.log(`  maxDrawdown(snapshot): ${maxDD2.toFixed(4)}`);

  assertClose(maxDD1, maxDD2, 0.0001, 'Both calling patterns should work');

  console.log('  ✅ PASS');
}

// Test 5: Edge case - zero initial capital
function testZeroCapital() {
  console.log('\n[TEST 5] Edge case - zero capital');

  const streaming = new ExecutionState(0, { streamingMaxDD: true });
  streaming.recordEquity(BigInt(1000000));

  const snapshot = streaming.snapshot();

  console.log(`  MaxDD: ${snapshot.maxDrawdown}`);
  console.log(`  Peak equity: ${snapshot.peakEquity}`);

  // Should not crash, maxDD should be 0 (no drawdown possible from 0)
  assert(snapshot.maxDrawdown === 0, 'MaxDD should be 0 for zero capital');

  console.log('  ✅ PASS');
}

// Test 6: Memory comparison
function testMemoryFootprint() {
  console.log('\n[TEST 6] Memory footprint comparison');

  const legacy = new ExecutionState(10000, { streamingMaxDD: false });
  const streaming = new ExecutionState(10000, { streamingMaxDD: true });

  // Simulate 10,000 equity updates
  const numUpdates = 10000;

  for (let i = 0; i < numUpdates; i++) {
    const ts = BigInt(1000000 + i * 1000);
    const equity = 10000 + Math.sin(i / 100) * 1000;  // Oscillating equity

    const pos1 = legacy.getPosition('TEST');
    pos1.realizedPnl = equity - 10000;
    legacy.recordEquity(ts);

    const pos2 = streaming.getPosition('TEST');
    pos2.realizedPnl = equity - 10000;
    streaming.recordEquity(ts);
  }

  const legacySnapshot = legacy.snapshot();
  const streamingSnapshot = streaming.snapshot();

  // Estimate memory usage
  const legacyMemory = legacySnapshot.equityCurve.length * 24;  // 24 bytes per point (bigint + number)
  const streamingMemory = 24;  // 3 numbers (peak, maxDD, current) = 24 bytes

  console.log(`  Legacy equity curve: ${legacySnapshot.equityCurve.length} points`);
  console.log(`  Legacy memory: ~${(legacyMemory / 1024).toFixed(2)} KB`);
  console.log(`  Streaming memory: ~${streamingMemory} bytes`);
  console.log(`  Reduction: ${((1 - streamingMemory / legacyMemory) * 100).toFixed(2)}%`);

  assert(legacySnapshot.equityCurve.length === numUpdates, 'Legacy should have all points');
  assert(!streamingSnapshot.equityCurve || streamingSnapshot.equityCurve.length === 0, 'Streaming should have no equity curve');
  assert(streamingSnapshot.maxDrawdown !== undefined, 'Streaming should have pre-computed maxDD');

  console.log('  ✅ PASS');
}

// Run all tests
async function runTests() {
  console.log('=== Streaming MaxDrawdown Unit Tests ===');

  try {
    testSimpleDrawdown();
    testNoDrawdown();
    testMultipleDrawdowns();
    testBackwardCompatibility();
    testZeroCapital();
    testMemoryFootprint();

    console.log('\n✅ All tests passed!');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

runTests();
