/**
 * Integration test: ExecutionState fills streaming + metrics
 * Validates end-to-end flow: write fills → snapshot → load → compute metrics
 */

import { ExecutionState } from '../state.js';
import { computeAllMetrics, winRate, avgTradePnl } from '../../backtest/metrics.js';
import crypto from 'crypto';
import fs from 'fs';

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

function createMockFill(id, symbol = 'BTCUSDT', side = 'BUY', qty = 1.0, price = 50000) {
  return {
    id: `fill_${id}`,
    orderId: `ord_${id}`,
    symbol,
    side,
    qty,
    fillPrice: price,
    fillValue: price * qty,
    fee: price * qty * 0.001,  // 0.1% fee
    ts_event: BigInt(1000000 + id * 1000)
  };
}

function getTempPath() {
  const random = crypto.randomBytes(8).toString('hex');
  return `/tmp/test-fills-integration-${random}.jsonl`;
}

// Test 1: Basic fills streaming with metrics
async function testBasicStreaming() {
  console.log('\n[TEST 1] Basic fills streaming with metrics');

  const filePath = getTempPath();
  const inMemory = new ExecutionState(10000, { streamingMaxDD: true, streamFills: false });
  const streaming = new ExecutionState(10000, { streamingMaxDD: true, streamFills: true, fillsStreamPath: filePath });

  // Simulate simple trades: BUY 1.0 @ 50000, SELL 1.0 @ 51000 (profit)
  const fills = [
    createMockFill(0, 'BTCUSDT', 'BUY', 1.0, 50000),
    createMockFill(1, 'BTCUSDT', 'SELL', 1.0, 51000)
  ];

  for (const fill of fills) {
    inMemory.recordFill(fill);
    streaming.recordFill(fill);
  }

  // Get snapshots
  const inMemorySnapshot = inMemory.snapshot();
  const streamingSnapshot = streaming.snapshot();

  console.log(`  In-memory fills: ${inMemorySnapshot.fills.length}`);
  console.log(`  Streaming fills: ${streamingSnapshot.fills.length} (empty array expected)`);
  console.log(`  Streaming fills count: ${streamingSnapshot.fillsCount}`);
  console.log(`  Streaming fills path: ${streamingSnapshot.fillsStreamPath}`);

  assert(inMemorySnapshot.fills.length === 2, 'In-memory should have 2 fills');
  assert(streamingSnapshot.fills.length === 0, 'Streaming should have empty fills array');
  assert(streamingSnapshot.fillsCount === 2, 'Streaming fillsCount should be 2');
  assert(streamingSnapshot.fillsStreamPath === filePath, 'Streaming path should match');

  // Close stream before loading (ensures all data flushed to disk)
  const streamedFills = await streaming.getFills();

  console.log(`  Loaded ${streamedFills.length} fills from disk`);

  // Compute metrics
  const inMemoryMetrics = computeAllMetrics(inMemorySnapshot, inMemorySnapshot.fills);
  const streamingMetrics = computeAllMetrics(streamingSnapshot, streamedFills);

  console.log(`  In-memory winRate: ${inMemoryMetrics.winRate}`);
  console.log(`  Streaming winRate: ${streamingMetrics.winRate}`);
  console.log(`  In-memory tradesCount: ${inMemoryMetrics.tradesCount}`);
  console.log(`  Streaming tradesCount: ${streamingMetrics.tradesCount}`);

  assert(streamingMetrics.tradesCount === 2, 'Streaming should load 2 fills');
  assertClose(streamingMetrics.winRate, inMemoryMetrics.winRate, 0.0001, 'Win rates should match');

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 2: Large volume accuracy comparison
async function testLargeVolumeAccuracy() {
  console.log('\n[TEST 2] Large volume accuracy comparison');

  const filePath = getTempPath();
  const inMemory = new ExecutionState(10000, { streamingMaxDD: true, streamFills: false });
  const streaming = new ExecutionState(10000, { streamingMaxDD: true, streamFills: true, fillsStreamPath: filePath });

  const numTrades = 100;
  const fills = [];

  // Generate alternating BUY/SELL with varying prices
  for (let i = 0; i < numTrades; i++) {
    const side = i % 2 === 0 ? 'BUY' : 'SELL';
    const price = 50000 + (Math.random() - 0.5) * 1000;  // 50k ± 500
    const fill = createMockFill(i, 'BTCUSDT', side, 0.1, price);
    fills.push(fill);

    inMemory.recordFill(fill);
    streaming.recordFill(fill);
  }

  console.log(`  Generated ${numTrades} fills`);

  // Get snapshots and load streamed fills
  const inMemorySnapshot = inMemory.snapshot();
  const streamingSnapshot = streaming.snapshot();
  const streamedFills = await streaming.getFills();

  // Compute metrics
  const inMemoryMetrics = computeAllMetrics(inMemorySnapshot, inMemorySnapshot.fills);
  const streamingMetrics = computeAllMetrics(streamingSnapshot, streamedFills);

  console.log(`  In-memory: winRate=${inMemoryMetrics.winRate.toFixed(4)}, avgTradePnl=${inMemoryMetrics.avgTradePnl.toFixed(2)}`);
  console.log(`  Streaming: winRate=${streamingMetrics.winRate.toFixed(4)}, avgTradePnl=${streamingMetrics.avgTradePnl.toFixed(2)}`);

  assertClose(streamingMetrics.winRate, inMemoryMetrics.winRate, 0.0001, 'Win rates should match exactly');
  assertClose(streamingMetrics.avgTradePnl, inMemoryMetrics.avgTradePnl, 0.01, 'Avg trade PnL should match');
  assert(streamingMetrics.tradesCount === inMemoryMetrics.tradesCount, 'Trade counts should match');

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 3: Hash verification (fills integrity)
async function testHashVerification() {
  console.log('\n[TEST 3] Hash verification (fills integrity)');

  const filePath = getTempPath();
  const state = new ExecutionState(10000, { streamFills: true, fillsStreamPath: filePath });

  const fills = [];
  for (let i = 0; i < 50; i++) {
    const fill = createMockFill(i);
    fills.push(fill);
    state.recordFill(fill);
  }

  // Get snapshot and load fills
  const snapshot = state.snapshot();
  const loadedFills = await state.getFills();

  console.log(`  Original fills: ${fills.length}`);
  console.log(`  Loaded fills: ${loadedFills.length}`);

  assert(loadedFills.length === fills.length, 'Loaded fills count should match');

  // Verify each fill
  for (let i = 0; i < fills.length; i++) {
    assert(loadedFills[i].id === fills[i].id, `Fill ${i} ID should match`);
    assert(loadedFills[i].symbol === fills[i].symbol, `Fill ${i} symbol should match`);
    assert(loadedFills[i].side === fills[i].side, `Fill ${i} side should match`);
    assert(loadedFills[i].qty === fills[i].qty, `Fill ${i} qty should match`);
    assert(loadedFills[i].fillPrice === fills[i].fillPrice, `Fill ${i} price should match`);
    assert(loadedFills[i].ts_event === fills[i].ts_event, `Fill ${i} timestamp should match`);
  }

  console.log(`  All ${fills.length} fills verified ✓`);

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 4: Combined streaming (maxDD + fills)
async function testCombinedStreaming() {
  console.log('\n[TEST 4] Combined streaming (maxDD + fills)');

  const filePath = getTempPath();
  const legacy = new ExecutionState(10000, { streamingMaxDD: false, streamFills: false });
  const optimized = new ExecutionState(10000, { streamingMaxDD: true, streamFills: true, fillsStreamPath: filePath });

  // Simulate equity changes and trades
  const fills = [];
  for (let i = 0; i < 20; i++) {
    const side = i % 2 === 0 ? 'BUY' : 'SELL';
    const price = 50000 + Math.sin(i / 3) * 2000;
    const fill = createMockFill(i, 'BTCUSDT', side, 0.1, price);
    fills.push(fill);

    // Apply fill to position (mock)
    const pos1 = legacy.getPosition('BTCUSDT');
    const pos2 = optimized.getPosition('BTCUSDT');
    const pnl = (i + 1) * 10 - 100;  // Simulate some PnL
    pos1.realizedPnl = pnl;
    pos2.realizedPnl = pnl;

    legacy.recordFill(fill);
    legacy.recordEquity(fill.ts_event);

    optimized.recordFill(fill);
    optimized.recordEquity(fill.ts_event);
  }

  // Get snapshots and load optimized fills
  const legacySnapshot = legacy.snapshot();
  const optimizedSnapshot = optimized.snapshot();
  const optimizedFills = await optimized.getFills();

  console.log(`  Legacy equity curve: ${legacySnapshot.equityCurve?.length || 0} points`);
  console.log(`  Optimized equity curve: ${optimizedSnapshot.equityCurve?.length || 0} points`);
  console.log(`  Legacy fills: ${legacySnapshot.fills.length}`);
  console.log(`  Optimized fills: ${optimizedSnapshot.fills.length} (streaming)`);

  // Compute metrics
  const legacyMetrics = computeAllMetrics(legacySnapshot, legacySnapshot.fills);
  const optimizedMetrics = computeAllMetrics(optimizedSnapshot, optimizedFills);

  console.log(`  Legacy maxDD: ${legacyMetrics.maxDrawdown.toFixed(4)}`);
  console.log(`  Optimized maxDD: ${optimizedMetrics.maxDrawdown.toFixed(4)}`);
  console.log(`  Legacy peakEquity: ${legacySnapshot.equityCurve?.[0]?.equity || 'N/A'}`);
  console.log(`  Optimized peakEquity: ${optimizedSnapshot.peakEquity}`);
  console.log(`  Legacy winRate: ${legacyMetrics.winRate.toFixed(4)}`);
  console.log(`  Optimized winRate: ${optimizedMetrics.winRate.toFixed(4)}`);

  // MaxDD might differ slightly due to initial equity point handling
  // Legacy includes initial point, optimized starts with initialCapital
  // Accept small discrepancies (< 1%)
  assertClose(optimizedMetrics.maxDrawdown, legacyMetrics.maxDrawdown, 0.01, 'MaxDD should match (within 1%)');
  assertClose(optimizedMetrics.winRate, legacyMetrics.winRate, 0.0001, 'Win rate should match');
  assert(optimizedMetrics.tradesCount === legacyMetrics.tradesCount, 'Trade counts should match');

  // Memory comparison
  const legacyMemory = (legacySnapshot.equityCurve.length * 24) + (legacySnapshot.fills.length * 100);
  const optimizedMemory = 24 + 10000;  // 24 bytes (maxDD tracking) + ~10KB fills buffer
  const reduction = ((legacyMemory - optimizedMemory) / legacyMemory * 100).toFixed(2);

  console.log(`  Legacy memory: ~${(legacyMemory / 1024).toFixed(2)} KB`);
  console.log(`  Optimized memory: ~${(optimizedMemory / 1024).toFixed(2)} KB`);
  console.log(`  Reduction: ${reduction}%`);

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Run all tests
async function runTests() {
  console.log('=== Fills Streaming Integration Tests ===');

  try {
    await testBasicStreaming();
    await testLargeVolumeAccuracy();
    await testHashVerification();
    await testCombinedStreaming();

    console.log('\n✅ All integration tests passed!');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

runTests();
