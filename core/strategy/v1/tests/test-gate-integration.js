#!/usr/bin/env node
/**
 * SignalGate Integration Test with StrategyV1
 *
 * Tests that gate properly filters trades in strategy context
 */

import { StrategyV1 } from '../StrategyV1.js';
import { DEFAULT_CONFIG } from '../config.js';

function createMockContext() {
  const orders = [];
  const logs = [];

  return {
    symbol: 'btcusdt',
    logger: {
      info: (...args) => logs.push(['INFO', ...args]),
      warn: (...args) => logs.push(['WARN', ...args]),
      debug: (...args) => logs.push(['DEBUG', ...args])
    },
    placeOrder: (order) => {
      orders.push(order);
    },
    orders,
    logs
  };
}

function createMockBBOEvent(ts, bid, ask) {
  return {
    ts,
    ts_event: ts,
    symbol: 'btcusdt',
    bid_price: bid,
    bid_qty: 1.0,
    ask_price: ask,
    ask_qty: 1.0
  };
}

async function testGateDisabled() {
  console.log('\n=== Test 1: Gate Disabled (Baseline) ===');

  const config = {
    ...DEFAULT_CONFIG,
    gate: { enabled: false },
    execution: { minConfidence: 0.3 },  // Very low to allow trades
    featureReportPath: null  // Use fallback features
  };

  const strategy = new StrategyV1(config);
  const ctx = createMockContext();

  await strategy.onStart(ctx);

  // Generate 100 events with varying prices (should trigger signals)
  for (let i = 0; i < 100; i++) {
    const bid = 50000 + Math.sin(i / 10) * 100;
    const ask = bid + 1;
    const event = createMockBBOEvent(Date.now() + i * 1000, bid, ask);
    await strategy.onEvent(event, ctx);
  }

  await strategy.onEnd(ctx);

  const state = strategy.getState();
  console.log(`Total trades: ${ctx.orders.length}`);
  console.log(`Total signals: ${state.signalCount}`);
  console.log(`Gate stats: ${state.gateStats ? 'enabled' : 'disabled'}`);

  if (ctx.orders.length > 0) {
    console.log('✓ Strategy generates trades when gate disabled');
  }

  return { trades: ctx.orders.length, signals: state.signalCount };
}

async function testGateEnabled() {
  console.log('\n=== Test 2: Gate Enabled (Strict) ===');

  const config = {
    ...DEFAULT_CONFIG,
    gate: {
      enabled: true,
      minSignalScore: 0.7,      // High threshold
      cooldownMs: 10000,        // 10 second cooldown
      maxSpreadNormalized: 0.0005
    },
    execution: { minConfidence: 0.3 },  // Lower than gate threshold
    featureReportPath: null
  };

  const strategy = new StrategyV1(config);
  const ctx = createMockContext();

  await strategy.onStart(ctx);

  // Generate same events
  for (let i = 0; i < 100; i++) {
    const bid = 50000 + Math.sin(i / 10) * 100;
    const ask = bid + 1;
    const event = createMockBBOEvent(Date.now() + i * 1000, bid, ask);
    await strategy.onEvent(event, ctx);
  }

  await strategy.onEnd(ctx);

  const state = strategy.getState();
  console.log(`Total trades: ${ctx.orders.length}`);
  console.log(`Total signals: ${state.signalCount}`);

  if (state.gateStats) {
    console.log('Gate statistics:', state.gateStats);
    console.log('✓ Gate statistics tracked');
  }

  return { trades: ctx.orders.length, signals: state.signalCount, gateStats: state.gateStats };
}

async function testCooldownEffect() {
  console.log('\n=== Test 3: Cooldown Effect ===');

  const config = {
    ...DEFAULT_CONFIG,
    gate: {
      enabled: true,
      minSignalScore: 0.5,
      cooldownMs: 5000,         // 5 second cooldown
      maxSpreadNormalized: 1.0  // Permissive
    },
    execution: { minConfidence: 0.5 },
    featureReportPath: null
  };

  const strategy = new StrategyV1(config);
  const ctx = createMockContext();

  await strategy.onStart(ctx);

  // Generate events with 1 second spacing (should trigger cooldown)
  const baseTime = Date.now();
  for (let i = 0; i < 50; i++) {
    const bid = 50000 + Math.random() * 100;
    const ask = bid + 0.5;
    const event = createMockBBOEvent(baseTime + i * 1000, bid, ask);  // 1s apart
    await strategy.onEvent(event, ctx);
  }

  await strategy.onEnd(ctx);

  const state = strategy.getState();
  console.log(`Total trades: ${ctx.orders.length}`);
  console.log(`Total signals: ${state.signalCount}`);

  if (state.gateStats) {
    console.log('Gate statistics:', state.gateStats);

    // Check if cooldown blocked trades
    const cooldownBlocks = Object.entries(state.gateStats.blockReasons)
      .filter(([reason]) => reason.includes('cooldown'))
      .reduce((sum, [, count]) => sum + count, 0);

    if (cooldownBlocks > 0) {
      console.log(`✓ Cooldown blocked ${cooldownBlocks} trades`);
    }
  }

  return { trades: ctx.orders.length, cooldownBlocks: state.gateStats };
}

async function main() {
  console.log('=================================');
  console.log('SignalGate Integration Tests');
  console.log('=================================');

  try {
    const baseline = await testGateDisabled();
    const gated = await testGateEnabled();
    const cooldown = await testCooldownEffect();

    console.log('\n=== Summary ===');
    console.log('Baseline (no gate):', baseline);
    console.log('Gated (strict):', { trades: gated.trades, signals: gated.signals });
    console.log('Cooldown test:', { trades: cooldown.trades });

    // Verify gate reduces trade frequency
    if (baseline.trades > 0 && gated.gateStats) {
      const reduction = ((baseline.trades - gated.trades) / baseline.trades * 100).toFixed(1);
      console.log(`\nGate reduced trades by ${reduction}%`);
      console.log('Gate pass rate:', gated.gateStats.passRate);
    }

    console.log('\n=================================');
    console.log('✅ Integration tests completed!');
    console.log('=================================\n');
  } catch (error) {
    console.error('\n❌ Integration test failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

main();
