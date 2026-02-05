#!/usr/bin/env node
/**
 * SignalGate Unit Tests
 *
 * Tests gate rules independently
 */

import { SignalGate, GATE_RULE } from './SignalGate.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

function testRegimeGate() {
  console.log('\n=== Test 1: Regime Gate ===');

  const gate = new SignalGate({
    regimeTrendMin: -0.5,
    regimeVolMin: 0,
    regimeSpreadMax: 2,
    minSignalScore: 0.5,
    cooldownMs: 5000,
    maxSpreadNormalized: 0.001
  });

  // Test 1a: Good regime - should pass
  let result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === true, 'Good regime should pass');
  console.log('✓ Good regime passes');

  // Test 1b: Bad trend - should block
  result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: -1, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === false, 'Bad trend should block');
  assert(result.reason.includes(GATE_RULE.REGIME_TREND), 'Should mention trend rule');
  console.log('✓ Bad trend blocks');

  // Test 1c: Wide spread regime - should block
  result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 3 },  // VERY_WIDE
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === false, 'Wide spread regime should block');
  assert(result.reason.includes(GATE_RULE.REGIME_SPREAD), 'Should mention spread rule');
  console.log('✓ Wide spread regime blocks');
}

function testSignalStrength() {
  console.log('\n=== Test 2: Signal Strength Gate ===');

  const gate = new SignalGate({
    minSignalScore: 0.6,
    cooldownMs: 0,  // Disable cooldown for test
    maxSpreadNormalized: 1.0  // Disable spread check
  });

  // Test 2a: High confidence - should pass
  let result = gate.evaluate({
    signalScore: 0.8,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === true, 'High confidence should pass');
  console.log('✓ High confidence passes');

  // Test 2b: Low confidence - should block
  result = gate.evaluate({
    signalScore: 0.4,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === false, 'Low confidence should block');
  assert(result.reason.includes(GATE_RULE.SIGNAL_STRENGTH), 'Should mention signal strength');
  console.log('✓ Low confidence blocks');
}

function testCooldown() {
  console.log('\n=== Test 3: Cooldown Gate ===');

  const gate = new SignalGate({
    minSignalScore: 0.5,
    cooldownMs: 5000,
    maxSpreadNormalized: 1.0
  });

  const now = Date.now();
  const lastTrade = now - 2000;  // 2 seconds ago

  // Test 3a: Within cooldown - should block
  let result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: lastTrade,
    now: now
  });
  assert(result.allow === false, 'Within cooldown should block');
  assert(result.reason.includes(GATE_RULE.COOLDOWN), 'Should mention cooldown');
  console.log('✓ Within cooldown blocks');

  // Test 3b: After cooldown - should pass
  const laterTime = lastTrade + 6000;  // 6 seconds after last trade
  result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: lastTrade,
    now: laterTime
  });
  assert(result.allow === true, 'After cooldown should pass');
  console.log('✓ After cooldown passes');
}

function testSpreadPenalty() {
  console.log('\n=== Test 4: Spread Penalty Gate ===');

  const gate = new SignalGate({
    minSignalScore: 0.5,
    cooldownMs: 0,
    maxSpreadNormalized: 0.001  // 0.1% max spread
  });

  // Test 4a: Tight spread - should pass
  let result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.00005, mid_price: 1.0 },  // 0.005%
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === true, 'Tight spread should pass');
  console.log('✓ Tight spread passes');

  // Test 4b: Wide spread - should block
  result = gate.evaluate({
    signalScore: 0.7,
    features: { spread: 0.005, mid_price: 1.0 },  // 0.5%
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === false, 'Wide spread should block');
  assert(result.reason.includes(GATE_RULE.SPREAD_PENALTY), 'Should mention spread penalty');
  console.log('✓ Wide spread blocks');
}

function testStatistics() {
  console.log('\n=== Test 5: Statistics Tracking ===');

  const gate = new SignalGate({
    minSignalScore: 0.6,
    cooldownMs: 0,
    maxSpreadNormalized: 1.0
  });

  // Pass 2, block 3
  for (let i = 0; i < 5; i++) {
    gate.evaluate({
      signalScore: i < 2 ? 0.7 : 0.4,  // First 2 pass, rest block
      features: { spread: 0.0001, mid_price: 1.0 },
      regime: { trend: 0, volatility: 1, spread: 1 },
      mode: {},
      lastTradeTime: null,
      now: Date.now()
    });
  }

  const stats = gate.getStats();
  assert(stats.passed === 2, 'Should have 2 passes');
  assert(stats.blocked === 3, 'Should have 3 blocks');
  assert(stats.total === 5, 'Should have 5 total');
  console.log('✓ Statistics tracked correctly:', stats);

  // Test reset
  gate.resetStats();
  const resetStats = gate.getStats();
  assert(resetStats.total === 0, 'Stats should reset to 0');
  console.log('✓ Reset works');
}

function testConfigUpdate() {
  console.log('\n=== Test 6: Runtime Config Update ===');

  const gate = new SignalGate({
    minSignalScore: 0.6
  });

  // Initially blocks low confidence
  let result = gate.evaluate({
    signalScore: 0.5,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === false, 'Should block with initial config');

  // Update config
  gate.updateConfig({ minSignalScore: 0.4 });

  // Now passes
  result = gate.evaluate({
    signalScore: 0.5,
    features: { spread: 0.0001, mid_price: 1.0 },
    regime: { trend: 0, volatility: 1, spread: 1 },
    mode: {},
    lastTradeTime: null,
    now: Date.now()
  });
  assert(result.allow === true, 'Should pass with updated config');
  console.log('✓ Runtime config update works');
}

async function main() {
  console.log('=================================');
  console.log('SignalGate Unit Tests');
  console.log('=================================');

  try {
    testRegimeGate();
    testSignalStrength();
    testCooldown();
    testSpreadPenalty();
    testStatistics();
    testConfigUpdate();

    console.log('\n=================================');
    console.log('✅ All tests passed!');
    console.log('=================================\n');
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    process.exit(1);
  }
}

main();
