/**
 * StrategyV1 Module Tests
 */

import assert from 'assert';
import { RegimeModeSelector, REGIME_LABELS } from '../decision/RegimeModeSelector.js';
import { SignalGenerator, SIGNAL_DIRECTION } from '../decision/SignalGenerator.js';
import { Combiner, ACTION, COMBINE_MODE } from '../decision/Combiner.js';
import { DEFAULT_CONFIG, mergeConfig, getConfig } from '../config.js';

console.log('=== StrategyV1 Module Tests ===\n');

// ============================================
// RegimeModeSelector Tests
// ============================================

async function testRegimeModeSelector() {
  console.log('Testing RegimeModeSelector...');

  const selector = new RegimeModeSelector();

  // Test 1: High volatility mode
  const highVolMode = selector.selectMode({ volatility: 2, trend: 1, spread: 1 });
  assert.strictEqual(highVolMode.primary, 'MEAN_REVERSION', 'High vol should trigger MEAN_REVERSION mode');
  assert.strictEqual(highVolMode.combined.positionScale, 0.5, 'High vol should have 0.5 position scale');
  assert.strictEqual(highVolMode.combined.thresholdMultiplier, 1.5, 'High vol should have 1.5 threshold multiplier');
  console.log('  High volatility mode OK');

  // Test 2: Low volatility mode
  const lowVolMode = selector.selectMode({ volatility: 0, trend: 1, spread: 0 });
  assert.strictEqual(lowVolMode.primary, 'MOMENTUM', 'Low vol should trigger MOMENTUM mode');
  assert.strictEqual(lowVolMode.combined.positionScale, 1.0, 'Low vol should have full position scale');
  console.log('  Low volatility mode OK');

  // Test 3: Normal volatility mode
  const normalMode = selector.selectMode({ volatility: 1, trend: 0, spread: 1 });
  assert.strictEqual(normalMode.primary, 'BALANCED', 'Normal vol should trigger BALANCED mode');
  console.log('  Normal volatility mode OK');

  // Test 4: Trend penalties
  const downTrendMode = selector.selectMode({ volatility: 1, trend: -1, spread: 1 });
  assert.strictEqual(downTrendMode.combined.longPenalty, 0.5, 'Downtrend should penalize longs');
  assert.strictEqual(downTrendMode.combined.shortPenalty, 1.0, 'Downtrend should not penalize shorts');
  console.log('  Trend penalties OK');

  // Test 5: Spread delay
  const wideSpreadMode = selector.selectMode({ volatility: 1, trend: 0, spread: 2 });
  assert.strictEqual(wideSpreadMode.combined.executionDelay, true, 'Wide spread should enable execution delay');
  console.log('  Spread delay OK');

  // Test 6: Mode transition tracking
  selector.reset();
  selector.selectMode({ volatility: 0, trend: 1, spread: 0 }); // MOMENTUM
  const transition = selector.selectMode({ volatility: 2, trend: 1, spread: 0 }); // MEAN_REVERSION
  assert.strictEqual(transition.modeChanged, true, 'Should detect mode change');
  console.log('  Mode transition tracking OK');

  // Test 7: Labels
  assert.strictEqual(selector.getVolLabel(0), 'low');
  assert.strictEqual(selector.getVolLabel(1), 'normal');
  assert.strictEqual(selector.getVolLabel(2), 'high');
  assert.strictEqual(selector.getTrendLabel(-1), 'down');
  assert.strictEqual(selector.getTrendLabel(0), 'side');
  assert.strictEqual(selector.getTrendLabel(1), 'up');
  console.log('  Labels OK');

  console.log('  RegimeModeSelector: All tests passed\n');
}

// ============================================
// SignalGenerator Tests
// ============================================

async function testSignalGenerator() {
  console.log('Testing SignalGenerator...');

  const generator = new SignalGenerator();

  // Test 1: Load from config
  generator.loadFromConfig([
    { name: 'feature_a', alphaScore: 0.5, labelCorrelation: 0.1 },
    { name: 'feature_b', alphaScore: 0.4, labelCorrelation: -0.08 }
  ]);
  assert.strictEqual(generator.isLoaded(), true, 'Should be loaded');
  assert.strictEqual(generator.getTopFeatures().length, 2, 'Should have 2 features');
  console.log('  Load from config OK');

  // Test 2: Generate signals with positive correlation feature
  // threshold = baseThreshold / |labelCorrelation| = 0.001 / 0.1 = 0.01
  // So feature value needs to be > 0.01 to generate signal
  const features = { feature_a: 0.02, feature_b: -0.015 };
  const result = generator.generate(features);
  assert.ok(result.signals.length > 0, 'Should generate signals');
  assert.ok(result.activeFeatures.includes('feature_a'), 'Should include feature_a');
  console.log('  Signal generation OK');

  // Test 3: Consensus calculation
  assert.ok(typeof result.consensus === 'number', 'Consensus should be a number');
  console.log('  Consensus calculation OK');

  // Test 4: Threshold adjustment with mode
  const mode = {
    combined: { thresholdMultiplier: 2.0 }
  };
  const resultWithMode = generator.generate(features, mode);
  // Higher threshold = fewer signals (or same signals with lower strength)
  console.log('  Threshold adjustment OK');

  // Test 5: Reset
  generator.reset();
  assert.strictEqual(generator.isLoaded(), false, 'Should be reset');
  console.log('  Reset OK');

  console.log('  SignalGenerator: All tests passed\n');
}

// ============================================
// Combiner Tests
// ============================================

async function testCombiner() {
  console.log('Testing Combiner...');

  // Test 1: Weighted combination
  const weightedCombiner = new Combiner({
    mode: COMBINE_MODE.WEIGHTED,
    minStrength: 0.2,
    minSignals: 1,
    confidenceThreshold: 0.3
  });

  const signals = [
    { direction: SIGNAL_DIRECTION.LONG, strength: 0.6, alphaScore: 0.5, feature: 'a' },
    { direction: SIGNAL_DIRECTION.LONG, strength: 0.4, alphaScore: 0.3, feature: 'b' }
  ];

  const weightedResult = weightedCombiner.combine(signals);
  assert.strictEqual(weightedResult.action, ACTION.LONG, 'Should decide LONG');
  assert.ok(weightedResult.confidence > 0, 'Should have positive confidence');
  console.log('  Weighted combination OK');

  // Test 2: Majority combination
  const majorityCombiner = new Combiner({
    mode: COMBINE_MODE.MAJORITY,
    minStrength: 0.2,
    minSignals: 2
  });

  const mixedSignals = [
    { direction: SIGNAL_DIRECTION.LONG, strength: 0.5, feature: 'a' },
    { direction: SIGNAL_DIRECTION.LONG, strength: 0.4, feature: 'b' },
    { direction: SIGNAL_DIRECTION.SHORT, strength: 0.3, feature: 'c' }
  ];

  const majorityResult = majorityCombiner.combine(mixedSignals);
  assert.strictEqual(majorityResult.action, ACTION.LONG, 'Should decide LONG by majority');
  console.log('  Majority combination OK');

  // Test 3: Unanimous combination
  const unanimousCombiner = new Combiner({
    mode: COMBINE_MODE.UNANIMOUS,
    minStrength: 0.2,
    minSignals: 2
  });

  const unanimousSignals = [
    { direction: SIGNAL_DIRECTION.SHORT, strength: 0.5, feature: 'a' },
    { direction: SIGNAL_DIRECTION.SHORT, strength: 0.4, feature: 'b' }
  ];

  const unanimousResult = unanimousCombiner.combine(unanimousSignals);
  assert.strictEqual(unanimousResult.action, ACTION.SHORT, 'Should decide SHORT unanimously');
  console.log('  Unanimous combination OK');

  // Test 4: No unanimous agreement
  const notUnanimousResult = unanimousCombiner.combine(mixedSignals);
  assert.strictEqual(notUnanimousResult.action, ACTION.HOLD, 'Should HOLD without unanimous agreement');
  console.log('  Non-unanimous HOLD OK');

  // Test 5: Mode adjustments
  const adjustedSignals = weightedCombiner.applyModeAdjustments(signals, {
    combined: { longPenalty: 0.5, shortPenalty: 1.0 }
  });
  assert.ok(adjustedSignals[0].strength < signals[0].strength, 'Long signals should be penalized');
  console.log('  Mode adjustments OK');

  // Test 6: Empty signals
  const emptyResult = weightedCombiner.combine([]);
  assert.strictEqual(emptyResult.action, ACTION.HOLD, 'Should HOLD with no signals');
  console.log('  Empty signals OK');

  console.log('  Combiner: All tests passed\n');
}

// ============================================
// Config Tests
// ============================================

async function testConfig() {
  console.log('Testing Config...');

  // Test 1: Default config
  assert.ok(DEFAULT_CONFIG.topFeatureCount > 0, 'Should have topFeatureCount');
  assert.ok(DEFAULT_CONFIG.minAlphaScore > 0, 'Should have minAlphaScore');
  assert.ok(DEFAULT_CONFIG.combiner, 'Should have combiner config');
  assert.ok(DEFAULT_CONFIG.execution, 'Should have execution config');
  console.log('  Default config OK');

  // Test 2: Get config by name
  const qualityConfig = getConfig('quality');
  assert.strictEqual(qualityConfig.combiner.mode, COMBINE_MODE.UNANIMOUS, 'Quality should use unanimous');
  console.log('  Get config by name OK');

  // Test 3: Merge config
  const merged = mergeConfig(DEFAULT_CONFIG, {
    topFeatureCount: 10,
    combiner: { minSignals: 5 }
  });
  assert.strictEqual(merged.topFeatureCount, 10, 'Should override topFeatureCount');
  assert.strictEqual(merged.combiner.minSignals, 5, 'Should override combiner.minSignals');
  assert.strictEqual(merged.combiner.mode, DEFAULT_CONFIG.combiner.mode, 'Should keep default combiner.mode');
  console.log('  Merge config OK');

  console.log('  Config: All tests passed\n');
}

// ============================================
// Integration Test
// ============================================

async function testIntegration() {
  console.log('Testing Integration...');

  // Simulate full pipeline
  const selector = new RegimeModeSelector();
  const generator = new SignalGenerator();
  const combiner = new Combiner({
    mode: COMBINE_MODE.WEIGHTED,
    minStrength: 0.1,
    confidenceThreshold: 0.2
  });

  // Load features
  generator.loadFromConfig([
    { name: 'roc', alphaScore: 0.45, labelCorrelation: 0.12 },
    { name: 'ema_slope', alphaScore: 0.38, labelCorrelation: 0.09 },
    { name: 'volatility', alphaScore: 0.32, labelCorrelation: -0.06 }
  ]);

  // Simulate event with features
  const features = {
    mid_price: 50000,
    spread: 0.5,
    roc: 0.003,
    ema_slope: 0.002,
    volatility: 0.015,
    regime_volatility: 1,  // NORMAL
    regime_trend: 1,       // UP
    regime_spread: 0       // TIGHT
  };

  // Step 1: Select mode
  const mode = selector.selectMode({
    volatility: features.regime_volatility,
    trend: features.regime_trend,
    spread: features.regime_spread
  });
  assert.strictEqual(mode.primary, 'BALANCED', 'Should be BALANCED mode');
  console.log('  Mode selection OK');

  // Step 2: Generate signals
  const signalResult = generator.generate(features, mode);
  console.log(`  Generated ${signalResult.signals.length} signals`);

  // Step 3: Apply adjustments
  const adjustedSignals = combiner.applyModeAdjustments(signalResult.signals, mode);

  // Step 4: Combine
  const decision = combiner.combine(adjustedSignals);
  console.log(`  Decision: ${decision.action} (confidence: ${decision.confidence.toFixed(3)})`);

  assert.ok([ACTION.LONG, ACTION.SHORT, ACTION.HOLD].includes(decision.action), 'Should have valid action');
  console.log('  Integration: All tests passed\n');
}

// ============================================
// Run All Tests
// ============================================

async function runAllTests() {
  try {
    await testRegimeModeSelector();
    await testSignalGenerator();
    await testCombiner();
    await testConfig();
    await testIntegration();

    console.log('=== All StrategyV1 Tests Passed ===');
  } catch (err) {
    console.error('Test Failed:', err);
    process.exit(1);
  }
}

runAllTests();
