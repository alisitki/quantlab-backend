/**
 * test-decision-loader.js: Unit tests for DecisionLoader and applyDecision
 */
import { validateDecisionConfig, clearCache, getCacheStats } from '../decision/DecisionLoader.js';
import { applyDecision, getProbaStats } from '../decision/applyDecision.js';

function assertEqual(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${expected}, got ${actual}`);
  }
}

function assertNull(actual, msg) {
  if (actual !== null) {
    throw new Error(`${msg}: expected null, got ${actual}`);
  }
}

async function runTests() {
  console.log('--- DecisionLoader Unit Tests ---\n');
  let passed = 0;
  let failed = 0;

  // Test 1: validateDecisionConfig with valid config
  console.log('1. Testing validateDecisionConfig with valid config...');
  try {
    const validConfig = {
      symbol: 'btcusdt',
      featuresetVersion: 'v1',
      labelHorizonSec: 10,
      primaryMetric: 'f1_pos',
      bestThreshold: 0.55,
      thresholdGrid: [0.5, 0.55, 0.6],
      probaSource: 'pseudo_sigmoid',
      jobId: 'job-123',
      createdAt: '2025-01-01T00:00:00Z',
      configHash: 'abc123'
    };
    
    const error = validateDecisionConfig(validConfig);
    assertNull(error, 'Valid config should return null');
    
    console.log('✅ Test 1 PASSED: Valid config validation');
    passed++;
  } catch (err) {
    console.error('❌ Test 1 FAILED:', err.message);
    failed++;
  }

  // Test 2: validateDecisionConfig with invalid config (missing symbol)
  console.log('\n2. Testing validateDecisionConfig with missing symbol...');
  try {
    const invalidConfig = {
      bestThreshold: 0.5,
      thresholdGrid: [0.5],
      probaSource: 'model'
    };
    
    const error = validateDecisionConfig(invalidConfig);
    if (!error || !error.includes('symbol')) {
      throw new Error('Should detect missing symbol');
    }
    
    console.log(`  Error detected: ${error}`);
    console.log('✅ Test 2 PASSED: Detects missing symbol');
    passed++;
  } catch (err) {
    console.error('❌ Test 2 FAILED:', err.message);
    failed++;
  }

  // Test 3: validateDecisionConfig with invalid threshold
  console.log('\n3. Testing validateDecisionConfig with invalid threshold...');
  try {
    const invalidConfig = {
      symbol: 'btcusdt',
      bestThreshold: 1.5, // Invalid: > 1
      thresholdGrid: [0.5],
      probaSource: 'model'
    };
    
    const error = validateDecisionConfig(invalidConfig);
    if (!error || !error.includes('bestThreshold')) {
      throw new Error('Should detect invalid threshold range');
    }
    
    console.log(`  Error detected: ${error}`);
    console.log('✅ Test 3 PASSED: Detects invalid threshold range');
    passed++;
  } catch (err) {
    console.error('❌ Test 3 FAILED:', err.message);
    failed++;
  }

  // Test 4: applyDecision with threshold 0.5
  console.log('\n4. Testing applyDecision with threshold 0.5...');
  try {
    const probas = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    const decision = { bestThreshold: 0.5, probaSource: 'pseudo_sigmoid' };
    
    const result = applyDecision(probas, decision);
    
    // At threshold 0.5: [false, false, true, true, true, true]
    assertEqual(result.pred_pos_count, 4, 'pred_pos_count');
    assertEqual(result.thresholdUsed, 0.5, 'thresholdUsed');
    assertEqual(result.probaSource, 'pseudo_sigmoid', 'probaSource');
    assertEqual(result.signals[0], false, 'signals[0]');
    assertEqual(result.signals[2], true, 'signals[2] (exactly 0.5)');
    assertEqual(result.signals[5], true, 'signals[5]');
    
    console.log(`  Signals: ${result.signals.map(s => s ? '1' : '0').join(',')}`);
    console.log(`  Pred pos count: ${result.pred_pos_count}`);
    console.log('✅ Test 4 PASSED: applyDecision works correctly');
    passed++;
  } catch (err) {
    console.error('❌ Test 4 FAILED:', err.message);
    failed++;
  }

  // Test 5: applyDecision with higher threshold
  console.log('\n5. Testing applyDecision with threshold 0.7...');
  try {
    const probas = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    const decision = { bestThreshold: 0.7, probaSource: 'model' };
    
    const result = applyDecision(probas, decision);
    
    // At threshold 0.7: [false, false, false, false, true, true]
    assertEqual(result.pred_pos_count, 2, 'pred_pos_count');
    assertEqual(result.thresholdUsed, 0.7, 'thresholdUsed');
    
    console.log(`  Signals: ${result.signals.map(s => s ? '1' : '0').join(',')}`);
    console.log(`  Pred pos count: ${result.pred_pos_count}`);
    console.log('✅ Test 5 PASSED: Higher threshold = fewer signals');
    passed++;
  } catch (err) {
    console.error('❌ Test 5 FAILED:', err.message);
    failed++;
  }

  // Test 6: getProbaStats
  console.log('\n6. Testing getProbaStats...');
  try {
    const probas = [0.2, 0.4, 0.6, 0.8];
    const stats = getProbaStats(probas);
    
    assertEqual(stats.min, 0.2, 'min');
    assertEqual(stats.max, 0.8, 'max');
    assertEqual(stats.mean, 0.5, 'mean');
    
    console.log(`  Stats: min=${stats.min}, mean=${stats.mean}, max=${stats.max}`);
    console.log('✅ Test 6 PASSED: getProbaStats works correctly');
    passed++;
  } catch (err) {
    console.error('❌ Test 6 FAILED:', err.message);
    failed++;
  }

  // Test 7: Cache functionality
  console.log('\n7. Testing cache functionality...');
  try {
    clearCache();
    let stats = getCacheStats();
    assertEqual(stats.size, 0, 'Cache should be empty after clear');
    
    console.log('  Cache cleared successfully');
    console.log('✅ Test 7 PASSED: Cache clear works');
    passed++;
  } catch (err) {
    console.error('❌ Test 7 FAILED:', err.message);
    failed++;
  }

  // Test 8: Empty proba array handling
  console.log('\n8. Testing empty proba array handling...');
  try {
    const probas = [];
    const decision = { bestThreshold: 0.5, probaSource: 'model' };
    
    const result = applyDecision(probas, decision);
    assertEqual(result.pred_pos_count, 0, 'pred_pos_count');
    assertEqual(result.pred_pos_rate, 0, 'pred_pos_rate');
    assertEqual(result.signals.length, 0, 'signals.length');
    
    const stats = getProbaStats(probas);
    assertEqual(stats.mean, 0, 'mean for empty array');
    
    console.log('✅ Test 8 PASSED: Empty array handled correctly');
    passed++;
  } catch (err) {
    console.error('❌ Test 8 FAILED:', err.message);
    failed++;
  }

  // Summary
  console.log('\n' + '='.repeat(40));
  console.log(`SUMMARY: ${passed} passed, ${failed} failed`);
  console.log('='.repeat(40));

  if (failed > 0) {
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
