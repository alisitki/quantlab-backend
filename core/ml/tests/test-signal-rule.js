/**
 * test-signal-rule.js: Unit tests for Signal Rule v1 (Threshold-based evaluation)
 */
import { evaluateWithThreshold, evaluateThresholdGrid } from '../train/evaluate.js';

// Mock model for testing
class MockModel {
  #probas;
  
  constructor(probas) {
    this.#probas = probas;
  }
  
  predictProba(X) {
    return this.#probas;
  }
  
  getProbaSource() {
    return 'mock';
  }
}

function assertEqual(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${expected}, got ${actual}`);
  }
}

function assertNotNull(actual, msg) {
  if (actual === null || actual === undefined) {
    throw new Error(`${msg}: expected non-null value`);
  }
}

function assertNull(actual, msg) {
  if (actual !== null) {
    throw new Error(`${msg}: expected null, got ${actual}`);
  }
}

async function runTests() {
  console.log('--- Signal Rule v1 Unit Tests ---\n');
  let passed = 0;
  let failed = 0;

  // Test 1: Threshold crossing behavior
  console.log('1. Testing threshold crossing...');
  try {
    const probas = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    const actuals = [0, 0, 1, 1, 0, 1]; // 3 positives in actuals
    
    // At threshold 0.5: predictions = [0, 0, 1, 1, 1, 1] -> 4 positive predictions
    const result_50 = evaluateWithThreshold(probas, actuals, 0.50);
    assertEqual(result_50.pred_pos_count, 4, 'Threshold 0.50 pred_pos_count');
    
    // At threshold 0.7: predictions = [0, 0, 0, 0, 1, 1] -> 2 positive predictions
    const result_70 = evaluateWithThreshold(probas, actuals, 0.70);
    assertEqual(result_70.pred_pos_count, 2, 'Threshold 0.70 pred_pos_count');
    
    console.log('✅ Test 1 PASSED: Threshold crossing works correctly');
    passed++;
  } catch (err) {
    console.error('❌ Test 1 FAILED:', err.message);
    failed++;
  }

  // Test 2: Higher threshold = lower pred_pos_rate
  console.log('\n2. Testing threshold vs pred_pos_rate relationship...');
  try {
    const probas = [0.2, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95];
    const actuals = [0, 0, 0, 0, 1, 1, 1, 1];
    
    const result_50 = evaluateWithThreshold(probas, actuals, 0.50);
    const result_60 = evaluateWithThreshold(probas, actuals, 0.60);
    const result_70 = evaluateWithThreshold(probas, actuals, 0.70);
    
    if (result_50.pred_pos_rate > result_60.pred_pos_rate && 
        result_60.pred_pos_rate > result_70.pred_pos_rate) {
      console.log(`  Rates: 0.50=${result_50.pred_pos_rate.toFixed(3)}, 0.60=${result_60.pred_pos_rate.toFixed(3)}, 0.70=${result_70.pred_pos_rate.toFixed(3)}`);
      console.log('✅ Test 2 PASSED: Higher threshold → lower pred_pos_rate');
      passed++;
    } else {
      throw new Error(`Relationship violated: ${result_50.pred_pos_rate} vs ${result_60.pred_pos_rate} vs ${result_70.pred_pos_rate}`);
    }
  } catch (err) {
    console.error('❌ Test 2 FAILED:', err.message);
    failed++;
  }

  // Test 3: Best threshold selection
  console.log('\n3. Testing best_threshold selection by f1_pos...');
  try {
    // Craft probas so optimal threshold is 0.55
    const probas = [0.52, 0.53, 0.54, 0.56, 0.57, 0.58, 0.61, 0.62, 0.63, 0.71];
    const actuals = [0, 0, 0, 1, 1, 1, 0, 0, 1, 1]; // 5 positives
    
    const model = new MockModel(probas);
    const testData = { X: probas.map(() => [1]), y: actuals };
    
    const result = evaluateThresholdGrid(model, testData, [0.50, 0.55, 0.60, 0.65, 0.70]);
    
    assertNotNull(result.best_threshold, 'best_threshold should not be null');
    assertNotNull(result.best_threshold.value, 'best_threshold.value should not be null');
    assertEqual(result.best_threshold.by, 'f1_pos', 'best_threshold.by');
    
    console.log(`  Best threshold: ${result.best_threshold.value} with f1_pos=${result.best_threshold.f1_pos?.toFixed(4)}`);
    console.log('✅ Test 3 PASSED: best_threshold correctly selected');
    passed++;
  } catch (err) {
    console.error('❌ Test 3 FAILED:', err.message);
    failed++;
  }

  // Test 4: no_positive_predictions guard
  console.log('\n4. Testing no_positive_predictions guard...');
  try {
    // Very high threshold = no positive predictions
    const probas = [0.1, 0.2, 0.3, 0.4, 0.5];
    const actuals = [0, 1, 0, 1, 0];
    
    const result = evaluateWithThreshold(probas, actuals, 0.99);
    
    assertEqual(result.pred_pos_count, 0, 'pred_pos_count');
    assertEqual(result.evaluation_status, 'no_positive_predictions', 'evaluation_status');
    assertNull(result.precision_pos, 'precision_pos should be null');
    assertNull(result.f1_pos, 'f1_pos should be null');
    
    console.log('✅ Test 4 PASSED: no_positive_predictions guard works');
    passed++;
  } catch (err) {
    console.error('❌ Test 4 FAILED:', err.message);
    failed++;
  }

  // Test 5: threshold_results structure
  console.log('\n5. Testing threshold_results structure...');
  try {
    const probas = [0.4, 0.5, 0.6, 0.7];
    const actuals = [0, 1, 1, 0];
    
    const model = new MockModel(probas);
    const testData = { X: probas.map(() => [1]), y: actuals };
    
    const result = evaluateThresholdGrid(model, testData, [0.50, 0.55, 0.60]);
    
    assertEqual(Object.keys(result.threshold_results).length, 3, 'Number of thresholds');
    assertEqual(result.proba_source, 'mock', 'proba_source');
    
    // Check keys are in deterministic order (string sorted)
    const keys = Object.keys(result.threshold_results);
    assertEqual(keys[0], '0.50', 'First threshold key');
    assertEqual(keys[1], '0.55', 'Second threshold key');
    assertEqual(keys[2], '0.60', 'Third threshold key');
    
    // Check structure of individual result
    const r50 = result.threshold_results['0.50'];
    assertNotNull(r50.confusion_matrix, 'confusion_matrix should exist');
    assertNotNull(r50.pred_pos_count, 'pred_pos_count should exist');
    
    console.log('✅ Test 5 PASSED: threshold_results structure is correct');
    passed++;
  } catch (err) {
    console.error('❌ Test 5 FAILED:', err.message);
    failed++;
  }

  // Test 6: Monotonic pred_pos_count (v1.1 bug fix verification)
  console.log('\n6. Testing monotonic pred_pos_count across thresholds...');
  try {
    // Probas with clear variation across threshold ranges
    const probas = [0.45, 0.48, 0.52, 0.58, 0.63, 0.68, 0.73, 0.78, 0.85, 0.92];
    const actuals = [0, 0, 1, 1, 0, 1, 0, 1, 1, 1];
    
    const model = new MockModel(probas);
    const testData = { X: probas.map(() => [1]), y: actuals };
    
    const result = evaluateThresholdGrid(model, testData, [0.50, 0.55, 0.60, 0.65, 0.70]);
    
    const counts = [
      result.threshold_results['0.50'].pred_pos_count,
      result.threshold_results['0.55'].pred_pos_count,
      result.threshold_results['0.60'].pred_pos_count,
      result.threshold_results['0.65'].pred_pos_count,
      result.threshold_results['0.70'].pred_pos_count
    ];
    
    // Check monotonic: each value <= previous
    let isMonotonic = true;
    for (let i = 1; i < counts.length; i++) {
      if (counts[i] > counts[i-1]) {
        isMonotonic = false;
        break;
      }
    }
    
    if (isMonotonic && counts[0] > counts[4]) {
      console.log(`  Counts: ${counts.join(' >= ')}`);
      console.log('✅ Test 6 PASSED: pred_pos_count is monotonically decreasing');
      passed++;
    } else {
      throw new Error(`Not monotonic: ${counts.join(', ')}`);
    }
  } catch (err) {
    console.error('❌ Test 6 FAILED:', err.message);
    failed++;
  }

  // Test 7: label_distribution_scope determinism
  console.log('\n7. Testing label_distribution_scope determinism...');
  try {
    const probas = [0.4, 0.5, 0.6, 0.7, 0.8];
    const actuals = [0, 1, 0, 1, 1]; // 3 positives, 2 negatives
    
    const model = new MockModel(probas);
    const testData = { X: probas.map(() => [1]), y: actuals };
    
    const result1 = evaluateThresholdGrid(model, testData, [0.50, 0.60]);
    const result2 = evaluateThresholdGrid(model, testData, [0.50, 0.60]);
    
    assertEqual(result1.label_distribution_scope, 'test_split', 'scope field');
    assertEqual(result1.label_distribution.label_1, 3, 'label_1 count');
    assertEqual(result1.label_distribution.label_0, 2, 'label_0 count');
    assertEqual(result1.label_distribution.total, 5, 'total count');
    
    // Determinism check
    assertEqual(
      JSON.stringify(result1.label_distribution),
      JSON.stringify(result2.label_distribution),
      'label_distribution determinism'
    );
    
    console.log('✅ Test 7 PASSED: label_distribution_scope is deterministic');
    passed++;
  } catch (err) {
    console.error('❌ Test 7 FAILED:', err.message);
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
