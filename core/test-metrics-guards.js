import { evaluateModel } from './ml/train/evaluate.js';
import assert from 'assert';

console.log('Running Metrics Stabilization v1.1 Tests...');

// Mock Model
class MockModel {
  constructor(predictFn) {
    this.predictFn = predictFn;
  }
  predict(X) {
    return this.predictFn(X);
  }
}

// Case 1: No Directional Samples (Preds always 0)
{
  console.log('[Test] Case 1: No Positive Predictions');
  const X = [[1], [2], [3]];
  const y = [1, 0, 1];
  const model = new MockModel(() => [0, 0, 0]); // Always predict 0 (Neutral/Down)
  
  const result = evaluateModel(model, { X, y });
  
  assert.strictEqual(result.evaluation_status, 'no_directional_samples', 'Status mismatch');
  assert.strictEqual(result.reason, 'no positive predictions / no signals under rule', 'Reason mismatch');
  assert.strictEqual(result.directionalSampleSize, 0, 'SampleSize mismatch');
  assert.strictEqual(result.directionalHitRate, null, 'HitRate should be null');
  assert.strictEqual(result.maxDrawdown, null, 'MaxDD should be null');
  assert.strictEqual(result.label_distribution.total, 3, 'Total label count mismatch');
  console.log('✅ PASS');
}

// Case 2: Nominal Case (Some positive predictions)
{
  console.log('[Test] Case 2: Nominal Case (Positive Preds Exist)');
  const X = [[1], [2], [3], [4]];
  const y = [1, 1, 0, 0];
  const model = new MockModel(() => [1, 0, 1, 0]); 
  // Pred 1 (i=0): p=1, y=1 -> Hit
  // Pred 3 (i=2): p=1, y=0 -> Miss
  // Total Directional: 2, Hits: 1 -> Rate: 0.5
  
  const result = evaluateModel(model, { X, y });
  
  assert.strictEqual(result.evaluation_status, 'ok', 'Status should be ok');
  assert.strictEqual(result.directionalSampleSize, 2, 'Should have 2 positive samples');
  assert.strictEqual(result.directionalHitRate, 0.5, 'HitRate should be 0.5');
  assert.strictEqual(result.maxDrawdown, null, 'MaxDD should ALWAYS be null');
  console.log('✅ PASS');
}

// Case 3: Empty Input
{
  console.log('[Test] Case 3: Empty Input');
  const X = [];
  const y = [];
  const model = new MockModel(() => []);
  
  const result = evaluateModel(model, { X, y });
  
  assert.strictEqual(result.evaluation_status, 'no_directional_samples');
  assert.strictEqual(result.label_distribution.total, 0);
  console.log('✅ PASS');
}

console.log('All tests passed.');
