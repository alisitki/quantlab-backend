import { XGBoostModel } from '../models/XGBoostModel.js';
import { evaluateThresholdGrid } from '../train/evaluate.js';

async function runTests() {
  console.log('--- ModelProba v1 Unit Tests ---');

  // 1. Setup Mock Models
  const mockProbaModel = {
    predictProba: (X) => X.map(() => 0.8)
  };

  const mockAlreadyProbaModel = {
    predict: (X) => X.map(() => 0.6)
  };

  const mockLogitModel = {
    predict: (X) => [0, 2, -2] // 0 -> 0.5, 2 -> 0.88, -2 -> 0.12
  };

  // 2. Test Heuristic: Native predictProba
  console.log('\n1. Testing native predictProba detection...');
  const model1 = new XGBoostModel();
  model1.loadModelForTest(mockProbaModel); // We'll add this helper
  const p1 = model1.predictProba([[1]]);
  console.log(`   Source: ${model1.getProbaSource()}`);
  console.log(`   Result: ${p1[0]}`);
  if (model1.getProbaSource() !== 'model_predictProba') throw new Error('Source mismatch for native proba');

  // 3. Test Heuristic: Already Proba
  console.log('\n2. Testing already-proba detection (range [0,1])...');
  const model2 = new XGBoostModel();
  model2.loadModelForTest(mockAlreadyProbaModel);
  const p2 = model2.predictProba([[1]]);
  console.log(`   Source: ${model2.getProbaSource()}`);
  console.log(`   Result: ${p2[0]}`);
  if (model2.getProbaSource() !== 'model_predict_prob') throw new Error('Source mismatch for already-proba');

  // 4. Test Heuristic: Logit conversion
  console.log('\n3. Testing logit-to-sigmoid conversion...');
  const model3 = new XGBoostModel();
  model3.loadModelForTest(mockLogitModel);
  const p3 = model3.predictProba([[1], [1], [1]]);
  console.log(`   Source: ${model3.getProbaSource()}`);
  console.log(`   Results: ${p3.map(v => v.toFixed(3)).join(', ')}`);
  if (model3.getProbaSource() !== 'model_predict_logit_sigmoid') throw new Error('Source mismatch for logit');
  
  const expectedS0 = 1 / (1 + Math.exp(0));
  if (Math.abs(p3[0] - expectedS0) > 0.001) throw new Error('Sigmoid calculation error');

  // 5. Test Threshold Grid + Monotonicity
  console.log('\n4. Testing evaluateThresholdGrid & Monotonicity...');
  const testData = {
    X: [[0], [1], [2], [3], [4]],
    y: [0, 1, 0, 1, 0]
  };
  const gridModel = {
    predictProba: (X) => [0.1, 0.3, 0.5, 0.7, 0.9],
    getProbaSource: () => 'test_grid'
  };

  const thresholds = [0.2, 0.4, 0.6, 0.8];
  const metrics = evaluateThresholdGrid(gridModel, testData, thresholds);
  
  console.log(`   Proba Stats: min=${metrics.proba_stats.min}, mean=${metrics.proba_stats.mean}, max=${metrics.proba_stats.max}`);
  
  const counts = thresholds.map(t => metrics.threshold_results[t.toFixed(2)].pred_pos_count);
  console.log(`   Thresholds: ${thresholds.join(', ')}`);
  console.log(`   Pos Counts: ${counts.join(', ')}`);

  // Verify monotonicity: each next count must be <= previous
  for (let i = 1; i < counts.length; i++) {
    if (counts[i] > counts[i - 1]) {
      throw new Error(`Monotonicity violation at index ${i}: ${counts[i]} > ${counts[i-1]}`);
    }
  }
  console.log('✅ Monotonicity verified.');

  console.log('\n✅ ALL UNIT TESTS PASSED');
}

// Add a test-only helper to XGBoostModel if it's missing or use a trick
// Actually, let's just use prototype injection or update file
runTests().catch(err => {
  console.error('\n❌ TEST FAILED');
  console.error(err);
  process.exit(1);
});
