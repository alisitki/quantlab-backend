import assert from 'assert';
import { evaluateModel } from './ml/train/evaluate.js';
import { XGBoostModel } from './ml/models/XGBoostModel.js';

// Mock model for evaluation test
const mockEvaluator = {
  predict: (X) => X.map(x => x[0]) // Identity prediction
};

async function testEvaluate() {
  console.log('Testing evaluate.js Imbalance Metrics...');
  
  // Case 1: Nominal (Balanced-ish)
  const nominalData = {
    X: [[0], [1], [0], [1]],
    y: [0, 1, 0, 1]
  };
  const nominalRes = evaluateModel(mockEvaluator, nominalData);
  assert.strictEqual(nominalRes.pred_pos_count, 2, 'Nom: pred_pos_count');
  assert.strictEqual(nominalRes.precision_pos, 1, 'Nom: precision_pos');
  assert.strictEqual(nominalRes.recall_pos, 1, 'Nom: recall_pos');
  assert.strictEqual(nominalRes.confusion_matrix.tp, 2, 'Nom: tp');
  assert.strictEqual(nominalRes.evaluation_status, 'ok', 'Nom: status');

  // Case 2: Extreme Imbalance - No Positive Predictions
  const noPosPredsModel = { predict: (X) => new Array(X.length).fill(0) };
  const imbalanceData = {
    X: [[0], [1], [0], [1]],
    y: [1, 1, 0, 0]
  };
  const resNoPos = evaluateModel(noPosPredsModel, imbalanceData);
  assert.strictEqual(resNoPos.pred_pos_count, 0, 'NoPos: count');
  assert.strictEqual(resNoPos.precision_pos, null, 'NoPos: precision');
  assert.strictEqual(resNoPos.recall_pos, 0, 'NoPos: recall'); // label_1 > 0, but tp=0
  assert.strictEqual(resNoPos.evaluation_status, 'no_positive_predictions', 'NoPos: status');
  console.log('PASS: No Positive Predictions handled.');

  // Case 3: No Positive Labels
  const noPosLabelsModel = { predict: (X) => new Array(X.length).fill(0) };
  const noLabelsData = {
    X: [[0], [0]],
    y: [0, 0]
  };
  const resNoLabels = evaluateModel(noPosLabelsModel, noLabelsData);
  assert.strictEqual(resNoLabels.evaluation_status, 'no_positive_labels', 'NoLabels: status');
  console.log('PASS: No Positive Labels handled.');
  
  // Case 4: Balanced Accuracy Check
  // 1 Pos, 3 Neg. Model predicts all 0.
  // TPR (Recall) = 0/1 = 0.
  // TNR (Spec) = 3/3 = 1.
  // BalAcc = (0+1)/2 = 0.5.
  const balCheckData = {
      X: [[0],[0],[0],[0]], // Features dont matter for mock predict
      y: [0, 0, 0, 1]
  };
  const resBal = evaluateModel(noPosPredsModel, balCheckData);
  assert.strictEqual(resBal.label_distribution.label_1, 1);
  assert.strictEqual(resBal.recall_pos, 0);
  assert.strictEqual(resBal.balancedAccuracy, 0.5, 'BalAcc should be 0.5 for all-neg pred on imbalanced data');
  console.log('PASS: Balanced Accuracy calc.');
}

async function testXGBoostWeights() {
  console.log('Testing XGBoostModel Class Weights...');
  
  const model = new XGBoostModel({});
  const X = [[1], [2], [3], [4], [5]];
  const y = [0, 0, 0, 0, 1]; // 4 neg, 1 pos. Ratio: 4:1 = 4.0
  
  await model.train(X, y);
  
  const config = model.getConfig();
  assert.strictEqual(config.scale_pos_weight, 4, 'Scale Pos Weight should be 4');
  assert.strictEqual(config.max_delta_step, 1, 'Max Delta Step should be 1');
  
  console.log('PASS: XGBoost weights correct.');
}

(async () => {
    try {
        await testEvaluate();
        await testXGBoostWeights();
        console.log('ALL TESTS PASSED');
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
})();
