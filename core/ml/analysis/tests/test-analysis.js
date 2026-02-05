/**
 * Feature Analysis Module Tests
 */

import assert from 'assert';
import {
  calculatePearsonCorrelation,
  calculateSpearmanCorrelation,
  analyzeFeatureCorrelations,
  calculatePointBiserial,
  analyzeFeatureLabelRelationships,
  calculateLabelDistribution,
  calculateImbalanceRatio,
  analyzeLabelDistribution,
  calculateStats,
  detectOutliers,
  calculatePSI
} from '../index.js';

// Generate test data
function generateTestData(nSamples = 100, nFeatures = 5) {
  const X = [];
  const y = [];
  const timestamps = [];

  for (let i = 0; i < nSamples; i++) {
    const row = [];
    for (let j = 0; j < nFeatures; j++) {
      row.push(Math.random() * 10 - 5);
    }
    X.push(row);
    y.push(Math.random() > 0.5 ? 1 : 0);
    timestamps.push(Date.now() + i * 1000);
  }

  const featureNames = Array.from({ length: nFeatures }, (_, i) => `feature_${i}`);

  return { X, y, timestamps, featureNames };
}

async function testPearsonCorrelation() {
  console.log('Testing Pearson correlation...');

  // Perfect positive correlation
  const x1 = [1, 2, 3, 4, 5];
  const y1 = [2, 4, 6, 8, 10];
  const corr1 = calculatePearsonCorrelation(x1, y1);
  assert.ok(Math.abs(corr1 - 1.0) < 0.001, 'Perfect positive correlation should be 1');

  // Perfect negative correlation
  const y2 = [10, 8, 6, 4, 2];
  const corr2 = calculatePearsonCorrelation(x1, y2);
  assert.ok(Math.abs(corr2 - (-1.0)) < 0.001, 'Perfect negative correlation should be -1');

  // No correlation
  const x3 = [1, 2, 3, 4, 5];
  const y3 = [5, 1, 3, 2, 4];
  const corr3 = calculatePearsonCorrelation(x3, y3);
  assert.ok(Math.abs(corr3) < 0.5, 'Random data should have low correlation');

  console.log('  Pearson correlation OK');
}

async function testSpearmanCorrelation() {
  console.log('Testing Spearman correlation...');

  const x = [1, 2, 3, 4, 5];
  const y = [1, 2, 3, 4, 5];
  const corr = calculateSpearmanCorrelation(x, y);
  assert.ok(Math.abs(corr - 1.0) < 0.001, 'Same ranks should have correlation 1');

  console.log('  Spearman correlation OK');
}

async function testFeatureCorrelationAnalysis() {
  console.log('Testing feature correlation analysis...');

  const data = generateTestData(100, 5);

  // Make two features highly correlated
  for (let i = 0; i < data.X.length; i++) {
    data.X[i][1] = data.X[i][0] * 2 + Math.random() * 0.1; // f1 ≈ 2*f0
  }

  const result = analyzeFeatureCorrelations(data.X, data.featureNames);

  assert.ok(result.matrix.length === 5, 'Matrix should have 5 rows');
  assert.ok(result.highlyCorrelatedPairs.length > 0, 'Should find correlated pairs');
  assert.ok(result.redundancyScore !== undefined, 'Should have redundancy score');

  console.log(`  Found ${result.highlyCorrelatedPairs.length} correlated pairs`);
  console.log('  Feature correlation analysis OK');
}

async function testPointBiserial() {
  console.log('Testing point-biserial correlation...');

  // Feature values higher for label=1
  const feature = [1, 2, 1, 2, 5, 6, 5, 6];
  const label = [0, 0, 0, 0, 1, 1, 1, 1];

  const rpb = calculatePointBiserial(feature, label);
  assert.ok(rpb > 0.5, 'Should have strong positive correlation');

  console.log(`  Point-biserial: ${rpb.toFixed(3)}`);
  console.log('  Point-biserial OK');
}

async function testLabelDistribution() {
  console.log('Testing label distribution...');

  const y = [0, 0, 0, 1, 1, 1, 1, 1];
  const dist = calculateLabelDistribution(y);

  assert.strictEqual(dist.counts[0], 3);
  assert.strictEqual(dist.counts[1], 5);
  assert.strictEqual(dist.total, 8);

  const imbalance = calculateImbalanceRatio(y);
  assert.ok(imbalance > 1, 'Should detect imbalance');

  console.log(`  Imbalance ratio: ${imbalance.toFixed(2)}`);
  console.log('  Label distribution OK');
}

async function testFeatureStats() {
  console.log('Testing feature statistics...');

  const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const stats = calculateStats(values);

  assert.strictEqual(stats.mean, 5.5);
  assert.strictEqual(stats.min, 1);
  assert.strictEqual(stats.max, 10);
  assert.strictEqual(stats.median, 5.5);

  console.log('  Feature statistics OK');
}

async function testOutlierDetection() {
  console.log('Testing outlier detection...');

  const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]; // 100 is outlier
  const result = detectOutliers(values);

  assert.ok(result.outlierCount > 0, 'Should detect outlier');
  assert.ok(result.outlierIndices.includes(9), 'Should identify index 9 as outlier');

  console.log(`  Found ${result.outlierCount} outliers`);
  console.log('  Outlier detection OK');
}

async function testPSI() {
  console.log('Testing PSI calculation...');

  // Same distribution - PSI should be ~0
  const baseline = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const same = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const psi1 = calculatePSI(baseline, same);

  assert.ok(psi1.psi < 0.1, 'Same distribution should have low PSI');
  assert.strictEqual(psi1.status, 'STABLE');

  // Different distribution - PSI should be higher
  const different = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
  const psi2 = calculatePSI(baseline, different);

  assert.ok(psi2.psi > 0.1, 'Different distribution should have higher PSI');

  console.log(`  Same dist PSI: ${psi1.psi.toFixed(3)} (${psi1.status})`);
  console.log(`  Diff dist PSI: ${psi2.psi.toFixed(3)} (${psi2.status})`);
  console.log('  PSI calculation OK');
}

async function runAllTests() {
  console.log('=== Feature Analysis Module Tests ===\n');

  await testPearsonCorrelation();
  await testSpearmanCorrelation();
  await testFeatureCorrelationAnalysis();
  await testPointBiserial();
  await testLabelDistribution();
  await testFeatureStats();
  await testOutlierDetection();
  await testPSI();

  console.log('\n=== All Tests Passed ===');
}

runAllTests().catch(err => {
  console.error('Test Failed:', err);
  process.exit(1);
});
