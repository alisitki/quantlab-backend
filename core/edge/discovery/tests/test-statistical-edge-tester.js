/**
 * Test: StatisticalEdgeTester
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StatisticalEdgeTester } from '../StatisticalEdgeTester.js';

test('StatisticalEdgeTester - constructor', () => {
  const tester = new StatisticalEdgeTester();

  assert.ok(tester.minSampleSize > 0);
  assert.ok(tester.pValueThreshold > 0 && tester.pValueThreshold < 1);
  assert.ok(tester.minSharpe >= 0);
});

test('StatisticalEdgeTester - test with good edge (synthetic)', () => {
  const tester = new StatisticalEdgeTester({
    minSampleSize: 10,
    pValueThreshold: 0.1, // Relaxed for synthetic test
    minSharpe: 0.1
  });

  // Create synthetic dataset with known edge
  const dataset = {
    rows: [],
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  // Pattern: indices 0-49 have positive returns (planted edge)
  const matchingIndices = [];
  for (let i = 0; i < 100; i++) {
    const hasEdge = i < 50;
    const forwardReturn = hasEdge ? 0.002 + Math.random() * 0.001 : -0.0005 + Math.random() * 0.001;

    if (hasEdge) {
      matchingIndices.push(i);
    }

    dataset.rows.push({
      features: { micro_reversion: hasEdge ? 0.7 : 0.3 },
      regime: 0,
      forwardReturns: { h10: forwardReturn },
      timestamp: BigInt(i * 1000)
    });
  }

  const pattern = {
    id: 'test_pattern',
    type: 'threshold',
    conditions: [{ feature: 'micro_reversion', operator: '>', value: 0.5 }],
    regimes: null,
    direction: 'LONG',
    support: matchingIndices.length,
    forwardReturns: {
      mean: 0.0025,
      median: 0.0025,
      std: 0.0005,
      count: matchingIndices.length
    },
    horizon: 10,
    matchingIndices
  };

  const result = tester.test(pattern, dataset);

  assert.ok(result, 'Should return result');
  assert.equal(result.patternId, 'test_pattern');
  assert.ok(result.tests.sampleSizeTest.passed, 'Sample size test should pass');
  assert.ok(result.tests.tTest, 'Should have t-test result');
  assert.ok(result.tests.permutationTest, 'Should have permutation test result');
  assert.ok(result.tests.sharpeTest, 'Should have Sharpe test result');
  assert.ok(typeof result.overallScore === 'number', 'Should have overall score');
  assert.ok(['ACCEPT', 'WEAK', 'REJECT'].includes(result.recommendation), 'Should have valid recommendation');

  // With planted edge, should likely pass
  console.log(`  Result: ${result.recommendation}, score: ${result.overallScore.toFixed(2)}`);
});

test('StatisticalEdgeTester - test with random noise (should reject)', () => {
  const tester = new StatisticalEdgeTester({
    minSampleSize: 10,
    pValueThreshold: 0.05,
    minSharpe: 0.5
  });

  // Create synthetic dataset with no edge (pure noise)
  const dataset = {
    rows: [],
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  const matchingIndices = [];
  for (let i = 0; i < 100; i++) {
    const isMatching = i < 50;
    const forwardReturn = Math.random() * 0.002 - 0.001; // Random [-0.001, 0.001]

    if (isMatching) {
      matchingIndices.push(i);
    }

    dataset.rows.push({
      features: { micro_reversion: Math.random() },
      regime: 0,
      forwardReturns: { h10: forwardReturn },
      timestamp: BigInt(i * 1000)
    });
  }

  const pattern = {
    id: 'noise_pattern',
    type: 'threshold',
    conditions: [{ feature: 'micro_reversion', operator: '>', value: 0.5 }],
    regimes: null,
    direction: 'LONG',
    support: matchingIndices.length,
    forwardReturns: {
      mean: 0.0001,
      median: 0.0,
      std: 0.001,
      count: matchingIndices.length
    },
    horizon: 10,
    matchingIndices
  };

  const result = tester.test(pattern, dataset);

  // Random noise should fail Sharpe test
  assert.ok(!result.tests.sharpeTest.passed || result.overallScore < 0.75, 'Random noise should not pass all tests');
  console.log(`  Noise pattern result: ${result.recommendation}, score: ${result.overallScore.toFixed(2)}`);
});

test('StatisticalEdgeTester - insufficient sample size', () => {
  const tester = new StatisticalEdgeTester({
    minSampleSize: 30
  });

  const dataset = {
    rows: Array(20).fill(null).map((_, i) => ({
      features: { micro_reversion: 0.5 },
      regime: 0,
      forwardReturns: { h10: 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  const pattern = {
    id: 'small_sample',
    type: 'threshold',
    conditions: [],
    regimes: null,
    direction: 'LONG',
    support: 20,
    forwardReturns: { mean: 0.001, median: 0.001, std: 0.0005, count: 20 },
    horizon: 10,
    matchingIndices: Array(20).fill(null).map((_, i) => i)
  };

  const result = tester.test(pattern, dataset);

  assert.ok(!result.tests.sampleSizeTest.passed, 'Sample size test should fail');
  assert.equal(result.recommendation, 'REJECT', 'Should reject due to insufficient sample');
});

test('StatisticalEdgeTester - testBatch with Bonferroni correction', () => {
  const tester = new StatisticalEdgeTester({
    minSampleSize: 10,
    pValueThreshold: 0.05,
    multipleComparisonCorrection: true
  });

  const dataset = {
    rows: Array(100).fill(null).map((_, i) => ({
      features: { micro_reversion: Math.random() },
      regime: 0,
      forwardReturns: { h10: Math.random() * 0.002 - 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  const patterns = [
    {
      id: 'pattern1',
      type: 'threshold',
      conditions: [],
      regimes: null,
      direction: 'LONG',
      support: 50,
      forwardReturns: { mean: 0.001, median: 0.001, std: 0.001, count: 50 },
      horizon: 10,
      matchingIndices: Array(50).fill(null).map((_, i) => i)
    },
    {
      id: 'pattern2',
      type: 'threshold',
      conditions: [],
      regimes: null,
      direction: 'SHORT',
      support: 50,
      forwardReturns: { mean: -0.001, median: -0.001, std: 0.001, count: 50 },
      horizon: 10,
      matchingIndices: Array(50).fill(null).map((_, i) => i + 50)
    }
  ];

  const results = tester.testBatch(patterns, dataset);

  assert.equal(results.length, 2, 'Should return 2 results');
  assert.ok(results.every(r => r.recommendation), 'All results should have recommendations');

  console.log(`  Batch test: ${results.map(r => r.recommendation).join(', ')}`);
});
