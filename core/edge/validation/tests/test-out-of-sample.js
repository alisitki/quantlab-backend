/**
 * Test: OutOfSampleValidator
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { OutOfSampleValidator } from '../OutOfSampleValidator.js';
import { Edge } from '../../Edge.js';

test('OutOfSampleValidator - constructor', () => {
  const validator = new OutOfSampleValidator();

  assert.equal(validator.trainRatio, 0.7);
  assert.equal(validator.testRatio, 0.3);
  assert.ok(validator.minSharpeOOS > 0);
  assert.ok(validator.maxPerfDegradation > 0);
});

test('OutOfSampleValidator - validate with good edge', () => {
  const validator = new OutOfSampleValidator({
    minSharpeOOS: 0.1,  // Relaxed for synthetic test
    maxPerfDegradation: 1.0
  });

  // Create synthetic edge that works consistently
  const edge = new Edge({
    id: 'test_edge',
    name: 'Test Edge',
    entryCondition: (features, regime) => {
      return { active: features.signal > 0.5, direction: 'LONG' };
    },
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  // Create dataset with consistent pattern
  const dataset = {
    rows: [],
    featureNames: ['signal'],
    metadata: {}
  };

  for (let i = 0; i < 200; i++) {
    const signal = Math.random();
    const forwardReturn = signal > 0.5 ? 0.001 + Math.random() * 0.001 : -0.0005;

    dataset.rows.push({
      features: { signal },
      regime: 0,
      forwardReturns: { h10: forwardReturn },
      timestamp: BigInt(i * 1000)
    });
  }

  const result = validator.validate(edge, dataset);

  assert.ok(result, 'Should return result');
  assert.ok(result.inSample, 'Should have in-sample result');
  assert.ok(result.outOfSample, 'Should have out-of-sample result');
  assert.ok(typeof result.degradation === 'number', 'Should have degradation');
  assert.ok(typeof result.passed === 'boolean', 'Should have passed flag');
  assert.ok(typeof result.confidence === 'number', 'Should have confidence');

  console.log(`  IS Sharpe: ${result.inSample.sharpe.toFixed(3)}, OOS Sharpe: ${result.outOfSample.sharpe.toFixed(3)}`);
  console.log(`  Degradation: ${(result.degradation * 100).toFixed(1)}%, Passed: ${result.passed}`);
});

test('OutOfSampleValidator - validate with overfit edge', () => {
  const validator = new OutOfSampleValidator({
    minSharpeOOS: 0.5,
    maxPerfDegradation: 0.3
  });

  // Create edge that only works in-sample (overfit)
  const edge = new Edge({
    id: 'overfit_edge',
    name: 'Overfit Edge',
    entryCondition: (features, regime) => {
      // Very specific condition that works on first half but not second
      return { active: features.index < 70, direction: 'LONG' };
    },
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  // Dataset with overfit pattern
  const dataset = {
    rows: [],
    featureNames: ['index'],
    metadata: {}
  };

  for (let i = 0; i < 200; i++) {
    // First 70 rows have good returns, rest are random
    const forwardReturn = i < 70 ? 0.002 : Math.random() * 0.002 - 0.001;

    dataset.rows.push({
      features: { index: i },
      regime: 0,
      forwardReturns: { h10: forwardReturn },
      timestamp: BigInt(i * 1000)
    });
  }

  const result = validator.validate(edge, dataset);

  // Overfit edge should fail OOS
  assert.ok(!result.passed || result.outOfSample.sharpe < result.inSample.sharpe * 0.5,
    'Overfit edge should fail or show significant degradation');

  console.log(`  Overfit test - IS: ${result.inSample.sharpe.toFixed(3)}, OOS: ${result.outOfSample.sharpe.toFixed(3)}, Passed: ${result.passed}`);
});

test('OutOfSampleValidator - validate with no trades', () => {
  const validator = new OutOfSampleValidator();

  // Edge that never activates
  const edge = new Edge({
    id: 'no_trade_edge',
    name: 'No Trade Edge',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(100).fill(null).map((_, i) => ({
      features: { signal: Math.random() },
      regime: 0,
      forwardReturns: { h10: 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = validator.validate(edge, dataset);

  assert.equal(result.inSample.trades, 0, 'Should have 0 in-sample trades');
  assert.equal(result.outOfSample.trades, 0, 'Should have 0 out-of-sample trades');
  assert.equal(result.inSample.sharpe, 0, 'Sharpe should be 0 with no trades');
  assert.ok(!result.passed, 'Should not pass with no trades');
});
