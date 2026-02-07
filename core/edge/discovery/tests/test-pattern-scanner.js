/**
 * Test: PatternScanner
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PatternScanner } from '../PatternScanner.js';

test('PatternScanner - constructor with defaults', () => {
  const scanner = new PatternScanner();

  assert.ok(scanner.minSupport >= 0);
  assert.ok(scanner.returnThreshold >= 0);
  assert.ok(scanner.seed);
  assert.ok(Array.isArray(scanner.scanMethods));
});

test('PatternScanner - scan with synthetic data (threshold method)', () => {
  const scanner = new PatternScanner({
    minSupport: 5,
    returnThreshold: 0.0001,
    scanMethods: ['threshold'],
    thresholdLevels: [0.5]
  });

  // Create synthetic dataset with planted pattern:
  // When micro_reversion > 0.5, forward return is consistently positive
  const dataset = {
    rows: [],
    featureNames: ['micro_reversion', 'return_momentum'],
    metadata: {}
  };

  // Plant pattern: micro_reversion > 0.5 → positive return
  for (let i = 0; i < 100; i++) {
    const microReversion = i < 50 ? Math.random() * 0.4 : 0.6 + Math.random() * 0.4;
    const forwardReturn = microReversion > 0.5 ? 0.001 + Math.random() * 0.001 : -0.0005 + Math.random() * 0.001;

    dataset.rows.push({
      features: {
        micro_reversion: microReversion,
        return_momentum: Math.random() * 0.2 - 0.1
      },
      regime: 0,
      forwardReturns: {
        h10: forwardReturn
      },
      timestamp: BigInt(i * 1000)
    });
  }

  const patterns = scanner.scan(dataset);

  assert.ok(patterns.length > 0, 'Should find patterns');

  // Find the planted pattern
  const plantedPattern = patterns.find(p =>
    p.conditions.some(c => c.feature === 'micro_reversion' && c.operator === '>' && c.value === 0.5)
  );

  assert.ok(plantedPattern, 'Should find planted pattern (micro_reversion > 0.5)');
  assert.ok(plantedPattern.support >= 5, 'Pattern should have sufficient support');
  assert.ok(plantedPattern.forwardReturns.mean > 0, 'Pattern should have positive mean return');
});

test('PatternScanner - scan empty dataset', () => {
  const scanner = new PatternScanner({
    minSupport: 1,
    scanMethods: ['threshold']
  });

  const dataset = {
    rows: [],
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  const patterns = scanner.scan(dataset);

  assert.equal(patterns.length, 0, 'Should return empty array for empty dataset');
});

test('PatternScanner - pattern structure', () => {
  const scanner = new PatternScanner({
    minSupport: 2,
    returnThreshold: 0.0,
    scanMethods: ['threshold'],
    thresholdLevels: [0.3]
  });

  const dataset = {
    rows: [
      {
        features: { micro_reversion: 0.5 },
        regime: 0,
        forwardReturns: { h10: 0.001 },
        timestamp: BigInt(1000)
      },
      {
        features: { micro_reversion: 0.6 },
        regime: 0,
        forwardReturns: { h10: 0.002 },
        timestamp: BigInt(2000)
      },
      {
        features: { micro_reversion: 0.7 },
        regime: 0,
        forwardReturns: { h10: 0.0015 },
        timestamp: BigInt(3000)
      }
    ],
    featureNames: ['micro_reversion'],
    metadata: {}
  };

  const patterns = scanner.scan(dataset);

  if (patterns.length > 0) {
    const pattern = patterns[0];

    // Verify pattern structure
    assert.ok(pattern.id, 'Pattern should have id');
    assert.ok(pattern.type, 'Pattern should have type');
    assert.ok(Array.isArray(pattern.conditions), 'Pattern should have conditions array');
    assert.ok(['LONG', 'SHORT'].includes(pattern.direction), 'Pattern should have valid direction');
    assert.ok(typeof pattern.support === 'number', 'Pattern should have numeric support');
    assert.ok(pattern.forwardReturns, 'Pattern should have forwardReturns object');
    assert.ok(typeof pattern.forwardReturns.mean === 'number', 'forwardReturns should have mean');
    assert.ok(typeof pattern.horizon === 'number', 'Pattern should have horizon');
    assert.ok(Array.isArray(pattern.matchingIndices), 'Pattern should have matchingIndices array');
  }
});
