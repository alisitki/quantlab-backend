/**
 * Test: EdgeCandidateGenerator
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeCandidateGenerator } from '../EdgeCandidateGenerator.js';

test('EdgeCandidateGenerator - constructor', () => {
  const generator = new EdgeCandidateGenerator();

  assert.ok(generator.defaultTimeHorizon > 0);
  assert.ok(generator.minConfidenceScore >= 0 && generator.minConfidenceScore <= 1);
});

test('EdgeCandidateGenerator - generate edge from pattern', () => {
  const generator = new EdgeCandidateGenerator();

  const pattern = {
    id: 'test_pattern_1',
    type: 'threshold',
    conditions: [
      { feature: 'micro_reversion', operator: '>', value: 0.5 }
    ],
    regimes: [0, 1],
    direction: 'LONG',
    support: 50,
    forwardReturns: {
      mean: 0.002,
      median: 0.0018,
      std: 0.0008,
      count: 50
    },
    horizon: 10,
    matchingIndices: Array(50).fill(null).map((_, i) => i)
  };

  const testResult = {
    patternId: 'test_pattern_1',
    isSignificant: true,
    tests: {
      tTest: { statistic: 3.5, pValue: 0.001, passed: true },
      permutationTest: { pValue: 0.002, nPermutations: 1000, passed: true },
      sharpeTest: { sharpe: 0.8, passed: true },
      regimeRobustness: { perRegimeSharpe: { 0: 0.9, 1: 0.7 }, passed: true },
      sampleSizeTest: { count: 50, minRequired: 30, passed: true }
    },
    overallScore: 1.0,
    recommendation: 'ACCEPT'
  };

  const edge = generator.generate(pattern, testResult);

  // Verify Edge structure
  assert.ok(edge.id, 'Edge should have id');
  assert.ok(edge.name, 'Edge should have name');
  assert.ok(typeof edge.entryCondition === 'function', 'Should have entry condition function');
  assert.ok(typeof edge.exitCondition === 'function', 'Should have exit condition function');
  assert.deepEqual(edge.regimes, [0, 1], 'Should preserve regimes');
  assert.ok(edge.timeHorizon > 0, 'Should have time horizon');
  assert.equal(edge.status, 'CANDIDATE', 'Should have CANDIDATE status');
  assert.ok(edge.expectedAdvantage, 'Should have expectedAdvantage');
  assert.ok(edge.riskProfile, 'Should have riskProfile');
  assert.ok(edge.confidence, 'Should have confidence');
});

test('EdgeCandidateGenerator - entry condition works', () => {
  const generator = new EdgeCandidateGenerator();

  const pattern = {
    id: 'test',
    type: 'threshold',
    conditions: [
      { feature: 'micro_reversion', operator: '>', value: 0.6 }
    ],
    regimes: null,
    direction: 'LONG',
    support: 30,
    forwardReturns: { mean: 0.001, median: 0.001, std: 0.0005, count: 30 },
    horizon: 10,
    matchingIndices: []
  };

  const testResult = {
    patternId: 'test',
    isSignificant: true,
    tests: {
      sharpeTest: { sharpe: 0.5 },
      tTest: {}, permutationTest: {}, regimeRobustness: {}, sampleSizeTest: {}
    },
    overallScore: 0.8,
    recommendation: 'ACCEPT'
  };

  const edge = generator.generate(pattern, testResult);

  // Test entry condition
  const features1 = { micro_reversion: 0.7 };
  const result1 = edge.entryCondition(features1, 0);
  assert.ok(result1.active, 'Should activate when condition met');
  assert.equal(result1.direction, 'LONG');

  const features2 = { micro_reversion: 0.4 };
  const result2 = edge.entryCondition(features2, 0);
  assert.ok(!result2.active, 'Should not activate when condition not met');
});

test('EdgeCandidateGenerator - exit condition works', () => {
  const generator = new EdgeCandidateGenerator();

  const pattern = {
    id: 'test',
    type: 'threshold',
    conditions: [
      { feature: 'return_momentum', operator: '>', value: 0.3 }
    ],
    regimes: null,
    direction: 'LONG',
    support: 30,
    forwardReturns: { mean: 0.001, median: 0.001, std: 0.0005, count: 30 },
    horizon: 10,
    matchingIndices: []
  };

  const testResult = {
    patternId: 'test',
    isSignificant: true,
    tests: {
      sharpeTest: { sharpe: 0.5 },
      tTest: {}, permutationTest: {}, regimeRobustness: {}, sampleSizeTest: {}
    },
    overallScore: 0.8,
    recommendation: 'ACCEPT'
  };

  const edge = generator.generate(pattern, testResult);

  // Still meets condition - don't exit
  const features1 = { return_momentum: 0.5 };
  const exit1 = edge.exitCondition(features1, 0, 1000, 2000);
  assert.ok(!exit1.exit, 'Should not exit when condition still met');

  // Condition reversed - exit
  const features2 = { return_momentum: 0.1 };
  const exit2 = edge.exitCondition(features2, 0, 1000, 2000);
  assert.ok(exit2.exit, 'Should exit when condition reversed');
  assert.equal(exit2.reason, 'condition_reversed');
});

test('EdgeCandidateGenerator - generateBatch filters ACCEPT only', () => {
  const generator = new EdgeCandidateGenerator();

  const validatedPatterns = [
    {
      pattern: {
        id: 'p1',
        type: 'threshold',
        conditions: [{ feature: 'micro_reversion', operator: '>', value: 0.5 }],
        regimes: null,
        direction: 'LONG',
        support: 30,
        forwardReturns: { mean: 0.001, median: 0.001, std: 0.0005, count: 30 },
        horizon: 10,
        matchingIndices: []
      },
      testResult: {
        patternId: 'p1',
        recommendation: 'ACCEPT',
        tests: { sharpeTest: { sharpe: 0.6 }, tTest: {}, permutationTest: {}, regimeRobustness: {}, sampleSizeTest: {} },
        overallScore: 0.8
      }
    },
    {
      pattern: {
        id: 'p2',
        type: 'threshold',
        conditions: [{ feature: 'return_momentum', operator: '<', value: -0.3 }],
        regimes: null,
        direction: 'SHORT',
        support: 25,
        forwardReturns: { mean: -0.0008, median: -0.0008, std: 0.0004, count: 25 },
        horizon: 10,
        matchingIndices: []
      },
      testResult: {
        patternId: 'p2',
        recommendation: 'REJECT',
        tests: {},
        overallScore: 0.3
      }
    }
  ];

  const edges = generator.generateBatch(validatedPatterns);

  assert.equal(edges.length, 1, 'Should only generate edges from ACCEPT patterns');
  assert.ok(edges[0].id.includes('p1'), 'Should generate edge from p1');
});
