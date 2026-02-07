/**
 * Test: RegimeRobustnessTester
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RegimeRobustnessTester } from '../RegimeRobustnessTester.js';
import { Edge } from '../../Edge.js';

test('RegimeRobustnessTester - constructor', () => {
  const tester = new RegimeRobustnessTester();
  assert.ok(tester.minTradesPerRegime > 0);
});

test('RegimeRobustnessTester - universal edge', () => {
  const tester = new RegimeRobustnessTester({ minRegimeSharpe: 0.1 });

  const edge = new Edge({
    id: 'universal',
    name: 'Universal',
    entryCondition: () => ({ active: true, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    regimes: null,  // No regime constraint
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => ({
      features: {},
      regime: i % 4,  // 4 regimes
      forwardReturns: { h10: 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: [],
    metadata: {}
  };

  const result = tester.test(edge, dataset);

  assert.equal(result.regimeSelectivity, 0, 'Universal edge should have 0 selectivity');
  console.log(`  Universal edge: passed ${result.passed}`);
});

test('RegimeRobustnessTester - regime-specific edge', () => {
  const tester = new RegimeRobustnessTester({
    minTradesPerRegime: 10,
    minRegimeSharpe: 0.1,
    selectivityThreshold: 0.1
  });

  const edge = new Edge({
    id: 'regime_specific',
    name: 'Regime Specific',
    entryCondition: (features, regime) => ({
      active: regime === 0 || regime === 1,  // Only works in regime 0, 1
      direction: 'LONG'
    }),
    exitCondition: () => ({ exit: false }),
    regimes: [0, 1],
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => {
      const regime = i % 4;
      const forwardReturn = (regime === 0 || regime === 1) ? 0.002 : -0.001;
      return {
        features: {},
        regime,
        forwardReturns: { h10: forwardReturn },
        timestamp: BigInt(i * 1000)
      };
    }),
    featureNames: [],
    metadata: {}
  };

  const result = tester.test(edge, dataset);

  assert.ok(result.targetRegimePerformance > result.otherRegimePerformance,
    'Target regime should perform better than other regimes');
  console.log(`  Regime-specific: target ${result.targetRegimePerformance.toFixed(3)}, other ${result.otherRegimePerformance.toFixed(3)}, selectivity ${result.regimeSelectivity.toFixed(3)}`);
});
