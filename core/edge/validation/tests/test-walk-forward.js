/**
 * Test: WalkForwardAnalyzer
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WalkForwardAnalyzer } from '../WalkForwardAnalyzer.js';
import { Edge } from '../../Edge.js';

test('WalkForwardAnalyzer - constructor', () => {
  const analyzer = new WalkForwardAnalyzer();
  assert.ok(analyzer.windowSize > 0);
  assert.ok(analyzer.stepSize > 0);
});

test('WalkForwardAnalyzer - analyze stable edge', () => {
  const analyzer = new WalkForwardAnalyzer({
    windowSize: 50,
    stepSize: 25,
    minPositiveWindows: 0.6
  });

  const edge = new Edge({
    id: 'stable_edge',
    name: 'Stable Edge',
    entryCondition: (features) => ({ active: features.signal > 0.5, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => {
      const signal = Math.random();
      const forwardReturn = signal > 0.5 ? 0.001 : -0.0005;
      return {
        features: { signal },
        regime: 0,
        forwardReturns: { h10: forwardReturn },
        timestamp: BigInt(i * 1000)
      };
    }),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = analyzer.analyze(edge, dataset);

  assert.ok(result.windows.length > 0, 'Should have windows');
  assert.ok(typeof result.positiveWindowFraction === 'number');
  assert.ok(typeof result.sharpeTrend === 'number');
  assert.ok(typeof result.consistency === 'number');
  assert.ok(typeof result.passed === 'boolean');

  console.log(`  ${result.windows.length} windows, ${(result.positiveWindowFraction * 100).toFixed(0)}% positive, trend: ${result.sharpeTrend.toFixed(4)}`);
});

test('WalkForwardAnalyzer - analyze decaying edge', () => {
  const analyzer = new WalkForwardAnalyzer({
    windowSize: 50,
    stepSize: 25
  });

  const edge = new Edge({
    id: 'decay_edge',
    name: 'Decaying Edge',
    entryCondition: (features) => ({ active: true, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  // Create decaying returns over time
  const dataset = {
    rows: Array(200).fill(null).map((_, i) => {
      const decayFactor = 1 - (i / 200); // Linear decay
      const forwardReturn = 0.001 * decayFactor;
      return {
        features: { signal: 1 },
        regime: 0,
        forwardReturns: { h10: forwardReturn },
        timestamp: BigInt(i * 1000)
      };
    }),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = analyzer.analyze(edge, dataset);

  assert.ok(result.sharpeTrend < 0, 'Decaying edge should have negative trend');
  console.log(`  Decay test - trend: ${result.sharpeTrend.toFixed(4)}, passed: ${result.passed}`);
});
