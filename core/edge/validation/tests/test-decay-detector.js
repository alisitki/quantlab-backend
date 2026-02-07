/**
 * Test: DecayDetector
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DecayDetector } from '../DecayDetector.js';
import { Edge } from '../../Edge.js';

test('DecayDetector - constructor', () => {
  const detector = new DecayDetector();
  assert.ok(detector.windowSize > 0);
  assert.ok(detector.maxDecayRate < 0);
});

test('DecayDetector - detect stable edge', () => {
  const detector = new DecayDetector({
    windowSize: 50,
    maxDecayRate: -0.0001
  });

  const edge = new Edge({
    id: 'stable',
    name: 'Stable',
    entryCondition: () => ({ active: true, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => ({
      features: { signal: 1 },
      regime: 0,
      forwardReturns: { h10: 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = detector.detect(edge, dataset);

  assert.ok(!result.isDecaying, 'Stable edge should not be decaying');
  assert.ok(result.passed, 'Stable edge should pass');
  console.log(`  Stable: decay rate ${result.decayRate.toFixed(6)}, PSI ${result.psi.toFixed(3)}`);
});

test('DecayDetector - detect decaying edge', () => {
  const detector = new DecayDetector({
    windowSize: 50,
    maxDecayRate: -0.00001
  });

  const edge = new Edge({
    id: 'decay',
    name: 'Decaying',
    entryCondition: () => ({ active: true, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => ({
      features: { signal: 1 },
      regime: 0,
      forwardReturns: { h10: 0.001 * (1 - i / 200) },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = detector.detect(edge, dataset);

  assert.ok(result.decayRate < 0, 'Should have negative decay rate');
  assert.ok(result.halfLife !== null, 'Should estimate half-life');
  console.log(`  Decay: rate ${result.decayRate.toFixed(6)}, half-life ${result.halfLife?.toFixed(0)}`);
});
