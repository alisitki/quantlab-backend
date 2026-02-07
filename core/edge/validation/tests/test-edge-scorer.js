/**
 * Test: EdgeScorer
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeScorer } from '../EdgeScorer.js';

test('EdgeScorer - constructor', () => {
  const scorer = new EdgeScorer();
  assert.ok(scorer.weights);
  assert.ok(scorer.minScore > 0);
});

test('EdgeScorer - score perfect edge', () => {
  const scorer = new EdgeScorer({ minScore: 0.7 });

  const mockResults = {
    oos: { passed: true, confidence: 0.9, inSample: { trades: 50 }, outOfSample: { trades: 30 } },
    wf: { passed: true, positiveWindowFraction: 1.0, sharpeTrend: 0.1, consistency: 0.5 },
    decay: { passed: true, isDecaying: false, decayRate: 0 },
    regime: { passed: true, regimeSelectivity: 0.5, targetRegimePerformance: 1.0 }
  };

  const score = scorer.score(mockResults.oos, mockResults.wf, mockResults.decay, mockResults.regime);

  assert.ok(score.total > 0, 'Should have positive score');
  assert.equal(score.recommendation, 'VALIDATED', 'Perfect edge should be validated');
  assert.ok(score.summary, 'Should have summary');
  console.log(`  Perfect edge score: ${score.total.toFixed(3)}, ${score.recommendation}`);
});

test('EdgeScorer - score weak edge', () => {
  const scorer = new EdgeScorer({ minScore: 0.7, weakThreshold: 0.4 });

  const mockResults = {
    oos: { passed: false, confidence: 0.3, inSample: { trades: 20 }, outOfSample: { trades: 10 } },
    wf: { passed: false, positiveWindowFraction: 0.4, sharpeTrend: -0.2, consistency: 2.0 },
    decay: { passed: true, isDecaying: false, decayRate: 0 },
    regime: { passed: true, regimeSelectivity: 0.1, targetRegimePerformance: 0.5 }
  };

  const score = scorer.score(mockResults.oos, mockResults.wf, mockResults.decay, mockResults.regime);

  assert.ok(score.recommendation === 'MARGINAL' || score.recommendation === 'REJECTED',
    'Weak edge should be marginal or rejected');
  console.log(`  Weak edge score: ${score.total.toFixed(3)}, ${score.recommendation}`);
});
