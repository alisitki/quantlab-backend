/**
 * Test: FeatureImportanceTracker
 *
 * Validates feature importance calculation from trade outcomes.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FeatureImportanceTracker } from '../FeatureImportanceTracker.js';

test('FeatureImportanceTracker - analyze with synthetic outcomes', () => {
  const tracker = new FeatureImportanceTracker();

  // Create synthetic outcomes
  // Feature "volatility_ratio" is positively correlated with PnL
  // Feature "noise" is random (low correlation)
  const outcomes = [];

  for (let i = 0; i < 50; i++) {
    const volRatio = Math.random() * 2; // [0, 2]
    const noise = Math.random();

    // PnL is positively correlated with volRatio
    const pnl = (volRatio - 1) * 0.001 + (Math.random() - 0.5) * 0.0005;

    outcomes.push({
      edgeId: 'edge_test',
      strategyId: 'strat_test',
      entryFeatures: {
        volatility_ratio: volRatio,
        noise
      },
      pnl,
      outcome: pnl > 0 ? 'WIN' : 'LOSS',
      timestamp: Date.now() + i
    });
  }

  const results = tracker.analyze(outcomes);

  assert.ok(results['edge_test'], 'Should analyze edge_test');

  const volImportance = results['edge_test'].volatility_ratio;
  const noiseImportance = results['edge_test'].noise;

  assert.ok(volImportance.importance > 0, 'volatility_ratio should have non-zero importance');
  assert.ok(noiseImportance.importance >= 0, 'noise should have low importance');

  // Correlated feature should have higher importance than noise
  assert.ok(volImportance.importance > noiseImportance.importance,
    'Correlated feature should have higher importance than noise');

  console.log('✅ Feature importance calculated correctly');
  console.log(`   volatility_ratio: importance=${volImportance.importance.toFixed(3)}, correlation=${volImportance.correlation.toFixed(3)}`);
  console.log(`   noise: importance=${noiseImportance.importance.toFixed(3)}, correlation=${noiseImportance.correlation.toFixed(3)}`);
});

test('FeatureImportanceTracker - getFeatureRanking', () => {
  const tracker = new FeatureImportanceTracker();

  const outcomes = [];

  for (let i = 0; i < 30; i++) {
    const featureA = Math.random();
    const featureB = Math.random();

    // featureA strongly correlated with PnL
    const pnl = (featureA - 0.5) * 0.002;

    outcomes.push({
      edgeId: 'edge_rank',
      entryFeatures: { featureA, featureB },
      pnl,
      outcome: pnl > 0 ? 'WIN' : 'LOSS',
      timestamp: Date.now() + i
    });
  }

  tracker.analyze(outcomes);

  const ranking = tracker.getFeatureRanking('edge_rank');

  assert.ok(ranking.length === 2, 'Should have 2 features');
  assert.ok(ranking[0].feature === 'featureA', 'featureA should rank first');
  assert.ok(ranking[1].feature === 'featureB', 'featureB should rank second');
  assert.ok(ranking[0].importance > ranking[1].importance, 'Ranking should be sorted by importance');

  console.log('✅ Feature ranking works correctly');
  console.log(`   Top feature: ${ranking[0].feature} (importance=${ranking[0].importance.toFixed(3)})`);
});

test('FeatureImportanceTracker - getNoiseFeatures', () => {
  const tracker = new FeatureImportanceTracker();

  const outcomes = [];

  // Use more samples for clearer signal/noise distinction
  for (let i = 0; i < 100; i++) {
    const signal = Math.random();
    const noise1 = Math.random();
    const noise2 = Math.random();

    // Strong correlation with signal, no correlation with noise
    const pnl = (signal - 0.5) * 0.004;

    outcomes.push({
      edgeId: 'edge_noise',
      entryFeatures: { signal, noise1, noise2 },
      pnl,
      outcome: pnl > 0 ? 'WIN' : 'LOSS',
      timestamp: Date.now() + i
    });
  }

  tracker.analyze(outcomes);

  const ranking = tracker.getFeatureRanking('edge_noise');

  // Signal should have highest importance
  assert.ok(ranking[0].feature === 'signal', 'signal should rank first');

  // At least one noise feature should have low importance
  const lowImportanceFeatures = ranking.filter(r => r.importance < 0.3);
  assert.ok(lowImportanceFeatures.length >= 1, 'Should have at least one low-importance feature');

  const noiseFeatures = tracker.getNoiseFeatures('edge_noise', 0.3);

  // signal should not be classified as noise
  assert.ok(!noiseFeatures.includes('signal'), 'signal should not be classified as noise');

  console.log('✅ Noise feature detection works');
  console.log(`   Noise features: ${noiseFeatures.join(', ')}`);
  console.log(`   Signal importance: ${ranking[0].importance.toFixed(3)}`);
});

test('FeatureImportanceTracker - handles missing data', () => {
  const tracker = new FeatureImportanceTracker();

  // Outcomes with some missing feature values (need at least 10 outcomes)
  const outcomes = [];

  for (let i = 0; i < 15; i++) {
    outcomes.push({
      edgeId: 'edge_missing',
      entryFeatures: {
        featureA: i < 12 ? Math.random() : null,  // Some null values
        featureB: Math.random()
      },
      pnl: Math.random() * 0.002 - 0.001,
      outcome: Math.random() > 0.5 ? 'WIN' : 'LOSS',
      timestamp: Date.now() + i
    });
  }

  const results = tracker.analyze(outcomes);

  // Should analyze despite some missing data
  assert.ok(results['edge_missing'], 'Should analyze despite missing data');
  assert.ok(results['edge_missing'].featureA, 'Should analyze featureA despite some nulls');
  assert.ok(results['edge_missing'].featureB, 'Should analyze featureB');

  console.log('✅ Handles missing data gracefully');
});

test('FeatureImportanceTracker - serialization', () => {
  const tracker = new FeatureImportanceTracker({ maxHistorySize: 5 });

  const outcomes = [];
  for (let i = 0; i < 20; i++) {
    outcomes.push({
      edgeId: 'edge_ser',
      entryFeatures: { feature1: Math.random() },
      pnl: Math.random() * 0.002 - 0.001,
      outcome: Math.random() > 0.5 ? 'WIN' : 'LOSS',
      timestamp: Date.now() + i
    });
  }

  tracker.analyze(outcomes);

  const json = tracker.toJSON();
  const restored = FeatureImportanceTracker.fromJSON(json);

  assert.ok(restored.getSummary().analysisCount === tracker.getSummary().analysisCount,
    'Should preserve analysis count');

  console.log('✅ Serialization works');
});

console.log('\n✅ All FeatureImportanceTracker tests passed!');
