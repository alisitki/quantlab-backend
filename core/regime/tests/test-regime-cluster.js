import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RegimeCluster } from '../RegimeCluster.js';

describe('RegimeCluster - Milestone 3b', () => {
  describe('K-means Clustering', () => {
    let cluster;

    beforeEach(() => {
      cluster = new RegimeCluster({ K: 3, seed: 42, maxIterations: 50 });
    });

    it('creates cluster with default config', () => {
      const info = cluster.getInfo();
      assert.equal(info.trained, false);
      assert.equal(info.K, 3);
    });

    it('trains on synthetic data with clear clusters', () => {
      // Generate 3 clear clusters
      const data = [
        // Cluster 0: Low volatility, low trend
        ...Array.from({ length: 30 }, (_, i) => ({
          volatility_ratio: 0.3 + Math.random() * 0.2,
          trend_strength: -0.1 + Math.random() * 0.2,
          spread_ratio: 0.8 + Math.random() * 0.3
        })),
        // Cluster 1: High volatility, high trend
        ...Array.from({ length: 30 }, (_, i) => ({
          volatility_ratio: 2.5 + Math.random() * 0.5,
          trend_strength: 0.7 + Math.random() * 0.2,
          spread_ratio: 2.0 + Math.random() * 0.5
        })),
        // Cluster 2: Medium volatility, negative trend
        ...Array.from({ length: 30 }, (_, i) => ({
          volatility_ratio: 1.2 + Math.random() * 0.3,
          trend_strength: -0.7 + Math.random() * 0.2,
          spread_ratio: 1.5 + Math.random() * 0.3
        }))
      ];

      const result = cluster.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      assert.ok(result.iterations > 0, 'Should run iterations');
      assert.ok(result.converged, 'Should converge');
      assert.ok(result.inertia >= 0, 'Should calculate inertia');
      assert.equal(result.clusterSizes.length, 3);

      const info = cluster.getInfo();
      assert.equal(info.trained, true);
      assert.equal(info.featureCount, 3);
    });

    it('predicts cluster for new data point', () => {
      // Train
      const data = Array.from({ length: 100 }, (_, i) => ({
        volatility_ratio: (i < 50) ? 0.5 + Math.random() * 0.3 : 2.0 + Math.random() * 0.5,
        trend_strength: (i < 50) ? -0.2 + Math.random() * 0.3 : 0.6 + Math.random() * 0.3,
        spread_ratio: 1.0 + Math.random() * 0.5
      }));

      cluster.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      // Predict low vol point (should be cluster 0 or similar)
      const lowVolPoint = {
        volatility_ratio: 0.4,
        trend_strength: -0.1,
        spread_ratio: 1.0
      };

      const prediction1 = cluster.predict(lowVolPoint);
      assert.ok(prediction1.cluster !== null, 'Should predict cluster');
      assert.ok(prediction1.confidence >= 0 && prediction1.confidence <= 1, 'Confidence should be [0,1]');
      assert.ok(prediction1.distance >= 0, 'Distance should be non-negative');

      // Predict high vol point
      const highVolPoint = {
        volatility_ratio: 2.2,
        trend_strength: 0.7,
        spread_ratio: 1.2
      };

      const prediction2 = cluster.predict(highVolPoint);
      assert.ok(prediction2.cluster !== null, 'Should predict cluster');

      // Different points should potentially have different clusters
      // (not guaranteed with random data, but likely)
    });

    it('handles missing features in prediction', () => {
      const data = Array.from({ length: 50 }, () => ({
        volatility_ratio: 1.0 + Math.random(),
        trend_strength: 0 + Math.random() * 0.5,
        spread_ratio: 1.0 + Math.random()
      }));

      cluster.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      const missingFeatures = {
        volatility_ratio: 1.0,
        trend_strength: null, // Missing
        spread_ratio: 1.0
      };

      const prediction = cluster.predict(missingFeatures);
      assert.equal(prediction.cluster, null, 'Should return null cluster for missing features');
      assert.equal(prediction.confidence, 0, 'Should have 0 confidence for missing features');
    });

    it('throws error when training without data', () => {
      assert.throws(() => {
        cluster.train([], ['volatility_ratio']);
      }, /Training data cannot be empty/);
    });

    it('throws error when training without feature names', () => {
      const data = [{ volatility_ratio: 1.0 }];
      assert.throws(() => {
        cluster.train(data, []);
      }, /Feature names must be specified/);
    });

    it('throws error when predicting before training', () => {
      assert.throws(() => {
        cluster.predict({ volatility_ratio: 1.0 });
      }, /Model not trained/);
    });

    it('serializes and deserializes model', () => {
      const data = Array.from({ length: 60 }, () => ({
        volatility_ratio: 1.0 + Math.random(),
        trend_strength: 0.5 + Math.random() * 0.5,
        spread_ratio: 1.0 + Math.random()
      }));

      cluster.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      const json = cluster.toJSON();
      assert.ok(json.K);
      assert.ok(json.centroids);
      assert.ok(json.featureNames);
      assert.equal(json.trained, true);

      // Load model
      const loaded = RegimeCluster.fromJSON(json);
      const info = loaded.getInfo();
      assert.equal(info.trained, true);
      assert.equal(info.K, 3);

      // Prediction should work
      const testPoint = {
        volatility_ratio: 1.0,
        trend_strength: 0.5,
        spread_ratio: 1.0
      };

      const prediction = loaded.predict(testPoint);
      assert.ok(prediction.cluster !== null);
    });

    it('is deterministic with same seed', () => {
      const data = Array.from({ length: 60 }, (_, i) => ({
        volatility_ratio: 1.0 + (i % 10) * 0.1,
        trend_strength: 0.5 + (i % 8) * 0.05,
        spread_ratio: 1.0 + (i % 12) * 0.08
      }));

      // Train first model
      const cluster1 = new RegimeCluster({ K: 3, seed: 42 });
      const result1 = cluster1.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      // Train second model with same seed
      const cluster2 = new RegimeCluster({ K: 3, seed: 42 });
      const result2 = cluster2.train(data, ['volatility_ratio', 'trend_strength', 'spread_ratio']);

      // Results should be identical
      assert.equal(result1.iterations, result2.iterations);
      assert.deepEqual(result1.clusterSizes, result2.clusterSizes);

      // Predictions should be identical
      const testPoint = {
        volatility_ratio: 1.5,
        trend_strength: 0.6,
        spread_ratio: 1.2
      };

      const pred1 = cluster1.predict(testPoint);
      const pred2 = cluster2.predict(testPoint);

      assert.equal(pred1.cluster, pred2.cluster);
      assert.equal(pred1.distance, pred2.distance);
    });
  });

  describe('Regime Discovery Scenario', () => {
    it('discovers 4 market regimes from synthetic data', () => {
      const cluster = new RegimeCluster({ K: 4, seed: 123 });

      // Generate realistic regime data
      const data = [
        // Regime 0: Quiet/Low Vol/Sideways (30 samples)
        ...Array.from({ length: 30 }, () => ({
          volatility_ratio: 0.3 + Math.random() * 0.2,
          trend_strength: -0.1 + Math.random() * 0.2,
          spread_ratio: 0.8 + Math.random() * 0.2,
          liquidity_pressure: -0.1 + Math.random() * 0.2
        })),

        // Regime 1: Trending Up (30 samples)
        ...Array.from({ length: 30 }, () => ({
          volatility_ratio: 1.2 + Math.random() * 0.4,
          trend_strength: 0.6 + Math.random() * 0.3,
          spread_ratio: 1.0 + Math.random() * 0.3,
          liquidity_pressure: 0.3 + Math.random() * 0.3
        })),

        // Regime 2: Trending Down (30 samples)
        ...Array.from({ length: 30 }, () => ({
          volatility_ratio: 1.3 + Math.random() * 0.4,
          trend_strength: -0.7 + Math.random() * 0.2,
          spread_ratio: 1.1 + Math.random() * 0.3,
          liquidity_pressure: -0.4 + Math.random() * 0.3
        })),

        // Regime 3: Volatile/Chaotic (30 samples)
        ...Array.from({ length: 30 }, () => ({
          volatility_ratio: 2.5 + Math.random() * 0.8,
          trend_strength: -0.2 + Math.random() * 0.4,
          spread_ratio: 2.0 + Math.random() * 0.5,
          liquidity_pressure: -0.2 + Math.random() * 0.4
        }))
      ];

      const result = cluster.train(data, [
        'volatility_ratio',
        'trend_strength',
        'spread_ratio',
        'liquidity_pressure'
      ]);

      assert.equal(result.clusterSizes.length, 4, 'Should have 4 clusters');
      assert.ok(result.converged, 'Should converge');

      // Test prediction for each regime type
      const quietRegime = cluster.predict({
        volatility_ratio: 0.4,
        trend_strength: 0.0,
        spread_ratio: 0.9,
        liquidity_pressure: 0.0
      });

      const trendingUpRegime = cluster.predict({
        volatility_ratio: 1.3,
        trend_strength: 0.7,
        spread_ratio: 1.1,
        liquidity_pressure: 0.4
      });

      const volatileRegime = cluster.predict({
        volatility_ratio: 2.8,
        trend_strength: 0.0,
        spread_ratio: 2.2,
        liquidity_pressure: 0.0
      });

      // All should predict successfully
      assert.ok(quietRegime.cluster !== null);
      assert.ok(trendingUpRegime.cluster !== null);
      assert.ok(volatileRegime.cluster !== null);

      // Confidence should be reasonable
      assert.ok(quietRegime.confidence > 0.3, 'Should have reasonable confidence');
      assert.ok(trendingUpRegime.confidence > 0.3, 'Should have reasonable confidence');
    });
  });
});
