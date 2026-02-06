import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { VolatilityRatioFeature } from '../../builders/regime/VolatilityRatioFeature.js';
import { TrendStrengthFeature } from '../../builders/regime/TrendStrengthFeature.js';
import { SpreadRatioFeature } from '../../builders/regime/SpreadRatioFeature.js';

describe('Continuous Regime Features - Milestone 3a', () => {
  describe('VolatilityRatioFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new VolatilityRatioFeature({ shortWindow: 10, longWindow: 50 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 49; i++) {
        const price = 100 + Math.random();
        const result = feature.onEvent({ bid_price: price, ask_price: price + 1 });
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects volatility compression (ratio < 1)', () => {
      // Start with high volatility
      for (let i = 0; i < 60; i++) {
        const price = 100 + (Math.random() - 0.5) * 2;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      // Compress (low volatility)
      for (let i = 0; i < 15; i++) {
        const price = 100 + (Math.random() - 0.5) * 0.1;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 101 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < 1, 'Should detect compression (ratio < 1)');
      assert.ok(result >= 0, 'Should be non-negative');
      assert.ok(result <= 5, 'Should be capped at 5');
    });

    it('detects volatility expansion (ratio > 1)', () => {
      // Start with low volatility
      for (let i = 0; i < 60; i++) {
        const price = 100 + (Math.random() - 0.5) * 0.1;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      // Expand (high volatility)
      for (let i = 0; i < 15; i++) {
        const price = 100 + (Math.random() - 0.5) * 5;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 105, ask_price: 106 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 1, 'Should detect expansion (ratio > 1)');
      assert.ok(result <= 5, 'Should be capped at 5');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 60; i++) {
        const price = 100 + Math.sin(i / 10) * 2;
        events.push({ bid_price: price, ask_price: price + 1 });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('TrendStrengthFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new TrendStrengthFeature({ fastPeriod: 5, slowPeriod: 15, slopePeriod: 3 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 17; i++) {
        const price = 100 + i * 0.1;
        const result = feature.onEvent({ bid_price: price, ask_price: price + 1 });
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects strong uptrend (positive)', () => {
      // Strong uptrend
      for (let i = 0; i < 25; i++) {
        const price = 100 + i * 0.5;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 112, ask_price: 113 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0.3, 'Should detect uptrend (positive)');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('detects strong downtrend (negative)', () => {
      // Strong downtrend
      for (let i = 0; i < 25; i++) {
        const price = 100 - i * 0.5;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 88, ask_price: 89 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < -0.3, 'Should detect downtrend (negative)');
      assert.ok(result >= -1, 'Should be in range [-1, 1]');
    });

    it('detects sideways market (near zero)', () => {
      // Sideways
      for (let i = 0; i < 25; i++) {
        const price = 100 + (Math.random() - 0.5) * 0.5;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 101 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(Math.abs(result) < 0.3, 'Should detect sideways (near zero)');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 30; i++) {
        const price = 100 + i * 0.3;
        events.push({ bid_price: price, ask_price: price + 1 });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('SpreadRatioFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new SpreadRatioFeature({ window: 30 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 29; i++) {
        const result = feature.onEvent({ bid_price: 100, ask_price: 101 });
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects tight spread (ratio < 1)', () => {
      // Start with normal spread
      for (let i = 0; i < 35; i++) {
        feature.onEvent({ bid_price: 100, ask_price: 101 });
      }

      // Tight spread
      for (let i = 0; i < 5; i++) {
        feature.onEvent({ bid_price: 100, ask_price: 100.3 });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 100.3 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < 1, 'Should detect tight spread (ratio < 1)');
      assert.ok(result >= 0, 'Should be non-negative');
      assert.ok(result <= 5, 'Should be capped at 5');
    });

    it('detects wide spread (ratio > 1)', () => {
      // Start with normal spread
      for (let i = 0; i < 35; i++) {
        feature.onEvent({ bid_price: 100, ask_price: 101 });
      }

      // Wide spread
      for (let i = 0; i < 5; i++) {
        feature.onEvent({ bid_price: 100, ask_price: 103 });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 103 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 1.5, 'Should detect wide spread (ratio > 1.5)');
      assert.ok(result <= 5, 'Should be capped at 5');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 40; i++) {
        const spread = 0.5 + Math.random() * 1.5;
        events.push({ bid_price: 100, ask_price: 100 + spread });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('Integration: Continuous Regime Features in Registry', () => {
    it('can register and instantiate continuous regime features', async () => {
      const { FeatureRegistry } = await import('../../FeatureRegistry.js');

      const builder = FeatureRegistry.createFeatureBuilder('BTCUSDT', {
        enabledFeatures: [
          'mid_price',
          'volatility_ratio',
          'trend_strength',
          'spread_ratio'
        ]
      });

      assert.ok(builder, 'FeatureBuilder should be created');

      // Generate sufficient events
      const events = [];
      for (let i = 0; i < 150; i++) {
        const price = 100 + Math.sin(i / 20) * 5;
        events.push({
          bid_price: price,
          ask_price: price + 0.5 + Math.random() * 0.5
        });
      }

      let result = null;
      for (const event of events) {
        result = builder.onEvent(event);
      }

      assert.ok(result !== null, 'Should eventually warm up');
      assert.ok('volatility_ratio' in result, 'Should include volatility_ratio');
      assert.ok('trend_strength' in result, 'Should include trend_strength');
      assert.ok('spread_ratio' in result, 'Should include spread_ratio');

      // Validate ranges
      assert.ok(result.volatility_ratio >= 0 && result.volatility_ratio <= 5,
        'volatility_ratio should be in [0, 5]');
      assert.ok(result.trend_strength >= -1 && result.trend_strength <= 1,
        'trend_strength should be in [-1, 1]');
      assert.ok(result.spread_ratio >= 0 && result.spread_ratio <= 5,
        'spread_ratio should be in [0, 5]');
    });
  });
});
