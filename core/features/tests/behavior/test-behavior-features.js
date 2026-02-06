import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { LiquidityPressureFeature } from '../../builders/behavior/LiquidityPressureFeature.js';
import { ReturnMomentumFeature } from '../../builders/behavior/ReturnMomentumFeature.js';
import { RegimeStabilityFeature } from '../../builders/behavior/RegimeStabilityFeature.js';

describe('Behavior Features - Phase 2 Minimal Viable', () => {
  describe('LiquidityPressureFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new LiquidityPressureFeature({ period: 5 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      const event = { bid_price: 100, bid_qty: 100, ask_price: 101, ask_qty: 50 };

      // First few events should return null (warmup)
      for (let i = 0; i < 4; i++) {
        const result = feature.onEvent(event);
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('returns positive pressure when bid_qty > ask_qty', () => {
      const event = { bid_price: 100, bid_qty: 100, ask_price: 101, ask_qty: 50 };

      // Warmup
      for (let i = 0; i < 5; i++) {
        feature.onEvent(event);
      }

      const result = feature.onEvent(event);
      assert.ok(result > 0, 'Should return positive pressure');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('returns negative pressure when ask_qty > bid_qty', () => {
      const event = { bid_price: 100, bid_qty: 50, ask_price: 101, ask_qty: 100 };

      // Warmup
      for (let i = 0; i < 5; i++) {
        feature.onEvent(event);
      }

      const result = feature.onEvent(event);
      assert.ok(result < 0, 'Should return negative pressure');
      assert.ok(result >= -1, 'Should be in range [-1, 1]');
    });

    it('returns 0 when quantities are equal', () => {
      const event = { bid_price: 100, bid_qty: 50, ask_price: 101, ask_qty: 50 };

      // Warmup
      for (let i = 0; i < 5; i++) {
        feature.onEvent(event);
      }

      const result = feature.onEvent(event);
      assert.ok(Math.abs(result) < 0.1, 'Should return near 0 for balanced quantities');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [
        { bid_price: 100, bid_qty: 100, ask_price: 101, ask_qty: 50 },
        { bid_price: 100, bid_qty: 80, ask_price: 101, ask_qty: 60 },
        { bid_price: 100, bid_qty: 60, ask_price: 101, ask_qty: 80 },
        { bid_price: 100, bid_qty: 90, ask_price: 101, ask_qty: 55 },
        { bid_price: 100, bid_qty: 70, ask_price: 101, ask_qty: 70 },
        { bid_price: 100, bid_qty: 85, ask_price: 101, ask_qty: 45 },
      ];

      // First run
      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();

      // Second run
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('ReturnMomentumFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new ReturnMomentumFeature({ window: 10, weightByMagnitude: true });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      const events = [
        { bid_price: 100, ask_price: 101 },
        { bid_price: 101, ask_price: 102 },
        { bid_price: 102, ask_price: 103 },
      ];

      for (const event of events) {
        const result = feature.onEvent(event);
        assert.equal(result, null, 'Should return null during warmup');
      }
    });

    it('returns positive momentum for upward trend', () => {
      // Simulate upward trend
      for (let i = 0; i < 12; i++) {
        const price = 100 + i;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 112, ask_price: 113 });
      assert.ok(result > 0, 'Should return positive momentum for uptrend');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('returns negative momentum for downward trend', () => {
      // Simulate downward trend
      for (let i = 0; i < 12; i++) {
        const price = 100 - i;
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 88, ask_price: 89 });
      assert.ok(result < 0, 'Should return negative momentum for downtrend');
      assert.ok(result >= -1, 'Should be in range [-1, 1]');
    });

    it('returns near 0 for choppy movement', () => {
      // Simulate choppy movement (up, down, up, down)
      const prices = [100, 101, 100, 101, 100, 101, 100, 101, 100, 101, 100, 101];

      for (const price of prices) {
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 101 });
      assert.ok(Math.abs(result) < 0.3, 'Should return near 0 for choppy movement');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 15; i++) {
        events.push({ bid_price: 100 + i * 0.5, ask_price: 101 + i * 0.5 });
      }

      // First run
      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();

      // Second run
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('RegimeStabilityFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new RegimeStabilityFeature({ window: 20 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null when regime features are missing', () => {
      const features = {
        regime_volatility: null,
        regime_trend: null,
        regime_spread: null
      };

      const result = feature.onEvent(features);
      assert.equal(result, null, 'Should return null when regime features missing');
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 19; i++) {
        const features = {
          regime_volatility: 1,
          regime_trend: 0,
          regime_spread: 1
        };
        const result = feature.onEvent(features);
        assert.equal(result, null, 'Should return null during warmup');
      }
    });

    it('returns high stability for consistent regime', () => {
      // Simulate stable regime (same values)
      for (let i = 0; i < 21; i++) {
        const features = {
          regime_volatility: 1,  // NORMAL
          regime_trend: 0,       // SIDE
          regime_spread: 1       // NORMAL
        };
        feature.onEvent(features);
      }

      const result = feature.onEvent({
        regime_volatility: 1,
        regime_trend: 0,
        regime_spread: 1
      });

      assert.ok(result > 0.9, 'Should return high stability for consistent regime');
      assert.ok(result <= 1, 'Should be in range [0, 1]');
    });

    it('returns low stability for changing regime', () => {
      // Simulate changing regime
      for (let i = 0; i < 21; i++) {
        const features = {
          regime_volatility: i % 3,     // Rotating 0, 1, 2
          regime_trend: (i % 3) - 1,    // Rotating -1, 0, 1
          regime_spread: i % 3          // Rotating 0, 1, 2
        };
        feature.onEvent(features);
      }

      const result = feature.onEvent({
        regime_volatility: 0,
        regime_trend: -1,
        regime_spread: 0
      });

      assert.ok(result < 0.5, 'Should return low stability for changing regime');
      assert.ok(result >= 0, 'Should be in range [0, 1]');
    });

    it('is deterministic (replay-safe)', () => {
      const featureSequence = [];
      for (let i = 0; i < 25; i++) {
        featureSequence.push({
          regime_volatility: Math.floor(Math.random() * 3),
          regime_trend: Math.floor(Math.random() * 3) - 1,
          regime_spread: Math.floor(Math.random() * 3)
        });
      }

      // First run
      const results1 = featureSequence.map(f => feature.onEvent(f));
      feature.reset();

      // Second run
      const results2 = featureSequence.map(f => feature.onEvent(f));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('Integration: FeatureRegistry with Behavior Features', () => {
    it('can be registered and instantiated via FeatureRegistry', async () => {
      const { FeatureRegistry } = await import('../../FeatureRegistry.js');

      const builder = FeatureRegistry.createFeatureBuilder('BTCUSDT', {
        enabledFeatures: [
          'mid_price',
          'regime_volatility',
          'regime_trend',
          'regime_spread',
          'liquidity_pressure',
          'return_momentum',
          'regime_stability'
        ]
      });

      assert.ok(builder, 'FeatureBuilder should be created');

      // Simulate events (need enough for warmup - regime features need 100+)
      const events = [];
      for (let i = 0; i < 250; i++) {
        events.push({
          bid_price: 100 + Math.sin(i / 10) * 2,
          bid_qty: 50 + Math.random() * 50,
          ask_price: 101 + Math.sin(i / 10) * 2,
          ask_qty: 50 + Math.random() * 50
        });
      }

      let result = null;
      for (const event of events) {
        result = builder.onEvent(event);
      }

      assert.ok(result !== null, 'Should eventually warm up and return features');
      assert.ok('liquidity_pressure' in result, 'Should include liquidity_pressure');
      assert.ok('return_momentum' in result, 'Should include return_momentum');
      assert.ok('regime_stability' in result, 'Should include regime_stability');

      // Validate ranges
      assert.ok(result.liquidity_pressure >= -1 && result.liquidity_pressure <= 1,
        'liquidity_pressure should be in [-1, 1]');
      assert.ok(result.return_momentum >= -1 && result.return_momentum <= 1,
        'return_momentum should be in [-1, 1]');
      assert.ok(result.regime_stability >= 0 && result.regime_stability <= 1,
        'regime_stability should be in [0, 1]');
    });
  });
});
