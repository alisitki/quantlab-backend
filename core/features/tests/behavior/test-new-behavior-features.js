import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SpreadCompressionFeature } from '../../builders/behavior/SpreadCompressionFeature.js';
import { ImbalanceAccelerationFeature } from '../../builders/behavior/ImbalanceAccelerationFeature.js';
import { MicroReversionFeature } from '../../builders/behavior/MicroReversionFeature.js';
import { QuoteIntensityFeature } from '../../builders/behavior/QuoteIntensityFeature.js';
import { BehaviorDivergenceFeature } from '../../builders/behavior/BehaviorDivergenceFeature.js';
import { VolatilityCompressionScoreFeature } from '../../builders/behavior/VolatilityCompressionScoreFeature.js';

describe('New Behavior Features - Milestone 2', () => {
  describe('SpreadCompressionFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new SpreadCompressionFeature({ fastPeriod: 5, slowPeriod: 20 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      const event = { bid_price: 100, ask_price: 101 };

      for (let i = 0; i < 19; i++) {
        const result = feature.onEvent(event);
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects spread compression (narrowing)', () => {
      // Start with wide spread, narrow it
      for (let i = 0; i < 25; i++) {
        const spread = 1.0 - (i * 0.01); // Narrowing from 1.0 to 0.75
        feature.onEvent({ bid_price: 100, ask_price: 100 + spread });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 100.7 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0, 'Should detect compression (positive value)');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('detects spread expansion (widening)', () => {
      // Start with narrow spread, widen it
      for (let i = 0; i < 25; i++) {
        const spread = 0.5 + (i * 0.01); // Widening from 0.5 to 0.75
        feature.onEvent({ bid_price: 100, ask_price: 100 + spread });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 100.8 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < 0, 'Should detect expansion (negative value)');
      assert.ok(result >= -1, 'Should be in range [-1, 1]');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 30; i++) {
        events.push({ bid_price: 100, ask_price: 101 - (i * 0.01) });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('ImbalanceAccelerationFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new ImbalanceAccelerationFeature({ period: 5, smoothPeriod: 3 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      const event = { bid_price: 100, bid_qty: 50, ask_price: 101, ask_qty: 50 };

      for (let i = 0; i < 7; i++) {
        const result = feature.onEvent(event);
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects increasing buy pressure acceleration', () => {
      // Gradually increase bid quantity (acceleration in buy pressure)
      for (let i = 0; i < 15; i++) {
        const bidQty = 50 + (i * 5); // Increasing
        feature.onEvent({ bid_price: 100, bid_qty: bidQty, ask_price: 101, ask_qty: 50 });
      }

      const result = feature.onEvent({ bid_price: 100, bid_qty: 120, ask_price: 101, ask_qty: 50 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0, 'Should detect positive acceleration');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 20; i++) {
        events.push({ bid_price: 100, bid_qty: 50 + i, ask_price: 101, ask_qty: 50 });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('MicroReversionFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new MicroReversionFeature({ window: 20 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 19; i++) {
        const price = 100 + (i % 2 === 0 ? 0.1 : -0.1); // Oscillating
        const result = feature.onEvent({ bid_price: price, ask_price: price + 1 });
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects high reversion in oscillating prices', () => {
      // Perfect mean reversion: up, down, up, down
      for (let i = 0; i < 25; i++) {
        const price = 100 + (i % 2 === 0 ? 0.1 : -0.1);
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 100.1, ask_price: 101.1 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0.7, 'Should detect high reversion (>0.7)');
      assert.ok(result <= 1, 'Should be in range [0, 1]');
    });

    it('detects low reversion in trending prices', () => {
      // Strong trend: all up
      for (let i = 0; i < 25; i++) {
        const price = 100 + (i * 0.1);
        feature.onEvent({ bid_price: price, ask_price: price + 1 });
      }

      const result = feature.onEvent({ bid_price: 102.5, ask_price: 103.5 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < 0.3, 'Should detect low reversion (<0.3)');
      assert.ok(result >= 0, 'Should be in range [0, 1]');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      for (let i = 0; i < 30; i++) {
        const price = 100 + Math.sin(i / 5) * 0.5;
        events.push({ bid_price: price, ask_price: price + 1 });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('QuoteIntensityFeature', () => {
    let feature;

    beforeEach(() => {
      feature = new QuoteIntensityFeature({ window: 10, longWindow: 50 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      let ts = 1000000;
      for (let i = 0; i < 49; i++) {
        ts += 100; // 100ms interval
        const result = feature.onEvent({ bid_price: 100, ask_price: 101, ts_event: ts });
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects high intensity (fast quotes)', () => {
      let ts = 1000000;
      // Long warm-up with normal pace
      for (let i = 0; i < 60; i++) {
        ts += 100; // 100ms interval = 10 quotes/sec
        feature.onEvent({ bid_price: 100, ask_price: 101, ts_event: ts });
      }

      // Speed up (high intensity)
      for (let i = 0; i < 15; i++) {
        ts += 20; // 20ms interval = 50 quotes/sec
        feature.onEvent({ bid_price: 100, ask_price: 101, ts_event: ts });
      }

      const result = feature.onEvent({ bid_price: 100, ask_price: 101, ts_event: ts + 20 });
      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0.5, 'Should detect high intensity (>0.5 percentile)');
      assert.ok(result <= 1, 'Should be in range [0, 1]');
    });

    it('is deterministic (replay-safe)', () => {
      const events = [];
      let ts = 1000000;
      for (let i = 0; i < 60; i++) {
        ts += 50 + (i % 10) * 10; // Variable interval
        events.push({ bid_price: 100, ask_price: 101, ts_event: ts });
      }

      const results1 = events.map(e => feature.onEvent(e));
      feature.reset();
      const results2 = events.map(e => feature.onEvent(e));

      assert.deepEqual(results1, results2, 'Should produce identical output on replay');
    });
  });

  describe('BehaviorDivergenceFeature (Derived)', () => {
    let feature;

    beforeEach(() => {
      feature = new BehaviorDivergenceFeature();
    });

    it('returns null when dependencies are missing', () => {
      const features = {
        return_momentum: null,
        liquidity_pressure: 0.5
      };

      const result = feature.onEvent(features);
      assert.equal(result, null, 'Should return null when dependencies missing');
    });

    it('detects bullish momentum with bearish pressure (divergence)', () => {
      const features = {
        return_momentum: 0.8,    // Strong upward momentum
        liquidity_pressure: -0.6  // Strong sell pressure
      };

      const result = feature.onEvent(features);
      assert.ok(result !== null, 'Should return value');
      assert.ok(result > 0.5, 'Should detect positive divergence (momentum > pressure)');
      assert.ok(result <= 1, 'Should be in range [-1, 1]');
    });

    it('detects bearish momentum with bullish pressure (divergence)', () => {
      const features = {
        return_momentum: -0.7,   // Strong downward momentum
        liquidity_pressure: 0.5   // Buy pressure
      };

      const result = feature.onEvent(features);
      assert.ok(result !== null, 'Should return value');
      assert.ok(result < -0.5, 'Should detect negative divergence (momentum < pressure)');
      assert.ok(result >= -1, 'Should be in range [-1, 1]');
    });

    it('detects agreement (low divergence)', () => {
      const features = {
        return_momentum: 0.5,
        liquidity_pressure: 0.6  // Both bullish
      };

      const result = feature.onEvent(features);
      assert.ok(result !== null, 'Should return value');
      assert.ok(Math.abs(result) < 0.2, 'Should detect low divergence (agreement)');
    });

    it('has static isDerived property', () => {
      assert.equal(BehaviorDivergenceFeature.isDerived, true, 'Should be marked as derived');
      assert.ok(Array.isArray(BehaviorDivergenceFeature.dependencies), 'Should have dependencies array');
    });
  });

  describe('VolatilityCompressionScoreFeature (Derived)', () => {
    let feature;

    beforeEach(() => {
      feature = new VolatilityCompressionScoreFeature({ window: 20 });
    });

    afterEach(() => {
      feature.reset();
    });

    it('returns null during warmup', () => {
      for (let i = 0; i < 19; i++) {
        const features = {
          regime_volatility: 1,
          spread_compression: 0.5,
          volatility: 0.01
        };
        const result = feature.onEvent(features);
        assert.equal(result, null, `Event ${i} should return null during warmup`);
      }
    });

    it('detects high compression (low vol + compression)', () => {
      // Warmup with varying volatility
      for (let i = 0; i < 25; i++) {
        const features = {
          regime_volatility: 0,      // LOW volatility regime
          spread_compression: 0.8,   // High spread compression
          volatility: 0.005 + Math.random() * 0.01
        };
        feature.onEvent(features);
      }

      const result = feature.onEvent({
        regime_volatility: 0,
        spread_compression: 0.9,
        volatility: 0.003  // Very low volatility
      });

      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result > 0.6, 'Should detect high compression score');
      assert.ok(result <= 1, 'Should be in range [0, 1]');
    });

    it('detects low compression (high vol + expansion)', () => {
      // Warmup with varying volatility
      for (let i = 0; i < 25; i++) {
        const features = {
          regime_volatility: 2,      // HIGH volatility regime
          spread_compression: -0.5,  // Spread expanding
          volatility: 0.01 + Math.random() * 0.02
        };
        feature.onEvent(features);
      }

      const result = feature.onEvent({
        regime_volatility: 2,
        spread_compression: -0.8,
        volatility: 0.03  // High volatility
      });

      assert.ok(result !== null, 'Should return value after warmup');
      assert.ok(result < 0.4, 'Should detect low compression score');
      assert.ok(result >= 0, 'Should be in range [0, 1]');
    });

    it('has static isDerived property', () => {
      assert.equal(VolatilityCompressionScoreFeature.isDerived, true, 'Should be marked as derived');
      assert.ok(Array.isArray(VolatilityCompressionScoreFeature.dependencies), 'Should have dependencies array');
    });
  });

  describe('Integration: FeatureRegistry with All New Features', () => {
    it('can register and instantiate all new features', async () => {
      const { FeatureRegistry } = await import('../../FeatureRegistry.js');

      const builder = FeatureRegistry.createFeatureBuilder('BTCUSDT', {
        enabledFeatures: [
          'mid_price',
          'spread',
          'volatility',
          'regime_volatility',
          'regime_trend',
          'regime_spread',
          'liquidity_pressure',
          'return_momentum',
          'spread_compression',
          'imbalance_acceleration',
          'micro_reversion',
          'quote_intensity',
          'regime_stability',
          'behavior_divergence',
          'volatility_compression_score'
        ]
      });

      assert.ok(builder, 'FeatureBuilder should be created');

      // Generate sufficient events for warmup
      const events = [];
      let ts = 1000000;
      for (let i = 0; i < 300; i++) {
        ts += 100;
        events.push({
          ts_event: ts,
          bid_price: 100 + Math.sin(i / 20) * 2,
          bid_qty: 50 + Math.random() * 50,
          ask_price: 101 + Math.sin(i / 20) * 2,
          ask_qty: 50 + Math.random() * 50
        });
      }

      let result = null;
      for (const event of events) {
        result = builder.onEvent(event);
      }

      assert.ok(result !== null, 'Should eventually warm up and return features');

      // Check all new features are present
      assert.ok('spread_compression' in result, 'Should include spread_compression');
      assert.ok('imbalance_acceleration' in result, 'Should include imbalance_acceleration');
      assert.ok('micro_reversion' in result, 'Should include micro_reversion');
      assert.ok('quote_intensity' in result, 'Should include quote_intensity');
      assert.ok('behavior_divergence' in result, 'Should include behavior_divergence');
      assert.ok('volatility_compression_score' in result, 'Should include volatility_compression_score');

      // Validate ranges
      assert.ok(result.spread_compression >= -1 && result.spread_compression <= 1,
        'spread_compression should be in [-1, 1]');
      assert.ok(result.imbalance_acceleration >= -1 && result.imbalance_acceleration <= 1,
        'imbalance_acceleration should be in [-1, 1]');
      assert.ok(result.micro_reversion >= 0 && result.micro_reversion <= 1,
        'micro_reversion should be in [0, 1]');
      assert.ok(result.quote_intensity >= 0 && result.quote_intensity <= 1,
        'quote_intensity should be in [0, 1]');
      assert.ok(result.behavior_divergence >= -1 && result.behavior_divergence <= 1,
        'behavior_divergence should be in [-1, 1]');
      assert.ok(result.volatility_compression_score >= 0 && result.volatility_compression_score <= 1,
        'volatility_compression_score should be in [0, 1]');
    });
  });
});
