/**
 * Tests for strategy templates (MeanReversion, Momentum, Breakout)
 *
 * Validates template-specific behavior:
 * - Mean Reversion: volatility-inverse sizing, profit targets
 * - Momentum: trailing stops, trend-scaled sizing
 * - Breakout: activation delay, fixed sizing
 */

import { test } from 'node:test';
import assert from 'node:assert';
import { MeanReversionTemplate } from '../templates/MeanReversionTemplate.js';
import { MomentumTemplate } from '../templates/MomentumTemplate.js';
import { BreakoutTemplate } from '../templates/BreakoutTemplate.js';

// Mock Edge for testing
class MockEdge {
  constructor(overrides = {}) {
    this.id = 'test_edge_1';
    this.name = 'Test Edge';
    this.timeHorizon = 10000;
    this.expectedAdvantage = { mean: 0.001, sharpe: 1.0 };
    this.riskProfile = { maxDrawdown: 0.03 };
    Object.assign(this, overrides);
  }

  evaluateEntry(features, regime) {
    return { active: true, direction: 'LONG', confidence: 0.7 };
  }

  evaluateExit(features, regime, entryTime, currentTime) {
    return { exit: false, reason: null };
  }
}

// =========================
// MEAN REVERSION TEMPLATE
// =========================

test('MeanReversionTemplate - volatility-inverse position sizing', () => {
  const edge = new MockEdge();
  const template = new MeanReversionTemplate({
    edge,
    config: { baseQuantity: 10, maxQuantity: 50 }
  });

  // Low volatility (0.5) → larger position
  template._entryFeatures = { volatility_ratio: 0.5 };
  const sizeLowVol = template._calculatePositionSize(0.7);

  // High volatility (2.0) → smaller position
  template._entryFeatures = { volatility_ratio: 2.0 };
  const sizeHighVol = template._calculatePositionSize(0.7);

  assert.ok(sizeLowVol > sizeHighVol, 'Low volatility should result in larger position');
  assert.ok(sizeHighVol > 0, 'High volatility should still allow some position');
});

test('MeanReversionTemplate - blocks entry on excessive volatility', async () => {
  const edge = new MockEdge();
  const template = new MeanReversionTemplate({
    edge,
    config: { maxVolatilityRatio: 2.0 }
  });

  let orderPlaced = false;
  const mockCtx = {
    placeOrder: () => { orderPlaced = true; },
    logger: { info: () => {} }
  };

  const event = { symbol: 'TEST', ts_event: Date.now() };
  const features = { mid_price: 1.0, volatility_ratio: 2.5 }; // Exceeds max
  const regime = 0;

  await template._handleEntry(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, false, 'Should not enter on excessive volatility');
});

test('MeanReversionTemplate - profit target exit', async () => {
  const edge = new MockEdge();
  const template = new MeanReversionTemplate({
    edge,
    config: { baseQuantity: 10, profitTargetPct: 0.001 } // 0.1% target
  });

  // Simulate entry
  template._position = 'LONG';
  template._entryPrice = 1.0;
  template._entryTime = Date.now();
  template._tradeCount = 1;
  template._entryFeatures = { mid_price: 1.0 };

  let orderPlaced = false;
  let exitReason = null;
  const mockCtx = {
    placeOrder: (order) => {
      orderPlaced = true;
      assert.strictEqual(order.side, 'SELL', 'Should sell to close LONG');
    },
    logger: { info: (msg) => {
      if (msg.includes('profit_target')) exitReason = 'profit_target';
    }}
  };

  const event = { symbol: 'TEST', ts_event: Date.now() + 5000 };
  const features = { mid_price: 1.0015 }; // 0.15% profit (exceeds 0.1% target)
  const regime = 0;

  await template._handleExit(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, true, 'Should exit on profit target');
  assert.strictEqual(exitReason, 'profit_target', 'Exit reason should be profit_target');
  assert.strictEqual(template._position, 'FLAT', 'Position should be flat after exit');
});

// =========================
// MOMENTUM TEMPLATE
// =========================

test('MomentumTemplate - trend-scaled position sizing', () => {
  const edge = new MockEdge();
  const template = new MomentumTemplate({
    edge,
    config: { baseQuantity: 10, maxQuantity: 50 }
  });

  // Weak trend (0.3) → smaller position
  template._entryFeatures = { trend_strength: 0.3 };
  const sizeWeakTrend = template._calculatePositionSize(0.7);

  // Strong trend (0.9) → larger position
  template._entryFeatures = { trend_strength: 0.9 };
  const sizeStrongTrend = template._calculatePositionSize(0.7);

  assert.ok(sizeStrongTrend > sizeWeakTrend, 'Strong trend should result in larger position');
});

test('MomentumTemplate - blocks entry on weak trend', async () => {
  const edge = new MockEdge();
  const template = new MomentumTemplate({
    edge,
    config: { minTrendStrength: 0.3 }
  });

  let orderPlaced = false;
  const mockCtx = {
    placeOrder: () => { orderPlaced = true; },
    logger: { info: () => {} }
  };

  const event = { symbol: 'TEST', ts_event: Date.now() };
  const features = { mid_price: 1.0, trend_strength: 0.2 }; // Below minimum
  const regime = 0;

  await template._handleEntry(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, false, 'Should not enter on weak trend');
});

test('MomentumTemplate - trailing stop for LONG', async () => {
  const edge = new MockEdge();
  const template = new MomentumTemplate({
    edge,
    config: { baseQuantity: 10, trailingStopPct: 0.02 } // 2% trailing stop
  });

  // Simulate entry
  template._position = 'LONG';
  template._entryPrice = 1.0;
  template._entryTime = Date.now();
  template._tradeCount = 1;
  template._maxFavorablePrice = 1.05; // Peak at 5% profit

  let orderPlaced = false;
  let exitReason = null;
  const mockCtx = {
    placeOrder: (order) => {
      orderPlaced = true;
      assert.strictEqual(order.side, 'SELL', 'Should sell to close LONG');
    },
    logger: { info: (msg) => {
      if (msg.includes('trailing_stop')) exitReason = 'trailing_stop';
    }}
  };

  const event = { symbol: 'TEST', ts_event: Date.now() + 5000 };
  const features = { mid_price: 1.029 }; // Retraced 2% from peak (1.05 * 0.98)
  const regime = 0;

  await template._handleExit(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, true, 'Should exit on trailing stop');
  assert.strictEqual(exitReason, 'trailing_stop', 'Exit reason should be trailing_stop');
  assert.strictEqual(template._position, 'FLAT', 'Position should be flat after exit');
});

test('MomentumTemplate - trailing stop for SHORT', async () => {
  const edge = new MockEdge();
  const template = new MomentumTemplate({
    edge,
    config: { baseQuantity: 10, trailingStopPct: 0.02 } // 2% trailing stop
  });

  // Simulate entry
  template._position = 'SHORT';
  template._entryPrice = 1.0;
  template._entryTime = Date.now();
  template._tradeCount = 1;
  template._maxFavorablePrice = 0.95; // Peak at 5% profit (price dropped)

  let orderPlaced = false;
  const mockCtx = {
    placeOrder: (order) => {
      orderPlaced = true;
      assert.strictEqual(order.side, 'BUY', 'Should buy to close SHORT');
    },
    logger: { info: () => {} }
  };

  const event = { symbol: 'TEST', ts_event: Date.now() + 5000 };
  const features = { mid_price: 0.97 }; // Retraced 2% from peak (0.95 * 1.02)
  const regime = 0;

  await template._handleExit(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, true, 'Should exit SHORT on trailing stop');
  assert.strictEqual(template._position, 'FLAT', 'Position should be flat after exit');
});

// =========================
// BREAKOUT TEMPLATE
// =========================

test('BreakoutTemplate - fixed position sizing', () => {
  const edge = new MockEdge();
  const template = new BreakoutTemplate({
    edge,
    config: { baseQuantity: 20 }
  });

  // Position size should not vary with confidence
  const size1 = template._calculatePositionSize(0.3);
  const size2 = template._calculatePositionSize(0.9);

  assert.strictEqual(size1, 20, 'Should use fixed base quantity');
  assert.strictEqual(size2, 20, 'Position size should not scale with confidence');
  assert.strictEqual(size1, size2, 'All sizes should be equal (fixed)');
});

test('BreakoutTemplate - activation delay prevents immediate entry', async () => {
  const edge = new MockEdge();
  const template = new BreakoutTemplate({
    edge,
    config: { activationDelay: 5 }
  });

  let orderPlaced = false;
  const mockCtx = {
    placeOrder: () => { orderPlaced = true; },
    logger: { info: () => {} }
  };

  const event = { symbol: 'TEST', ts_event: Date.now() };
  const features = { mid_price: 1.0 };
  const regime = 0;

  // First signal - should not enter immediately
  await template._handleEntry(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, false, 'Should not enter on first signal');
  assert.ok(template._breakoutSignalTime !== null, 'Should track signal time');
});

test('BreakoutTemplate - confirmed breakout enters position', async () => {
  const edge = new MockEdge();
  const template = new BreakoutTemplate({
    edge,
    config: { activationDelay: 0 } // No delay for this test
  });

  let orderPlaced = false;
  const mockCtx = {
    placeOrder: () => { orderPlaced = true; },
    logger: { info: () => {} }
  };

  const startTime = Date.now();

  // First signal
  const event1 = { symbol: 'TEST', ts_event: startTime };
  const features1 = { mid_price: 1.0 };
  await template._handleEntry(event1, features1, 0, mockCtx);

  // Confirm after delay (price moved in expected direction)
  const event2 = { symbol: 'TEST', ts_event: startTime + 1000 };
  const features2 = { mid_price: 1.001 }; // Slight move in LONG direction
  await template._handleEntry(event2, features2, 0, mockCtx);

  assert.strictEqual(orderPlaced, true, 'Should enter on confirmed breakout');
  assert.strictEqual(template._position, 'LONG', 'Position should be LONG');
  assert.strictEqual(template._breakoutConfirmed, true, 'Breakout should be marked as confirmed');
});

test('BreakoutTemplate - exits on no progress', async () => {
  const edge = new MockEdge();
  const template = new BreakoutTemplate({
    edge,
    config: { baseQuantity: 10, maxNoProgressEvents: 10 }
  });

  // Simulate entry
  template._position = 'LONG';
  template._entryPrice = 1.0;
  template._entryTime = Date.now();
  template._tradeCount = 1;
  template._eventsSinceEntry = 10; // Reached max

  let orderPlaced = false;
  let exitReason = null;
  const mockCtx = {
    placeOrder: () => { orderPlaced = true; },
    logger: { info: (msg) => {
      if (msg.includes('no_progress')) exitReason = 'no_progress';
    }}
  };

  const event = { symbol: 'TEST', ts_event: Date.now() + 5000 };
  const features = { mid_price: 1.0001 }; // Negligible profit
  const regime = 0;

  await template._handleExit(event, features, regime, mockCtx);

  assert.strictEqual(orderPlaced, true, 'Should exit on no progress');
  assert.strictEqual(exitReason, 'no_progress', 'Exit reason should be no_progress');
  assert.strictEqual(template._position, 'FLAT', 'Position should be flat after exit');
});
