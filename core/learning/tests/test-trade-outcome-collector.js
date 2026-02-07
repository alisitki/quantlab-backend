/**
 * Trade Outcome Collector Tests
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { TradeOutcomeCollector } from '../TradeOutcomeCollector.js';

const TEST_LOG_DIR = '/tmp/test-outcome-collector';

describe('TradeOutcomeCollector', () => {
  let collector;

  beforeEach(async () => {
    // Clean test directory
    try {
      await fs.rm(TEST_LOG_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }

    collector = new TradeOutcomeCollector({
      logDir: TEST_LOG_DIR,
      flushIntervalMs: 100  // Fast flush for testing
    });
  });

  afterEach(async () => {
    if (collector) {
      await collector.close();
    }

    try {
      await fs.rm(TEST_LOG_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }
  });

  it('should record entry and track pending trade', () => {
    collector.recordEntry('trade_1', {
      features: { liquidity_pressure: 0.72, volatility_ratio: 1.3 },
      regime: { cluster: 2 },
      edgeId: 'edge_test_1',
      direction: 'LONG',
      price: 0.4523,
      timestamp: 1738800000000
    });

    const summary = collector.getSummary();
    assert.equal(summary.pendingTrades, 1);
    assert.equal(summary.bufferedOutcomes, 0);
  });

  it('should record exit and complete outcome', () => {
    collector.recordEntry('trade_2', {
      features: { liquidity_pressure: 0.72 },
      regime: { cluster: 2 },
      edgeId: 'edge_test_1',
      direction: 'LONG',
      price: 0.4523,
      timestamp: 1738800000000
    });

    const outcome = collector.recordExit('trade_2', {
      price: 0.4531,
      timestamp: 1738800060000,
      pnl: 0.0008,
      exitReason: 'signal_exit'
    });

    assert.ok(outcome);
    assert.equal(outcome.tradeId, 'trade_2');
    assert.equal(outcome.edgeId, 'edge_test_1');
    assert.equal(outcome.direction, 'LONG');
    assert.equal(outcome.entryPrice, 0.4523);
    assert.equal(outcome.exitPrice, 0.4531);
    assert.equal(outcome.pnl, 0.0008);
    assert.equal(outcome.exitReason, 'signal_exit');
    assert.equal(outcome.holdingPeriodMs, 60000);

    const summary = collector.getSummary();
    assert.equal(summary.pendingTrades, 0);
    assert.equal(summary.bufferedOutcomes, 1);
  });

  it('should return null for exit without entry', () => {
    const outcome = collector.recordExit('nonexistent_trade', {
      price: 0.45,
      timestamp: Date.now(),
      pnl: 0,
      exitReason: 'error'
    });

    assert.equal(outcome, null);
  });

  it('should compact features to specified decimals', () => {
    collector.recordEntry('trade_3', {
      features: {
        liquidity_pressure: 0.123456789,
        volatility_ratio: 1.987654321
      },
      regime: { cluster: 0 },
      edgeId: 'edge_test_2',
      direction: 'SHORT',
      price: 0.45,
      timestamp: Date.now()
    });

    const outcome = collector.recordExit('trade_3', {
      price: 0.44,
      timestamp: Date.now(),
      pnl: -0.01,
      exitReason: 'stop_loss'
    });

    assert.equal(outcome.entryFeatures.liquidity_pressure, 0.123457);
    assert.equal(outcome.entryFeatures.volatility_ratio, 1.987654);
  });

  it('should flush outcomes to disk', async () => {
    collector.recordEntry('trade_4', {
      features: { test: 1 },
      regime: { cluster: 0 },
      edgeId: 'edge_test_3',
      direction: 'LONG',
      price: 0.45,
      timestamp: Date.now()
    });

    collector.recordExit('trade_4', {
      price: 0.46,
      timestamp: Date.now(),
      pnl: 0.01,
      exitReason: 'target'
    });

    await collector.flush();

    const summary = collector.getSummary();
    assert.equal(summary.bufferedOutcomes, 0);
    assert.ok(summary.currentFile);
    assert.ok(summary.bytesFlushed > 0);

    // Verify file exists
    const exists = await fs.access(summary.currentFile).then(() => true).catch(() => false);
    assert.ok(exists);
  });

  it('should read outcomes from file', async () => {
    // Record multiple outcomes
    for (let i = 0; i < 5; i++) {
      const tradeId = `trade_read_${i}`;
      const timestamp = 1738800000000 + i * 60000;

      collector.recordEntry(tradeId, {
        features: { test: i },
        regime: { cluster: i % 3 },
        edgeId: `edge_${i % 2}`,
        direction: 'LONG',
        price: 0.45 + i * 0.001,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.46 + i * 0.001,
        timestamp: timestamp + 30000,
        pnl: 0.01,
        exitReason: 'signal_exit'
      });
    }

    await collector.flush();

    // Read all outcomes
    const outcomes = await collector.readOutcomes();
    assert.equal(outcomes.length, 5);

    // Read with timestamp filter
    const recentOutcomes = await collector.readOutcomes({
      since: 1738800000000 + 3 * 60000
    });
    assert.equal(recentOutcomes.length, 2);

    // Read with edgeId filter
    const edge0Outcomes = await collector.readOutcomes({
      edgeId: 'edge_0'
    });
    assert.equal(edge0Outcomes.length, 3);

    // Read with limit
    const limitedOutcomes = await collector.readOutcomes({
      limit: 2
    });
    assert.equal(limitedOutcomes.length, 2);
  });

  it('should auto-flush large buffer', async () => {
    // Record 100+ outcomes (trigger auto-flush)
    for (let i = 0; i < 105; i++) {
      const tradeId = `trade_auto_${i}`;
      const timestamp = Date.now();

      collector.recordEntry(tradeId, {
        features: { test: i },
        regime: { cluster: 0 },
        edgeId: 'edge_auto',
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.46,
        timestamp: timestamp + 1000,
        pnl: 0.01,
        exitReason: 'exit'
      });
    }

    // Give time for auto-flush
    await new Promise(resolve => setTimeout(resolve, 50));

    const summary = collector.getSummary();
    assert.ok(summary.bufferedOutcomes < 105);  // Should have flushed
  });

  it('should close and cleanup', async () => {
    collector.recordEntry('trade_close', {
      features: { test: 1 },
      regime: { cluster: 0 },
      edgeId: 'edge_close',
      direction: 'LONG',
      price: 0.45,
      timestamp: Date.now()
    });

    collector.recordExit('trade_close', {
      price: 0.46,
      timestamp: Date.now(),
      pnl: 0.01,
      exitReason: 'exit'
    });

    await collector.close();

    const summary = collector.getSummary();
    assert.equal(summary.bufferedOutcomes, 0);

    // Verify file was written
    const outcomes = await collector.readOutcomes();
    assert.equal(outcomes.length, 1);
  });

  it('should handle regime as number or object', () => {
    // Regime as number
    collector.recordEntry('trade_regime_num', {
      features: { test: 1 },
      regime: 2,
      edgeId: 'edge_regime',
      direction: 'LONG',
      price: 0.45,
      timestamp: Date.now()
    });

    let outcome = collector.recordExit('trade_regime_num', {
      price: 0.46,
      timestamp: Date.now(),
      pnl: 0.01,
      exitReason: 'exit'
    });

    assert.equal(outcome.entryRegime, 2);

    // Regime as object
    collector.recordEntry('trade_regime_obj', {
      features: { test: 1 },
      regime: { cluster: 1, volatility: 'high' },
      edgeId: 'edge_regime',
      direction: 'SHORT',
      price: 0.45,
      timestamp: Date.now()
    });

    outcome = collector.recordExit('trade_regime_obj', {
      price: 0.44,
      timestamp: Date.now(),
      pnl: -0.01,
      exitReason: 'exit'
    });

    assert.deepEqual(outcome.entryRegime, { cluster: 1, volatility: 'high' });
  });
});
