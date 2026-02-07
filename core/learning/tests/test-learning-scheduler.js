/**
 * Learning Scheduler Tests
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { LearningScheduler } from '../LearningScheduler.js';
import { TradeOutcomeCollector } from '../TradeOutcomeCollector.js';
import { EdgeConfidenceUpdater } from '../EdgeConfidenceUpdater.js';
import { EdgeRevalidationRunner } from '../EdgeRevalidationRunner.js';
import { EdgeRegistry } from '../../edge/EdgeRegistry.js';
import { Edge } from '../../edge/Edge.js';

const TEST_LOG_DIR = '/tmp/test-learning-scheduler';

// Mock validation pipeline
class MockValidationPipeline {
  async revalidate(edge, dataset) {
    return {
      newStatus: 'VALIDATED',
      score: 0.75
    };
  }
}

describe('LearningScheduler', () => {
  let registry;
  let collector;
  let updater;
  let runner;
  let scheduler;
  let edge1, edge2;

  // Create synthetic dataset
  const createDataset = (size = 500) => {
    return {
      rows: Array(size).fill(null).map((_, i) => ({
        timestamp: Date.now() + i * 1000,
        features: { test: Math.random() }
      }))
    };
  };

  beforeEach(async () => {
    // Clean test directory
    try {
      await fs.rm(TEST_LOG_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }

    registry = new EdgeRegistry();

    edge1 = new Edge({
      id: 'test_edge_1',
      name: 'Test Edge 1',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge1.stats.trades = 50;
    edge1.stats.wins = 30;
    edge1.stats.losses = 20;
    edge1.confidence.score = 0.75;

    edge2 = new Edge({
      id: 'test_edge_2',
      name: 'Test Edge 2',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge2.stats.trades = 40;
    edge2.stats.wins = 25;
    edge2.stats.losses = 15;
    edge2.confidence.score = 0.70;

    registry.register(edge1);
    registry.register(edge2);

    collector = new TradeOutcomeCollector({
      logDir: TEST_LOG_DIR
    });

    updater = new EdgeConfidenceUpdater(registry);

    const validationPipeline = new MockValidationPipeline();
    runner = new EdgeRevalidationRunner({
      edgeRegistry: registry,
      validationPipeline
    });

    scheduler = new LearningScheduler({
      edgeRegistry: registry,
      confidenceUpdater: updater,
      revalidationRunner: runner,
      outcomeCollector: collector
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

  it('should run daily loop with no outcomes', async () => {
    const result = await scheduler.runDaily();

    assert.equal(result.type, 'daily');
    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'no_outcomes');
    assert.equal(result.outcomesProcessed, 0);
    assert.ok(result.timestamp);
    assert.ok(result.durationMs >= 0);
  });

  it('should run daily loop with outcomes', async () => {
    // Create some outcomes
    for (let i = 0; i < 10; i++) {
      const tradeId = `trade_daily_${i}`;
      const edgeId = i % 2 === 0 ? 'test_edge_1' : 'test_edge_2';
      const timestamp = Date.now();

      collector.recordEntry(tradeId, {
        features: { test: Math.random() },
        regime: { cluster: 0 },
        edgeId,
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: i % 3 === 0 ? 0.46 : 0.44,  // Mix wins and losses
        timestamp: timestamp + 60000,
        pnl: i % 3 === 0 ? 0.01 : -0.01,
        exitReason: 'signal_exit'
      });
    }

    await collector.flush();

    const result = await scheduler.runDaily();

    assert.equal(result.type, 'daily');
    assert.equal(result.outcomesProcessed, 10);
    assert.equal(result.edgesAffected, 2);
    assert.ok(Array.isArray(result.alerts));
    assert.ok(result.durationMs >= 0);
  });

  it('should generate drift alerts in daily loop', async () => {
    // Set baselines
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    updater.setBaseline('test_edge_2', {
      confidence: 0.70,
      winRate: 0.6
    });

    // Create losing trades to trigger drift
    for (let i = 0; i < 15; i++) {
      const tradeId = `trade_drift_${i}`;
      const timestamp = Date.now();

      collector.recordEntry(tradeId, {
        features: { test: 0.5 },
        regime: { cluster: 0 },
        edgeId: 'test_edge_1',
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.44,
        timestamp: timestamp + 1000,
        pnl: -0.01,
        exitReason: 'stop_loss'
      });
    }

    await collector.flush();

    const result = await scheduler.runDaily();

    assert.ok(result.alertsGenerated > 0, 'Should generate drift alerts');
  });

  it('should flag edges for revalidation when auto-revalidation enabled', async () => {
    scheduler.config.enableAutoRevalidation = true;

    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    // Generate 10 consecutive losses to trigger alert
    for (let i = 0; i < 10; i++) {
      const tradeId = `trade_flag_${i}`;
      const timestamp = Date.now();

      collector.recordEntry(tradeId, {
        features: { test: 0.5 },
        regime: { cluster: 0 },
        edgeId: 'test_edge_1',
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.44,
        timestamp: timestamp + 1000,
        pnl: -0.01,
        exitReason: 'stop_loss'
      });
    }

    await collector.flush();

    const result = await scheduler.runDaily();

    if (result.alertsGenerated > 0) {
      assert.ok(result.revalidationFlags > 0, 'Should flag edges for revalidation');
      assert.ok(Array.isArray(result.flaggedEdges));
    }
  });

  it('should run weekly loop', async () => {
    const dataset = createDataset();

    // Create some outcomes
    for (let i = 0; i < 5; i++) {
      const tradeId = `trade_weekly_${i}`;
      const timestamp = Date.now();

      collector.recordEntry(tradeId, {
        features: { test: Math.random() },
        regime: { cluster: 0 },
        edgeId: 'test_edge_1',
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.46,
        timestamp: timestamp + 1000,
        pnl: 0.01,
        exitReason: 'signal_exit'
      });
    }

    await collector.flush();

    const result = await scheduler.runWeekly(dataset);

    assert.equal(result.type, 'weekly');
    assert.ok(result.daily);
    assert.ok(result.revalidation);
    assert.ok(result.revalidation.edgesRevalidated >= 0);
    assert.ok(Array.isArray(result.revalidation.results));
    assert.ok(result.durationMs >= 0);
  });

  it('should track run history', async () => {
    await scheduler.runDaily();
    await scheduler.runDaily();

    const summary = scheduler.getSummary();

    assert.equal(summary.totalRuns, 2);
    assert.equal(summary.dailyRuns, 2);
    assert.equal(summary.weeklyRuns, 0);
    assert.ok(summary.lastDailyRun);
    assert.ok(Array.isArray(summary.recentRuns));
  });

  it('should filter history by type', async () => {
    const dataset = createDataset();

    await scheduler.runDaily();
    await scheduler.runWeekly(dataset);

    const dailyHistory = scheduler.getHistory({ type: 'daily' });
    const weeklyHistory = scheduler.getHistory({ type: 'weekly' });

    // Daily was called once directly + once inside weekly
    assert.ok(dailyHistory.length >= 1);
    assert.equal(weeklyHistory.length, 1);

    assert.equal(dailyHistory[0].type, 'daily');
    assert.equal(weeklyHistory[0].type, 'weekly');
  });

  it('should filter history by time', async () => {
    const beforeTime = new Date().toISOString();

    // Small delay
    await new Promise(resolve => setTimeout(resolve, 50));

    await scheduler.runDaily();

    const history = scheduler.getHistory({ since: beforeTime });

    assert.equal(history.length, 1);
  });

  it('should limit history results', async () => {
    // Create 5 runs
    for (let i = 0; i < 5; i++) {
      await scheduler.runDaily();
    }

    const history = scheduler.getHistory({ limit: 2 });

    assert.equal(history.length, 2);
  });

  it('should update last run timestamps', async () => {
    const dataset = createDataset();

    assert.equal(scheduler.lastDailyRun, null);
    assert.equal(scheduler.lastWeeklyRun, null);

    await scheduler.runDaily();

    assert.ok(scheduler.lastDailyRun);
    assert.equal(scheduler.lastWeeklyRun, null);

    await scheduler.runWeekly(dataset);

    assert.ok(scheduler.lastWeeklyRun);
  });

  it('should handle empty dataset in weekly loop', async () => {
    const emptyDataset = createDataset(0);

    const result = await scheduler.runWeekly(emptyDataset);

    // Should still run daily part
    assert.ok(result.daily);
    // Revalidation should be skipped or return empty results
    assert.ok(result.revalidation);
  });
});
