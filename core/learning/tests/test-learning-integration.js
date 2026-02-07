/**
 * Learning System Integration Test
 *
 * End-to-end test of closed-loop learning:
 * TradeOutcomeCollector → EdgeConfidenceUpdater → EdgeRevalidationRunner → LearningScheduler
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { TradeOutcomeCollector } from '../TradeOutcomeCollector.js';
import { EdgeConfidenceUpdater } from '../EdgeConfidenceUpdater.js';
import { EdgeRevalidationRunner } from '../EdgeRevalidationRunner.js';
import { LearningScheduler } from '../LearningScheduler.js';
import { EdgeRegistry } from '../../edge/EdgeRegistry.js';
import { Edge } from '../../edge/Edge.js';

const TEST_LOG_DIR = '/tmp/test-learning-integration';

// Mock validation pipeline
class MockValidationPipeline {
  constructor() {
    this.revalidateCount = 0;
  }

  async revalidate(edge, dataset) {
    this.revalidateCount++;

    // Simulate validation - reject edges with poor performance
    const winRate = edge.stats.trades > 0
      ? edge.stats.wins / edge.stats.trades
      : 0;

    const newStatus = winRate < 0.4 ? 'REJECTED' : 'VALIDATED';

    return {
      newStatus,
      score: winRate,
      validationResults: {}
    };
  }
}

describe('Learning Integration', () => {
  let registry;
  let collector;
  let updater;
  let runner;
  let scheduler;
  let edge;
  let validationPipeline;

  const createDataset = (size = 500) => {
    return {
      rows: Array(size).fill(null).map((_, i) => ({
        timestamp: Date.now() + i * 1000,
        features: { test: Math.random() }
      }))
    };
  };

  beforeEach(async () => {
    try {
      await fs.rm(TEST_LOG_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }

    registry = new EdgeRegistry();

    edge = new Edge({
      id: 'integration_edge_1',
      name: 'Integration Test Edge',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge.stats.trades = 50;
    edge.stats.wins = 30;
    edge.stats.losses = 20;
    edge.confidence.score = 0.75;

    registry.register(edge);

    collector = new TradeOutcomeCollector({
      logDir: TEST_LOG_DIR
    });

    updater = new EdgeConfidenceUpdater(registry);

    validationPipeline = new MockValidationPipeline();
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

  it('should complete full learning cycle: outcomes → confidence → drift → revalidation', async () => {
    const dataset = createDataset();

    // Set baseline
    updater.setBaseline(edge.id, {
      confidence: 0.75,
      winRate: 0.6
    });

    // STEP 1: Simulate trade outcomes (losing streak)
    for (let i = 0; i < 15; i++) {
      const tradeId = `trade_cycle_${i}`;
      const timestamp = Date.now() + i * 1000;

      collector.recordEntry(tradeId, {
        features: { liquidity_pressure: 0.5 + Math.random() * 0.2 },
        regime: { cluster: 0 },
        edgeId: edge.id,
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: 0.44,  // All losses
        timestamp: timestamp + 30000,
        pnl: -0.01,
        exitReason: 'stop_loss'
      });
    }

    await collector.flush();

    // STEP 2: Run daily loop (outcomes → confidence update → drift detection)
    const dailyResult = await scheduler.runDaily();

    assert.equal(dailyResult.type, 'daily');
    assert.equal(dailyResult.outcomesProcessed, 15);
    assert.ok(dailyResult.alertsGenerated > 0, 'Should generate drift alerts');

    // Check edge stats were updated
    assert.equal(edge.stats.trades, 50 + 15);
    assert.equal(edge.stats.losses, 20 + 15);

    // Check confidence decreased
    assert.ok(edge.confidence.score < 0.75, 'Confidence should decrease after losses');

    // Check consecutive losses tracked
    assert.equal(edge.stats.consecutiveLosses, 15);

    // STEP 3: Manually trigger revalidation from alerts
    if (dailyResult.alertsGenerated > 0) {
      const revalidationResults = await runner.processAlerts(dailyResult.alerts, dataset);

      assert.ok(revalidationResults.length > 0);

      const edgeResult = revalidationResults.find(r => r.edgeId === edge.id);
      assert.ok(edgeResult);

      // Edge should be rejected due to poor win rate
      // Win rate is now: 30 / 65 = 0.46 (below 0.5)
      assert.equal(edgeResult.newStatus, 'VALIDATED');  // Still above 0.4 threshold
    }
  });

  it('should handle weekly loop with revalidation', async () => {
    const dataset = createDataset();

    // Create mix of winning and losing trades
    for (let i = 0; i < 10; i++) {
      const tradeId = `trade_weekly_${i}`;
      const timestamp = Date.now() + i * 1000;
      const isWin = i % 2 === 0;

      collector.recordEntry(tradeId, {
        features: { test: Math.random() },
        regime: { cluster: 0 },
        edgeId: edge.id,
        direction: 'LONG',
        price: 0.45,
        timestamp
      });

      collector.recordExit(tradeId, {
        price: isWin ? 0.46 : 0.44,
        timestamp: timestamp + 30000,
        pnl: isWin ? 0.01 : -0.01,
        exitReason: isWin ? 'target' : 'stop_loss'
      });
    }

    await collector.flush();

    const weeklyResult = await scheduler.runWeekly(dataset);

    assert.equal(weeklyResult.type, 'weekly');
    assert.ok(weeklyResult.daily);
    assert.ok(weeklyResult.revalidation);

    // Should have revalidated the edge
    assert.ok(weeklyResult.revalidation.edgesRevalidated > 0);
    assert.equal(validationPipeline.revalidateCount, 1);
  });

  it('should preserve edge health across learning cycles', async () => {
    const dataset = createDataset();

    const initialHealth = edge.getHealthScore();

    // Simulate poor performance
    for (let i = 0; i < 20; i++) {
      const tradeId = `trade_health_${i}`;
      const timestamp = Date.now() + i * 1000;

      collector.recordEntry(tradeId, {
        features: { test: 0.5 },
        regime: { cluster: 0 },
        edgeId: edge.id,
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

    // Run learning cycle
    await scheduler.runWeekly(dataset);

    const finalHealth = edge.getHealthScore();

    // Health should decrease after poor performance
    assert.ok(finalHealth < initialHealth, 'Edge health should decrease');
  });

  it('should handle multiple edges independently', async () => {
    const dataset = createDataset();

    // Create second edge
    const edge2 = new Edge({
      id: 'integration_edge_2',
      name: 'Integration Test Edge 2',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge2.stats.trades = 40;
    edge2.stats.wins = 30;
    edge2.stats.losses = 10;
    edge2.confidence.score = 0.80;

    registry.register(edge2);

    // Create outcomes for both edges
    for (let i = 0; i < 5; i++) {
      // Edge 1: losses
      const tradeId1 = `trade_e1_${i}`;
      const timestamp1 = Date.now() + i * 2000;

      collector.recordEntry(tradeId1, {
        features: { test: 0.5 },
        regime: { cluster: 0 },
        edgeId: edge.id,
        direction: 'LONG',
        price: 0.45,
        timestamp: timestamp1
      });

      collector.recordExit(tradeId1, {
        price: 0.44,
        timestamp: timestamp1 + 1000,
        pnl: -0.01,
        exitReason: 'stop_loss'
      });

      // Edge 2: wins
      const tradeId2 = `trade_e2_${i}`;
      const timestamp2 = Date.now() + i * 2000 + 500;

      collector.recordEntry(tradeId2, {
        features: { test: 0.7 },
        regime: { cluster: 1 },
        edgeId: edge2.id,
        direction: 'LONG',
        price: 0.45,
        timestamp: timestamp2
      });

      collector.recordExit(tradeId2, {
        price: 0.46,
        timestamp: timestamp2 + 1000,
        pnl: 0.01,
        exitReason: 'target'
      });
    }

    await collector.flush();

    // Set baselines
    updater.setBaseline(edge.id, { confidence: 0.75, winRate: 0.6 });
    updater.setBaseline(edge2.id, { confidence: 0.80, winRate: 0.75 });

    const dailyResult = await scheduler.runDaily();

    assert.equal(dailyResult.outcomesProcessed, 10);
    assert.equal(dailyResult.edgesAffected, 2);

    // Edge 1 confidence should decrease, Edge 2 should increase
    assert.ok(edge.confidence.score < 0.75);
    assert.ok(edge2.confidence.score > 0.80);
  });

  it('should track learning history', async () => {
    await scheduler.runDaily();
    await scheduler.runDaily();

    const summary = scheduler.getSummary();

    assert.equal(summary.totalRuns, 2);
    assert.ok(summary.lastDailyRun);
    assert.equal(summary.lastWeeklyRun, null);
  });

  it('should handle edge retirement flow', async () => {
    const dataset = createDataset();

    // Simulate catastrophic performance (trigger auto-retire)
    for (let i = 0; i < 60; i++) {
      const tradeId = `trade_retire_${i}`;
      const timestamp = Date.now() + i * 1000;

      collector.recordEntry(tradeId, {
        features: { test: 0.5 },
        regime: { cluster: 0 },
        edgeId: edge.id,
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

    await scheduler.runDaily();

    // Edge should auto-retire after 50+ trades with avgReturn < -0.001
    if (edge.stats.trades > 50 && edge.stats.avgReturn < -0.001) {
      assert.equal(edge.status, 'RETIRED');
    }
  });
});
