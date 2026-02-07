/**
 * Edge Confidence Updater Tests
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeConfidenceUpdater } from '../EdgeConfidenceUpdater.js';
import { EdgeRegistry } from '../../edge/EdgeRegistry.js';
import { Edge } from '../../edge/Edge.js';

describe('EdgeConfidenceUpdater', () => {
  let registry;
  let updater;
  let edge;

  beforeEach(() => {
    registry = new EdgeRegistry();

    edge = new Edge({
      id: 'test_edge_1',
      name: 'Test Edge',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    // Initialize with some stats
    edge.stats.trades = 50;
    edge.stats.wins = 30;
    edge.stats.losses = 20;
    edge.stats.consecutiveLosses = 0;
    edge.confidence.score = 0.75;

    registry.register(edge);

    updater = new EdgeConfidenceUpdater(registry);
  });

  it('should update edge stats from outcome', () => {
    const outcome = {
      tradeId: 't_1',
      edgeId: 'test_edge_1',
      pnl: 0.01,
      entryPrice: 0.45,
      exitPrice: 0.46
    };

    updater.processOutcome(outcome);

    assert.equal(edge.stats.trades, 51);
    assert.equal(edge.stats.wins, 31);
    assert.equal(edge.stats.consecutiveLosses, 0);
  });

  it('should update confidence with EMA on winning trade', () => {
    const initialConfidence = edge.confidence.score;

    const outcome = {
      tradeId: 't_2',
      edgeId: 'test_edge_1',
      pnl: 0.01
    };

    updater.processOutcome(outcome);

    // Winning trade should slightly increase confidence
    // New confidence = 0.75 * 0.95 + 1.0 * 0.05 = 0.7625
    assert.ok(edge.confidence.score > initialConfidence);
    assert.ok(Math.abs(edge.confidence.score - 0.7625) < 0.001);
  });

  it('should update confidence with EMA on losing trade', () => {
    const initialConfidence = edge.confidence.score;

    const outcome = {
      tradeId: 't_3',
      edgeId: 'test_edge_1',
      pnl: -0.01
    };

    updater.processOutcome(outcome);

    // Losing trade should decrease confidence
    // New confidence = 0.75 * 0.95 + 0.0 * 0.05 = 0.7125
    assert.ok(edge.confidence.score < initialConfidence);
    assert.ok(Math.abs(edge.confidence.score - 0.7125) < 0.001);
  });

  it('should track consecutive losses', () => {
    // Process 3 losing trades
    for (let i = 0; i < 3; i++) {
      updater.processOutcome({
        tradeId: `t_loss_${i}`,
        edgeId: 'test_edge_1',
        pnl: -0.01
      });
    }

    assert.equal(edge.stats.consecutiveLosses, 3);

    // Win should reset
    updater.processOutcome({
      tradeId: 't_win',
      edgeId: 'test_edge_1',
      pnl: 0.01
    });

    assert.equal(edge.stats.consecutiveLosses, 0);
  });

  it('should detect confidence drop drift', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    // Simulate enough losing trades to drop confidence > 15%
    for (let i = 0; i < 30; i++) {
      updater.processOutcome({
        tradeId: `t_drop_${i}`,
        edgeId: 'test_edge_1',
        pnl: -0.01
      });
    }

    // Check if confidence dropped significantly
    const confidenceDrop = 0.75 - edge.confidence.score;
    assert.ok(confidenceDrop > 0.15, `Confidence drop ${confidenceDrop} should be > 0.15`);

    // Last processOutcome should have generated alert
    const alerts = updater.processOutcome({
      tradeId: 't_final',
      edgeId: 'test_edge_1',
      pnl: -0.01
    });

    assert.ok(alerts);
    assert.ok(alerts.length > 0);

    const confidenceAlert = alerts.find(a => a.type === 'CONFIDENCE_DROP');
    assert.ok(confidenceAlert);
    assert.equal(confidenceAlert.edgeId, 'test_edge_1');
    assert.ok(confidenceAlert.drop > 0.15);
  });

  it('should detect consecutive losses drift', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    // Process 9 losing trades (not yet at threshold)
    for (let i = 0; i < 9; i++) {
      const alerts = updater.processOutcome({
        tradeId: `t_consec_${i}`,
        edgeId: 'test_edge_1',
        pnl: -0.01
      });
      // May generate confidence drop alerts, but not consecutive losses yet
      if (alerts) {
        const lossAlert = alerts.find(a => a.type === 'CONSECUTIVE_LOSSES');
        assert.equal(lossAlert, undefined, 'Should not have consecutive losses alert yet');
      }
    }

    // 10th losing trade should trigger consecutive losses alert
    const alerts = updater.processOutcome({
      tradeId: 't_consec_10',
      edgeId: 'test_edge_1',
      pnl: -0.01
    });

    assert.ok(alerts);
    const lossAlert = alerts.find(a => a.type === 'CONSECUTIVE_LOSSES');
    assert.ok(lossAlert, 'Should have consecutive losses alert');
    assert.equal(lossAlert.count, 10);
  });

  it('should detect win rate drop drift', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6  // 60% win rate baseline
    });

    // Current win rate: 30/50 = 0.6
    // Process 20 losing trades to drop below 50%
    for (let i = 0; i < 20; i++) {
      updater.processOutcome({
        tradeId: `t_wr_${i}`,
        edgeId: 'test_edge_1',
        pnl: -0.01
      });
    }

    // New win rate: 30/70 = 0.428 (drop of 0.172)
    const currentWinRate = edge.stats.wins / edge.stats.trades;
    assert.ok(currentWinRate < 0.5);

    const alerts = updater.processOutcome({
      tradeId: 't_wr_final',
      edgeId: 'test_edge_1',
      pnl: -0.01
    });

    assert.ok(alerts);
    const wrAlert = alerts.find(a => a.type === 'WINRATE_DROP');
    assert.ok(wrAlert);
    assert.ok(wrAlert.drop > 0.10);
  });

  it('should process batch of outcomes', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    const outcomes = [];

    // Generate outcomes
    for (let i = 0; i < 15; i++) {
      outcomes.push({
        tradeId: `t_batch_${i}`,
        edgeId: 'test_edge_1',
        pnl: i % 2 === 0 ? 0.01 : -0.01
      });
    }

    const alerts = updater.processBatch(outcomes);

    assert.ok(Array.isArray(alerts));
    // Some alerts may have been generated
  });

  it('should handle outcome for non-existent edge', () => {
    const outcome = {
      tradeId: 't_missing',
      edgeId: 'nonexistent_edge',
      pnl: 0.01
    };

    const alerts = updater.processOutcome(outcome);
    assert.equal(alerts, null);
  });

  it('should not generate alerts without baseline', () => {
    // No baseline set for edge
    const outcome = {
      tradeId: 't_no_baseline',
      edgeId: 'test_edge_1',
      pnl: -0.01
    };

    const alerts = updater.processOutcome(outcome);
    assert.equal(alerts, null);
  });

  it('should set and get baseline', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.8,
      winRate: 0.65,
      sharpe: 1.5
    });

    const baseline = updater.getBaseline('test_edge_1');
    assert.ok(baseline);
    assert.equal(baseline.confidence, 0.8);
    assert.equal(baseline.winRate, 0.65);
    assert.equal(baseline.sharpe, 1.5);
    assert.ok(baseline.setAt);
  });

  it('should clear baseline', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    assert.ok(updater.getBaseline('test_edge_1'));

    updater.clearBaseline('test_edge_1');

    assert.equal(updater.getBaseline('test_edge_1'), null);
  });

  it('should provide summary', () => {
    updater.setBaseline('test_edge_1', {
      confidence: 0.75,
      winRate: 0.6
    });

    updater.setBaseline('test_edge_2', {
      confidence: 0.8,
      winRate: 0.55
    });

    const summary = updater.getSummary();

    assert.equal(summary.trackedEdges, 2);
    assert.equal(summary.baselines.length, 2);
  });

  it('should only update confidence after minSampleSize', () => {
    // Create edge with few trades
    const newEdge = new Edge({
      id: 'test_edge_low_sample',
      name: 'Low Sample Edge',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'CANDIDATE'
    });

    newEdge.stats.trades = 10;  // Below minSampleSize (30)
    newEdge.stats.wins = 5;
    newEdge.stats.losses = 5;
    newEdge.confidence.score = 0.5;

    registry.register(newEdge);

    const initialConfidence = newEdge.confidence.score;

    updater.processOutcome({
      tradeId: 't_low_sample',
      edgeId: 'test_edge_low_sample',
      pnl: 0.01
    });

    // Confidence should NOT have changed (below minSampleSize)
    assert.equal(newEdge.confidence.score, initialConfidence);
  });
});
