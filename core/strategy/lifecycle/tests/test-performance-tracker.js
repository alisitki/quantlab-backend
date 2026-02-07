/**
 * Tests for PerformanceTracker
 *
 * Verifies run recording, rolling metrics calculation, and persistence.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PerformanceTracker } from '../PerformanceTracker.js';

describe('PerformanceTracker', () => {
  it('should record run results correctly', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-1';

    const runResult = {
      runId: 'run-001',
      completedAt: '2026-01-01T10:00:00Z',
      trades: 10,
      pnl: 100,
      returnPct: 2.5,
      maxDrawdownPct: 1.2,
      winRate: 0.6,
      sharpe: 1.5,
      stopReason: 'completed',
      durationMs: 60000
    };

    tracker.recordRun(strategyId, runResult);

    const history = tracker.getRunHistory(strategyId);
    assert.equal(history.length, 1);
    assert.equal(history[0].runId, 'run-001');
    assert.equal(history[0].trades, 10);
    assert.equal(history[0].sharpe, 1.5);
  });

  it('should calculate all-time metrics correctly', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-2';

    // Add 3 runs
    tracker.recordRun(strategyId, {
      runId: 'run-001',
      completedAt: '2026-01-01T10:00:00Z',
      trades: 10,
      pnl: 100,
      returnPct: 2.0,
      maxDrawdownPct: 1.0,
      winRate: 0.6,
      sharpe: 1.5
    });

    tracker.recordRun(strategyId, {
      runId: 'run-002',
      completedAt: '2026-01-02T10:00:00Z',
      trades: 15,
      pnl: -50,
      returnPct: -1.0,
      maxDrawdownPct: 2.0,
      winRate: 0.4,
      sharpe: 0.5
    });

    tracker.recordRun(strategyId, {
      runId: 'run-003',
      completedAt: '2026-01-03T10:00:00Z',
      trades: 20,
      pnl: 200,
      returnPct: 3.0,
      maxDrawdownPct: 1.5,
      winRate: 0.7,
      sharpe: 2.0
    });

    const metrics = tracker.getAllTimeMetrics(strategyId);

    assert.equal(metrics.totalRuns, 3);
    assert.equal(metrics.totalTrades, 45);
    assert.equal(metrics.avgReturn, (2.0 - 1.0 + 3.0) / 3);
    assert.equal(metrics.avgSharpe, (1.5 + 0.5 + 2.0) / 3);
    assert.equal(metrics.avgWinRate, (0.6 + 0.4 + 0.7) / 3);
    assert.equal(metrics.maxDrawdownPct, 2.0);
    assert.equal(metrics.positiveRunFraction, 2 / 3);
  });

  it('should calculate rolling window metrics correctly', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-3';

    const now = new Date();
    const old = new Date(now);
    old.setDate(old.getDate() - 35); // 35 days ago (outside 30-day window)

    // Old run (should be excluded)
    tracker.recordRun(strategyId, {
      runId: 'run-old',
      completedAt: old.toISOString(),
      trades: 5,
      returnPct: 10.0,
      sharpe: 3.0
    });

    // Recent run (should be included)
    tracker.recordRun(strategyId, {
      runId: 'run-recent',
      completedAt: now.toISOString(),
      trades: 10,
      returnPct: 2.0,
      sharpe: 1.5
    });

    const rollingMetrics = tracker.getRollingMetrics(strategyId, 30);
    const allTimeMetrics = tracker.getAllTimeMetrics(strategyId);

    // Rolling should only include recent run
    assert.equal(rollingMetrics.totalRuns, 1);
    assert.equal(rollingMetrics.totalTrades, 10);

    // All-time should include both
    assert.equal(allTimeMetrics.totalRuns, 2);
    assert.equal(allTimeMetrics.totalTrades, 15);
  });

  it('should calculate consecutive loss days correctly', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-4';

    // Day 1: Loss
    tracker.recordRun(strategyId, {
      runId: 'run-001',
      completedAt: '2026-01-01T10:00:00Z',
      returnPct: -1.0
    });

    // Day 2: Loss
    tracker.recordRun(strategyId, {
      runId: 'run-002',
      completedAt: '2026-01-02T10:00:00Z',
      returnPct: -0.5
    });

    // Day 3: Loss
    tracker.recordRun(strategyId, {
      runId: 'run-003',
      completedAt: '2026-01-03T10:00:00Z',
      returnPct: -2.0
    });

    // Day 4: Win (resets counter)
    tracker.recordRun(strategyId, {
      runId: 'run-004',
      completedAt: '2026-01-04T10:00:00Z',
      returnPct: 1.0
    });

    // Day 5: Loss
    tracker.recordRun(strategyId, {
      runId: 'run-005',
      completedAt: '2026-01-05T10:00:00Z',
      returnPct: -0.5
    });

    const metrics = tracker.getAllTimeMetrics(strategyId);

    // Max consecutive loss days should be 3
    assert.equal(metrics.consecutiveLossDays, 3);
  });

  it('should handle multiple runs on same day for consecutive loss calculation', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-5';

    // Day 1: Two losses (counts as 1 day)
    tracker.recordRun(strategyId, {
      runId: 'run-001',
      completedAt: '2026-01-01T10:00:00Z',
      returnPct: -1.0
    });

    tracker.recordRun(strategyId, {
      runId: 'run-002',
      completedAt: '2026-01-01T14:00:00Z',
      returnPct: -0.5
    });

    // Day 2: Loss
    tracker.recordRun(strategyId, {
      runId: 'run-003',
      completedAt: '2026-01-02T10:00:00Z',
      returnPct: -2.0
    });

    const metrics = tracker.getAllTimeMetrics(strategyId);

    // Should be 2 days, not 3
    assert.equal(metrics.consecutiveLossDays, 2);
  });

  it('should return null for non-existent strategy', () => {
    const tracker = new PerformanceTracker();
    const metrics = tracker.getRollingMetrics('non-existent');
    assert.equal(metrics, null);

    const allTime = tracker.getAllTimeMetrics('non-existent');
    assert.equal(allTime, null);

    const history = tracker.getRunHistory('non-existent');
    assert.deepEqual(history, []);
  });

  it('should serialize and deserialize correctly', () => {
    const tracker = new PerformanceTracker();
    const strategyId = 'test-strategy-6';

    tracker.recordRun(strategyId, {
      runId: 'run-001',
      completedAt: '2026-01-01T10:00:00Z',
      trades: 10,
      pnl: 100,
      returnPct: 2.5,
      maxDrawdownPct: 1.2,
      winRate: 0.6,
      sharpe: 1.5
    });

    tracker.recordRun(strategyId, {
      runId: 'run-002',
      completedAt: '2026-01-02T10:00:00Z',
      trades: 15,
      pnl: 150,
      returnPct: 3.0,
      maxDrawdownPct: 1.0,
      winRate: 0.7,
      sharpe: 2.0
    });

    // Serialize
    const json = tracker.toJSON();

    // Deserialize
    const restored = PerformanceTracker.fromJSON(json);

    // Verify
    const history = restored.getRunHistory(strategyId);
    assert.equal(history.length, 2);
    assert.equal(history[0].runId, 'run-001');
    assert.equal(history[1].runId, 'run-002');

    const metrics = restored.getAllTimeMetrics(strategyId);
    assert.equal(metrics.totalRuns, 2);
    assert.equal(metrics.totalTrades, 25);
  });
});
