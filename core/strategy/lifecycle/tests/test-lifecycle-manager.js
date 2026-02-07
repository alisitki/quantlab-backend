/**
 * Tests for StrategyLifecycleManager
 *
 * Verifies full lifecycle orchestration and integration.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { StrategyLifecycleManager } from '../StrategyLifecycleManager.js';
import { LifecycleStage } from '../LifecycleStage.js';

const TEST_DIR = 'data/lifecycle-manager-test';
const TEST_FILE = 'manager-state.json';

describe('StrategyLifecycleManager', () => {
  let manager;

  beforeEach(async () => {
    manager = new StrategyLifecycleManager(TEST_DIR, TEST_FILE);

    // Clean up before each test
    try {
      await fs.rm(TEST_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }
  });

  afterEach(async () => {
    try {
      await fs.rm(TEST_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }
  });

  it('should register a new strategy', () => {
    const deployResult = {
      strategyId: 'strat-1',
      edgeId: 'edge-1',
      templateType: 'momentum',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 5 },
      validationScore: 0.85,
      promotionGuards: {}
    };

    const strategyId = manager.register(deployResult);

    assert.equal(strategyId, 'strat-1');

    const record = manager.getStrategy(strategyId);
    assert.ok(record);
    assert.equal(record.currentStage, LifecycleStage.CANDIDATE);
    assert.equal(record.edgeId, 'edge-1');
    assert.equal(record.stageHistory.length, 1);
  });

  it('should record run results and track performance', () => {
    const deployResult = {
      strategyId: 'strat-2',
      edgeId: 'edge-2',
      templateType: 'momentum',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 5 }
    };

    manager.register(deployResult);

    const runResult = {
      runId: 'run-001',
      completedAt: new Date().toISOString(),
      trades: 10,
      pnl: 100,
      returnPct: 2.5,
      maxDrawdownPct: 1.2,
      winRate: 0.6,
      sharpe: 1.5
    };

    manager.recordRunResult('strat-2', runResult);

    // Should be able to evaluate after recording
    const evaluation = manager.evaluateStrategy('strat-2');
    assert.ok(evaluation);
    assert.equal(evaluation.strategyId, 'strat-2');
  });

  it('should promote strategy when criteria met', () => {
    const deployResult = {
      strategyId: 'strat-3',
      edgeId: 'edge-3',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 3 }
    };

    manager.register(deployResult);

    // Set entry date 5 days ago to meet PAPER minDays requirement
    const fiveDaysAgo = new Date();
    fiveDaysAgo.setDate(fiveDaysAgo.getDate() - 5);
    manager.getStrategy('strat-3').stageHistory[0].enteredAt = fiveDaysAgo.toISOString();

    // Add sufficient runs to meet PAPER criteria
    for (let i = 0; i < 10; i++) {
      manager.recordRunResult('strat-3', {
        runId: `run-${i}`,
        completedAt: new Date().toISOString(),
        trades: 15,
        returnPct: 2.0,
        maxDrawdownPct: 2.0,
        winRate: 0.5,
        sharpe: 1.2
      });
    }

    // Promote CANDIDATE → PAPER
    const success = manager.promote('strat-3', { actor: 'test', reason: 'Test promotion' });

    assert.ok(success);
    const record = manager.getStrategy('strat-3');
    assert.equal(record.currentStage, LifecycleStage.PAPER);
    assert.equal(record.stageHistory.length, 2);
  });

  it('should handle promotion approval requirement', () => {
    const deployResult = {
      strategyId: 'strat-4',
      edgeId: 'edge-4',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 3 }
    };

    manager.register(deployResult);

    // Move to PAPER first
    manager.getStrategy('strat-4').currentStage = LifecycleStage.PAPER;
    manager.getStrategy('strat-4').stageHistory[0].stage = LifecycleStage.PAPER;

    // Set entry date 10 days ago
    const tenDaysAgo = new Date();
    tenDaysAgo.setDate(tenDaysAgo.getDate() - 10);
    manager.getStrategy('strat-4').stageHistory[0].enteredAt = tenDaysAgo.toISOString();

    // Add sufficient runs to meet CANARY criteria
    for (let i = 0; i < 15; i++) {
      manager.recordRunResult('strat-4', {
        runId: `run-${i}`,
        completedAt: new Date().toISOString(),
        trades: 20,
        returnPct: 2.0,
        maxDrawdownPct: 5.0,
        winRate: 0.45,
        sharpe: 0.8
      });
    }

    // First promotion attempt should set pending approval
    const firstAttempt = manager.promote('strat-4');
    assert.equal(firstAttempt, false);
    assert.equal(manager.getStrategy('strat-4').pendingApproval, true);

    // Approve promotion
    const approved = manager.approvePromotion('strat-4', { actor: 'admin' });
    assert.ok(approved);
    assert.equal(manager.getStrategy('strat-4').currentStage, LifecycleStage.CANARY);
    assert.equal(manager.getStrategy('strat-4').pendingApproval, false);
  });

  it('should reject promotion when requested', () => {
    const deployResult = {
      strategyId: 'strat-5',
      edgeId: 'edge-5'
    };

    manager.register(deployResult);
    manager.getStrategy('strat-5').pendingApproval = true;

    const rejected = manager.rejectPromotion('strat-5', {
      actor: 'admin',
      reason: 'Not ready yet'
    });

    assert.ok(rejected);
    assert.equal(manager.getStrategy('strat-5').pendingApproval, false);
    assert.equal(manager.getStrategy('strat-5').currentStage, LifecycleStage.CANDIDATE);
  });

  it('should demote strategy on poor performance', () => {
    const deployResult = {
      strategyId: 'strat-6',
      edgeId: 'edge-6',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 5 }
    };

    manager.register(deployResult);

    // Manually set to PAPER stage
    manager.getStrategy('strat-6').currentStage = LifecycleStage.PAPER;

    const success = manager.demote('strat-6', LifecycleStage.CANDIDATE, {
      actor: 'admin',
      reason: 'Performance degraded'
    });

    assert.ok(success);
    assert.equal(manager.getStrategy('strat-6').currentStage, LifecycleStage.CANDIDATE);
  });

  it('should retire strategy', () => {
    const deployResult = {
      strategyId: 'strat-7',
      edgeId: 'edge-7'
    };

    manager.register(deployResult);

    const success = manager.retire('strat-7', {
      actor: 'admin',
      reason: 'Edge decayed'
    });

    assert.ok(success);
    assert.equal(manager.getStrategy('strat-7').currentStage, LifecycleStage.RETIRED);
  });

  it('should list strategies by stage', () => {
    manager.register({ strategyId: 'strat-8a', edgeId: 'edge-8a' });
    manager.register({ strategyId: 'strat-8b', edgeId: 'edge-8b' });
    manager.register({ strategyId: 'strat-8c', edgeId: 'edge-8c' });

    // Move one to PAPER
    manager.getStrategy('strat-8b').currentStage = LifecycleStage.PAPER;

    const candidates = manager.listByStage(LifecycleStage.CANDIDATE);
    const paper = manager.listByStage(LifecycleStage.PAPER);

    assert.equal(candidates.length, 2);
    assert.equal(paper.length, 1);
  });

  it('should generate system summary', () => {
    manager.register({ strategyId: 'strat-9a', edgeId: 'edge-9a' });
    manager.register({ strategyId: 'strat-9b', edgeId: 'edge-9b' });
    manager.getStrategy('strat-9b').currentStage = LifecycleStage.PAPER;
    manager.getStrategy('strat-9a').pendingApproval = true;

    const summary = manager.getSummary();

    assert.equal(summary.totalStrategies, 2);
    assert.equal(summary.byStage[LifecycleStage.CANDIDATE], 1);
    assert.equal(summary.byStage[LifecycleStage.PAPER], 1);
    assert.equal(summary.pendingApprovals, 1);
  });

  it('should persist and restore state', async () => {
    const deployResult = {
      strategyId: 'strat-10',
      edgeId: 'edge-10',
      backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 5 }
    };

    manager.register(deployResult);

    manager.recordRunResult('strat-10', {
      runId: 'run-001',
      completedAt: new Date().toISOString(),
      trades: 10,
      returnPct: 2.0,
      sharpe: 1.2
    });

    // Persist
    await manager.persist();

    // Create new manager and restore
    const newManager = new StrategyLifecycleManager(TEST_DIR, TEST_FILE);
    await newManager.restore();

    // Verify
    const restored = newManager.getStrategy('strat-10');
    assert.ok(restored);
    assert.equal(restored.strategyId, 'strat-10');
    assert.equal(restored.edgeId, 'edge-10');
  });

  it('should evaluate all strategies', () => {
    manager.register({ strategyId: 'strat-11a', edgeId: 'edge-11a' });
    manager.register({ strategyId: 'strat-11b', edgeId: 'edge-11b' });

    // Add some runs
    manager.recordRunResult('strat-11a', {
      runId: 'run-001',
      completedAt: new Date().toISOString(),
      trades: 10,
      returnPct: 2.0,
      sharpe: 1.2
    });

    const evaluations = manager.evaluateAll();

    assert.equal(evaluations.length, 2);
    assert.ok(evaluations[0].strategyId);
  });
});
