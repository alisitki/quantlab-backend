/**
 * Tests for PromotionEvaluator
 *
 * Verifies promotion decision logic for each stage transition.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { evaluate } from '../PromotionEvaluator.js';
import { LifecycleStage } from '../LifecycleStage.js';

describe('PromotionEvaluator', () => {
  it('should approve CANDIDATE → PAPER promotion when criteria met', () => {
    const strategyRecord = {
      strategyId: 'test-1',
      currentStage: LifecycleStage.CANDIDATE,
      stageHistory: [
        { stage: LifecycleStage.CANDIDATE, enteredAt: '2026-01-01T00:00:00Z' }
      ],
      backtestSummary: {}
    };

    const metrics = {
      totalRuns: 5,
      totalTrades: 50,
      avgReturn: 2.0,
      avgSharpe: 1.2,
      avgWinRate: 0.5,
      maxDrawdownPct: 3.0,
      consecutiveLossDays: 0,
      positiveRunFraction: 0.8
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, true);
    assert.equal(result.currentStage, LifecycleStage.CANDIDATE);
    assert.equal(result.targetStage, LifecycleStage.PAPER);
    assert.equal(result.requiresApproval, false);
  });

  it('should reject CANDIDATE → PAPER when insufficient trades', () => {
    const strategyRecord = {
      strategyId: 'test-2',
      currentStage: LifecycleStage.CANDIDATE,
      stageHistory: [
        { stage: LifecycleStage.CANDIDATE, enteredAt: '2026-01-01T00:00:00Z' }
      ]
    };

    const metrics = {
      totalRuns: 5,
      totalTrades: 5, // < 10 required
      avgSharpe: 1.2,
      maxDrawdownPct: 3.0
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, false);
    assert.ok(result.reasons.some(r => r.includes('Insufficient trades')));
  });

  it('should approve PAPER → CANARY promotion with approval required', () => {
    const strategyRecord = {
      strategyId: 'test-3',
      currentStage: LifecycleStage.PAPER,
      stageHistory: [
        { stage: LifecycleStage.CANDIDATE, enteredAt: '2026-01-01T00:00:00Z' },
        { stage: LifecycleStage.PAPER, enteredAt: '2026-01-05T00:00:00Z' }
      ]
    };

    // 8 days ago to meet CANARY minDays requirement (7)
    const eightDaysAgo = new Date();
    eightDaysAgo.setDate(eightDaysAgo.getDate() - 8);

    strategyRecord.stageHistory[0].enteredAt = eightDaysAgo.toISOString();

    const metrics = {
      totalRuns: 10,
      totalTrades: 100,
      avgReturn: 2.0,
      avgSharpe: 0.8,
      avgWinRate: 0.45,
      maxDrawdownPct: 8.0,
      consecutiveLossDays: 0,
      positiveRunFraction: 0.7
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, true);
    assert.equal(result.targetStage, LifecycleStage.CANARY);
    assert.equal(result.requiresApproval, true); // CANARY requires approval
  });

  it('should reject PAPER → CANARY when insufficient days', () => {
    const strategyRecord = {
      strategyId: 'test-4',
      currentStage: LifecycleStage.PAPER,
      stageHistory: [
        { stage: LifecycleStage.PAPER, enteredAt: new Date().toISOString() } // Just entered
      ]
    };

    const metrics = {
      totalRuns: 10,
      totalTrades: 100,
      avgSharpe: 0.8,
      avgWinRate: 0.45,
      maxDrawdownPct: 8.0
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, false);
    assert.ok(result.reasons.some(r => r.includes('Insufficient time')));
  });

  it('should approve SHADOW → LIVE with approval and consistency check', () => {
    const strategyRecord = {
      strategyId: 'test-5',
      currentStage: LifecycleStage.SHADOW,
      stageHistory: [
        { stage: LifecycleStage.SHADOW, enteredAt: '2026-01-01T00:00:00Z' }
      ]
    };

    // 15 days ago
    const fifteenDaysAgo = new Date();
    fifteenDaysAgo.setDate(fifteenDaysAgo.getDate() - 15);
    strategyRecord.stageHistory[0].enteredAt = fifteenDaysAgo.toISOString();

    const metrics = {
      totalRuns: 25,
      totalTrades: 200,
      avgReturn: 2.5,
      avgSharpe: 1.0,
      avgWinRate: 0.5,
      maxDrawdownPct: 8.0,
      consecutiveLossDays: 0,
      positiveRunFraction: 0.7 // > 0.6 required
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, true);
    assert.equal(result.targetStage, LifecycleStage.LIVE);
    assert.equal(result.requiresApproval, true);
  });

  it('should reject SHADOW → LIVE when consistency too low', () => {
    const strategyRecord = {
      strategyId: 'test-6',
      currentStage: LifecycleStage.SHADOW,
      stageHistory: [
        { stage: LifecycleStage.SHADOW, enteredAt: '2026-01-01T00:00:00Z' }
      ]
    };

    const fifteenDaysAgo = new Date();
    fifteenDaysAgo.setDate(fifteenDaysAgo.getDate() - 15);
    strategyRecord.stageHistory[0].enteredAt = fifteenDaysAgo.toISOString();

    const metrics = {
      totalRuns: 25,
      totalTrades: 200,
      avgSharpe: 1.0,
      avgWinRate: 0.5,
      maxDrawdownPct: 8.0,
      positiveRunFraction: 0.5 // < 0.6 required
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldPromote, false);
    assert.ok(result.reasons.some(r => r.includes('Low consistency')));
  });

  it('should not allow promotion from LIVE or RETIRED', () => {
    const liveRecord = {
      strategyId: 'test-7',
      currentStage: LifecycleStage.LIVE,
      stageHistory: []
    };

    const retiredRecord = {
      strategyId: 'test-8',
      currentStage: LifecycleStage.RETIRED,
      stageHistory: []
    };

    const metrics = {
      totalRuns: 100,
      totalTrades: 1000,
      avgSharpe: 5.0,
      avgWinRate: 0.9,
      maxDrawdownPct: 1.0,
      positiveRunFraction: 1.0
    };

    const liveResult = evaluate(liveRecord, metrics);
    assert.equal(liveResult.shouldPromote, false);
    assert.equal(liveResult.targetStage, null);

    const retiredResult = evaluate(retiredRecord, metrics);
    assert.equal(retiredResult.shouldPromote, false);
    assert.equal(retiredResult.targetStage, null);
  });
});
