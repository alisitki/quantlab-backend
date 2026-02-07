/**
 * Tests for DemotionEvaluator
 *
 * Verifies demotion and retirement decision logic.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { evaluate, Severity } from '../DemotionEvaluator.js';
import { LifecycleStage } from '../LifecycleStage.js';

describe('DemotionEvaluator', () => {
  it('should immediately retire on catastrophic Sharpe ratio', () => {
    const strategyRecord = {
      strategyId: 'test-1',
      currentStage: LifecycleStage.LIVE,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 100,
      avgSharpe: -0.8, // < -0.5 threshold
      avgWinRate: 0.3,
      maxDrawdownPct: 8.0,
      consecutiveLossDays: 2
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.RETIRE);
    assert.equal(result.targetStage, LifecycleStage.RETIRED);
    assert.ok(result.reasons.some(r => r.includes('Catastrophic Sharpe')));
  });

  it('should immediately retire on excessive drawdown', () => {
    const strategyRecord = {
      strategyId: 'test-2',
      currentStage: LifecycleStage.CANARY,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 50,
      avgSharpe: 0.5,
      avgWinRate: 0.4,
      maxDrawdownPct: 12.0, // > 5 * 2.0 = 10
      consecutiveLossDays: 1
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.RETIRE);
    assert.equal(result.targetStage, LifecycleStage.RETIRED);
    assert.ok(result.reasons.some(r => r.includes('Excessive drawdown')));
  });

  it('should immediately retire on edge decay', () => {
    const strategyRecord = {
      strategyId: 'test-3',
      currentStage: LifecycleStage.SHADOW,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 50,
      avgSharpe: 0.8,
      maxDrawdownPct: 6.0,
      consecutiveLossDays: 2
    };

    const edgeHealth = 0.15; // < 0.2 threshold

    const result = evaluate(strategyRecord, metrics, edgeHealth);

    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.RETIRE);
    assert.equal(result.targetStage, LifecycleStage.RETIRED);
    assert.ok(result.reasons.some(r => r.includes('Edge decay')));
  });

  it('should demote one stage on consecutive loss days', () => {
    const strategyRecord = {
      strategyId: 'test-4',
      currentStage: LifecycleStage.SHADOW,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 50,
      avgSharpe: 0.5,
      avgWinRate: 0.4,
      maxDrawdownPct: 8.0,
      consecutiveLossDays: 7 // > 5 threshold
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.DEMOTE);
    assert.equal(result.targetStage, LifecycleStage.CANARY);
    assert.ok(result.reasons.some(r => r.includes('Consecutive loss days')));
  });

  it('should demote one stage on low Sharpe', () => {
    const strategyRecord = {
      strategyId: 'test-5',
      currentStage: LifecycleStage.PAPER,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 20,
      avgSharpe: 0.2, // < 0.3 (PAPER minSharpe)
      avgWinRate: 0.4,
      maxDrawdownPct: 8.0,
      consecutiveLossDays: 2
    };

    const result = evaluate(strategyRecord, metrics);

    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.DEMOTE);
    assert.equal(result.targetStage, LifecycleStage.CANDIDATE);
    assert.ok(result.reasons.some(r => r.includes('Sharpe below stage minimum')));
  });

  it('should not demote when performance is acceptable', () => {
    const strategyRecord = {
      strategyId: 'test-6',
      currentStage: LifecycleStage.LIVE,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 100,
      avgSharpe: 1.2,
      avgWinRate: 0.6,
      maxDrawdownPct: 7.0, // < 10 (2x backtest)
      consecutiveLossDays: 2
    };

    const edgeHealth = 0.8;

    const result = evaluate(strategyRecord, metrics, edgeHealth);

    assert.equal(result.shouldDemote, false);
    assert.equal(result.severity, Severity.WARNING);
    assert.equal(result.targetStage, null);
    assert.ok(result.reasons.some(r => r.includes('acceptable bounds')));
  });

  it('should retire CANDIDATE instead of demotion when no step-back possible', () => {
    const strategyRecord = {
      strategyId: 'test-7',
      currentStage: LifecycleStage.CANDIDATE,
      backtestSummary: { maxDrawdownPct: 5 }
    };

    const metrics = {
      totalRuns: 10,
      avgSharpe: 0.3, // < 0.5 (CANDIDATE minSharpe)
      avgWinRate: 0.3,
      maxDrawdownPct: 4.0,
      consecutiveLossDays: 2
    };

    const result = evaluate(strategyRecord, metrics);

    // CANDIDATE can't step back, should retire instead
    assert.equal(result.shouldDemote, true);
    assert.equal(result.severity, Severity.RETIRE);
    assert.equal(result.targetStage, LifecycleStage.RETIRED);
  });
});
