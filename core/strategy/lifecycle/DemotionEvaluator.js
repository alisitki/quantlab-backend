/**
 * Demotion Evaluator for Strategy Lifecycle
 *
 * Stateless evaluator that determines if a strategy should be demoted
 * or retired based on poor performance, edge decay, or safety violations.
 */

import { LIFECYCLE_CONFIG } from './config.js';
import { getPrevStage, canDemote, LifecycleStage } from './LifecycleStage.js';

/**
 * Severity levels for demotion decisions
 */
export const Severity = Object.freeze({
  WARNING: 'WARNING',     // Performance degrading but not critical
  DEMOTE: 'DEMOTE',       // Should step back one stage
  RETIRE: 'RETIRE'        // Should be retired immediately
});

/**
 * Evaluates if a strategy should be demoted or retired
 * @param {StrategyRecord} strategyRecord - Strategy metadata and history
 * @param {RollingMetrics} performanceMetrics - Rolling performance data
 * @param {number|null} edgeHealth - Edge health score [0-1], null if unavailable
 * @returns {DemotionResult}
 */
export function evaluate(strategyRecord, performanceMetrics, edgeHealth = null) {
  const currentStage = strategyRecord.currentStage;
  const reasons = [];
  let shouldDemote = false;
  let severity = Severity.WARNING;
  let targetStage = null;

  const config = LIFECYCLE_CONFIG.demotion;
  const stageConfig = LIFECYCLE_CONFIG.stages[currentStage];
  const criteria = stageConfig?.criteria || {};

  // Rule 1: Immediate retire on catastrophic Sharpe ratio
  if (performanceMetrics.avgSharpe < config.minSharpe) {
    shouldDemote = true;
    severity = Severity.RETIRE;
    targetStage = LifecycleStage.RETIRED;
    reasons.push(`Catastrophic Sharpe ratio: ${performanceMetrics.avgSharpe.toFixed(3)} < ${config.minSharpe} threshold`);
    return { shouldDemote, currentStage, targetStage, reasons, severity };
  }

  // Rule 2: Immediate retire on excessive drawdown (2x backtest)
  if (strategyRecord.backtestSummary?.maxDrawdownPct) {
    const backtestDD = strategyRecord.backtestSummary.maxDrawdownPct;
    const maxAllowedDD = backtestDD * config.maxDrawdownMultiplier;

    if (performanceMetrics.maxDrawdownPct > maxAllowedDD) {
      shouldDemote = true;
      severity = Severity.RETIRE;
      targetStage = LifecycleStage.RETIRED;
      reasons.push(`Excessive drawdown: ${performanceMetrics.maxDrawdownPct.toFixed(2)}% > ${maxAllowedDD.toFixed(2)}% (${config.maxDrawdownMultiplier}x backtest)`);
      return { shouldDemote, currentStage, targetStage, reasons, severity };
    }
  }

  // Rule 3: Immediate retire on edge decay
  if (config.edgeDecayTrigger && edgeHealth !== null && edgeHealth < 0.2) {
    shouldDemote = true;
    severity = Severity.RETIRE;
    targetStage = LifecycleStage.RETIRED;
    reasons.push(`Edge decay detected: health ${edgeHealth.toFixed(3)} < 0.2 threshold`);
    return { shouldDemote, currentStage, targetStage, reasons, severity };
  }

  // Rule 4: Step-back demotion on consecutive loss days
  if (performanceMetrics.consecutiveLossDays > config.maxConsecutiveLossDays) {
    const prevStage = getPrevStage(currentStage);
    if (prevStage && canDemote(currentStage, prevStage)) {
      shouldDemote = true;
      severity = Severity.DEMOTE;
      targetStage = prevStage;
      reasons.push(`Consecutive loss days: ${performanceMetrics.consecutiveLossDays} > ${config.maxConsecutiveLossDays} threshold`);
    } else if (canDemote(currentStage, LifecycleStage.RETIRED)) {
      // Cannot step back, retire instead
      shouldDemote = true;
      severity = Severity.RETIRE;
      targetStage = LifecycleStage.RETIRED;
      reasons.push(`Consecutive loss days: ${performanceMetrics.consecutiveLossDays} > ${config.maxConsecutiveLossDays}, no demotion path available`);
    }
  }

  // Rule 5: Step-back demotion on low Sharpe relative to stage minimum
  // Only check if there's a minSharpe requirement for the current stage
  if (criteria.minSharpe !== undefined) {
    if (performanceMetrics.avgSharpe < criteria.minSharpe) {
      // Only demote if we haven't already decided to demote
      if (!shouldDemote) {
        const prevStage = getPrevStage(currentStage);
        if (prevStage && canDemote(currentStage, prevStage)) {
          shouldDemote = true;
          severity = Severity.DEMOTE;
          targetStage = prevStage;
          reasons.push(`Sharpe below stage minimum: ${performanceMetrics.avgSharpe.toFixed(3)} < ${criteria.minSharpe} required`);
        } else if (canDemote(currentStage, LifecycleStage.RETIRED)) {
          shouldDemote = true;
          severity = Severity.RETIRE;
          targetStage = LifecycleStage.RETIRED;
          reasons.push(`Sharpe below stage minimum: ${performanceMetrics.avgSharpe.toFixed(3)} < ${criteria.minSharpe}, no demotion path available`);
        }
      }
    }
  }

  // If no demotion triggered
  if (!shouldDemote) {
    reasons.push('Performance within acceptable bounds');
  }

  return {
    shouldDemote,
    currentStage,
    targetStage,
    reasons,
    severity
  };
}

/**
 * @typedef {Object} StrategyRecord
 * @property {string} strategyId
 * @property {string} currentStage
 * @property {Object} backtestSummary
 * @property {number} backtestSummary.maxDrawdownPct
 */

/**
 * @typedef {Object} RollingMetrics
 * @property {number} totalRuns
 * @property {number} totalTrades
 * @property {number} avgReturn
 * @property {number} avgSharpe
 * @property {number} avgWinRate
 * @property {number} maxDrawdownPct
 * @property {number} consecutiveLossDays
 * @property {number} positiveRunFraction
 */

/**
 * @typedef {Object} DemotionResult
 * @property {boolean} shouldDemote
 * @property {string} currentStage
 * @property {string|null} targetStage
 * @property {string[]} reasons
 * @property {string} severity - 'WARNING', 'DEMOTE', or 'RETIRE'
 */
