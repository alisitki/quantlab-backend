/**
 * Promotion Evaluator for Strategy Lifecycle
 *
 * Stateless evaluator that determines if a strategy should be promoted
 * to the next lifecycle stage based on performance metrics and criteria.
 */

import { LIFECYCLE_CONFIG } from './config.js';
import { getNextStage, canPromote } from './LifecycleStage.js';

/**
 * Evaluates if a strategy should be promoted
 * @param {StrategyRecord} strategyRecord - Strategy metadata and history
 * @param {RollingMetrics} performanceMetrics - Rolling performance data
 * @returns {PromotionResult}
 */
export function evaluate(strategyRecord, performanceMetrics) {
  const currentStage = strategyRecord.currentStage;
  const targetStage = getNextStage(currentStage);

  // Cannot promote from terminal stages
  if (!targetStage || !canPromote(currentStage, targetStage)) {
    return {
      shouldPromote: false,
      currentStage,
      targetStage: null,
      reasons: ['No valid promotion path from current stage'],
      requiresApproval: false,
      metricsSnapshot: null
    };
  }

  // Use TARGET stage for minRuns/minDays, but CURRENT stage for performance criteria
  const targetStageConfig = LIFECYCLE_CONFIG.stages[targetStage];
  const currentStageConfig = LIFECYCLE_CONFIG.stages[currentStage];

  // Use target stage criteria if available, otherwise fall back to current stage
  const criteria = targetStageConfig.criteria || currentStageConfig.criteria || {};

  const reasons = [];
  let shouldPromote = true;

  // Check minimum runs requirement (total runs across all stages)
  const minRuns = targetStageConfig.minRuns || 0;
  if (performanceMetrics.totalRuns < minRuns) {
    shouldPromote = false;
    reasons.push(`Insufficient runs: ${performanceMetrics.totalRuns} < ${minRuns} required`);
  }

  // Check minimum days requirement (days in current stage)
  const minDays = targetStageConfig.minDays || 0;
  const daysInStage = calculateDaysInStage(strategyRecord);
  if (daysInStage < minDays) {
    shouldPromote = false;
    reasons.push(`Insufficient time: ${daysInStage} days < ${minDays} required`);
  }

  // Check Sharpe ratio
  if (criteria.minSharpe !== undefined) {
    if (performanceMetrics.avgSharpe < criteria.minSharpe) {
      shouldPromote = false;
      reasons.push(`Low Sharpe: ${performanceMetrics.avgSharpe.toFixed(3)} < ${criteria.minSharpe} required`);
    }
  }

  // Check drawdown
  if (criteria.maxDrawdownPct !== undefined) {
    if (performanceMetrics.maxDrawdownPct > criteria.maxDrawdownPct) {
      shouldPromote = false;
      reasons.push(`Excessive drawdown: ${performanceMetrics.maxDrawdownPct.toFixed(2)}% > ${criteria.maxDrawdownPct}% limit`);
    }
  }

  // Check win rate
  if (criteria.minWinRate !== undefined) {
    if (performanceMetrics.avgWinRate < criteria.minWinRate) {
      shouldPromote = false;
      reasons.push(`Low win rate: ${(performanceMetrics.avgWinRate * 100).toFixed(1)}% < ${criteria.minWinRate * 100}% required`);
    }
  }

  // Check minimum trades
  if (criteria.minTrades !== undefined) {
    if (performanceMetrics.totalTrades < criteria.minTrades) {
      shouldPromote = false;
      reasons.push(`Insufficient trades: ${performanceMetrics.totalTrades} < ${criteria.minTrades} required`);
    }
  }

  // Check consistency (for SHADOW stage)
  if (criteria.minConsistency !== undefined) {
    if (performanceMetrics.positiveRunFraction < criteria.minConsistency) {
      shouldPromote = false;
      reasons.push(`Low consistency: ${(performanceMetrics.positiveRunFraction * 100).toFixed(1)}% < ${criteria.minConsistency * 100}% required`);
    }
  }

  // If all checks passed
  if (shouldPromote) {
    reasons.push('All promotion criteria met');
  }

  // Determine if approval required
  const requiresApproval = criteria.requireApproval === true;

  return {
    shouldPromote,
    currentStage,
    targetStage,
    reasons,
    requiresApproval,
    metricsSnapshot: { ...performanceMetrics }
  };
}

/**
 * Calculate days since strategy entered current stage
 * @private
 * @param {StrategyRecord} strategyRecord
 * @returns {number}
 */
function calculateDaysInStage(strategyRecord) {
  if (!strategyRecord.stageHistory || strategyRecord.stageHistory.length === 0) {
    return 0;
  }

  // Find most recent stage entry
  const currentStageEntry = [...strategyRecord.stageHistory]
    .reverse()
    .find(h => h.stage === strategyRecord.currentStage && !h.exitedAt);

  if (!currentStageEntry || !currentStageEntry.enteredAt) {
    return 0;
  }

  const enteredAt = new Date(currentStageEntry.enteredAt);
  const now = new Date();
  const diffMs = now.getTime() - enteredAt.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  return diffDays;
}

/**
 * @typedef {Object} StrategyRecord
 * @property {string} strategyId
 * @property {string} currentStage
 * @property {StageHistoryEntry[]} stageHistory
 * @property {Object} backtestSummary
 */

/**
 * @typedef {Object} StageHistoryEntry
 * @property {string} stage
 * @property {string} enteredAt - ISO timestamp
 * @property {string} [exitedAt] - ISO timestamp
 * @property {string} [reason]
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
 * @typedef {Object} PromotionResult
 * @property {boolean} shouldPromote
 * @property {string} currentStage
 * @property {string|null} targetStage
 * @property {string[]} reasons
 * @property {boolean} requiresApproval
 * @property {RollingMetrics|null} metricsSnapshot
 */
