/**
 * Performance Tracker for Strategy Lifecycle
 *
 * Records run results per strategy and calculates rolling performance metrics.
 * Used by StrategyLifecycleManager to evaluate promotion/demotion decisions.
 */

import { LIFECYCLE_CONFIG } from './config.js';

/**
 * Tracks performance metrics across multiple runs for each strategy
 */
export class PerformanceTracker {
  constructor() {
    // Map<strategyId, RunRecord[]>
    this.runs = new Map();
  }

  /**
   * Record a completed run result
   * @param {string} strategyId
   * @param {RunResult} runResult
   */
  recordRun(strategyId, runResult) {
    if (!this.runs.has(strategyId)) {
      this.runs.set(strategyId, []);
    }

    const record = {
      runId: runResult.runId,
      completedAt: runResult.completedAt || new Date().toISOString(),
      trades: runResult.trades || 0,
      pnl: runResult.pnl || 0,
      returnPct: runResult.returnPct || 0,
      maxDrawdownPct: runResult.maxDrawdownPct || 0,
      winRate: runResult.winRate || 0,
      sharpe: runResult.sharpe || 0,
      stopReason: runResult.stopReason || 'unknown',
      durationMs: runResult.durationMs || 0
    };

    this.runs.get(strategyId).push(record);
  }

  /**
   * Get rolling metrics for a strategy over a time window
   * @param {string} strategyId
   * @param {number} windowDays - Default 30 days
   * @returns {RollingMetrics|null}
   */
  getRollingMetrics(strategyId, windowDays = LIFECYCLE_CONFIG.evaluation.rollingWindowDays) {
    const runs = this.runs.get(strategyId);
    if (!runs || runs.length === 0) return null;

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - windowDays);
    const cutoffMs = cutoffDate.getTime();

    const windowRuns = runs.filter(r => new Date(r.completedAt).getTime() >= cutoffMs);
    if (windowRuns.length === 0) return null;

    return this.#calculateMetrics(windowRuns, strategyId);
  }

  /**
   * Get all-time metrics for a strategy
   * @param {string} strategyId
   * @returns {RollingMetrics|null}
   */
  getAllTimeMetrics(strategyId) {
    const runs = this.runs.get(strategyId);
    if (!runs || runs.length === 0) return null;
    return this.#calculateMetrics(runs, strategyId);
  }

  /**
   * Get run history for a strategy
   * @param {string} strategyId
   * @returns {RunRecord[]}
   */
  getRunHistory(strategyId) {
    return this.runs.get(strategyId) || [];
  }

  /**
   * Calculate metrics from a set of runs
   * @private
   * @param {RunRecord[]} runs
   * @param {string} strategyId
   * @returns {RollingMetrics}
   */
  #calculateMetrics(runs, strategyId) {
    const totalRuns = runs.length;
    const totalTrades = runs.reduce((sum, r) => sum + r.trades, 0);

    // Average return
    const avgReturn = runs.length > 0
      ? runs.reduce((sum, r) => sum + r.returnPct, 0) / runs.length
      : 0;

    // Average Sharpe (filter out runs with Sharpe, some might not have it)
    const runsWithSharpe = runs.filter(r => r.sharpe !== undefined && r.sharpe !== null);
    const avgSharpe = runsWithSharpe.length > 0
      ? runsWithSharpe.reduce((sum, r) => sum + r.sharpe, 0) / runsWithSharpe.length
      : 0;

    // Average win rate
    const runsWithWinRate = runs.filter(r => r.winRate !== undefined && r.winRate !== null);
    const avgWinRate = runsWithWinRate.length > 0
      ? runsWithWinRate.reduce((sum, r) => sum + r.winRate, 0) / runsWithWinRate.length
      : 0;

    // Max drawdown across all runs
    const maxDrawdownPct = Math.max(...runs.map(r => r.maxDrawdownPct || 0));

    // Consecutive loss days
    const consecutiveLossDays = this.#calculateConsecutiveLossDays(runs);

    // Positive run fraction
    const positiveRuns = runs.filter(r => r.returnPct > 0).length;
    const positiveRunFraction = totalRuns > 0 ? positiveRuns / totalRuns : 0;

    return {
      totalRuns,
      totalTrades,
      avgReturn,
      avgSharpe,
      avgWinRate,
      maxDrawdownPct,
      consecutiveLossDays,
      positiveRunFraction
    };
  }

  /**
   * Calculate consecutive loss days from run history
   * @private
   * @param {RunRecord[]} runs
   * @returns {number}
   */
  #calculateConsecutiveLossDays(runs) {
    if (runs.length === 0) return 0;

    // Sort by completion date
    const sorted = [...runs].sort((a, b) =>
      new Date(a.completedAt).getTime() - new Date(b.completedAt).getTime()
    );

    let maxConsecutive = 0;
    let currentConsecutive = 0;
    let lastDate = null;

    for (const run of sorted) {
      const runDate = new Date(run.completedAt);
      const dateStr = runDate.toISOString().split('T')[0]; // YYYY-MM-DD

      if (run.returnPct < 0) {
        // Loss run
        if (dateStr !== lastDate) {
          // New day
          currentConsecutive++;
          lastDate = dateStr;
        }
        maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
      } else {
        // Winning run resets counter
        currentConsecutive = 0;
        lastDate = null;
      }
    }

    return maxConsecutive;
  }

  /**
   * Serialize to JSON
   * @returns {object}
   */
  toJSON() {
    const data = {};
    for (const [strategyId, runs] of this.runs.entries()) {
      data[strategyId] = runs;
    }
    return data;
  }

  /**
   * Deserialize from JSON
   * @param {object} data
   * @returns {PerformanceTracker}
   */
  static fromJSON(data) {
    const tracker = new PerformanceTracker();
    for (const [strategyId, runs] of Object.entries(data)) {
      tracker.runs.set(strategyId, runs);
    }
    return tracker;
  }
}

/**
 * @typedef {Object} RunResult
 * @property {string} runId
 * @property {string} completedAt - ISO timestamp
 * @property {number} trades
 * @property {number} pnl
 * @property {number} returnPct
 * @property {number} maxDrawdownPct
 * @property {number} winRate
 * @property {number} sharpe
 * @property {string} stopReason
 * @property {number} durationMs
 */

/**
 * @typedef {Object} RunRecord
 * @property {string} runId
 * @property {string} completedAt
 * @property {number} trades
 * @property {number} pnl
 * @property {number} returnPct
 * @property {number} maxDrawdownPct
 * @property {number} winRate
 * @property {number} sharpe
 * @property {string} stopReason
 * @property {number} durationMs
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
