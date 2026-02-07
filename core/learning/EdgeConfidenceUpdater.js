/**
 * Edge Confidence Updater
 *
 * Updates edge confidence scores from live trade outcomes using EMA.
 * Detects drift and generates re-validation alerts.
 *
 * Drift Detection:
 * - Confidence drop > 15% from baseline
 * - Consecutive losses > 10
 * - Win rate drop > 10% from baseline
 *
 * Usage:
 *   const updater = new EdgeConfidenceUpdater(edgeRegistry);
 *   updater.setBaseline(edgeId, { confidence: 0.75, winRate: 0.55 });
 *   const alerts = updater.processOutcome(outcome);
 *   if (alerts) {
 *     // Trigger re-validation
 *   }
 */

import { LEARNING_CONFIG } from './config.js';

export class EdgeConfidenceUpdater {
  constructor(edgeRegistry, config = {}) {
    this.registry = edgeRegistry;
    this.config = { ...LEARNING_CONFIG.confidence, ...config };
    this.edgeBaselines = new Map();  // edgeId → baseline metrics
  }

  /**
   * Process single trade outcome
   * @param {Object} outcome - Trade outcome from TradeOutcomeCollector
   * @returns {Array|null} - Drift alerts or null
   */
  processOutcome(outcome) {
    const edge = this.registry.get(outcome.edgeId);
    if (!edge) {
      console.warn(`EdgeConfidenceUpdater: Edge ${outcome.edgeId} not found`);
      return null;
    }

    // Update edge stats (existing method)
    const win = outcome.pnl > 0;
    edge.updateStats({
      return: outcome.pnl,
      returnPct: outcome.pnl,
      win
    });

    // Update confidence with EMA
    if (edge.stats.trades >= this.config.minSampleSize) {
      this.#updateConfidence(edge, win);
    }

    // Check for drift
    return this.#checkDrift(edge);
  }

  /**
   * Process batch of outcomes
   * @param {Array} outcomes - Array of trade outcomes
   * @returns {Array} - All drift alerts
   */
  processBatch(outcomes) {
    const allAlerts = [];

    for (const outcome of outcomes) {
      const alerts = this.processOutcome(outcome);
      if (alerts && alerts.length > 0) {
        allAlerts.push(alerts);
      }
    }

    return allAlerts;
  }

  /**
   * Set baseline metrics for drift detection
   * @param {string} edgeId - Edge ID
   * @param {Object} metrics - Baseline metrics
   */
  setBaseline(edgeId, metrics) {
    this.edgeBaselines.set(edgeId, {
      confidence: metrics.confidence || 0.5,
      winRate: metrics.winRate || 0.5,
      sharpe: metrics.sharpe || 0,
      setAt: Date.now()
    });
  }

  /**
   * Get baseline for edge
   * @param {string} edgeId - Edge ID
   * @returns {Object|null}
   */
  getBaseline(edgeId) {
    return this.edgeBaselines.get(edgeId) || null;
  }

  /**
   * Clear baseline for edge
   * @param {string} edgeId - Edge ID
   */
  clearBaseline(edgeId) {
    this.edgeBaselines.delete(edgeId);
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    return {
      trackedEdges: this.edgeBaselines.size,
      baselines: Array.from(this.edgeBaselines.entries()).map(([edgeId, baseline]) => ({
        edgeId,
        ...baseline
      }))
    };
  }

  /**
   * Update confidence using EMA
   * @private
   */
  #updateConfidence(edge, win) {
    const alpha = this.config.decayWeight;
    const tradeScore = win ? 1.0 : 0.0;

    // EMA update: new_value = old_value * (1 - alpha) + new_sample * alpha
    edge.confidence.score = edge.confidence.score * (1 - alpha) + tradeScore * alpha;
    edge.confidence.sampleSize = edge.stats.trades;
    edge.confidence.lastUpdated = Date.now();
  }

  /**
   * Check for drift against baseline
   * @private
   */
  #checkDrift(edge) {
    const baseline = this.edgeBaselines.get(edge.id);
    if (!baseline) {
      return null;
    }

    const triggers = this.config.revalidationTrigger;
    const alerts = [];

    // 1. Confidence drop
    const confidenceDrop = baseline.confidence - edge.confidence.score;
    if (confidenceDrop > triggers.confidenceDrop) {
      alerts.push({
        type: 'CONFIDENCE_DROP',
        edgeId: edge.id,
        baseline: baseline.confidence,
        current: edge.confidence.score,
        drop: confidenceDrop,
        threshold: triggers.confidenceDrop
      });
    }

    // 2. Consecutive losses
    const consecutiveLosses = edge.stats.consecutiveLosses || 0;
    if (consecutiveLosses >= triggers.consecutiveLosses) {
      alerts.push({
        type: 'CONSECUTIVE_LOSSES',
        edgeId: edge.id,
        count: consecutiveLosses,
        threshold: triggers.consecutiveLosses
      });
    }

    // 3. Win rate drop
    const currentWinRate = edge.stats.trades > 0
      ? edge.stats.wins / edge.stats.trades
      : 0;
    const winRateDrop = baseline.winRate - currentWinRate;

    if (edge.stats.trades >= this.config.minSampleSize && winRateDrop > triggers.winRateDrop) {
      alerts.push({
        type: 'WINRATE_DROP',
        edgeId: edge.id,
        baseline: baseline.winRate,
        current: currentWinRate,
        drop: winRateDrop,
        threshold: triggers.winRateDrop
      });
    }

    return alerts.length > 0 ? alerts : null;
  }
}
