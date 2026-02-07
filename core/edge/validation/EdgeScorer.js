/**
 * EdgeScorer - Combine all validation results into a single score
 *
 * Weighted scoring system for edge validation.
 */

import { VALIDATION_CONFIG } from './config.js';

export class EdgeScorer {
  /**
   * @param {Object} config
   * @param {Object} config.weights - Scoring weights
   * @param {number} config.minScore - Minimum score to pass (default: 0.5)
   * @param {number} config.weakThreshold - Threshold for marginal edges (default: 0.4)
   */
  constructor(config = {}) {
    this.weights = config.weights || VALIDATION_CONFIG.scorer.weights;
    this.minScore = config.minScore || VALIDATION_CONFIG.scorer.minScore;
    this.weakThreshold = config.weakThreshold || VALIDATION_CONFIG.scorer.weakThreshold;
  }

  /**
   * Score an edge based on all validation results
   * @param {OOSResult} oosResult
   * @param {WalkForwardResult} wfResult
   * @param {DecayResult} decayResult
   * @param {RegimeRobustnessResult} regimeResult
   * @returns {EdgeScore}
   *
   * EdgeScore = {
   *   total: number,           // 0-1 weighted composite score
   *   components: Object,      // Individual component scores
   *   recommendation: 'VALIDATED'|'REJECTED'|'MARGINAL',
   *   summary: string
   * }
   */
  score(oosResult, wfResult, decayResult, regimeResult) {
    const components = {};

    // OOS component (0-1)
    components.oos = this.#scoreOOS(oosResult);

    // Walk-forward component (0-1)
    components.walkForward = this.#scoreWalkForward(wfResult);

    // Decay component (0-1)
    components.decay = this.#scoreDecay(decayResult);

    // Regime robustness component (0-1)
    components.regimeRobustness = this.#scoreRegimeRobustness(regimeResult);

    // Sample size component (0-1)
    components.sampleSize = this.#scoreSampleSize(oosResult);

    // Calculate weighted total
    const total =
      components.oos * this.weights.oos +
      components.walkForward * this.weights.walkForward +
      components.decay * this.weights.decay +
      components.regimeRobustness * this.weights.regimeRobustness +
      components.sampleSize * this.weights.sampleSize;

    // Determine recommendation
    let recommendation;
    if (total >= this.minScore) {
      recommendation = 'VALIDATED';
    } else if (total >= this.weakThreshold) {
      recommendation = 'MARGINAL';
    } else {
      recommendation = 'REJECTED';
    }

    // Generate summary
    const summary = this.#generateSummary(components, total, recommendation);

    return {
      total,
      components,
      recommendation,
      summary
    };
  }

  #scoreOOS(oosResult) {
    if (!oosResult.passed) return 0;

    // Normalize confidence to [0, 1]
    return Math.min(1, Math.max(0, oosResult.confidence));
  }

  #scoreWalkForward(wfResult) {
    if (!wfResult.passed) return 0;

    // Combine positive window fraction and consistency
    const windowScore = wfResult.positiveWindowFraction;
    const trendScore = wfResult.sharpeTrend >= 0 ? 1 : Math.max(0, 1 + wfResult.sharpeTrend);
    const consistencyScore = wfResult.consistency < 1 ? 1 : 1 / (1 + wfResult.consistency);

    return (windowScore + trendScore + consistencyScore) / 3;
  }

  #scoreDecay(decayResult) {
    if (!decayResult.passed) return 0;

    // No decay = 1, some decay = proportional
    if (!decayResult.isDecaying) return 1;

    // Normalize decay rate
    const decayScore = Math.max(0, 1 + decayResult.decayRate / 0.001);

    return decayScore;
  }

  #scoreRegimeRobustness(regimeResult) {
    if (!regimeResult.passed) return 0;

    // Selectivity score
    const selectivityScore = Math.min(1, regimeResult.regimeSelectivity / 1.0);

    // Target performance score
    const targetScore = Math.min(1, regimeResult.targetRegimePerformance / 1.0);

    return (selectivityScore + targetScore) / 2;
  }

  #scoreSampleSize(oosResult) {
    const totalTrades = oosResult.inSample.trades + oosResult.outOfSample.trades;

    // Normalize: 100+ trades = 1.0, < 30 trades = low
    return Math.min(1, totalTrades / 100);
  }

  #generateSummary(components, total, recommendation) {
    const parts = [];

    parts.push(`Total score: ${total.toFixed(3)}`);
    parts.push(`Recommendation: ${recommendation}`);

    const sortedComponents = Object.entries(components)
      .sort(([, a], [, b]) => b - a);

    parts.push(`Top scores: ${sortedComponents.slice(0, 3).map(([k, v]) => `${k}=${v.toFixed(2)}`).join(', ')}`);

    return parts.join('. ');
  }
}
