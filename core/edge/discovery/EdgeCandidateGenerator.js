/**
 * EdgeCandidateGenerator - Convert validated patterns into Edge objects
 *
 * Generates entry/exit condition functions from pattern conditions.
 * Maps statistical test results to Edge properties.
 */

import { Edge } from '../Edge.js';
import { DISCOVERY_CONFIG } from './config.js';

export class EdgeCandidateGenerator {
  /**
   * @param {Object} config
   * @param {number} config.defaultTimeHorizon - Default holding period ms (default: 10000)
   * @param {number} config.minConfidenceScore - Minimum confidence score (default: 0.6)
   */
  constructor(config = {}) {
    this.defaultTimeHorizon = config.defaultTimeHorizon || DISCOVERY_CONFIG.generator.defaultTimeHorizon;
    this.minConfidenceScore = config.minConfidenceScore || DISCOVERY_CONFIG.generator.minConfidenceScore;
  }

  /**
   * Generate Edge from validated pattern + test result
   * @param {PatternCandidate} pattern
   * @param {EdgeTestResult} testResult
   * @returns {Edge} Edge instance with status=CANDIDATE
   */
  generate(pattern, testResult) {
    // Build entry/exit conditions
    const entryCondition = this.#buildEntryCondition(
      pattern.conditions,
      pattern.regimes,
      pattern.direction
    );

    const exitCondition = this.#buildExitCondition(pattern.conditions);

    // Map statistics
    const edgeStats = this.#buildEdgeStatistics(testResult, pattern);

    // Generate edge ID and name
    const edgeId = `discovered_${pattern.type}_${pattern.id}`;
    const edgeName = this.#generateEdgeName(pattern);

    // Create Edge instance
    const edge = new Edge({
      id: edgeId,
      name: edgeName,
      entryCondition,
      exitCondition,
      regimes: pattern.regimes,
      timeHorizon: this.#estimateTimeHorizon(pattern.horizon),
      expectedAdvantage: edgeStats.expectedAdvantage,
      riskProfile: edgeStats.riskProfile,
      decayFunction: edgeStats.decayFunction,
      discoveryMethod: `auto_${pattern.type}`,
      status: 'CANDIDATE',
      confidence: edgeStats.confidence
    });

    return edge;
  }

  /**
   * Generate multiple edges from batch results
   * @param {Array<{pattern, testResult}>} validatedPatterns
   * @returns {Array<Edge>}
   */
  generateBatch(validatedPatterns) {
    console.log(`[EdgeCandidateGenerator] Generating ${validatedPatterns.length} edge candidates...`);

    const edges = validatedPatterns
      .filter(({ testResult }) => testResult.recommendation === 'ACCEPT')
      .map(({ pattern, testResult }) => this.generate(pattern, testResult));

    console.log(`[EdgeCandidateGenerator] Generated ${edges.length} edges`);

    return edges;
  }

  /**
   * Build entry condition function from pattern conditions
   * @param {Array<{feature, operator, value}>} conditions
   * @param {number[]|null} regimes
   * @param {string} direction
   * @returns {Function} (features, regime) => { active, direction, confidence }
   */
  #buildEntryCondition(conditions, regimes, direction) {
    return (features, regime) => {
      // Check regime constraint
      if (regimes && !regimes.includes(regime)) {
        return { active: false, reason: 'regime_mismatch' };
      }

      // Check all conditions
      for (const condition of conditions) {
        const featureValue = features[condition.feature];

        if (featureValue === null || featureValue === undefined) {
          return { active: false, reason: 'missing_feature' };
        }

        let conditionMet = false;

        switch (condition.operator) {
          case '>':
            conditionMet = featureValue > condition.value;
            break;
          case '<':
            conditionMet = featureValue < condition.value;
            break;
          case '>=':
            conditionMet = featureValue >= condition.value;
            break;
          case '<=':
            conditionMet = featureValue <= condition.value;
            break;
          case '==':
            conditionMet = featureValue === condition.value;
            break;
          default:
            console.warn(`Unknown operator: ${condition.operator}`);
            return { active: false, reason: 'unknown_operator' };
        }

        if (!conditionMet) {
          return { active: false, reason: 'condition_not_met' };
        }
      }

      // All conditions met
      return {
        active: true,
        direction,
        confidence: this.#calculateEntryConfidence(features, conditions)
      };
    };
  }

  /**
   * Build exit condition function from pattern characteristics
   * Exits when the entry condition is no longer met OR time horizon exceeded
   * @param {Array<{feature, operator, value}>} conditions
   * @returns {Function} (features, regime, entryTime, currentTime) => { exit, reason }
   */
  #buildExitCondition(conditions) {
    return (features, regime, entryTime, currentTime) => {
      // Check if entry conditions are still met (inverse logic for exit)
      for (const condition of conditions) {
        const featureValue = features[condition.feature];

        if (featureValue === null || featureValue === undefined) {
          return { exit: true, reason: 'missing_feature' };
        }

        let conditionStillMet = false;

        switch (condition.operator) {
          case '>':
            conditionStillMet = featureValue > condition.value;
            break;
          case '<':
            conditionStillMet = featureValue < condition.value;
            break;
          case '>=':
            conditionStillMet = featureValue >= condition.value;
            break;
          case '<=':
            conditionStillMet = featureValue <= condition.value;
            break;
          case '==':
            conditionStillMet = featureValue === condition.value;
            break;
          default:
            return { exit: true, reason: 'unknown_operator' };
        }

        // If condition no longer met, exit
        if (!conditionStillMet) {
          return { exit: true, reason: 'condition_reversed' };
        }
      }

      // All conditions still met, don't exit
      return { exit: false };
    };
  }

  /**
   * Map test results to expectedAdvantage and riskProfile
   * @param {EdgeTestResult} testResult
   * @param {PatternCandidate} pattern
   * @returns {Object}
   */
  #buildEdgeStatistics(testResult, pattern) {
    const expectedAdvantage = {
      mean: pattern.forwardReturns.mean,
      std: pattern.forwardReturns.std,
      sharpe: testResult.tests.sharpeTest.sharpe,
      winRate: this.#estimateWinRate(pattern.forwardReturns)
    };

    const riskProfile = {
      maxDrawdown: pattern.forwardReturns.std * 2, // Conservative estimate
      maxLoss: Math.abs(pattern.forwardReturns.mean) + pattern.forwardReturns.std * 2,
      tailRisk: pattern.forwardReturns.std * 3
    };

    const decayFunction = {
      halfLife: null, // Unknown until live monitoring
      mechanism: 'unknown'
    };

    const confidence = {
      score: testResult.overallScore,
      sampleSize: pattern.support,
      lastValidated: Date.now()
    };

    return {
      expectedAdvantage,
      riskProfile,
      decayFunction,
      confidence
    };
  }

  /**
   * Calculate entry confidence based on feature values
   */
  #calculateEntryConfidence(features, conditions) {
    // Simple heuristic: average "distance" from threshold
    let totalConfidence = 0;

    for (const condition of conditions) {
      const featureValue = features[condition.feature];
      const threshold = condition.value;

      let distance = 0;

      switch (condition.operator) {
        case '>':
        case '>=':
          distance = Math.max(0, Math.min(1, (featureValue - threshold) / (1 - threshold)));
          break;
        case '<':
        case '<=':
          distance = Math.max(0, Math.min(1, (threshold - featureValue) / (threshold - (-1))));
          break;
        case '==':
          distance = 1; // Exact match
          break;
      }

      totalConfidence += distance;
    }

    return Math.min(1, totalConfidence / conditions.length);
  }

  /**
   * Estimate win rate from forward returns distribution
   */
  #estimateWinRate(forwardReturns) {
    // If mean > 0, assume majority are winners
    // Simple heuristic based on mean/std ratio
    const meanStdRatio = forwardReturns.std === 0 ? 0 : forwardReturns.mean / forwardReturns.std;

    // Normal distribution approximation
    // z-score for mean=0: P(X > 0) ≈ Φ(mean/std)
    const winRate = 0.5 + 0.5 * Math.tanh(meanStdRatio); // Bounded to [0, 1]

    return Math.max(0.3, Math.min(0.9, winRate)); // Clamp to reasonable range
  }

  /**
   * Estimate time horizon in milliseconds from event horizon
   */
  #estimateTimeHorizon(eventHorizon) {
    // Assume ~1 event per second on average
    // 10 events ≈ 10 seconds, 50 events ≈ 50 seconds, 100 events ≈ 100 seconds
    return eventHorizon * 1000;
  }

  /**
   * Generate human-readable edge name
   */
  #generateEdgeName(pattern) {
    if (pattern.type === 'threshold' && pattern.conditions.length > 0) {
      const cond = pattern.conditions[0];
      const featureName = cond.feature.replace(/_/g, ' ');
      const op = cond.operator === '>' ? 'High' : 'Low';
      return `${op} ${featureName} (${pattern.direction})`;
    }

    if (pattern.type === 'cluster') {
      return `Cluster ${pattern.conditions[0].value} (${pattern.direction})`;
    }

    if (pattern.type === 'quantile') {
      return `Quantile Extreme (${pattern.direction})`;
    }

    return `Pattern ${pattern.id}`;
  }
}
