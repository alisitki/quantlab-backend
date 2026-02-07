/**
 * Edge: A formalized, testable market pattern with expected advantage
 *
 * An edge is NOT just a strategy. An edge is:
 * - A specific, repeatable pattern in market behavior
 * - With measurable expected advantage
 * - That decays over time (due to usage or market adaptation)
 * - That works in specific regimes
 *
 * Edges are DISCOVERED (from data) or HYPOTHESIZED (from theory).
 * Strategies are GENERATED from validated edges.
 */
export class Edge {
  /**
   * @param {Object} definition - Edge definition
   * @param {string} definition.id - Unique identifier
   * @param {string} definition.name - Human-readable name
   * @param {Function} definition.entryCondition - (features, regime) => boolean
   * @param {Function} definition.exitCondition - (features, regime, entryTime) => boolean
   * @param {Array<string>} [definition.regimes] - Valid regime labels
   * @param {number} [definition.timeHorizon] - Expected holding period (ms)
   * @param {Object} [definition.expectedAdvantage] - Expected return statistics
   * @param {Object} [definition.riskProfile] - Risk characteristics
   * @param {Object} [definition.decayFunction] - Edge decay characteristics
   * @param {string} [definition.discoveryMethod] - How was this edge discovered?
   */
  constructor(definition) {
    // Required fields
    this.id = definition.id;
    this.name = definition.name;
    this.entryCondition = definition.entryCondition;
    this.exitCondition = definition.exitCondition;

    // Optional regime constraints
    this.regimes = definition.regimes || null; // null = works in any regime

    // Time characteristics
    this.timeHorizon = definition.timeHorizon || 10000; // Default 10 seconds

    // Expected advantage (from validation)
    this.expectedAdvantage = definition.expectedAdvantage || {
      mean: 0,
      std: 0,
      sharpe: 0,
      winRate: 0.5
    };

    // Risk profile
    this.riskProfile = definition.riskProfile || {
      maxDrawdown: 0,
      maxLoss: 0,
      tailRisk: 0
    };

    // Decay characteristics
    this.decayFunction = definition.decayFunction || {
      halfLife: null, // Unknown
      mechanism: 'unknown'
    };

    // Metadata
    this.discovered = definition.discovered || Date.now();
    this.discoveryMethod = definition.discoveryMethod || 'manual';
    this.status = definition.status || 'CANDIDATE';

    // Performance tracking
    this.stats = {
      trades: 0,
      wins: 0,
      losses: 0,
      totalReturn: 0,
      totalReturnPct: 0,
      avgReturn: 0,
      lastUpdated: null
    };

    // Confidence tracking
    this.confidence = {
      score: definition.confidence?.score || 0,
      sampleSize: definition.confidence?.sampleSize || 0,
      lastValidated: definition.confidence?.lastValidated || null
    };
  }

  /**
   * Evaluate if this edge is active for given features and regime
   * @param {Object} features - Feature vector
   * @param {string|number} regime - Current regime
   * @returns {Object} { active: boolean, direction?: 'LONG'|'SHORT', confidence?: number }
   */
  evaluateEntry(features, regime) {
    // Check regime constraint
    if (this.regimes && !this.regimes.includes(regime)) {
      return { active: false, reason: 'regime_mismatch' };
    }

    // Check if edge is retired
    if (this.status === 'RETIRED') {
      return { active: false, reason: 'retired' };
    }

    // Evaluate entry condition
    try {
      const result = this.entryCondition(features, regime);

      if (typeof result === 'boolean') {
        return { active: result };
      }

      // Entry condition can return { active, direction, confidence }
      return result;
    } catch (error) {
      console.error(`Edge ${this.id} entry evaluation error:`, error.message);
      return { active: false, reason: 'evaluation_error' };
    }
  }

  /**
   * Evaluate if this edge should exit
   * @param {Object} features - Current feature vector
   * @param {string|number} regime - Current regime
   * @param {number} entryTime - Entry timestamp (ms)
   * @param {number} currentTime - Current timestamp (ms)
   * @returns {Object} { exit: boolean, reason?: string }
   */
  evaluateExit(features, regime, entryTime, currentTime) {
    // Time-based exit (if timeHorizon exceeded)
    if (this.timeHorizon && (currentTime - entryTime) > this.timeHorizon) {
      return { exit: true, reason: 'time_horizon_exceeded' };
    }

    // Evaluate exit condition
    try {
      const result = this.exitCondition(features, regime, entryTime);

      if (typeof result === 'boolean') {
        return { exit: result };
      }

      return result;
    } catch (error) {
      console.error(`Edge ${this.id} exit evaluation error:`, error.message);
      return { exit: true, reason: 'evaluation_error' };
    }
  }

  /**
   * Update edge statistics with trade outcome
   * @param {Object} trade - Trade result
   */
  updateStats(trade) {
    this.stats.trades++;

    const win = trade.return > 0 || (trade.returnPct && trade.returnPct > 0);

    if (win) {
      this.stats.wins++;
      this.stats.consecutiveLosses = 0;
    } else if (trade.return < 0 || (trade.returnPct && trade.returnPct < 0)) {
      this.stats.losses++;
      this.stats.consecutiveLosses = (this.stats.consecutiveLosses || 0) + 1;
    } else {
      // Neutral trade (return = 0)
      this.stats.consecutiveLosses = 0;
    }

    this.stats.totalReturn += trade.return;
    this.stats.totalReturnPct += trade.returnPct || 0;
    this.stats.avgReturn = this.stats.totalReturn / this.stats.trades;
    this.stats.lastUpdated = Date.now();

    // Auto-retire if performance degrades severely
    if (this.stats.trades > 50 && this.stats.avgReturn < -0.001) {
      this.status = 'RETIRED';
    }
  }

  /**
   * Serialize edge for storage
   */
  toJSON() {
    return {
      id: this.id,
      name: this.name,
      regimes: this.regimes,
      timeHorizon: this.timeHorizon,
      expectedAdvantage: this.expectedAdvantage,
      riskProfile: this.riskProfile,
      decayFunction: this.decayFunction,
      discovered: this.discovered,
      discoveryMethod: this.discoveryMethod,
      status: this.status,
      stats: this.stats,
      confidence: this.confidence,
      // Note: entryCondition and exitCondition are functions, not serializable
      // They must be reconstructed from definition on load
    };
  }

  /**
   * Get edge health score [0-1]
   * Based on: recent performance, confidence, sample size
   */
  getHealthScore() {
    if (this.stats.trades === 0) return this.confidence.score;

    // Recent performance weight
    const recentWinRate = this.stats.wins / this.stats.trades;
    const performanceScore = Math.max(0, Math.min(1, recentWinRate * 2)); // 0.5 win rate = 1.0 score

    // Confidence weight
    const confidenceScore = this.confidence.score;

    // Sample size weight (more trades = more reliable)
    const sampleScore = Math.min(1, this.stats.trades / 100);

    // Weighted average
    return 0.5 * performanceScore + 0.3 * confidenceScore + 0.2 * sampleScore;
  }

  /**
   * Check if edge should be retired
   */
  shouldRetire() {
    // Already retired
    if (this.status === 'RETIRED') return true;

    // Insufficient data
    if (this.stats.trades < 30) return false;

    // Poor performance
    if (this.stats.avgReturn < -0.001) return true;

    // Low win rate with insufficient sample
    if (this.stats.trades < 100 && (this.stats.wins / this.stats.trades) < 0.3) return true;

    // Health score too low
    if (this.getHealthScore() < 0.2) return true;

    return false;
  }
}
