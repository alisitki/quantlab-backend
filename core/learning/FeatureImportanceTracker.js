/**
 * FeatureImportanceTracker: Analyze feature importance from trade outcomes
 *
 * Purpose: Identify which features drive edge performance by correlating
 * entry feature values with trade outcomes (PnL, win/loss).
 *
 * Methods:
 * 1. Pearson correlation between feature values and PnL
 * 2. Win/loss distribution comparison
 * 3. Rolling window tracking
 * 4. Noise feature detection (low correlation + low variance)
 *
 * This is a LEARNING component - results inform behavior refinement proposals.
 */

export class FeatureImportanceTracker {
  #history = []; // Rolling window of importance calculations
  #maxHistorySize = 10;

  constructor(config = {}) {
    this.#maxHistorySize = config.maxHistorySize || 10;
  }

  /**
   * Analyze feature importance from trade outcomes
   * @param {Array<TradeOutcome>} outcomes - Array of trade outcomes
   * @returns {Object} { edgeId: { feature: { importance, correlation, pValue, winRate, distributionShift } } }
   *
   * TradeOutcome = {
   *   edgeId, strategyId, entryFeatures, pnl, outcome ('WIN'|'LOSS'),
   *   timestamp, confidence
   * }
   */
  analyze(outcomes) {
    if (!outcomes || outcomes.length === 0) {
      return {};
    }

    // Group outcomes by edge
    const outcomesByEdge = this.#groupByEdge(outcomes);

    const results = {};

    for (const [edgeId, edgeOutcomes] of Object.entries(outcomesByEdge)) {
      if (edgeOutcomes.length < 10) {
        // Need at least 10 outcomes for meaningful statistics
        continue;
      }

      // Extract feature names from first outcome
      const featureNames = Object.keys(edgeOutcomes[0].entryFeatures || {});

      const featureImportance = {};

      for (const featureName of featureNames) {
        const importance = this.#calculateFeatureImportance(
          edgeOutcomes,
          featureName
        );

        featureImportance[featureName] = importance;
      }

      results[edgeId] = featureImportance;
    }

    // Store in history
    this.#history.push({
      timestamp: Date.now(),
      results
    });

    if (this.#history.length > this.#maxHistorySize) {
      this.#history.shift();
    }

    return results;
  }

  /**
   * Calculate importance for a single feature
   * @param {Array<TradeOutcome>} outcomes
   * @param {string} featureName
   * @returns {Object} { importance, correlation, pValue, winRate, distributionShift }
   */
  #calculateFeatureImportance(outcomes, featureName) {
    // Extract feature values and PnL
    const data = outcomes
      .map(o => ({
        featureValue: o.entryFeatures[featureName],
        pnl: o.pnl,
        isWin: o.outcome === 'WIN'
      }))
      .filter(d => d.featureValue !== null && d.featureValue !== undefined);

    if (data.length === 0) {
      return this.#nullImportance();
    }

    // 1. Pearson correlation with PnL
    const correlation = this.#pearsonCorrelation(
      data.map(d => d.featureValue),
      data.map(d => d.pnl)
    );

    // 2. P-value (t-test approximation)
    const pValue = this.#correlationPValue(correlation, data.length);

    // 3. Win rate by feature quantile
    const winRateByQuantile = this.#winRateByQuantile(data);

    // 4. Distribution shift (KS-like metric)
    const distributionShift = this.#distributionShift(data);

    // Combined importance score (weighted average)
    // Higher correlation + lower p-value + stronger distribution shift = higher importance
    const absCorrelation = Math.abs(correlation);
    const pValueScore = Math.max(0, 1 - pValue); // Convert p-value to score
    const importance = (
      0.5 * absCorrelation +
      0.3 * pValueScore +
      0.2 * distributionShift
    );

    return {
      importance: Math.min(1, importance), // Clamp to [0, 1]
      correlation,
      pValue,
      winRateByQuantile,
      distributionShift,
      sampleSize: data.length
    };
  }

  /**
   * Pearson correlation coefficient
   */
  #pearsonCorrelation(x, y) {
    const n = x.length;
    if (n === 0) return 0;

    const meanX = x.reduce((sum, v) => sum + v, 0) / n;
    const meanY = y.reduce((sum, v) => sum + v, 0) / n;

    let numerator = 0;
    let denomX = 0;
    let denomY = 0;

    for (let i = 0; i < n; i++) {
      const dx = x[i] - meanX;
      const dy = y[i] - meanY;
      numerator += dx * dy;
      denomX += dx * dx;
      denomY += dy * dy;
    }

    const denom = Math.sqrt(denomX * denomY);
    if (denom === 0) return 0;

    const r = numerator / denom;
    return isNaN(r) ? 0 : r;
  }

  /**
   * Approximate p-value for correlation coefficient
   * Uses t-distribution approximation
   */
  #correlationPValue(r, n) {
    if (n < 3) return 1;

    const t = r * Math.sqrt((n - 2) / (1 - r * r));
    const absT = Math.abs(t);

    // Rough p-value approximation (two-tailed)
    // t > 2.0 → p < 0.05
    // t > 2.6 → p < 0.01
    if (absT > 2.6) return 0.01;
    if (absT > 2.0) return 0.05;
    if (absT > 1.5) return 0.15;
    return 0.5;
  }

  /**
   * Calculate win rate by feature quantile
   * Divides feature range into quartiles, calculates win rate for each
   */
  #winRateByQuantile(data) {
    // Sort by feature value
    const sorted = [...data].sort((a, b) => a.featureValue - b.featureValue);

    const quartileSize = Math.floor(sorted.length / 4);
    if (quartileSize === 0) return [];

    const quantiles = [];

    for (let q = 0; q < 4; q++) {
      const start = q * quartileSize;
      const end = q === 3 ? sorted.length : (q + 1) * quartileSize;
      const slice = sorted.slice(start, end);

      const wins = slice.filter(d => d.isWin).length;
      const total = slice.length;
      const winRate = total > 0 ? wins / total : 0;

      quantiles.push({
        quantile: q,
        winRate,
        sampleSize: total
      });
    }

    return quantiles;
  }

  /**
   * Distribution shift between wins and losses
   * Uses difference in means normalized by pooled std
   * Similar to Cohen's d effect size
   */
  #distributionShift(data) {
    const wins = data.filter(d => d.isWin).map(d => d.featureValue);
    const losses = data.filter(d => !d.isWin).map(d => d.featureValue);

    if (wins.length === 0 || losses.length === 0) return 0;

    const meanWin = wins.reduce((sum, v) => sum + v, 0) / wins.length;
    const meanLoss = losses.reduce((sum, v) => sum + v, 0) / losses.length;

    // Calculate pooled standard deviation
    const varWin = wins.reduce((sum, v) => sum + Math.pow(v - meanWin, 2), 0) / wins.length;
    const varLoss = losses.reduce((sum, v) => sum + Math.pow(v - meanLoss, 2), 0) / losses.length;
    const pooledStd = Math.sqrt((varWin + varLoss) / 2);

    if (pooledStd === 0) return 0;

    // Effect size (Cohen's d)
    const effectSize = Math.abs(meanWin - meanLoss) / pooledStd;

    // Normalize to [0, 1] (d > 0.8 is large effect)
    return Math.min(1, effectSize / 0.8);
  }

  /**
   * Get feature ranking for an edge
   * @param {string} edgeId
   * @returns {Array<{feature, importance, trend}>}
   */
  getFeatureRanking(edgeId) {
    if (this.#history.length === 0) return [];

    // Use latest analysis
    const latest = this.#history[this.#history.length - 1];
    const edgeData = latest.results[edgeId];

    if (!edgeData) return [];

    // Convert to array and sort by importance
    const ranking = Object.entries(edgeData).map(([feature, stats]) => ({
      feature,
      importance: stats.importance,
      correlation: stats.correlation,
      pValue: stats.pValue,
      trend: this.#calculateTrend(edgeId, feature)
    }));

    ranking.sort((a, b) => b.importance - a.importance);

    return ranking;
  }

  /**
   * Calculate importance trend over history
   */
  #calculateTrend(edgeId, featureName) {
    if (this.#history.length < 2) return 'STABLE';

    const recentValues = this.#history
      .slice(-5) // Last 5 windows
      .map(h => h.results[edgeId]?.[featureName]?.importance || 0);

    if (recentValues.length < 2) return 'STABLE';

    const first = recentValues[0];
    const last = recentValues[recentValues.length - 1];
    const change = last - first;

    if (change > 0.1) return 'RISING';
    if (change < -0.1) return 'FALLING';
    return 'STABLE';
  }

  /**
   * Get noise features (low importance, stable)
   * @param {string} edgeId
   * @param {number} threshold - Importance threshold (default: 0.15)
   * @returns {Array<string>} Feature names
   */
  getNoiseFeatures(edgeId, threshold = 0.15) {
    const ranking = this.getFeatureRanking(edgeId);

    return ranking
      .filter(r => r.importance < threshold && r.trend === 'STABLE')
      .map(r => r.feature);
  }

  /**
   * Null importance (for missing data)
   */
  #nullImportance() {
    return {
      importance: 0,
      correlation: 0,
      pValue: 1,
      winRateByQuantile: [],
      distributionShift: 0,
      sampleSize: 0
    };
  }

  /**
   * Group outcomes by edge
   */
  #groupByEdge(outcomes) {
    const groups = {};

    for (const outcome of outcomes) {
      const edgeId = outcome.edgeId;
      if (!groups[edgeId]) {
        groups[edgeId] = [];
      }
      groups[edgeId].push(outcome);
    }

    return groups;
  }

  /**
   * Serialize to JSON
   */
  toJSON() {
    return {
      history: this.#history,
      maxHistorySize: this.#maxHistorySize
    };
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json) {
    const tracker = new FeatureImportanceTracker({
      maxHistorySize: json.maxHistorySize
    });

    tracker.#history = json.history || [];

    return tracker;
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    if (this.#history.length === 0) {
      return {
        analysisCount: 0,
        edgeCount: 0,
        totalOutcomes: 0
      };
    }

    const latest = this.#history[this.#history.length - 1];
    const edgeCount = Object.keys(latest.results).length;

    return {
      analysisCount: this.#history.length,
      edgeCount,
      timestamp: latest.timestamp
    };
  }
}
