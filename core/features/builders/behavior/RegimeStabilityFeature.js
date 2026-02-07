/**
 * RegimeStabilityFeature: Measure reliability of current market regime
 *
 * Stable regime = edges are more likely to work
 * Unstable regime = regime transitions, edges unreliable
 *
 * Hypothesis: Low regime stability predicts edge failure.
 *
 * Algorithm:
 * 1. Track history of regime features (volatility, trend, spread)
 * 2. Calculate variance of each regime dimension over window
 * 3. Low variance = stable regime (high stability score)
 * 4. High variance = transitioning regime (low stability score)
 *
 * Range: [0, 1]
 *   1 = very stable (regime consistent over time)
 *   0 = unstable (frequent regime changes)
 *
 * Note: This feature depends on regime features being enabled:
 * - regime_volatility
 * - regime_trend
 * - regime_spread
 */
export class RegimeStabilityFeature {
  static isDerived = true;
  static dependencies = ['volatility_ratio', 'trend_strength', 'spread_ratio'];

  #window;
  #volHistory = [];
  #trendHistory = [];
  #spreadHistory = [];
  #weights;

  constructor(config = {}) {
    this.#window = config.window || 100;
    this.#weights = config.weights || {
      volatility: 0.4,
      trend: 0.4,
      spread: 0.2
    };
  }

  /**
   * onEvent receives the full feature set (from FeatureBuilder)
   * We extract regime features and track their stability
   */
  onEvent(features) {
    // Extract regime features (using correct feature names)
    const regimeVol = features.volatility_ratio;
    const regimeTrend = features.trend_strength;
    const regimeSpread = features.spread_ratio;

    // If any regime feature is missing, we can't calculate stability
    if (regimeVol === null || regimeVol === undefined ||
        regimeTrend === null || regimeTrend === undefined ||
        regimeSpread === null || regimeSpread === undefined) {
      return null;
    }

    // Add to history
    this.#volHistory.push(regimeVol);
    this.#trendHistory.push(regimeTrend);
    this.#spreadHistory.push(regimeSpread);

    // Trim history
    if (this.#volHistory.length > this.#window) {
      this.#volHistory.shift();
      this.#trendHistory.shift();
      this.#spreadHistory.shift();
    }

    // Warmup: need full window
    if (this.#volHistory.length < this.#window) return null;

    // Calculate stability
    return this.#calculateStability();
  }

  #calculateStability() {
    // Calculate variance for each regime dimension
    const volVariance = this.#calculateVariance(this.#volHistory);
    const trendVariance = this.#calculateVariance(this.#trendHistory);
    const spreadVariance = this.#calculateVariance(this.#spreadHistory);

    // Normalize variances to [0, 1]
    // For categorical regime features:
    // - regime_volatility: 0, 1, 2 (max variance ≈ 0.67)
    // - regime_trend: -1, 0, 1 (max variance ≈ 0.67)
    // - regime_spread: 0, 1, 2 (max variance ≈ 0.67)
    const maxVariance = 0.67;

    const volStability = 1 - Math.min(volVariance / maxVariance, 1);
    const trendStability = 1 - Math.min(trendVariance / maxVariance, 1);
    const spreadStability = 1 - Math.min(spreadVariance / maxVariance, 1);

    // Weighted combination
    const stability =
      this.#weights.volatility * volStability +
      this.#weights.trend * trendStability +
      this.#weights.spread * spreadStability;

    return stability;
  }

  #calculateVariance(values) {
    if (values.length === 0) return 0;

    // Mean
    const mean = values.reduce((sum, val) => sum + val, 0) / values.length;

    // Variance
    const variance = values.reduce((sum, val) => {
      const diff = val - mean;
      return sum + diff * diff;
    }, 0) / values.length;

    return variance;
  }

  reset() {
    this.#volHistory = [];
    this.#trendHistory = [];
    this.#spreadHistory = [];
  }
}
