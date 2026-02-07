/**
 * VolatilityCompressionScoreFeature: Multi-timeframe volatility compression detection
 *
 * Volatility compression (low volatility) often precedes expansion (breakout).
 * Combines multiple signals: spread compression + low volatility regime
 *
 * Hypothesis: High compression score predicts imminent breakout.
 *
 * Algorithm:
 * 1. Check regime_volatility (LOW = compression)
 * 2. Check spread_compression (positive = narrowing)
 * 3. Check volatility feature (low value = compression)
 * 4. Combine into composite score
 *
 * Range: [0, 1]
 *   0 = expanding/volatile (no compression)
 *   1 = maximum compression (breakout imminent)
 *
 * This is a DERIVED feature (depends on other features).
 */
export class VolatilityCompressionScoreFeature {
  static isDerived = true;
  static dependencies = ['volatility_ratio', 'spread_compression', 'volatility'];

  #window;
  #volHistory = [];
  #weights;

  constructor(config = {}) {
    this.#window = config.window || 50;
    this.#weights = config.weights || {
      regime: 0.4,
      spread: 0.3,
      volatility: 0.3
    };
  }

  /**
   * onEvent receives the full feature vector
   */
  onEvent(features) {
    const regimeVol = features.volatility_ratio; // Fixed: use correct feature name
    const spreadComp = features.spread_compression;
    const volatility = features.volatility;

    // If any required feature is missing, we can't calculate
    if (regimeVol === null || regimeVol === undefined ||
        spreadComp === null || spreadComp === undefined ||
        volatility === null || volatility === undefined) {
      return null;
    }

    // Track volatility history for percentile calculation
    this.#volHistory.push(volatility);
    if (this.#volHistory.length > this.#window) {
      this.#volHistory.shift();
    }

    // Warmup
    if (this.#volHistory.length < this.#window) return null;

    // Component 1: Regime score (LOW vol = 1, HIGH vol = 0)
    // volatility_ratio: <1 = LOW, ~1 = NORMAL, >1 = HIGH
    // Invert and clamp: low ratio = high score
    const regimeScore = Math.max(0, Math.min(1, 2 - regimeVol)); // 0.5→1.5, 1.0→1.0, 1.5→0.5, clamped to [0,1]

    // Component 2: Spread compression score
    // spread_compression: [-1, +1] where +1 = compressing
    // Map to [0, 1]
    const spreadScore = (spreadComp + 1) / 2;

    // Component 3: Volatility percentile score (inverted - low vol = high score)
    const sortedVol = [...this.#volHistory].sort((a, b) => a - b);
    const rank = sortedVol.filter(v => v <= volatility).length;
    const percentile = rank / sortedVol.length;
    const volScore = 1 - percentile; // Invert: low volatility = high score

    // Weighted combination
    const compressionScore =
      this.#weights.regime * regimeScore +
      this.#weights.spread * spreadScore +
      this.#weights.volatility * volScore;

    return compressionScore;
  }

  reset() {
    this.#volHistory = [];
  }
}
