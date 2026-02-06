/**
 * BehaviorDivergenceFeature: Detect divergence between momentum and liquidity pressure
 *
 * Divergence occurs when price momentum and liquidity pressure disagree.
 * Example: Price moving up (positive momentum) but sell pressure increasing
 * This divergence can signal reversals or weakening trends.
 *
 * Hypothesis: High divergence predicts trend exhaustion or reversal.
 *
 * Algorithm:
 * 1. Compare return_momentum (directional consistency) vs liquidity_pressure (L1 imbalance)
 * 2. divergence = momentum - pressure
 * 3. High absolute divergence = contradictory signals
 *
 * Range: [-2, +2] normalized to [-1, +1]
 *   +1 = momentum bullish but pressure bearish (weak uptrend)
 *   -1 = momentum bearish but pressure bullish (weak downtrend)
 *    0 = momentum and pressure agree (strong trend)
 *
 * This is a DERIVED feature (depends on other features).
 */
export class BehaviorDivergenceFeature {
  static isDerived = true;
  static dependencies = ['return_momentum', 'liquidity_pressure'];

  constructor(config = {}) {
    // No internal state needed for this derived feature
  }

  /**
   * onEvent receives the full feature vector
   */
  onEvent(features) {
    const momentum = features.return_momentum;
    const pressure = features.liquidity_pressure;

    // If either feature is missing, we can't calculate divergence
    if (momentum === null || momentum === undefined ||
        pressure === null || pressure === undefined) {
      return null;
    }

    // Divergence: difference between momentum and pressure
    // Range: [-2, +2] (both features are [-1, +1])
    const divergence = momentum - pressure;

    // Normalize to [-1, +1]
    return divergence / 2;
  }

  reset() {
    // No internal state to reset
  }
}
