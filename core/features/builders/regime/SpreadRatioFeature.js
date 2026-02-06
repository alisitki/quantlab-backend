/**
 * SpreadRatioFeature: Continuous spread regime metric
 *
 * Unlike SpreadRegimeFeature (categorical: 0/1/2), this outputs the raw ratio
 * of current spread to average spread.
 *
 * Algorithm:
 * 1. Track rolling average spread
 * 2. ratio = current_spread / average_spread
 *
 * Range: [0, inf) capped to [0, 5]
 *   <0.5 = tight spread (TIGHT regime)
 *   0.5-2.0 = normal spread (NORMAL regime)
 *   >2.0 = wide spread (WIDE regime)
 *
 * This continuous output enables clustering and finer spread distinctions.
 */
export class SpreadRatioFeature {
  #window;
  #spreadHistory = [];

  constructor(config = {}) {
    this.#window = config.window || 100;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    const spread = ask - bid;

    if (spread <= 0) return null; // Invalid spread

    this.#spreadHistory.push(spread);

    // Trim to window
    if (this.#spreadHistory.length > this.#window) {
      this.#spreadHistory.shift();
    }

    // Warmup: need full window
    if (this.#spreadHistory.length < this.#window) return null;

    // Calculate average spread
    const avgSpread = this.#spreadHistory.reduce((sum, s) => sum + s, 0) / this.#window;

    // Avoid division by zero
    if (avgSpread === 0) return 1.0;

    // Spread ratio
    const ratio = spread / avgSpread;

    // Cap to [0, 5] to prevent extreme outliers
    return Math.min(5, Math.max(0, ratio));
  }

  reset() {
    this.#spreadHistory = [];
  }
}
