/**
 * SpreadRegimeFeature: Detects spread/liquidity regime
 *
 * Compares current spread to rolling average spread.
 * Output: 0=TIGHT, 1=NORMAL, 2=WIDE
 */
export class SpreadRegimeFeature {
  #window;
  #lowThreshold;
  #highThreshold;
  #spreadHistory = [];

  constructor(config = {}) {
    this.#window = config.window || 100;
    this.#lowThreshold = config.lowThreshold || 0.5;
    this.#highThreshold = config.highThreshold || 2.0;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const spread = ask - bid;

    this.#spreadHistory.push(spread);
    if (this.#spreadHistory.length > this.#window) {
      this.#spreadHistory.shift();
    }

    // Warm-up: need full window
    if (this.#spreadHistory.length < this.#window) return null;

    const avgSpread = this.#spreadHistory.reduce((a, b) => a + b, 0) / this.#window;
    if (avgSpread === 0) return 1; // NORMAL if no spread

    const ratio = spread / avgSpread;

    if (ratio < this.#lowThreshold) return 0;       // TIGHT
    if (ratio > this.#highThreshold) return 2;      // WIDE
    return 1;                                        // NORMAL
  }

  reset() {
    this.#spreadHistory = [];
  }
}
