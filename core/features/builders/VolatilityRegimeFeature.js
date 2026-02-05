/**
 * VolatilityRegimeFeature: Detects volatility regime
 *
 * Compares short-term volatility to long-term average.
 * Output: 0=LOW, 1=NORMAL, 2=HIGH
 */
export class VolatilityRegimeFeature {
  #shortWindow;
  #longWindow;
  #lowThreshold;
  #highThreshold;
  #prevMid = null;
  #returns = [];
  #volHistory = [];

  constructor(config = {}) {
    this.#shortWindow = config.shortWindow || 20;
    this.#longWindow = config.longWindow || 100;
    this.#lowThreshold = config.lowThreshold || 0.5;
    this.#highThreshold = config.highThreshold || 2.0;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;

    // Calculate return
    if (this.#prevMid !== null && this.#prevMid > 0) {
      const ret = (mid - this.#prevMid) / this.#prevMid;
      this.#returns.push(ret);
      if (this.#returns.length > this.#shortWindow) {
        this.#returns.shift();
      }
    }
    this.#prevMid = mid;

    // Short-term volatility
    if (this.#returns.length < this.#shortWindow) return null;
    const currentVol = this.#calculateStdDev(this.#returns);

    // Long-term volatility history
    this.#volHistory.push(currentVol);
    if (this.#volHistory.length > this.#longWindow) {
      this.#volHistory.shift();
    }

    // Warm-up: need long window of vol history
    if (this.#volHistory.length < this.#longWindow) return null;

    const avgVol = this.#volHistory.reduce((a, b) => a + b, 0) / this.#volHistory.length;
    if (avgVol === 0) return 1; // NORMAL if no volatility

    const ratio = currentVol / avgVol;

    if (ratio < this.#lowThreshold) return 0;       // LOW
    if (ratio > this.#highThreshold) return 2;      // HIGH
    return 1;                                        // NORMAL
  }

  #calculateStdDev(values) {
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const sqDiffs = values.map(v => Math.pow(v - mean, 2));
    return Math.sqrt(sqDiffs.reduce((a, b) => a + b, 0) / values.length);
  }

  reset() {
    this.#prevMid = null;
    this.#returns = [];
    this.#volHistory = [];
  }
}
