/**
 * VolatilityRatioFeature: Continuous volatility regime metric
 *
 * Unlike VolatilityRegimeFeature (categorical: 0/1/2), this outputs the raw ratio
 * of short-term to long-term volatility, preserving information for clustering.
 *
 * Algorithm:
 * 1. Calculate short-term volatility (rolling std dev, fast window)
 * 2. Calculate long-term volatility (rolling std dev, slow window)
 * 3. ratio = short_vol / long_vol
 *
 * Range: [0, inf) capped to [0, 5]
 *   <0.5 = volatility compression (LOW regime)
 *   0.5-2.0 = normal volatility (NORMAL regime)
 *   >2.0 = volatility expansion (HIGH regime)
 *
 * This continuous output enables clustering and finer regime distinctions.
 */
export class VolatilityRatioFeature {
  #shortWindow;
  #longWindow;
  #returns = [];
  #prevMid = null;

  constructor(config = {}) {
    this.#shortWindow = config.shortWindow || 20;
    this.#longWindow = config.longWindow || 100;
  }

  onEvent(event) {
    const mid = (Number(event.bid_price) + Number(event.ask_price)) / 2;

    // Need previous mid to calculate return
    if (this.#prevMid === null) {
      this.#prevMid = mid;
      return null; // Warmup
    }

    // Calculate return
    const ret = (mid - this.#prevMid) / this.#prevMid;
    this.#prevMid = mid;

    this.#returns.push(ret);

    // Trim to long window
    if (this.#returns.length > this.#longWindow) {
      this.#returns.shift();
    }

    // Warmup: need long window
    if (this.#returns.length < this.#longWindow) return null;

    // Calculate short-term volatility (most recent)
    const shortReturns = this.#returns.slice(-this.#shortWindow);
    const shortVol = this.#calculateStdDev(shortReturns);

    // Calculate long-term volatility (full window)
    const longVol = this.#calculateStdDev(this.#returns);

    // Avoid division by zero
    if (longVol === 0) return 1.0;

    // Volatility ratio
    const ratio = shortVol / longVol;

    // Cap to [0, 5] to prevent extreme outliers
    return Math.min(5, Math.max(0, ratio));
  }

  #calculateStdDev(values) {
    if (values.length === 0) return 0;

    const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
    const variance = values.reduce((sum, val) => {
      const diff = val - mean;
      return sum + diff * diff;
    }, 0) / values.length;

    return Math.sqrt(variance);
  }

  reset() {
    this.#returns = [];
    this.#prevMid = null;
  }
}
