/**
 * VolatilityFeature: rolling standard deviation of returns
 */
export class VolatilityFeature {
  #windowSize;
  #returns = [];
  #prevMid = null;

  constructor(config = {}) {
    this.#windowSize = config.windowSize || 20;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    
    if (isNaN(bid) || isNaN(ask)) return null;
    
    const mid = (bid + ask) / 2;
    
    if (this.#prevMid !== null && this.#prevMid > 0) {
      const ret = (mid - this.#prevMid) / this.#prevMid;
      this.#returns.push(ret);
      if (this.#returns.length > this.#windowSize) {
        this.#returns.shift();
      }
    }
    
    this.#prevMid = mid;

    if (this.#returns.length < this.#windowSize) {
      return null;
    }

    return this.#calculateStdDev(this.#returns);
  }

  #calculateStdDev(values) {
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const sqDiffs = values.map(v => Math.pow(v - mean, 2));
    const avgSqDiff = sqDiffs.reduce((a, b) => a + b, 0) / values.length;
    return Math.sqrt(avgSqDiff);
  }

  reset() {
    this.#returns = [];
    this.#prevMid = null;
  }
}
