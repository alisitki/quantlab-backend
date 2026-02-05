/**
 * EMASlopeFeature: Slope of EMA over lookback period
 *
 * Formula: slope = (EMA[t] - EMA[t-lookback]) / lookback
 * Measures trend velocity
 */
export class EMASlopeFeature {
  #period;
  #lookback;
  #alpha;
  #prevEma = null;
  #emaHistory = [];
  #count = 0;

  constructor(config = {}) {
    this.#period = config.period || 14;
    this.#lookback = config.lookback || 5;
    this.#alpha = 2 / (this.#period + 1);
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;
    this.#count++;

    if (this.#prevEma === null) {
      this.#prevEma = mid;
      return null;
    }

    const ema = this.#alpha * mid + (1 - this.#alpha) * this.#prevEma;
    this.#prevEma = ema;

    this.#emaHistory.push(ema);
    if (this.#emaHistory.length > this.#lookback + 1) {
      this.#emaHistory.shift();
    }

    // Warm-up: need period + lookback events
    if (this.#count < this.#period + this.#lookback) return null;

    const oldEma = this.#emaHistory[0];
    const currentEma = this.#emaHistory[this.#emaHistory.length - 1];

    return (currentEma - oldEma) / this.#lookback;
  }

  reset() {
    this.#prevEma = null;
    this.#emaHistory = [];
    this.#count = 0;
  }
}
