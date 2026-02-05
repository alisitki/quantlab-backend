/**
 * EMAFeature: Exponential Moving Average of mid price
 *
 * Formula: EMA(t) = alpha * mid(t) + (1 - alpha) * EMA(t-1)
 * where alpha = 2 / (period + 1)
 */
export class EMAFeature {
  #period;
  #alpha;
  #prevEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#period = config.period || 14;
    this.#alpha = 2 / (this.#period + 1);
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;
    this.#count++;

    if (this.#prevEma === null) {
      // Initialize EMA with first mid value
      this.#prevEma = mid;
      return null; // Not warm yet
    }

    const ema = this.#alpha * mid + (1 - this.#alpha) * this.#prevEma;
    this.#prevEma = ema;

    // Warm-up: need at least 'period' events
    if (this.#count < this.#period) return null;

    return ema;
  }

  reset() {
    this.#prevEma = null;
    this.#count = 0;
  }
}
