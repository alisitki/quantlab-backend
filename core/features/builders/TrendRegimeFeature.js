/**
 * TrendRegimeFeature: Detects trend regime using dual EMA
 *
 * Compares fast EMA vs slow EMA and checks slope.
 * Output: -1=DOWNTREND, 0=SIDEWAYS, 1=UPTREND
 */
export class TrendRegimeFeature {
  #fastPeriod;
  #slowPeriod;
  #slopeThreshold;
  #fastAlpha;
  #slowAlpha;
  #fastEma = null;
  #slowEma = null;
  #prevFastEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#fastPeriod = config.fastPeriod || 10;
    this.#slowPeriod = config.slowPeriod || 30;
    this.#slopeThreshold = config.slopeThreshold || 0.0001;
    this.#fastAlpha = 2 / (this.#fastPeriod + 1);
    this.#slowAlpha = 2 / (this.#slowPeriod + 1);
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;
    this.#count++;

    // Initialize EMAs
    if (this.#fastEma === null) {
      this.#fastEma = mid;
      this.#slowEma = mid;
      return null;
    }

    // Store previous fast EMA for slope calculation
    this.#prevFastEma = this.#fastEma;

    // Update EMAs
    this.#fastEma = this.#fastAlpha * mid + (1 - this.#fastAlpha) * this.#fastEma;
    this.#slowEma = this.#slowAlpha * mid + (1 - this.#slowAlpha) * this.#slowEma;

    // Warm-up: need slowPeriod events
    if (this.#count < this.#slowPeriod) return null;

    // Calculate slope (change per tick)
    const slope = this.#fastEma - this.#prevFastEma;

    // Determine regime
    const emaAbove = this.#fastEma > this.#slowEma;
    const emaBelow = this.#fastEma < this.#slowEma;

    if (emaAbove && slope > this.#slopeThreshold) return 1;   // UPTREND
    if (emaBelow && slope < -this.#slopeThreshold) return -1; // DOWNTREND
    return 0;                                                  // SIDEWAYS
  }

  reset() {
    this.#fastEma = null;
    this.#slowEma = null;
    this.#prevFastEma = null;
    this.#count = 0;
  }
}
