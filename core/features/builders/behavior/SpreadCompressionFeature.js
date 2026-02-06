/**
 * SpreadCompressionFeature: Detect spread compression (narrowing) and expansion
 *
 * Spread compression often precedes breakout moves.
 * Compression = spread narrowing relative to recent average
 * Expansion = spread widening
 *
 * Hypothesis: High compression predicts breakout volatility.
 *
 * Algorithm:
 * 1. Track dual EMA of spread (fast vs slow)
 * 2. compression = (slow_ema - fast_ema) / slow_ema
 * 3. Positive = compressing (narrowing), negative = expanding (widening)
 *
 * Range: [-1, +1] (capped)
 *   +1 = maximum compression (spread narrowing)
 *   -1 = maximum expansion (spread widening)
 *    0 = stable spread
 */
export class SpreadCompressionFeature {
  #fastPeriod;
  #slowPeriod;
  #fastAlpha;
  #slowAlpha;
  #fastEma = null;
  #slowEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#fastPeriod = config.fastPeriod || 10;
    this.#slowPeriod = config.slowPeriod || 50;
    this.#fastAlpha = 2 / (this.#fastPeriod + 1);
    this.#slowAlpha = 2 / (this.#slowPeriod + 1);
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    const spread = ask - bid;

    if (spread <= 0) return null; // Invalid spread

    this.#count++;

    // Initialize EMAs
    if (this.#fastEma === null) {
      this.#fastEma = spread;
      this.#slowEma = spread;
      return null; // Warmup
    }

    // Update EMAs
    this.#fastEma = this.#fastAlpha * spread + (1 - this.#fastAlpha) * this.#fastEma;
    this.#slowEma = this.#slowAlpha * spread + (1 - this.#slowAlpha) * this.#slowEma;

    // Warmup: need slow period
    if (this.#count < this.#slowPeriod) return null;

    // Calculate compression ratio
    if (this.#slowEma === 0) return 0;

    const compression = (this.#slowEma - this.#fastEma) / this.#slowEma;

    // Cap to [-1, +1]
    return Math.max(-1, Math.min(1, compression));
  }

  reset() {
    this.#fastEma = null;
    this.#slowEma = null;
    this.#count = 0;
  }
}
