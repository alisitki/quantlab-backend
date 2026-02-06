/**
 * LiquidityPressureFeature: Detect buying/selling pressure from L1 order book imbalance
 *
 * Measures the imbalance between bid and ask quantities at best price level.
 * High bid quantity relative to ask = buying pressure (bullish)
 * High ask quantity relative to bid = selling pressure (bearish)
 *
 * Hypothesis: High liquidity pressure predicts short-term price movement in that direction.
 *
 * Formula: pressure = (bid_qty - ask_qty) / (bid_qty + ask_qty)
 * Range: [-1, +1]
 *   +1 = all liquidity on bid (strong buying pressure)
 *   -1 = all liquidity on ask (strong selling pressure)
 *    0 = balanced
 *
 * Smoothed with EMA to filter spikes.
 */
export class LiquidityPressureFeature {
  #period;
  #alpha;
  #prevEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#period = config.period || 10;
    this.#alpha = 2 / (this.#period + 1);
  }

  onEvent(event) {
    const bidQty = Number(event.bid_qty ?? event.bid_size ?? 0);
    const askQty = Number(event.ask_qty ?? event.ask_size ?? 0);

    const total = bidQty + askQty;
    if (total === 0) return null;

    // Raw pressure: [-1, +1]
    const pressure = (bidQty - askQty) / total;
    this.#count++;

    // Initialize EMA
    if (this.#prevEma === null) {
      this.#prevEma = pressure;
      return null; // Warmup
    }

    // EMA smoothing
    const ema = this.#alpha * pressure + (1 - this.#alpha) * this.#prevEma;
    this.#prevEma = ema;

    // Warmup: need period events
    if (this.#count < this.#period) return null;

    return ema;
  }

  reset() {
    this.#prevEma = null;
    this.#count = 0;
  }
}
