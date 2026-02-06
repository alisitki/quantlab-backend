/**
 * ImbalanceAccelerationFeature: Rate of change of bid/ask imbalance
 *
 * Measures how quickly liquidity imbalance is shifting.
 * Rising acceleration = increasing directional pressure
 *
 * Hypothesis: High acceleration predicts short-term directional moves.
 *
 * Algorithm:
 * 1. Calculate raw imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty)
 * 2. Smooth with fast EMA
 * 3. Acceleration = change in EMA
 * 4. Smooth acceleration with slower EMA
 *
 * Range: [-1, +1] (normalized via tanh)
 *   +1 = rapid increase in buy pressure
 *   -1 = rapid increase in sell pressure
 *    0 = stable imbalance
 */
export class ImbalanceAccelerationFeature {
  #period;
  #alpha;
  #smoothPeriod;
  #smoothAlpha;
  #imbalanceEma = null;
  #prevImbalanceEma = null;
  #accelerationEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#period = config.period || 10;
    this.#smoothPeriod = config.smoothPeriod || 5;
    this.#alpha = 2 / (this.#period + 1);
    this.#smoothAlpha = 2 / (this.#smoothPeriod + 1);
  }

  onEvent(event) {
    const bidQty = Number(event.bid_qty ?? event.bid_size ?? 0);
    const askQty = Number(event.ask_qty ?? event.ask_size ?? 0);

    const total = bidQty + askQty;
    if (total === 0) return null;

    // Raw imbalance: [-1, +1]
    const imbalance = (bidQty - askQty) / total;
    this.#count++;

    // Initialize
    if (this.#imbalanceEma === null) {
      this.#imbalanceEma = imbalance;
      return null; // Warmup
    }

    // Update imbalance EMA
    this.#prevImbalanceEma = this.#imbalanceEma;
    this.#imbalanceEma = this.#alpha * imbalance + (1 - this.#alpha) * this.#imbalanceEma;

    // Need at least 2 values for acceleration
    if (this.#prevImbalanceEma === null) return null;

    // Calculate acceleration (rate of change)
    const acceleration = this.#imbalanceEma - this.#prevImbalanceEma;

    // Initialize acceleration EMA
    if (this.#accelerationEma === null) {
      this.#accelerationEma = acceleration;
      return null; // Warmup
    }

    // Smooth acceleration
    this.#accelerationEma = this.#smoothAlpha * acceleration + (1 - this.#smoothAlpha) * this.#accelerationEma;

    // Warmup: need full period
    if (this.#count < this.#period + this.#smoothPeriod) return null;

    // Normalize via tanh (maps to [-1, +1])
    // Scale factor: typical acceleration is small, multiply for sensitivity
    return Math.tanh(this.#accelerationEma * 100);
  }

  reset() {
    this.#imbalanceEma = null;
    this.#prevImbalanceEma = null;
    this.#accelerationEma = null;
    this.#count = 0;
  }
}
