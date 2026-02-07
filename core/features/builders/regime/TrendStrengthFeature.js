/**
 * TrendStrengthFeature: Continuous trend regime metric
 *
 * Unlike TrendRegimeFeature (categorical: -1/0/1), this outputs the continuous
 * strength and direction of the trend.
 *
 * Algorithm:
 * 1. Calculate dual EMA (fast vs slow)
 * 2. Calculate separation: (fast_ema - slow_ema) / slow_ema
 * 3. Also check slope of fast EMA
 * 4. Combine into single trend strength metric
 *
 * Range: [-1, +1]
 *   -1 = strong downtrend
 *    0 = sideways/neutral
 *   +1 = strong uptrend
 *
 * This continuous output enables clustering and finer trend distinctions.
 */
export class TrendStrengthFeature {
  #fastPeriod;
  #slowPeriod;
  #slopePeriod;
  #fastAlpha;
  #slowAlpha;
  #fastEma = null;
  #slowEma = null;
  #emaHistory = [];
  #count = 0;

  constructor(config = {}) {
    this.#fastPeriod = config.fastPeriod || 10;
    this.#slowPeriod = config.slowPeriod || 30;
    this.#slopePeriod = config.slopePeriod || 5;
    this.#fastAlpha = 2 / (this.#fastPeriod + 1);
    this.#slowAlpha = 2 / (this.#slowPeriod + 1);
  }

  onEvent(event) {
    const mid = (Number(event.bid_price) + Number(event.ask_price)) / 2;
    this.#count++;

    // Initialize EMAs
    if (this.#fastEma === null) {
      this.#fastEma = mid;
      this.#slowEma = mid;
      return null; // Warmup
    }

    // Update EMAs
    this.#fastEma = this.#fastAlpha * mid + (1 - this.#fastAlpha) * this.#fastEma;
    this.#slowEma = this.#slowAlpha * mid + (1 - this.#slowAlpha) * this.#slowEma;

    // Track fast EMA history for slope calculation
    this.#emaHistory.push(this.#fastEma);
    if (this.#emaHistory.length > this.#slopePeriod) {
      this.#emaHistory.shift();
    }

    // Warmup: need slow period + slope period
    if (this.#count < this.#slowPeriod + this.#slopePeriod) return null;

    // Component 1: EMA separation (normalized)
    const separation = (this.#fastEma - this.#slowEma) / this.#slowEma;

    // Component 2: EMA slope (rate of change)
    const oldEma = this.#emaHistory[0];
    const slope = (this.#fastEma - oldEma) / oldEma;

    // Combine: separation gives direction, slope gives momentum
    // Weight separation more heavily (70/30)
    const trendStrength = 0.7 * separation + 0.3 * slope;

    // Normalize to [-1, +1] via tanh
    // Scale factor: typical values are small, multiply for sensitivity
    const normalized = Math.tanh(trendStrength * 100);
    return isNaN(normalized) ? 0 : normalized; // 0 = no trend
  }

  reset() {
    this.#fastEma = null;
    this.#slowEma = null;
    this.#emaHistory = [];
    this.#count = 0;
  }
}
