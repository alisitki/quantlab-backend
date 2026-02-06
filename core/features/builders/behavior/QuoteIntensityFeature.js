/**
 * QuoteIntensityFeature: Measure rate of quote updates
 *
 * High quote intensity = high market activity (often precedes moves)
 * Low quote intensity = quiet market
 *
 * Hypothesis: Sudden increase in quote intensity precedes directional moves.
 *
 * Algorithm:
 * 1. Track inter-event time deltas using ts_event
 * 2. Calculate events-per-second over rolling window
 * 3. Normalize relative to long-term average
 * 4. Use percentile normalization to [0, 1]
 *
 * Range: [0, 1]
 *   0 = very low activity (below average)
 *   0.5 = average activity
 *   1 = very high activity (above average)
 */
export class QuoteIntensityFeature {
  #window;
  #longWindow;
  #timestamps = [];
  #intensityHistory = [];

  constructor(config = {}) {
    this.#window = config.window || 20; // Short-term window
    this.#longWindow = config.longWindow || 200; // Long-term window for normalization
  }

  onEvent(event) {
    const ts = Number(event.ts_event);
    if (!ts || ts <= 0) return null;

    this.#timestamps.push(ts);

    // Trim to long window
    if (this.#timestamps.length > this.#longWindow) {
      this.#timestamps.shift();
    }

    // Warmup: need at least window size
    if (this.#timestamps.length < this.#window) return null;

    // Calculate events per second over short window
    const recent = this.#timestamps.slice(-this.#window);
    const timeSpanMs = recent[recent.length - 1] - recent[0];

    if (timeSpanMs <= 0) return null; // No time passed

    const eventsPerSecond = (recent.length - 1) / (timeSpanMs / 1000);

    // Track intensity history for normalization
    this.#intensityHistory.push(eventsPerSecond);
    if (this.#intensityHistory.length > this.#longWindow) {
      this.#intensityHistory.shift();
    }

    // Need long window for normalization
    if (this.#intensityHistory.length < this.#longWindow) return null;

    // Calculate percentile rank (normalized [0, 1])
    const sortedHistory = [...this.#intensityHistory].sort((a, b) => a - b);
    const rank = sortedHistory.filter(v => v <= eventsPerSecond).length;
    const percentile = rank / sortedHistory.length;

    return percentile;
  }

  reset() {
    this.#timestamps = [];
    this.#intensityHistory = [];
  }
}
