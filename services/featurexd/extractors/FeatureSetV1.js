/**
 * FeatureSetV1 — deterministic microstructure features.
 */

export const FEATURE_SET_ID = 'microstructure';
export const FEATURE_SET_VERSION = 'v1';
export const FEATURE_COLUMNS = [
  'f_mid_price',
  'f_spread',
  'f_log_return_1s',
  'f_log_return_5s',
  'f_book_imbalance',
  'f_trade_intensity'
];

const RETURN_1S_MS = 1000;
const RETURN_5S_MS = 5000;
const INTENSITY_WINDOW_MS = 1000;
const ROUND_SCALE = 1e8;

export class FeatureSetV1 {
  #midHistory = [];
  #eventTimes = [];

  onEvent(event) {
    const bid = Number(event.bid_price ?? event.bid ?? NaN);
    const ask = Number(event.ask_price ?? event.ask ?? NaN);
    const bidSize = Number(event.bid_size ?? event.bid_qty ?? 0);
    const askSize = Number(event.ask_size ?? event.ask_qty ?? 0);

    if (!Number.isFinite(bid) || !Number.isFinite(ask)) {
      return null;
    }

    const tsEvent = this.#toNumber(event.ts_event);
    if (tsEvent === null) return null;

    const mid = (bid + ask) / 2;
    const spread = ask - bid;
    const logReturn1s = this.#logReturn(tsEvent, mid, RETURN_1S_MS);
    const logReturn5s = this.#logReturn(tsEvent, mid, RETURN_5S_MS);
    const imbalance = this.#bookImbalance(bidSize, askSize);
    const intensity = this.#tradeIntensity(tsEvent);

    this.#midHistory.push({ ts: tsEvent, mid });
    this.#eventTimes.push(tsEvent);

    return {
      f_mid_price: round(mid),
      f_spread: round(spread),
      f_log_return_1s: round(logReturn1s),
      f_log_return_5s: round(logReturn5s),
      f_book_imbalance: round(imbalance),
      f_trade_intensity: round(intensity)
    };
  }

  #logReturn(tsEvent, mid, horizonMs) {
    const target = tsEvent - horizonMs;
    let candidate = null;

    while (this.#midHistory.length > 0 && this.#midHistory[0].ts <= target) {
      candidate = this.#midHistory.shift();
    }

    if (!candidate || candidate.mid <= 0 || mid <= 0) return 0;
    return Math.log(mid / candidate.mid);
  }

  #bookImbalance(bidSize, askSize) {
    const denom = bidSize + askSize;
    if (!Number.isFinite(denom) || denom === 0) return 0;
    return (bidSize - askSize) / denom;
  }

  #tradeIntensity(tsEvent) {
    const cutoff = tsEvent - INTENSITY_WINDOW_MS;
    while (this.#eventTimes.length > 0 && this.#eventTimes[0] < cutoff) {
      this.#eventTimes.shift();
    }
    return this.#eventTimes.length;
  }

  #toNumber(value) {
    if (typeof value === 'bigint') return Number(value);
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  }
}

function round(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
}
