/**
 * QuantLab Baseline Strategy v1 — Feature Layer
 * 
 * Minimal feature computation from BBO events.
 * Features are computed inline (no FeatureBuilder abstraction).
 * 
 * Features:
 * - mid_price = (bid + ask) / 2
 * - spread = ask - bid
 * - return_1 = (mid - prev_mid) / prev_mid
 */

/**
 * @typedef {Object} FeatureState
 * @property {number|null} prev_mid - Previous mid price for return calculation
 * @property {boolean} initialized - Whether we have seen at least one event
 */

/**
 * @typedef {Object} Features
 * @property {number} mid_price - Current mid price
 * @property {number} spread - Current bid-ask spread
 * @property {number} return_1 - One-tick return (0 if first event)
 * @property {boolean} valid - Whether features are valid for trading (not first tick)
 */

/**
 * Create initial feature state
 * @returns {FeatureState}
 */
export function createFeatureState() {
  return {
    prev_mid: null,
    initialized: false
  };
}

/**
 * Compute features from BBO event
 * Updates state in-place for efficiency
 * 
 * @param {Object} event - BBO event with bid_price and ask_price
 * @param {FeatureState} state - Mutable feature state
 * @returns {Features}
 */
export function computeFeatures(event, state) {
  const bid = Number(event.bid_price);
  const ask = Number(event.ask_price);
  
  const mid_price = (bid + ask) / 2;
  const spread = ask - bid;
  
  // Calculate return_1 (0 if first event)
  let return_1 = 0;
  let valid = false;
  
  if (state.prev_mid !== null && state.prev_mid > 0) {
    return_1 = (mid_price - state.prev_mid) / state.prev_mid;
    valid = true;
  }
  
  // Update state for next tick
  state.prev_mid = mid_price;
  state.initialized = true;
  
  return {
    mid_price,
    spread,
    return_1,
    valid
  };
}
