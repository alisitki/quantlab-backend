/**
 * QuantLab Backtest Result — Equity Utilities
 * Pure functions for equity curve validation and return computation.
 * 
 * BACKTEST v1 — READ-ONLY, DETERMINISTIC
 */

/**
 * @typedef {Object} ValidationResult
 * @property {boolean} valid - Whether the curve is valid
 * @property {string|null} error - Error message if invalid
 */

/**
 * Validate equity curve
 * Checks:
 * - Non-empty
 * - Monotonic ts_event (non-decreasing)
 * - Finite equity values
 * 
 * @param {Array<{ts_event: bigint, equity: number}>} curve - Equity curve
 * @returns {ValidationResult}
 */
export function validateEquityCurve(curve) {
  // Check empty
  if (!curve || curve.length === 0) {
    return { valid: false, error: 'Equity curve is empty' };
  }

  let lastTs = null;

  for (let i = 0; i < curve.length; i++) {
    const point = curve[i];

    // Check ts_event exists
    if (point.ts_event === undefined || point.ts_event === null) {
      return { valid: false, error: `Point ${i}: missing ts_event` };
    }

    // Check monotonic ts_event
    if (lastTs !== null && point.ts_event < lastTs) {
      return { 
        valid: false, 
        error: `Point ${i}: ts_event ${point.ts_event} is before previous ${lastTs}` 
      };
    }
    lastTs = point.ts_event;

    // Check finite equity
    if (typeof point.equity !== 'number' || !Number.isFinite(point.equity)) {
      return { valid: false, error: `Point ${i}: equity is not finite` };
    }
  }

  return { valid: true, error: null };
}

/**
 * Compute simple returns from equity curve
 * Simple return = (equity[i] - equity[i-1]) / equity[i-1]
 * 
 * @param {Array<{ts_event: bigint, equity: number}>} curve - Equity curve
 * @returns {Array<{ts_event: bigint, return: number}>} - Returns array
 */
export function computeReturns(curve) {
  if (!curve || curve.length < 2) {
    return [];
  }

  const returns = [];

  for (let i = 1; i < curve.length; i++) {
    const prevEquity = curve[i - 1].equity;
    const currEquity = curve[i].equity;

    // Avoid division by zero
    const simpleReturn = prevEquity !== 0 
      ? (currEquity - prevEquity) / prevEquity 
      : 0;

    returns.push({
      ts_event: curve[i].ts_event,
      return: simpleReturn
    });
  }

  return returns;
}
