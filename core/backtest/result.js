/**
 * QuantLab Backtest Result — Result Builder
 * Normalizes ExecutionStateSnapshot into BacktestResult.
 * 
 * BACKTEST v1 — READ-ONLY, DETERMINISTIC
 * - No random
 * - No wall-clock
 * - Pure functions only
 */

/**
 * @typedef {Object} BacktestResult
 * @property {number} startEquity - Starting equity
 * @property {number} endEquity - Ending equity
 * @property {number} totalPnl - Total PnL (endEquity - startEquity)
 * @property {Array<{ts_event: bigint, equity: number}>} equityCurve - Equity curve points
 * @property {number} tradesCount - Number of trades (fills)
 * @property {bigint|null} startTs - First timestamp
 * @property {bigint|null} endTs - Last timestamp
 */

/**
 * Build BacktestResult from ExecutionStateSnapshot
 * 
 * @param {import('../execution/state.js').ExecutionStateSnapshot} snapshot - Execution state snapshot
 * @param {number} [initialCapital=10000] - Initial capital (used if equity curve is empty)
 * @returns {BacktestResult}
 */
export function buildBacktestResult(snapshot, initialCapital = 10000) {
  if (!snapshot) {
    throw new Error('BACKTEST_ERROR: snapshot is required');
  }

  const equityCurve = snapshot.equityCurve || [];
  const fills = snapshot.fills || [];

  // Start equity: first point in curve or initial capital
  const startEquity = equityCurve.length > 0 
    ? equityCurve[0].equity 
    : initialCapital;

  // End equity: last point in curve or current equity
  const endEquity = equityCurve.length > 0 
    ? equityCurve[equityCurve.length - 1].equity 
    : snapshot.equity;

  // Total PnL
  const totalPnl = endEquity - startEquity;

  // Timestamps
  const startTs = equityCurve.length > 0 ? equityCurve[0].ts_event : null;
  const endTs = equityCurve.length > 0 ? equityCurve[equityCurve.length - 1].ts_event : null;

  return {
    startEquity,
    endEquity,
    totalPnl,
    equityCurve: [...equityCurve], // Immutable copy
    tradesCount: fills.length,
    startTs,
    endTs
  };
}
