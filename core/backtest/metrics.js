/**
 * QuantLab Backtest Result — Metrics
 * Pure functions for computing backtest performance metrics.
 *
 * BACKTEST v1 — REQUIRED METRICS:
 * - totalReturn
 * - maxDrawdown
 * - winRate
 * - avgTradePnl
 * - tradesCount
 *
 * NO Sharpe ratio in v1 (avoids risk-free rate debates)
 */

import { FillsStream } from '../execution/FillsStream.js';

/**
 * Calculate total return percentage
 * @param {import('./result.js').BacktestResult} result - Backtest result
 * @returns {number} - Total return as decimal (0.05 = 5%)
 */
export function totalReturn(result) {
  if (!result || result.startEquity === 0) {
    return 0;
  }
  return (result.endEquity - result.startEquity) / result.startEquity;
}

/**
 * Calculate maximum drawdown from equity curve OR snapshot
 * Drawdown = (peak - trough) / peak
 *
 * Supports three calling patterns:
 * 1. Streaming mode: Pre-computed maxDD from snapshot (snapshot.maxDrawdown)
 * 2. Legacy snapshot: Extract equity curve from snapshot (snapshot.equityCurve)
 * 3. Direct array: Compute from equity curve array
 *
 * @param {Array<{ts_event: bigint, equity: number}>|Object} equityCurveOrSnapshot - Equity curve or snapshot
 * @returns {number} - Max drawdown as negative decimal (-0.10 = -10%)
 */
export function maxDrawdown(equityCurveOrSnapshot) {
  // Handle null/undefined
  if (!equityCurveOrSnapshot) {
    return 0;
  }

  // Streaming mode: Use pre-computed maxDrawdown from snapshot
  if (
    typeof equityCurveOrSnapshot === 'object' &&
    !Array.isArray(equityCurveOrSnapshot) &&
    'maxDrawdown' in equityCurveOrSnapshot &&
    typeof equityCurveOrSnapshot.maxDrawdown === 'number'
  ) {
    // Return as negative value (consistent with legacy calculation)
    return -equityCurveOrSnapshot.maxDrawdown;
  }

  // Legacy snapshot mode: Extract equityCurve from snapshot
  if (
    typeof equityCurveOrSnapshot === 'object' &&
    !Array.isArray(equityCurveOrSnapshot) &&
    'equityCurve' in equityCurveOrSnapshot &&
    Array.isArray(equityCurveOrSnapshot.equityCurve)
  ) {
    return maxDrawdown(equityCurveOrSnapshot.equityCurve);  // Recursive call with array
  }

  // Direct array mode: Compute from full equity curve
  const equityCurve = equityCurveOrSnapshot;

  if (!Array.isArray(equityCurve) || equityCurve.length === 0) {
    return 0;
  }

  let peak = equityCurve[0].equity;
  let maxDd = 0;

  for (const point of equityCurve) {
    const equity = point.equity;

    // Update peak
    if (equity > peak) {
      peak = equity;
    }

    // Calculate current drawdown (negative value)
    if (peak > 0) {
      const dd = (equity - peak) / peak;
      if (dd < maxDd) {
        maxDd = dd;
      }
    }
  }

  return maxDd;
}

/**
 * Calculate win rate from fills
 * Win rate = winning trades / total trades
 * 
 * A trade is "winning" if fillValue - fee > 0 for sells
 * or if the position was later closed with profit.
 * 
 * v1 SIMPLIFIED: We track by counting fills that resulted in realized profit
 * This requires grouping fills by position round-trips.
 * 
 * For v1, we use a simpler heuristic:
 * - Group fills into pairs (entry + exit)
 * - Compare exit fillValue - entry fillValue - fees
 * 
 * @param {import('../execution/fill.js').FillResult[]} fills - Array of fills
 * @returns {number} - Win rate as decimal (0.60 = 60%)
 */
export function winRate(fills) {
  if (!fills || fills.length === 0) {
    return 0;
  }

  // Group fills by symbol and compute round-trip PnL
  const roundTrips = computeRoundTrips(fills);
  
  if (roundTrips.length === 0) {
    return 0;
  }

  const wins = roundTrips.filter(rt => rt.pnl > 0).length;
  return wins / roundTrips.length;
}

/**
 * Compute round-trip trades from fills
 * A round-trip is a sequence of fills that opens and closes a position
 * 
 * @param {import('../execution/fill.js').FillResult[]} fills - Array of fills
 * @returns {Array<{symbol: string, pnl: number, entryFee: number, exitFee: number}>}
 */
function computeRoundTrips(fills) {
  const roundTrips = [];
  const openPositions = new Map(); // symbol -> { qty, costBasis, fees }

  for (const fill of fills) {
    const symbol = fill.symbol;
    const side = fill.side;
    const qty = fill.qty;
    const value = fill.fillValue;
    const fee = fill.fee;

    if (!openPositions.has(symbol)) {
      openPositions.set(symbol, { qty: 0, costBasis: 0, fees: 0 });
    }

    const pos = openPositions.get(symbol);

    if (side === 'BUY') {
      // Opening or adding to long
      pos.qty += qty;
      pos.costBasis += value;
      pos.fees += fee;
    } else {
      // SELL - closing position
      if (pos.qty > 0) {
        // Calculate PnL for this portion
        const avgEntry = pos.costBasis / pos.qty;
        const exitValue = qty * fill.fillPrice;
        const entryValue = qty * avgEntry;
        const pnl = exitValue - entryValue - fee - (pos.fees * (qty / pos.qty));

        roundTrips.push({
          symbol,
          pnl,
          entryFee: pos.fees * (qty / pos.qty),
          exitFee: fee
        });

        // Reduce position
        pos.costBasis -= entryValue;
        pos.fees -= pos.fees * (qty / pos.qty);
        pos.qty -= qty;
      } else {
        // Opening short (tracking separately)
        pos.qty -= qty;
        pos.costBasis -= value;
        pos.fees += fee;
      }
    }
  }

  return roundTrips;
}

/**
 * Calculate average trade PnL
 * @param {import('../execution/fill.js').FillResult[]} fills - Array of fills
 * @returns {number} - Average PnL per round-trip trade
 */
export function avgTradePnl(fills) {
  if (!fills || fills.length === 0) {
    return 0;
  }

  const roundTrips = computeRoundTrips(fills);
  
  if (roundTrips.length === 0) {
    return 0;
  }

  const totalPnl = roundTrips.reduce((sum, rt) => sum + rt.pnl, 0);
  return totalPnl / roundTrips.length;
}

/**
 * Get total trades count
 * @param {import('../execution/fill.js').FillResult[]} fills - Array of fills
 * @returns {number} - Number of fills
 */
export function tradesCount(fills) {
  if (!fills) {
    return 0;
  }
  return fills.length;
}

/**
 * Load fills from snapshot (streaming or in-memory)
 * @param {import('../execution/state.js').ExecutionStateSnapshot} snapshot - Execution state snapshot
 * @returns {import('../execution/fill.js').FillResult[]} - Array of fills
 */
function loadFills(snapshot) {
  // Streaming mode: load from disk
  if (snapshot.fillsStreamPath && (!snapshot.fills || snapshot.fills.length === 0)) {
    return FillsStream.readFills(snapshot.fillsStreamPath);
  }

  // In-memory mode: use fills array
  return snapshot.fills || [];
}

/**
 * Get all metrics as an object
 * @param {import('./result.js').BacktestResult|import('../execution/state.js').ExecutionStateSnapshot} result - Backtest result or snapshot
 * @param {import('../execution/fill.js').FillResult[]} [fills] - Array of fills (optional, will load from snapshot if not provided)
 * @returns {Object} - All metrics
 */
export function computeAllMetrics(result, fills) {
  // Load fills if not provided
  const fillsArray = fills || loadFills(result);

  return {
    totalReturn: totalReturn(result),
    maxDrawdown: maxDrawdown(result.equityCurve || result),  // Pass snapshot if no equityCurve
    winRate: winRate(fillsArray),
    avgTradePnl: avgTradePnl(fillsArray),
    tradesCount: tradesCount(fillsArray)
  };
}
