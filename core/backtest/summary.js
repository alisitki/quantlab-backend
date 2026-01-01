/**
 * QuantLab Backtest Result — Summary Builder
 * Main entry point for building backtest summary from execution state.
 * 
 * BACKTEST v1 — DETERMINISTIC SUMMARY OUTPUT
 */

import { buildBacktestResult } from './result.js';
import { validateEquityCurve } from './equity.js';
import { totalReturn, maxDrawdown, winRate, avgTradePnl, tradesCount } from './metrics.js';

/**
 * @typedef {Object} BacktestSummary
 * @property {number} equity_start - Starting equity
 * @property {number} equity_end - Ending equity
 * @property {number} total_pnl - Total PnL
 * @property {number} return_pct - Return percentage
 * @property {number} max_drawdown_pct - Max drawdown percentage
 * @property {number} trades - Number of trades
 * @property {number} win_rate - Win rate (0-1)
 * @property {number} avg_trade_pnl - Average PnL per trade
 */

/**
 * Build complete backtest summary from execution state snapshot
 * 
 * @param {import('../execution/state.js').ExecutionStateSnapshot} executionState - Execution state snapshot
 * @param {Object} [options={}] - Options
 * @param {number} [options.initialCapital=10000] - Initial capital
 * @returns {BacktestSummary}
 */
export function buildBacktestSummary(executionState, options = {}) {
  const { initialCapital = 10000 } = options;

  if (!executionState) {
    throw new Error('BACKTEST_ERROR: executionState is required');
  }

  // Build backtest result
  const result = buildBacktestResult(executionState, initialCapital);
  const fills = executionState.fills || [];

  // Validate equity curve (log warning but don't fail)
  const validation = validateEquityCurve(result.equityCurve);
  if (!validation.valid && result.equityCurve.length > 0) {
    console.warn('[BACKTEST] Equity curve validation warning:', validation.error);
  }

  // Compute metrics
  const returnPct = totalReturn(result) * 100;
  const maxDdPct = maxDrawdown(result.equityCurve) * 100;
  const winRateVal = winRate(fills);
  const avgPnl = avgTradePnl(fills);
  const trades = tradesCount(fills);

  // Build summary with consistent precision
  const summary = {
    equity_start: roundTo(result.startEquity, 2),
    equity_end: roundTo(result.endEquity, 2),
    total_pnl: roundTo(result.totalPnl, 2),
    return_pct: roundTo(returnPct, 2),
    max_drawdown_pct: roundTo(maxDdPct, 2),
    trades,
    win_rate: roundTo(winRateVal, 4),
    avg_trade_pnl: roundTo(avgPnl, 4)
  };

  return summary;
}

/**
 * Round to specified decimal places (deterministic)
 * @param {number} value 
 * @param {number} decimals 
 * @returns {number}
 */
function roundTo(value, decimals) {
  const factor = Math.pow(10, decimals);
  return Math.round(value * factor) / factor;
}

/**
 * Print summary to console in formatted way
 * @param {BacktestSummary} summary 
 */
export function printBacktestSummary(summary) {
  console.log('\n=== BACKTEST SUMMARY ===');
  console.log(`Equity:     ${summary.equity_start.toFixed(2)} → ${summary.equity_end.toFixed(2)}`);
  console.log(`PnL:        ${summary.total_pnl >= 0 ? '+' : ''}${summary.total_pnl.toFixed(2)}`);
  console.log(`Return:     ${summary.return_pct >= 0 ? '+' : ''}${summary.return_pct.toFixed(2)}%`);
  console.log(`Max DD:     ${summary.max_drawdown_pct.toFixed(2)}%`);
  console.log(`Trades:     ${summary.trades}`);
  console.log(`Win Rate:   ${(summary.win_rate * 100).toFixed(1)}%`);
  console.log(`Avg Trade:  ${summary.avg_trade_pnl >= 0 ? '+' : ''}${summary.avg_trade_pnl.toFixed(4)}`);
  console.log('========================\n');
}
