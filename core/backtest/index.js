/**
 * QuantLab Backtest Module
 * 
 * BACKTEST v1 — READ-ONLY, DETERMINISTIC
 * 
 * This module provides:
 * - BacktestResult builder
 * - Equity curve utilities
 * - Performance metrics
 * - Summary builder
 */

// Result builder
export { buildBacktestResult } from './result.js';

// Equity utilities
export { validateEquityCurve, computeReturns } from './equity.js';

// Metrics
export { 
  totalReturn, 
  maxDrawdown, 
  winRate, 
  avgTradePnl, 
  tradesCount,
  computeAllMetrics 
} from './metrics.js';

// Summary builder
export { buildBacktestSummary, printBacktestSummary } from './summary.js';
