/**
 * AutoBacktester - Run automated backtest on assembled strategy
 *
 * Evaluates strategy performance on historical data.
 */

import { ReplayEngine } from '../../replay/ReplayEngine.js';
import { ExecutionEngine } from '../../execution/engine.js';
import { runReplayWithStrategy } from '../../strategy/Runner.js';
import { FACTORY_CONFIG } from './config.js';

export class AutoBacktester {
  /**
   * @param {Object} config
   * @param {number} config.minTrades - Min trades in backtest (default: 10)
   * @param {number} config.minSharpe - Min Sharpe for passing (default: 0.5)
   * @param {number} config.maxDrawdownPct - Max drawdown percent (default: 5)
   */
  constructor(config = {}) {
    this.minTrades = config.minTrades || FACTORY_CONFIG.backtest.minTrades;
    this.minSharpe = config.minSharpe || FACTORY_CONFIG.backtest.minSharpe;
    this.maxDrawdownPct = config.maxDrawdownPct || FACTORY_CONFIG.backtest.maxDrawdownPct;
  }

  /**
   * Run backtest for a factory-generated strategy
   * @param {BaseTemplate} strategy
   * @param {Object} dataConfig
   * @param {string} dataConfig.parquetPath
   * @param {string} dataConfig.metaPath
   * @param {string} dataConfig.symbol
   * @returns {Promise<BacktestResult>}
   *
   * BacktestResult = {
   *   strategyId: string,
   *   trades: number,
   *   pnl: number,
   *   returnPct: number,
   *   sharpe: number,
   *   maxDrawdownPct: number,
   *   winRate: number,
   *   passed: boolean,
   *   executionSnapshot: Object,
   *   metadata: { events, duration }
   * }
   */
  async run(strategy, dataConfig) {
    console.log(`[AutoBacktester] Running backtest for ${strategy.getStrategyId()}`);

    const startTime = Date.now();

    // Initialize replay engine
    const replayEngine = new ReplayEngine({
      parquet: dataConfig.parquetPath,
      meta: dataConfig.metaPath
    });

    // Initialize execution engine
    const executionEngine = new ExecutionEngine({
      initialCapital: 10000,
      symbol: dataConfig.symbol
    });

    // Run strategy
    const ctx = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        executionEngine,
        parquetPath: dataConfig.parquetPath,
        metaPath: dataConfig.metaPath,
        symbol: dataConfig.symbol
      }
    });

    // Get execution state
    const state = executionEngine.getState();

    // Calculate metrics
    const trades = state.fills.length / 2; // Assuming pairs of fills (entry/exit)
    const pnl = state.realizedPnL;
    const returnPct = (pnl / 10000) * 100;

    // Calculate Sharpe (simplified)
    const sharpe = this._calculateSharpe(state.fills);

    // Calculate max drawdown
    const maxDrawdownPct = Math.abs(state.maxDrawdown) * 100;

    // Calculate win rate
    const winRate = this._calculateWinRate(state.fills);

    // Check pass conditions
    const passedTrades = trades >= this.minTrades;
    const passedSharpe = sharpe >= this.minSharpe;
    const passedDrawdown = maxDrawdownPct <= this.maxDrawdownPct;
    const passed = passedTrades && passedSharpe && passedDrawdown;

    const duration = Date.now() - startTime;

    console.log(`[AutoBacktester] Trades: ${trades}, Return: ${returnPct.toFixed(2)}%, Sharpe: ${sharpe.toFixed(2)}, MaxDD: ${maxDrawdownPct.toFixed(2)}%`);
    console.log(`[AutoBacktester] Passed: ${passed} (${duration}ms)`);

    return {
      strategyId: strategy.getStrategyId(),
      trades,
      pnl,
      returnPct,
      sharpe,
      maxDrawdownPct,
      winRate,
      passed,
      executionSnapshot: {
        realizedPnL: state.realizedPnL,
        unrealizedPnL: state.unrealizedPnL,
        totalPnL: state.totalPnL,
        position: state.position,
        fills: state.fills.length
      },
      metadata: {
        events: ctx.stats.processed,
        duration
      }
    };
  }

  /**
   * Calculate Sharpe ratio from fills
   */
  _calculateSharpe(fills) {
    if (fills.length < 2) return 0;

    // Extract returns from fill pairs
    const returns = [];
    for (let i = 1; i < fills.length; i += 2) {
      if (fills[i] && fills[i - 1]) {
        const entryPrice = fills[i - 1].price;
        const exitPrice = fills[i].price;
        const ret = (exitPrice - entryPrice) / entryPrice;
        returns.push(ret);
      }
    }

    if (returns.length === 0) return 0;

    const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
    const std = Math.sqrt(variance);

    if (std === 0) return 0;

    // Annualize (assuming ~250 trading days)
    const annualizedMean = mean * 250;
    const annualizedStd = std * Math.sqrt(250);

    return annualizedMean / annualizedStd;
  }

  /**
   * Calculate win rate from fills
   */
  _calculateWinRate(fills) {
    if (fills.length < 2) return 0;

    let wins = 0;
    let totalTrades = 0;

    for (let i = 1; i < fills.length; i += 2) {
      if (fills[i] && fills[i - 1]) {
        const entryPrice = fills[i - 1].price;
        const exitPrice = fills[i].price;
        const pnl = exitPrice - entryPrice;

        if (pnl > 0) wins++;
        totalTrades++;
      }
    }

    return totalTrades === 0 ? 0 : wins / totalTrades;
  }
}
