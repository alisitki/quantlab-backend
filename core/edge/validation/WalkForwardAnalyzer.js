/**
 * WalkForwardAnalyzer - Rolling window validation
 *
 * Tests edge stability over time using sliding windows.
 */

import { VALIDATION_CONFIG } from './config.js';

export class WalkForwardAnalyzer {
  /**
   * @param {Object} config
   * @param {number} config.windowSize - Rolling window size in rows (default: 5000)
   * @param {number} config.stepSize - Step between windows (default: 1000)
   * @param {number} config.minWindowSharpe - Min Sharpe per window (default: 0)
   * @param {number} config.minPositiveWindows - Fraction of windows with positive Sharpe (default: 0.6)
   */
  constructor(config = {}) {
    this.windowSize = config.windowSize || VALIDATION_CONFIG.walkForward.windowSize;
    this.stepSize = config.stepSize || VALIDATION_CONFIG.walkForward.stepSize;
    this.minWindowSharpe = config.minWindowSharpe || VALIDATION_CONFIG.walkForward.minWindowSharpe;
    this.minPositiveWindows = config.minPositiveWindows || VALIDATION_CONFIG.walkForward.minPositiveWindows;
  }

  /**
   * Run walk-forward analysis
   * @param {Edge} edge
   * @param {DiscoveryDataset} dataset
   * @returns {WalkForwardResult}
   *
   * WalkForwardResult = {
   *   windows: Array<{startIdx, endIdx, sharpe, winRate, trades, meanReturn}>,
   *   positiveWindowFraction: number,
   *   sharpeTrend: number,         // Slope of Sharpe over windows (negative = decay)
   *   consistency: number,          // Std of window Sharpes (lower = more consistent)
   *   passed: boolean,
   *   overallSharpe: number
   * }
   */
  analyze(edge, dataset) {
    console.log(`[WalkForwardAnalyzer] Analyzing edge ${edge.id} with ${this.windowSize} window, ${this.stepSize} step`);

    const windows = [];
    const totalRows = dataset.rows.length;

    // Slide windows
    for (let startIdx = 0; startIdx + this.windowSize <= totalRows; startIdx += this.stepSize) {
      const endIdx = startIdx + this.windowSize;
      const windowRows = dataset.rows.slice(startIdx, endIdx);

      const windowResult = this.#evaluateWindow(edge, windowRows);

      windows.push({
        startIdx,
        endIdx,
        ...windowResult
      });
    }

    if (windows.length === 0) {
      return {
        windows: [],
        positiveWindowFraction: 0,
        sharpeTrend: 0,
        consistency: 0,
        passed: false,
        overallSharpe: 0
      };
    }

    // Calculate statistics
    const sharpes = windows.map(w => w.sharpe);
    const positiveWindows = sharpes.filter(s => s > 0).length;
    const positiveWindowFraction = positiveWindows / windows.length;

    // Calculate Sharpe trend (linear regression)
    const sharpeTrend = this.#calculateTrend(sharpes);

    // Calculate consistency (std of Sharpes)
    const meanSharpe = sharpes.reduce((a, b) => a + b, 0) / sharpes.length;
    const variance = sharpes.reduce((sum, s) => sum + Math.pow(s - meanSharpe, 2), 0) / sharpes.length;
    const consistency = Math.sqrt(variance);

    // Overall Sharpe (all trades combined)
    const allReturns = [];
    for (const window of windows) {
      // Note: we can't directly get returns from window result
      // For simplicity, use mean Sharpe as proxy
    }
    const overallSharpe = meanSharpe;

    // Check pass conditions
    const passed = positiveWindowFraction >= this.minPositiveWindows &&
                   sharpeTrend >= -0.5; // Allow slight negative trend

    console.log(`[WalkForwardAnalyzer] ${windows.length} windows, ${(positiveWindowFraction * 100).toFixed(1)}% positive`);
    console.log(`[WalkForwardAnalyzer] Sharpe trend: ${sharpeTrend.toFixed(4)}, Consistency (std): ${consistency.toFixed(3)}`);
    console.log(`[WalkForwardAnalyzer] Passed: ${passed}`);

    return {
      windows,
      positiveWindowFraction,
      sharpeTrend,
      consistency,
      passed,
      overallSharpe
    };
  }

  /**
   * Evaluate edge on a single window
   */
  #evaluateWindow(edge, rows) {
    const returns = [];
    let wins = 0;
    let trades = 0;

    for (const row of rows) {
      const entryEval = edge.evaluateEntry(row.features, row.regime);
      if (!entryEval.active) continue;

      const horizonKey = `h${Math.round(edge.timeHorizon / 1000)}`;
      let forwardReturn = row.forwardReturns[horizonKey];

      if (forwardReturn === null || forwardReturn === undefined) {
        const availableHorizons = Object.keys(row.forwardReturns)
          .filter(k => row.forwardReturns[k] !== null);
        if (availableHorizons.length > 0) {
          forwardReturn = row.forwardReturns[availableHorizons[0]];
        } else {
          continue;
        }
      }

      const adjustedReturn = entryEval.direction === 'LONG' ? forwardReturn : -forwardReturn;
      returns.push(adjustedReturn);
      trades++;

      if (adjustedReturn > 0) wins++;
    }

    if (returns.length === 0) {
      return { sharpe: 0, winRate: 0, trades: 0, meanReturn: 0 };
    }

    const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - meanReturn, 2), 0) / returns.length;
    const std = Math.sqrt(variance);

    const eventsPerYear = 7200000;
    const annualizedMean = meanReturn * eventsPerYear;
    const annualizedStd = std * Math.sqrt(eventsPerYear);
    const sharpe = annualizedStd === 0 ? 0 : annualizedMean / annualizedStd;

    const winRate = wins / trades;

    return { sharpe, winRate, trades, meanReturn };
  }

  /**
   * Calculate linear trend (slope) of values
   */
  #calculateTrend(values) {
    const n = values.length;
    if (n < 2) return 0;

    const xMean = (n - 1) / 2;
    const yMean = values.reduce((a, b) => a + b, 0) / n;

    let numerator = 0;
    let denominator = 0;

    for (let i = 0; i < n; i++) {
      numerator += (i - xMean) * (values[i] - yMean);
      denominator += Math.pow(i - xMean, 2);
    }

    return denominator === 0 ? 0 : numerator / denominator;
  }
}
