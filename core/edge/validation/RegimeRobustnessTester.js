/**
 * RegimeRobustnessTester - Test edge performance in target vs other regimes
 *
 * Validates that regime-specific edges actually work in their target regimes
 * and don't work (or work less well) in other regimes.
 */

import { VALIDATION_CONFIG } from './config.js';

export class RegimeRobustnessTester {
  /**
   * @param {Object} config
   * @param {number} config.minTradesPerRegime - Min trades per regime (default: 20)
   * @param {number} config.minRegimeSharpe - Min Sharpe in target regimes (default: 0.3)
   * @param {number} config.selectivityThreshold - Min difference target vs other (default: 0.2)
   */
  constructor(config = {}) {
    this.minTradesPerRegime = config.minTradesPerRegime || VALIDATION_CONFIG.regime.minTradesPerRegime;
    this.minRegimeSharpe = config.minRegimeSharpe || VALIDATION_CONFIG.regime.minRegimeSharpe;
    this.selectivityThreshold = config.selectivityThreshold || VALIDATION_CONFIG.regime.selectivityThreshold;
  }

  /**
   * Test regime robustness
   * @param {Edge} edge
   * @param {DiscoveryDataset} dataset
   * @returns {RegimeRobustnessResult}
   *
   * RegimeRobustnessResult = {
   *   perRegime: Object<regimeId, {sharpe, winRate, trades, meanReturn}>,
   *   targetRegimePerformance: number,  // Avg Sharpe in target regimes
   *   otherRegimePerformance: number,   // Avg Sharpe outside target regimes
   *   regimeSelectivity: number,         // target - other (higher = more selective)
   *   passed: boolean
   * }
   */
  test(edge, dataset) {
    console.log(`[RegimeRobustnessTester] Testing regime robustness for edge ${edge.id}`);

    // Get unique regimes
    const uniqueRegimes = [...new Set(dataset.rows.map(r => r.regime))];

    // Evaluate edge in each regime
    const perRegime = {};

    for (const regime of uniqueRegimes) {
      const regimeRows = dataset.rows.filter(r => r.regime === regime);
      perRegime[regime] = this.#evaluateInRegime(edge, regimeRows);
    }

    // If edge has no regime constraints, it's universal
    if (!edge.regimes || edge.regimes.length === 0) {
      const allSharpes = Object.values(perRegime).map(r => r.sharpe);
      const avgSharpe = allSharpes.reduce((a, b) => a + b, 0) / allSharpes.length;

      console.log(`[RegimeRobustnessTester] Universal edge, avg Sharpe: ${avgSharpe.toFixed(3)}`);

      return {
        perRegime,
        targetRegimePerformance: avgSharpe,
        otherRegimePerformance: avgSharpe,
        regimeSelectivity: 0,
        passed: avgSharpe >= this.minRegimeSharpe
      };
    }

    // Calculate target regime performance
    const targetSharpes = [];
    const otherSharpes = [];

    for (const [regime, result] of Object.entries(perRegime)) {
      const regimeId = parseInt(regime);

      if (result.trades < this.minTradesPerRegime) {
        continue; // Skip regimes with insufficient data
      }

      if (edge.regimes.includes(regimeId)) {
        targetSharpes.push(result.sharpe);
      } else {
        otherSharpes.push(result.sharpe);
      }
    }

    const targetRegimePerformance = targetSharpes.length > 0
      ? targetSharpes.reduce((a, b) => a + b, 0) / targetSharpes.length
      : 0;

    const otherRegimePerformance = otherSharpes.length > 0
      ? otherSharpes.reduce((a, b) => a + b, 0) / otherSharpes.length
      : 0;

    const regimeSelectivity = targetRegimePerformance - otherRegimePerformance;

    // Pass conditions
    const passedTarget = targetRegimePerformance >= this.minRegimeSharpe;
    const passedSelectivity = regimeSelectivity >= this.selectivityThreshold;
    const passed = passedTarget && passedSelectivity;

    console.log(`[RegimeRobustnessTester] Target Sharpe: ${targetRegimePerformance.toFixed(3)}, Other Sharpe: ${otherRegimePerformance.toFixed(3)}`);
    console.log(`[RegimeRobustnessTester] Selectivity: ${regimeSelectivity.toFixed(3)}, Passed: ${passed}`);

    return {
      perRegime,
      targetRegimePerformance,
      otherRegimePerformance,
      regimeSelectivity,
      passed
    };
  }

  /**
   * Evaluate edge in a specific regime
   */
  #evaluateInRegime(edge, rows) {
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
}
