/**
 * OutOfSampleValidator - Test edge performance on unseen data
 *
 * Splits data temporally into train/test sets.
 * Evaluates edge on both splits to detect overfitting.
 */

import { VALIDATION_CONFIG } from './config.js';

export class OutOfSampleValidator {
  /**
   * @param {Object} config
   * @param {number} config.trainRatio - Fraction for training (default: 0.7)
   * @param {number} config.testRatio - Fraction for testing (default: 0.3)
   * @param {number} config.minSharpeOOS - Min out-of-sample Sharpe (default: 0.5)
   * @param {number} config.maxPerfDegradation - Max IS-to-OOS perf drop (default: 0.5)
   */
  constructor(config = {}) {
    this.trainRatio = config.trainRatio || VALIDATION_CONFIG.oos.trainRatio;
    this.testRatio = config.testRatio || VALIDATION_CONFIG.oos.testRatio;
    this.minSharpeOOS = config.minSharpeOOS || VALIDATION_CONFIG.oos.minSharpeOOS;
    this.maxPerfDegradation = config.maxPerfDegradation || VALIDATION_CONFIG.oos.maxPerfDegradation;
  }

  /**
   * Validate edge on out-of-sample data
   * @param {Edge} edge - Edge to validate
   * @param {DiscoveryDataset} dataset - Full dataset
   * @returns {OOSResult}
   *
   * OOSResult = {
   *   inSample: { sharpe, winRate, meanReturn, trades },
   *   outOfSample: { sharpe, winRate, meanReturn, trades },
   *   degradation: number,  // (IS_sharpe - OOS_sharpe) / IS_sharpe
   *   passed: boolean,
   *   confidence: number    // 0-1
   * }
   */
  validate(edge, dataset) {
    // Split dataset temporally
    const splitIdx = Math.floor(dataset.rows.length * this.trainRatio);

    const inSampleRows = dataset.rows.slice(0, splitIdx);
    const outOfSampleRows = dataset.rows.slice(splitIdx);

    console.log(`[OutOfSampleValidator] Validating edge ${edge.id}`);
    console.log(`[OutOfSampleValidator] In-sample: ${inSampleRows.length} rows, Out-of-sample: ${outOfSampleRows.length} rows`);

    // Evaluate on in-sample
    const inSampleResult = this.#evaluateEdge(edge, inSampleRows);

    // Evaluate on out-of-sample
    const outOfSampleResult = this.#evaluateEdge(edge, outOfSampleRows);

    // Calculate degradation
    const degradation = inSampleResult.sharpe === 0 ? 0 :
      (inSampleResult.sharpe - outOfSampleResult.sharpe) / Math.abs(inSampleResult.sharpe);

    // Check pass conditions
    const passedSharpe = outOfSampleResult.sharpe >= this.minSharpeOOS;
    const passedDegradation = degradation <= this.maxPerfDegradation;
    const passed = passedSharpe && passedDegradation;

    // Calculate confidence
    const sharpeConfidence = Math.min(1, outOfSampleResult.sharpe / this.minSharpeOOS);
    const degradationConfidence = Math.max(0, 1 - degradation / this.maxPerfDegradation);
    const sampleConfidence = Math.min(1, outOfSampleResult.trades / 30);
    const confidence = (sharpeConfidence + degradationConfidence + sampleConfidence) / 3;

    console.log(`[OutOfSampleValidator] IS Sharpe: ${inSampleResult.sharpe.toFixed(3)}, OOS Sharpe: ${outOfSampleResult.sharpe.toFixed(3)}`);
    console.log(`[OutOfSampleValidator] Degradation: ${(degradation * 100).toFixed(1)}%, Passed: ${passed}`);

    return {
      inSample: inSampleResult,
      outOfSample: outOfSampleResult,
      degradation,
      passed,
      confidence
    };
  }

  /**
   * Evaluate edge on a subset of data
   * @param {Edge} edge
   * @param {Array} rows
   * @returns {Object} { sharpe, winRate, meanReturn, trades }
   */
  #evaluateEdge(edge, rows) {
    const returns = [];
    let wins = 0;
    let trades = 0;

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // Check if edge would enter
      const entryEval = edge.evaluateEntry(row.features, row.regime);

      if (!entryEval.active) continue;

      // Get forward return at edge's horizon
      const horizonKey = `h${Math.round(edge.timeHorizon / 1000)}`; // Convert ms to events
      let forwardReturn = row.forwardReturns[horizonKey];

      // If exact horizon not available, use closest
      if (forwardReturn === null || forwardReturn === undefined) {
        const availableHorizons = Object.keys(row.forwardReturns)
          .filter(k => row.forwardReturns[k] !== null);

        if (availableHorizons.length > 0) {
          forwardReturn = row.forwardReturns[availableHorizons[0]];
        } else {
          continue;
        }
      }

      // Adjust return based on direction
      const adjustedReturn = entryEval.direction === 'LONG' ? forwardReturn : -forwardReturn;

      returns.push(adjustedReturn);
      trades++;

      if (adjustedReturn > 0) {
        wins++;
      }
    }

    if (returns.length === 0) {
      return {
        sharpe: 0,
        winRate: 0,
        meanReturn: 0,
        trades: 0
      };
    }

    // Calculate statistics
    const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - meanReturn, 2), 0) / returns.length;
    const std = Math.sqrt(variance);

    // Annualized Sharpe (assuming ~7.2M events per year)
    const eventsPerYear = 7200000;
    const annualizedMean = meanReturn * eventsPerYear;
    const annualizedStd = std * Math.sqrt(eventsPerYear);
    const sharpe = annualizedStd === 0 ? 0 : annualizedMean / annualizedStd;

    const winRate = wins / trades;

    return {
      sharpe,
      winRate,
      meanReturn,
      trades
    };
  }
}
