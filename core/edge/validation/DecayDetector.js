/**
 * DecayDetector - Detect if edge alpha is declining over time
 *
 * Uses linear regression on windowed performance to detect decay.
 * Uses PSI to detect feature distribution drift.
 */

import { VALIDATION_CONFIG } from './config.js';

export class DecayDetector {
  /**
   * @param {Object} config
   * @param {number} config.windowSize - Window size for decay calculation (default: 1000)
   * @param {number} config.maxDecayRate - Maximum acceptable decay rate (default: -0.001)
   * @param {number} config.psiThreshold - PSI threshold for distribution shift (default: 0.25)
   */
  constructor(config = {}) {
    this.windowSize = config.windowSize || VALIDATION_CONFIG.decay.windowSize;
    this.maxDecayRate = config.maxDecayRate || VALIDATION_CONFIG.decay.maxDecayRate;
    this.psiThreshold = config.psiThreshold || VALIDATION_CONFIG.decay.psiThreshold;
  }

  /**
   * Detect edge decay
   * @param {Edge} edge
   * @param {DiscoveryDataset} dataset
   * @returns {DecayResult}
   *
   * DecayResult = {
   *   decayRate: number,           // Slope of performance over time
   *   halfLife: number|null,       // Estimated half-life in rows (null if not decaying)
   *   isDecaying: boolean,
   *   performanceTrend: Array<{windowIdx, meanReturn}>,
   *   psi: number,                 // PSI between first and last quarter
   *   passed: boolean
   * }
   */
  detect(edge, dataset) {
    console.log(`[DecayDetector] Detecting decay for edge ${edge.id}`);

    // Calculate windowed performance
    const performanceTrend = [];
    const totalRows = dataset.rows.length;

    for (let startIdx = 0; startIdx + this.windowSize <= totalRows; startIdx += this.windowSize) {
      const endIdx = startIdx + this.windowSize;
      const windowRows = dataset.rows.slice(startIdx, endIdx);

      const returns = [];

      for (const row of windowRows) {
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
      }

      const meanReturn = returns.length > 0
        ? returns.reduce((a, b) => a + b, 0) / returns.length
        : 0;

      performanceTrend.push({
        windowIdx: performanceTrend.length,
        meanReturn
      });
    }

    if (performanceTrend.length < 2) {
      return {
        decayRate: 0,
        halfLife: null,
        isDecaying: false,
        performanceTrend,
        psi: 0,
        passed: true
      };
    }

    // Calculate decay rate (linear regression slope)
    const decayRate = this.#calculateTrend(performanceTrend.map(p => p.meanReturn));

    // Estimate half-life if decaying
    let halfLife = null;
    if (decayRate < 0) {
      // Half-life: t = ln(2) / |decay_rate|
      // But decay_rate is per window, so multiply by windowSize
      halfLife = Math.log(2) / Math.abs(decayRate);
    }

    const isDecaying = decayRate < this.maxDecayRate;

    // Calculate PSI between first and last quarter
    const psi = this.#calculatePSI(edge, dataset);

    const passed = !isDecaying && psi < this.psiThreshold;

    console.log(`[DecayDetector] Decay rate: ${decayRate.toFixed(6)}, Half-life: ${halfLife?.toFixed(0) || 'N/A'}`);
    console.log(`[DecayDetector] PSI: ${psi.toFixed(3)}, Passed: ${passed}`);

    return {
      decayRate,
      halfLife,
      isDecaying,
      performanceTrend,
      psi,
      passed
    };
  }

  /**
   * Calculate linear trend
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

  /**
   * Calculate PSI (Population Stability Index) between first and last quarter
   */
  #calculatePSI(edge, dataset) {
    const totalRows = dataset.rows.length;
    const quarterSize = Math.floor(totalRows / 4);

    if (quarterSize === 0) return 0;

    // First quarter
    const firstQuarter = dataset.rows.slice(0, quarterSize);

    // Last quarter
    const lastQuarter = dataset.rows.slice(-quarterSize);

    // For simplicity, calculate PSI on regime distribution
    // (In production, would use feature distributions)
    const firstRegimes = firstQuarter.map(r => r.regime);
    const lastRegimes = lastQuarter.map(r => r.regime);

    const psi = this.#calculatePSIBetweenArrays(firstRegimes, lastRegimes);

    return psi;
  }

  /**
   * Calculate PSI between two arrays (distribution shift)
   */
  #calculatePSIBetweenArrays(arr1, arr2) {
    // Get unique values
    const allValues = [...new Set([...arr1, ...arr2])];

    if (allValues.length === 0) return 0;

    let psi = 0;

    for (const value of allValues) {
      const count1 = arr1.filter(v => v === value).length;
      const count2 = arr2.filter(v => v === value).length;

      const p1 = (count1 + 1) / (arr1.length + allValues.length); // Laplace smoothing
      const p2 = (count2 + 1) / (arr2.length + allValues.length);

      psi += (p1 - p2) * Math.log(p1 / p2);
    }

    return Math.abs(psi);
  }
}
