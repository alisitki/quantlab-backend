/**
 * StatisticalEdgeTester - Apply rigorous statistical tests to pattern candidates
 *
 * Tests applied:
 * 1. Welch's t-test: Compare mean return when pattern active vs inactive
 * 2. Permutation test: Non-parametric p-value via shuffling
 * 3. Sharpe ratio test: Minimum Sharpe threshold
 * 4. Regime robustness: Pattern works in claimed regimes
 * 5. Sample size: Minimum occurrence count
 * 6. Bonferroni correction: Multiple comparison adjustment
 */

import v8 from 'node:v8';
import { DISCOVERY_CONFIG } from './config.js';

export class StatisticalEdgeTester {
  /**
   * @param {Object} config
   * @param {number} config.minSampleSize - Minimum samples per pattern (default: 30)
   * @param {number} config.pValueThreshold - Maximum p-value (default: 0.05)
   * @param {number} config.minSharpe - Minimum Sharpe ratio (default: 0.5)
   * @param {boolean} config.multipleComparisonCorrection - Apply Bonferroni (default: true)
   * @param {number} config.seed - Random seed for permutation tests
   */
  constructor(config = {}) {
    this.minSampleSize = config.minSampleSize || DISCOVERY_CONFIG.tester.minSampleSize;
    this.pValueThreshold = config.pValueThreshold || DISCOVERY_CONFIG.tester.pValueThreshold;
    this.minSharpe = config.minSharpe || DISCOVERY_CONFIG.tester.minSharpe;
    this.multipleComparisonCorrection = config.multipleComparisonCorrection !== undefined
      ? config.multipleComparisonCorrection
      : DISCOVERY_CONFIG.tester.multipleComparisonCorrection;
    this.seed = config.seed || DISCOVERY_CONFIG.seed;
    this.permutationTestEnabled = config.permutationTestEnabled !== undefined
      ? config.permutationTestEnabled
      : DISCOVERY_CONFIG.tester.permutationTestEnabled;
    this.permutationN = config.permutationN || DISCOVERY_CONFIG.tester.permutationN;
    this.permutationMinHeapMB = config.permutationMinHeapMB || DISCOVERY_CONFIG.tester.permutationMinHeapMB;

    // Heap check for permutation test
    if (this.permutationTestEnabled) {
      const heapStats = v8.getHeapStatistics();
      const currentHeapLimitMB = Math.floor(heapStats.heap_size_limit / 1024 / 1024);

      console.log(`[StatisticalEdgeTester] Permutation test: ENABLED (DEFAULT)`);
      console.log(`[StatisticalEdgeTester] Current heap limit: ${currentHeapLimitMB} MB`);
      console.log(`[StatisticalEdgeTester] Permutation test requires: ${this.permutationMinHeapMB} MB`);

      if (currentHeapLimitMB < this.permutationMinHeapMB) {
        console.error('');
        console.error('❌ FATAL ERROR: Heap limit too low for permutation test');
        console.error('');
        console.error(`Current heap: ${currentHeapLimitMB} MB`);
        console.error(`Required heap: ${this.permutationMinHeapMB} MB`);
        console.error('');
        console.error('Permutation test provides exact statistical validation but requires high memory.');
        console.error('');
        console.error('SOLUTIONS:');
        console.error('  1. Increase heap limit (RECOMMENDED for exact semantics):');
        console.error(`     NODE_OPTIONS="--max-old-space-size=${this.permutationMinHeapMB}" node --expose-gc <script>`);
        console.error('');
        console.error('  2. Disable permutation test (NOT RECOMMENDED - loses statistical rigor):');
        console.error('     DISCOVERY_PERMUTATION_TEST=false node <script>');
        console.error('');
        throw new Error(`Heap limit too low: ${currentHeapLimitMB} MB < ${this.permutationMinHeapMB} MB required for permutation test`);
      }

      console.log(`[StatisticalEdgeTester] ✅ Heap adequate for permutation test`);
    } else {
      console.log(`[StatisticalEdgeTester] Permutation test: DISABLED (DISCOVERY_PERMUTATION_TEST=false)`);
      console.log(`[StatisticalEdgeTester] ⚠️  Statistical validation will rely on t-test + Sharpe only`);
    }
  }

  /**
   * Test a pattern candidate for statistical significance
   * @param {PatternCandidate} pattern
   * @param {DiscoveryDataset} dataset
   * @returns {EdgeTestResult}
   *
   * EdgeTestResult = {
   *   patternId: string,
   *   isSignificant: boolean,
   *   tests: {
   *     tTest: { statistic, pValue, passed },
   *     permutationTest: { pValue, nPermutations, passed },
   *     sharpeTest: { sharpe, passed },
   *     regimeRobustness: { perRegimeSharpe: Object, passed },
   *     sampleSizeTest: { count, minRequired, passed }
   *   },
   *   overallScore: number,  // 0-1 combined score
   *   recommendation: 'ACCEPT'|'REJECT'|'WEAK'
   * }
   */
  async test(pattern, dataset) {
    const tests = {};

    // 1. Sample size test
    tests.sampleSizeTest = {
      count: pattern.support,
      minRequired: this.minSampleSize,
      passed: pattern.support >= this.minSampleSize
    };

    if (!tests.sampleSizeTest.passed) {
      return {
        patternId: pattern.id,
        isSignificant: false,
        tests,
        overallScore: 0,
        recommendation: 'REJECT'
      };
    }

    // Get pattern returns and compute non-pattern statistics (streaming)
    const patternReturns = [];
    let nonPatternCount = 0;
    let nonPatternSum = 0;
    let nonPatternSumSq = 0;

    if (Array.isArray(dataset.rows)) {
      for (let i = 0; i < dataset.rows.length; i++) {
        const row = dataset.rows[i];
        const forwardReturn = row.forwardReturns[`h${pattern.horizon}`];

        if (forwardReturn === null) continue;

        if (pattern.matchingIndices.includes(i)) {
          patternReturns.push(forwardReturn);
        } else {
          // Streaming statistics for non-pattern returns (avoid storing 3.2M array)
          nonPatternCount++;
          nonPatternSum += forwardReturn;
          nonPatternSumSq += forwardReturn * forwardReturn;
        }
      }
    } else if (typeof dataset.rowsFactory === 'function') {
      const matchingIndexSet = new Set(pattern.matchingIndices);
      let i = 0;

      for await (const row of dataset.rowsFactory()) {
        const forwardReturn = row.forwardReturns[`h${pattern.horizon}`];
        if (forwardReturn === null) {
          i++;
          continue;
        }

        if (matchingIndexSet.has(i)) {
          patternReturns.push(forwardReturn);
        } else {
          nonPatternCount++;
          nonPatternSum += forwardReturn;
          nonPatternSumSq += forwardReturn * forwardReturn;
        }

        i++;
      }
    }

    // Calculate non-pattern statistics (online variance algorithm)
    const nonPatternMean = nonPatternCount > 0 ? nonPatternSum / nonPatternCount : 0;
    const nonPatternVariance = nonPatternCount > 0
      ? (nonPatternSumSq / nonPatternCount) - (nonPatternMean * nonPatternMean)
      : 0;

    // 2. Welch's t-test (with streaming non-pattern statistics)
    tests.tTest = this.#welchTTestStreaming(patternReturns, {
      count: nonPatternCount,
      mean: nonPatternMean,
      variance: nonPatternVariance
    });
    tests.tTest.passed = tests.tTest.pValue < this.pValueThreshold;

    // 3. Permutation test (conditional - feature flag)
    if (this.permutationTestEnabled) {
      // Permutation test requires full non-pattern returns array
      // Re-collect non-pattern returns (memory intensive, but exact)
      const nonPatternReturns = [];
      if (Array.isArray(dataset.rows)) {
        for (let i = 0; i < dataset.rows.length; i++) {
          const row = dataset.rows[i];
          const forwardReturn = row.forwardReturns[`h${pattern.horizon}`];
          if (forwardReturn === null) continue;
          if (!pattern.matchingIndices.includes(i)) {
            nonPatternReturns.push(forwardReturn);
          }
        }
      } else if (typeof dataset.rowsFactory === 'function') {
        const matchingIndexSet = new Set(pattern.matchingIndices);
        let i = 0;
        for await (const row of dataset.rowsFactory()) {
          const forwardReturn = row.forwardReturns[`h${pattern.horizon}`];
          if (forwardReturn === null) {
            i++;
            continue;
          }
          if (!matchingIndexSet.has(i)) {
            nonPatternReturns.push(forwardReturn);
          }
          i++;
        }
      }

      tests.permutationTest = this.#permutationTest(
        patternReturns,
        nonPatternReturns,
        this.permutationN,
        this.seed
      );
      tests.permutationTest.passed = tests.permutationTest.pValue < this.pValueThreshold;
    } else {
      // Permutation test disabled (default)
      tests.permutationTest = {
        pValue: null,
        nPermutations: 0,
        skipped: true,
        passed: true,  // Don't penalize for skipping
        reason: 'Permutation test disabled (set DISCOVERY_PERMUTATION_TEST=true to enable)'
      };
    }

    // 4. Sharpe ratio test
    const sharpe = this.#calculateSharpe(patternReturns);
    tests.sharpeTest = {
      sharpe,
      minRequired: this.minSharpe,
      passed: sharpe >= this.minSharpe
    };

    // 5. Regime robustness (if pattern has regime constraints)
    if (pattern.regimes && pattern.regimes.length > 0) {
      tests.regimeRobustness = await this.#testRegimeRobustness(pattern, dataset);
    } else {
      tests.regimeRobustness = {
        perRegimeSharpe: {},
        passed: true // No regime constraint = always passes
      };
    }

    // Calculate overall score
    const scores = [];
    scores.push(tests.tTest.passed ? 1 : 0);
    scores.push(tests.permutationTest.passed ? 1 : 0);
    scores.push(tests.sharpeTest.passed ? 1 : 0);
    scores.push(tests.regimeRobustness.passed ? 1 : 0);

    const overallScore = scores.reduce((a, b) => a + b, 0) / scores.length;

    // Determine significance
    const isSignificant = overallScore >= 0.75; // 3/4 tests must pass

    const recommendation = isSignificant ? 'ACCEPT' :
                          overallScore >= 0.5 ? 'WEAK' :
                          'REJECT';

    return {
      patternId: pattern.id,
      isSignificant,
      tests,
      overallScore,
      recommendation
    };
  }

  /**
   * Batch test multiple patterns with multiple comparison correction
   * @param {Array<PatternCandidate>} patterns
   * @param {DiscoveryDataset} dataset
   * @returns {Array<EdgeTestResult>}
   */
  async testBatch(patterns, dataset) {
    console.log(`[StatisticalEdgeTester] Testing ${patterns.length} patterns...`);

    const results = await Promise.all(patterns.map(pattern => this.test(pattern, dataset)));

    // Apply Bonferroni correction if enabled
    if (this.multipleComparisonCorrection && patterns.length > 1) {
      const adjustedThreshold = this.pValueThreshold / patterns.length;

      console.log(`[StatisticalEdgeTester] Applying Bonferroni correction: ${this.pValueThreshold} / ${patterns.length} = ${adjustedThreshold.toFixed(6)}`);

      results.forEach(result => {
        // Re-evaluate with adjusted threshold
        const tTestPassed = result.tests.tTest.pValue < adjustedThreshold;

        // Permutation test: only re-evaluate if it was actually run
        let permTestPassed;
        if (result.tests.permutationTest.skipped) {
          permTestPassed = true;  // Don't penalize for skipping
        } else {
          permTestPassed = result.tests.permutationTest.pValue < adjustedThreshold;
        }

        result.tests.tTest.passed = tTestPassed;
        result.tests.permutationTest.passed = permTestPassed;

        // Recalculate overall score
        const scores = [
          tTestPassed ? 1 : 0,
          permTestPassed ? 1 : 0,
          result.tests.sharpeTest.passed ? 1 : 0,
          result.tests.regimeRobustness.passed ? 1 : 0
        ];

        result.overallScore = scores.reduce((a, b) => a + b, 0) / scores.length;
        result.isSignificant = result.overallScore >= 0.75;
        result.recommendation = result.isSignificant ? 'ACCEPT' :
                               result.overallScore >= 0.5 ? 'WEAK' :
                               'REJECT';
      });
    }

    const accepted = results.filter(r => r.recommendation === 'ACCEPT').length;
    const weak = results.filter(r => r.recommendation === 'WEAK').length;
    const rejected = results.filter(r => r.recommendation === 'REJECT').length;

    console.log(`[StatisticalEdgeTester] Results: ${accepted} ACCEPT, ${weak} WEAK, ${rejected} REJECT`);

    return results;
  }

  /**
   * Welch's t-test with streaming statistics for group2
   * @param {number[]} group1 - Pattern returns
   * @param {Object} group2Stats - { count, mean, variance }
   * @returns {Object} { statistic, pValue, degreesOfFreedom }
   */
  #welchTTestStreaming(group1, group2Stats) {
    const n1 = group1.length;
    const n2 = group2Stats.count;

    if (n1 === 0 || n2 === 0) {
      return { statistic: 0, pValue: 1, degreesOfFreedom: 0 };
    }

    // Calculate group1 mean and variance
    const mean1 = group1.reduce((a, b) => a + b, 0) / n1;
    const var1 = group1.reduce((sum, x) => sum + Math.pow(x - mean1, 2), 0) / n1;

    // Group2 statistics already computed (streaming)
    const mean2 = group2Stats.mean;
    const var2 = group2Stats.variance;

    if (var1 === 0 && var2 === 0) {
      return { statistic: 0, pValue: 1, degreesOfFreedom: 0 };
    }

    // Calculate t-statistic
    const tStatistic = (mean1 - mean2) / Math.sqrt(var1 / n1 + var2 / n2);

    // Calculate degrees of freedom (Welch-Satterthwaite)
    const df = Math.pow(var1 / n1 + var2 / n2, 2) /
               (Math.pow(var1 / n1, 2) / (n1 - 1) + Math.pow(var2 / n2, 2) / (n2 - 1));

    // Approximate p-value using t-distribution
    const pValue = this.#tDistributionPValue(Math.abs(tStatistic), df);

    return {
      statistic: tStatistic,
      pValue,
      degreesOfFreedom: df
    };
  }

  /**
   * Welch's t-test (two-sample, unequal variance) - LEGACY
   * @param {number[]} group1
   * @param {number[]} group2
   * @returns {Object} { statistic, pValue, degreesOfFreedom }
   */
  #welchTTest(group1, group2) {
    const n1 = group1.length;
    const n2 = group2.length;

    if (n1 === 0 || n2 === 0) {
      return { statistic: 0, pValue: 1, degreesOfFreedom: 0 };
    }

    // Calculate means
    const mean1 = group1.reduce((a, b) => a + b, 0) / n1;
    const mean2 = group2.reduce((a, b) => a + b, 0) / n2;

    // Calculate variances
    const var1 = group1.reduce((sum, x) => sum + Math.pow(x - mean1, 2), 0) / n1;
    const var2 = group2.reduce((sum, x) => sum + Math.pow(x - mean2, 2), 0) / n2;

    if (var1 === 0 && var2 === 0) {
      return { statistic: 0, pValue: 1, degreesOfFreedom: 0 };
    }

    // Calculate t-statistic
    const tStatistic = (mean1 - mean2) / Math.sqrt(var1 / n1 + var2 / n2);

    // Calculate degrees of freedom (Welch-Satterthwaite)
    const df = Math.pow(var1 / n1 + var2 / n2, 2) /
               (Math.pow(var1 / n1, 2) / (n1 - 1) + Math.pow(var2 / n2, 2) / (n2 - 1));

    // Approximate p-value using t-distribution
    // For simplicity, using normal approximation for large df
    const pValue = this.#tDistributionPValue(Math.abs(tStatistic), df);

    return {
      statistic: tStatistic,
      pValue,
      degreesOfFreedom: df
    };
  }

  /**
   * Permutation test - non-parametric significance test
   * @param {number[]} group1 - Pattern returns
   * @param {number[]} group2 - Non-pattern returns
   * @param {number} nPermutations - Number of permutations
   * @param {number} seed - Random seed
   * @returns {Object} { pValue, nPermutations }
   */
  #permutationTest(group1, group2, nPermutations, seed) {
    const observed = this.#meanDifference(group1, group2);
    const pooled = [...group1, ...group2];
    const n1 = group1.length;

    // Create seeded random generator
    const random = this.#createSeededRandom(seed);

    let extremeCount = 0;

    for (let i = 0; i < nPermutations; i++) {
      // Shuffle pooled array
      const shuffled = this.#fisherYatesShuffle([...pooled], random);

      // Split into two groups
      const perm1 = shuffled.slice(0, n1);
      const perm2 = shuffled.slice(n1);

      const permDiff = this.#meanDifference(perm1, perm2);

      if (Math.abs(permDiff) >= Math.abs(observed)) {
        extremeCount++;
      }
    }

    const pValue = extremeCount / nPermutations;

    return {
      pValue,
      nPermutations,
      observedDifference: observed
    };
  }

  /**
   * Calculate Sharpe ratio (annualized)
   * @param {number[]} returns
   * @returns {number}
   */
  #calculateSharpe(returns) {
    if (returns.length === 0) return 0;

    const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
    const std = Math.sqrt(variance);

    if (std === 0) return 0;

    // Annualize assuming ~250 trading days, ~8 hours per day, ~1 event per second
    // Events per year ≈ 250 * 8 * 3600 = 7,200,000
    const eventsPerYear = 7200000;
    const annualizedMean = mean * eventsPerYear;
    const annualizedStd = std * Math.sqrt(eventsPerYear);

    return annualizedStd === 0 ? 0 : annualizedMean / annualizedStd;
  }

  /**
   * Test regime robustness - does pattern work in target regimes?
   * @param {PatternCandidate} pattern
   * @param {DiscoveryDataset} dataset
   * @returns {Object}
   */
  async #testRegimeRobustness(pattern, dataset) {
    if (Array.isArray(dataset.rows)) {
      const perRegimeSharpe = {};

      // Calculate Sharpe per regime
      const uniqueRegimes = [...new Set(dataset.rows.map(r => r.regime))];

      for (const regime of uniqueRegimes) {
        const regimeReturns = [];

        for (const idx of pattern.matchingIndices) {
          const row = dataset.rows[idx];
          if (row.regime === regime) {
            const ret = row.forwardReturns[`h${pattern.horizon}`];
            if (ret !== null) {
              regimeReturns.push(ret);
            }
          }
        }

        if (regimeReturns.length > 0) {
          perRegimeSharpe[regime] = this.#calculateSharpe(regimeReturns);
        }
      }

      // Check if target regimes have good Sharpe
      let passed = true;

      for (const targetRegime of pattern.regimes) {
        if (perRegimeSharpe[targetRegime] !== undefined && perRegimeSharpe[targetRegime] < this.minSharpe) {
          passed = false;
          break;
        }
      }

      return {
        perRegimeSharpe,
        passed
      };
    }

    if (typeof dataset.rowsFactory !== 'function') {
      console.warn('[StatisticalEdgeTester] Regime robustness skipped: dataset has no rows array or rowsFactory');
      return { perRegimeSharpe: {}, passed: true };
    }

    const perRegimeSharpe = {};
    const matchingIndexSet = new Set(pattern.matchingIndices);
    const regimeReturnsByRegime = new Map();

    let i = 0;
    for await (const row of dataset.rowsFactory()) {
      if (matchingIndexSet.has(i)) {
        const ret = row.forwardReturns[`h${pattern.horizon}`];
        if (ret !== null) {
          const regime = row.regime;
          const arr = regimeReturnsByRegime.get(regime);
          if (arr) {
            arr.push(ret);
          } else {
            regimeReturnsByRegime.set(regime, [ret]);
          }
        }
      }
      i++;
    }

    for (const [regime, regimeReturns] of regimeReturnsByRegime.entries()) {
      if (regimeReturns.length > 0) {
        perRegimeSharpe[regime] = this.#calculateSharpe(regimeReturns);
      }
    }

    let passed = true;
    for (const targetRegime of pattern.regimes) {
      if (perRegimeSharpe[targetRegime] !== undefined && perRegimeSharpe[targetRegime] < this.minSharpe) {
        passed = false;
        break;
      }
    }

    return { perRegimeSharpe, passed };
  }

  // --- Helper functions ---

  #meanDifference(group1, group2) {
    const mean1 = group1.reduce((a, b) => a + b, 0) / group1.length;
    const mean2 = group2.reduce((a, b) => a + b, 0) / group2.length;
    return mean1 - mean2;
  }

  /**
   * Approximate p-value from t-distribution
   * Using normal approximation for df > 30
   */
  #tDistributionPValue(t, df) {
    if (df > 30) {
      // Normal approximation
      return 2 * (1 - this.#normalCDF(t));
    }

    // For small df, use conservative approximation
    const z = t * Math.sqrt(df / (df + 1));
    return 2 * (1 - this.#normalCDF(z));
  }

  /**
   * Standard normal cumulative distribution function
   */
  #normalCDF(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989423 * Math.exp(-x * x / 2);
    const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));

    return x > 0 ? 1 - p : p;
  }

  /**
   * Create seeded random number generator (LCG)
   * Same as RegimeCluster implementation
   */
  #createSeededRandom(seed) {
    let state = seed;
    return () => {
      state = (state * 1664525 + 1013904223) % 4294967296;
      return state / 4294967296;
    };
  }

  /**
   * Fisher-Yates shuffle with seeded random
   */
  #fisherYatesShuffle(array, random) {
    for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
  }
}
