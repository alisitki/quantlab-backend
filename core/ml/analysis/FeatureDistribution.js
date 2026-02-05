/**
 * FeatureDistribution - Feature distribution analysis
 *
 * Analyzes feature statistics, outliers, and distribution shifts.
 */

/**
 * Calculate basic statistics for an array
 * @param {number[]} values
 * @returns {Object} Statistics
 */
export function calculateStats(values) {
  if (!values || values.length === 0) {
    return { mean: NaN, std: NaN, min: NaN, max: NaN, median: NaN };
  }

  const n = values.length;
  const sorted = [...values].sort((a, b) => a - b);

  // Mean
  const mean = values.reduce((a, b) => a + b, 0) / n;

  // Standard deviation
  const variance = values.reduce((sum, x) => sum + Math.pow(x - mean, 2), 0) / n;
  const std = Math.sqrt(variance);

  // Min, max
  const min = sorted[0];
  const max = sorted[n - 1];

  // Median
  const median = n % 2 === 0
    ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2
    : sorted[Math.floor(n / 2)];

  // Quartiles
  const q1 = sorted[Math.floor(n * 0.25)];
  const q3 = sorted[Math.floor(n * 0.75)];
  const iqr = q3 - q1;

  // Skewness (Fisher-Pearson)
  let skewSum = 0;
  for (const x of values) {
    skewSum += Math.pow((x - mean) / (std || 1), 3);
  }
  const skewness = std > 0 ? skewSum / n : 0;

  // Kurtosis (excess kurtosis)
  let kurtSum = 0;
  for (const x of values) {
    kurtSum += Math.pow((x - mean) / (std || 1), 4);
  }
  const kurtosis = std > 0 ? kurtSum / n - 3 : 0;

  return {
    mean,
    std,
    min,
    max,
    median,
    q1,
    q3,
    iqr,
    skewness,
    kurtosis,
    count: n
  };
}

/**
 * Detect outliers using IQR method
 * @param {number[]} values
 * @param {number} multiplier - IQR multiplier (default 1.5)
 * @returns {Object} Outlier information
 */
export function detectOutliers(values, multiplier = 1.5) {
  if (!values || values.length === 0) {
    return { outlierIndices: [], outlierCount: 0, outlierPct: 0 };
  }

  const stats = calculateStats(values);
  const lowerBound = stats.q1 - multiplier * stats.iqr;
  const upperBound = stats.q3 + multiplier * stats.iqr;

  const outlierIndices = [];
  for (let i = 0; i < values.length; i++) {
    if (values[i] < lowerBound || values[i] > upperBound) {
      outlierIndices.push(i);
    }
  }

  return {
    outlierIndices,
    outlierCount: outlierIndices.length,
    outlierPct: (outlierIndices.length / values.length) * 100,
    lowerBound,
    upperBound
  };
}

/**
 * Calculate Kolmogorov-Smirnov statistic for distribution comparison
 * @param {number[]} sample1
 * @param {number[]} sample2
 * @returns {Object} KS test result
 */
export function calculateKSStatistic(sample1, sample2) {
  if (!sample1?.length || !sample2?.length) {
    return { statistic: NaN, pValue: NaN };
  }

  const sorted1 = [...sample1].sort((a, b) => a - b);
  const sorted2 = [...sample2].sort((a, b) => a - b);

  const n1 = sorted1.length;
  const n2 = sorted2.length;

  // Merge and sort all values
  const allValues = [...new Set([...sorted1, ...sorted2])].sort((a, b) => a - b);

  let maxD = 0;
  let idx1 = 0, idx2 = 0;

  for (const x of allValues) {
    // Count values <= x in each sample
    while (idx1 < n1 && sorted1[idx1] <= x) idx1++;
    while (idx2 < n2 && sorted2[idx2] <= x) idx2++;

    const cdf1 = idx1 / n1;
    const cdf2 = idx2 / n2;
    const d = Math.abs(cdf1 - cdf2);

    if (d > maxD) maxD = d;
  }

  // Approximate p-value (Asymptotic formula)
  const en = Math.sqrt((n1 * n2) / (n1 + n2));
  const lambda = (en + 0.12 + 0.11 / en) * maxD;

  // Kolmogorov distribution approximation
  let pValue = 0;
  for (let k = 1; k <= 100; k++) {
    pValue += 2 * Math.pow(-1, k - 1) * Math.exp(-2 * k * k * lambda * lambda);
  }
  pValue = Math.max(0, Math.min(1, pValue));

  return {
    statistic: maxD,
    pValue,
    significant: pValue < 0.05
  };
}

/**
 * Analyze feature distributions across splits
 * @param {number[][]} X - Feature matrix
 * @param {string[]} featureNames
 * @param {Object} splitIndices - { train: [indices], test: [indices] }
 * @returns {Object} Distribution analysis per feature
 */
export function analyzeFeatureDistributions(X, featureNames, splitIndices = null) {
  const nFeatures = featureNames.length;
  const results = {};

  for (let j = 0; j < nFeatures; j++) {
    const featureName = featureNames[j];
    const allValues = X.map(row => row[j]);

    const overallStats = calculateStats(allValues);
    const outlierInfo = detectOutliers(allValues);

    const featureResult = {
      overall: {
        ...overallStats,
        outlierPct: outlierInfo.outlierPct
      },
      outliers: outlierInfo
    };

    // If split indices provided, analyze per split
    if (splitIndices) {
      featureResult.bySplit = {};
      const splitNames = Object.keys(splitIndices);

      for (const splitName of splitNames) {
        const indices = splitIndices[splitName];
        const splitValues = indices.map(i => allValues[i]);
        featureResult.bySplit[splitName] = calculateStats(splitValues);
      }

      // Compare train vs test distribution
      if (splitIndices.train && splitIndices.test) {
        const trainValues = splitIndices.train.map(i => allValues[i]);
        const testValues = splitIndices.test.map(i => allValues[i]);
        const ksResult = calculateKSStatistic(trainValues, testValues);

        featureResult.distributionShift = {
          ksStatistic: ksResult.statistic,
          pValue: ksResult.pValue,
          significant: ksResult.significant,
          alert: ksResult.significant ? 'WARNING' : null
        };
      }
    }

    results[featureName] = featureResult;
  }

  return results;
}

/**
 * Generate comprehensive feature distribution report
 * @param {number[][]} X
 * @param {string[]} featureNames
 * @param {Object} options
 * @returns {Object} Complete analysis
 */
export function generateFeatureDistributionReport(X, featureNames, options = {}) {
  const {
    splitIndices = null,
    outlierThreshold = 5.0 // % outliers to flag
  } = options;

  const distributions = analyzeFeatureDistributions(X, featureNames, splitIndices);

  // Identify features with issues
  const alerts = [];
  const highOutlierFeatures = [];
  const shiftedFeatures = [];

  for (const [name, result] of Object.entries(distributions)) {
    // Check for high outlier percentage
    if (result.overall.outlierPct > outlierThreshold) {
      highOutlierFeatures.push(name);
      alerts.push({
        feature: name,
        type: 'HIGH_OUTLIERS',
        value: result.overall.outlierPct.toFixed(1) + '%'
      });
    }

    // Check for distribution shift
    if (result.distributionShift?.significant) {
      shiftedFeatures.push(name);
      alerts.push({
        feature: name,
        type: 'DISTRIBUTION_SHIFT',
        value: `KS=${result.distributionShift.ksStatistic.toFixed(3)}`
      });
    }

    // Check for high skewness
    if (Math.abs(result.overall.skewness) > 2) {
      alerts.push({
        feature: name,
        type: 'HIGH_SKEWNESS',
        value: result.overall.skewness.toFixed(2)
      });
    }
  }

  return {
    features: distributions,
    alerts,
    highOutlierFeatures,
    shiftedFeatures,
    summary: {
      totalFeatures: featureNames.length,
      featuresWithAlerts: new Set(alerts.map(a => a.feature)).size,
      highOutlierCount: highOutlierFeatures.length,
      distributionShiftCount: shiftedFeatures.length
    }
  };
}
