/**
 * LabelDistribution - Label distribution analysis
 *
 * Analyzes label imbalance, temporal patterns, and drift.
 */

/**
 * Calculate label distribution statistics
 * @param {number[]} y - Label array
 * @returns {Object} Distribution statistics
 */
export function calculateLabelDistribution(y) {
  if (!y || y.length === 0) {
    return { counts: {}, total: 0 };
  }

  const counts = {};
  for (const label of y) {
    counts[label] = (counts[label] || 0) + 1;
  }

  const total = y.length;
  const percentages = {};
  for (const [label, count] of Object.entries(counts)) {
    percentages[label] = (count / total) * 100;
  }

  return { counts, percentages, total };
}

/**
 * Calculate imbalance ratio (majority / minority)
 * @param {number[]} y - Label array
 * @returns {number} Imbalance ratio (>1 means imbalanced)
 */
export function calculateImbalanceRatio(y) {
  const { counts } = calculateLabelDistribution(y);
  const countValues = Object.values(counts);

  if (countValues.length < 2) return 1;

  const maxCount = Math.max(...countValues);
  const minCount = Math.min(...countValues);

  if (minCount === 0) return Infinity;

  return maxCount / minCount;
}

/**
 * Calculate entropy of label distribution
 * Higher entropy = more balanced distribution
 * @param {number[]} y - Label array
 * @returns {number} Entropy (0 to log2(nClasses))
 */
export function calculateEntropy(y) {
  const { counts, total } = calculateLabelDistribution(y);

  if (total === 0) return 0;

  let entropy = 0;
  for (const count of Object.values(counts)) {
    if (count > 0) {
      const p = count / total;
      entropy -= p * Math.log2(p);
    }
  }

  return entropy;
}

/**
 * Calculate normalized entropy (0 to 1)
 * 1 = perfectly balanced, 0 = completely imbalanced
 * @param {number[]} y - Label array
 * @returns {number} Normalized entropy
 */
export function calculateNormalizedEntropy(y) {
  const { counts } = calculateLabelDistribution(y);
  const nClasses = Object.keys(counts).length;

  if (nClasses <= 1) return 0;

  const entropy = calculateEntropy(y);
  const maxEntropy = Math.log2(nClasses);

  return entropy / maxEntropy;
}

/**
 * Detect temporal label drift using sliding windows
 * @param {number[]} y - Label array
 * @param {number[]} timestamps - Timestamps for each sample (optional)
 * @param {number} windowSize - Window size for drift detection
 * @returns {Object} Drift analysis results
 */
export function detectTemporalLabelDrift(y, timestamps = null, windowSize = 1000) {
  if (y.length < windowSize * 2) {
    return { detected: false, reason: 'Insufficient data for drift detection' };
  }

  // Use index-based windows if no timestamps
  const windows = [];
  const step = Math.floor(windowSize / 2); // 50% overlap

  for (let i = 0; i + windowSize <= y.length; i += step) {
    const windowLabels = y.slice(i, i + windowSize);
    const dist = calculateLabelDistribution(windowLabels);

    windows.push({
      startIdx: i,
      endIdx: i + windowSize,
      startTs: timestamps ? timestamps[i] : null,
      endTs: timestamps ? timestamps[i + windowSize - 1] : null,
      distribution: dist.percentages,
      entropy: calculateNormalizedEntropy(windowLabels)
    });
  }

  // Calculate baseline (first window)
  const baseline = windows[0];
  const driftWindows = [];

  // Detect drift: significant deviation from baseline
  for (let i = 1; i < windows.length; i++) {
    const window = windows[i];
    const driftScore = calculateDistributionDrift(baseline.distribution, window.distribution);

    if (driftScore > 0.2) { // 20% deviation threshold
      driftWindows.push({
        ...window,
        driftScore,
        alert: driftScore > 0.3 ? 'HIGH' : 'MODERATE'
      });
    }
  }

  return {
    detected: driftWindows.length > 0,
    windowSize,
    totalWindows: windows.length,
    driftWindows,
    baselineDistribution: baseline.distribution,
    summary: {
      driftWindowCount: driftWindows.length,
      maxDriftScore: driftWindows.length > 0
        ? Math.max(...driftWindows.map(w => w.driftScore)).toFixed(3)
        : 0
    }
  };
}

/**
 * Calculate distribution drift between two distributions
 * @param {Object} dist1 - First distribution (percentages)
 * @param {Object} dist2 - Second distribution (percentages)
 * @returns {number} Drift score (0 to 1)
 */
function calculateDistributionDrift(dist1, dist2) {
  const allLabels = new Set([...Object.keys(dist1), ...Object.keys(dist2)]);
  let totalDiff = 0;

  for (const label of allLabels) {
    const p1 = dist1[label] || 0;
    const p2 = dist2[label] || 0;
    totalDiff += Math.abs(p1 - p2);
  }

  // Normalize by 200 (max possible difference for percentages)
  return totalDiff / 200;
}

/**
 * Analyze label distribution across train/valid/test splits
 * @param {Object} splits - { train: { y }, valid: { y }, test: { y } }
 * @returns {Object} Split-wise distribution analysis
 */
export function analyzeSplitDistributions(splits) {
  const results = {};

  for (const [splitName, splitData] of Object.entries(splits)) {
    if (!splitData.y) continue;

    const dist = calculateLabelDistribution(splitData.y);
    const imbalance = calculateImbalanceRatio(splitData.y);
    const entropy = calculateNormalizedEntropy(splitData.y);

    results[splitName] = {
      counts: dist.counts,
      percentages: dist.percentages,
      total: dist.total,
      imbalanceRatio: imbalance,
      normalizedEntropy: entropy
    };
  }

  // Check for distribution mismatch between splits
  const splitNames = Object.keys(results);
  const mismatches = [];

  for (let i = 0; i < splitNames.length; i++) {
    for (let j = i + 1; j < splitNames.length; j++) {
      const drift = calculateDistributionDrift(
        results[splitNames[i]].percentages,
        results[splitNames[j]].percentages
      );

      if (drift > 0.05) { // 5% threshold
        mismatches.push({
          split1: splitNames[i],
          split2: splitNames[j],
          drift: drift.toFixed(3)
        });
      }
    }
  }

  return {
    bySplit: results,
    mismatches,
    warning: mismatches.length > 0
      ? `Distribution mismatch detected between splits: ${mismatches.map(m => `${m.split1}/${m.split2}`).join(', ')}`
      : null
  };
}

/**
 * Generate comprehensive label distribution analysis
 * @param {number[]} y - Label array
 * @param {Object} options - Analysis options
 * @returns {Object} Complete analysis
 */
export function analyzeLabelDistribution(y, options = {}) {
  const {
    timestamps = null,
    windowSize = 1000,
    splits = null
  } = options;

  const distribution = calculateLabelDistribution(y);
  const imbalanceRatio = calculateImbalanceRatio(y);
  const normalizedEntropy = calculateNormalizedEntropy(y);
  const temporalDrift = detectTemporalLabelDrift(y, timestamps, windowSize);

  // Determine balance status
  let balanceStatus;
  if (imbalanceRatio <= 1.2) {
    balanceStatus = 'BALANCED';
  } else if (imbalanceRatio <= 2.0) {
    balanceStatus = 'SLIGHTLY_IMBALANCED';
  } else if (imbalanceRatio <= 5.0) {
    balanceStatus = 'IMBALANCED';
  } else {
    balanceStatus = 'SEVERELY_IMBALANCED';
  }

  // Split analysis if provided
  const splitAnalysis = splits ? analyzeSplitDistributions(splits) : null;

  // Generate recommendations
  const recommendations = [];

  if (imbalanceRatio > 2.0) {
    recommendations.push('Consider using class weights or oversampling for minority class');
  }

  if (temporalDrift.detected) {
    recommendations.push('Temporal drift detected - consider time-aware cross-validation');
  }

  if (splitAnalysis?.mismatches?.length > 0) {
    recommendations.push('Split distribution mismatch - verify stratified sampling');
  }

  return {
    overall: {
      counts: distribution.counts,
      percentages: distribution.percentages,
      total: distribution.total,
      imbalanceRatio,
      normalizedEntropy,
      balanceStatus
    },
    temporalDrift,
    splitAnalysis,
    recommendations,
    summary: {
      totalSamples: distribution.total,
      nClasses: Object.keys(distribution.counts).length,
      imbalanceRatio: imbalanceRatio.toFixed(2),
      entropy: normalizedEntropy.toFixed(3),
      balanceStatus,
      driftDetected: temporalDrift.detected
    }
  };
}
