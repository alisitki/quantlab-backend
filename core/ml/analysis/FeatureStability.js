/**
 * FeatureStability - Feature stability analysis over time
 *
 * Uses Population Stability Index (PSI) to detect feature drift.
 */

import { calculateStats } from './FeatureDistribution.js';

/**
 * Create histogram bins for PSI calculation
 * @param {number[]} values
 * @param {number} nBins - Number of bins
 * @returns {Object} Bin edges and counts
 */
function createHistogram(values, nBins = 10) {
  if (!values || values.length === 0) {
    return { edges: [], counts: [], percentages: [] };
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const binWidth = (max - min) / nBins || 1;

  const edges = [];
  for (let i = 0; i <= nBins; i++) {
    edges.push(min + i * binWidth);
  }

  const counts = new Array(nBins).fill(0);

  for (const v of values) {
    let binIdx = Math.floor((v - min) / binWidth);
    if (binIdx >= nBins) binIdx = nBins - 1;
    if (binIdx < 0) binIdx = 0;
    counts[binIdx]++;
  }

  const total = values.length;
  const percentages = counts.map(c => c / total);

  return { edges, counts, percentages };
}

/**
 * Calculate Population Stability Index (PSI)
 * PSI measures how much a distribution has shifted from baseline
 *
 * PSI < 0.1: No significant shift
 * PSI 0.1-0.25: Moderate shift
 * PSI > 0.25: Significant shift
 *
 * @param {number[]} baseline - Baseline (expected) values
 * @param {number[]} comparison - Comparison (actual) values
 * @param {number} nBins - Number of bins
 * @returns {Object} PSI result
 */
export function calculatePSI(baseline, comparison, nBins = 10) {
  if (!baseline?.length || !comparison?.length) {
    return { psi: NaN, status: 'INSUFFICIENT_DATA' };
  }

  // Use baseline to define bin edges
  const min = Math.min(...baseline);
  const max = Math.max(...baseline);
  const binWidth = (max - min) / nBins || 1;

  // Calculate percentages for each bin
  const baselineHist = new Array(nBins).fill(0);
  const comparisonHist = new Array(nBins).fill(0);

  for (const v of baseline) {
    let binIdx = Math.floor((v - min) / binWidth);
    if (binIdx >= nBins) binIdx = nBins - 1;
    if (binIdx < 0) binIdx = 0;
    baselineHist[binIdx]++;
  }

  for (const v of comparison) {
    let binIdx = Math.floor((v - min) / binWidth);
    if (binIdx >= nBins) binIdx = nBins - 1;
    if (binIdx < 0) binIdx = 0;
    comparisonHist[binIdx]++;
  }

  // Convert to percentages with small epsilon to avoid division by zero
  const epsilon = 0.0001;
  const baselinePct = baselineHist.map(c => Math.max(c / baseline.length, epsilon));
  const comparisonPct = comparisonHist.map(c => Math.max(c / comparison.length, epsilon));

  // Calculate PSI
  let psi = 0;
  for (let i = 0; i < nBins; i++) {
    const diff = comparisonPct[i] - baselinePct[i];
    const ratio = comparisonPct[i] / baselinePct[i];
    psi += diff * Math.log(ratio);
  }

  // Determine status
  let status;
  if (psi < 0.1) {
    status = 'STABLE';
  } else if (psi < 0.25) {
    status = 'MODERATE_DRIFT';
  } else {
    status = 'SIGNIFICANT_DRIFT';
  }

  return {
    psi,
    status,
    nBins,
    baselineSamples: baseline.length,
    comparisonSamples: comparison.length
  };
}

/**
 * Calculate feature stability over time windows
 * @param {number[][]} X - Feature matrix
 * @param {number[]} timestamps - Timestamps for each row
 * @param {string[]} featureNames
 * @param {number} windowMs - Window size in milliseconds
 * @returns {Object} Stability analysis
 */
export function calculateFeatureStability(X, timestamps, featureNames, windowMs = 86400000) {
  if (!X?.length || !timestamps?.length) {
    return { windows: [], psiByFeature: {} };
  }

  const nSamples = X.length;
  const nFeatures = featureNames.length;

  // Sort by timestamp
  const sorted = timestamps.map((ts, idx) => ({ ts, idx })).sort((a, b) => a.ts - b.ts);
  const minTs = sorted[0].ts;
  const maxTs = sorted[nSamples - 1].ts;

  // Create time windows
  const windows = [];
  let windowStart = minTs;

  while (windowStart < maxTs) {
    const windowEnd = windowStart + windowMs;
    const indices = sorted
      .filter(s => s.ts >= windowStart && s.ts < windowEnd)
      .map(s => s.idx);

    if (indices.length > 0) {
      windows.push({
        startTs: windowStart,
        endTs: windowEnd,
        indices,
        sampleCount: indices.length
      });
    }

    windowStart = windowEnd;
  }

  if (windows.length < 2) {
    return {
      windows,
      psiByFeature: {},
      message: 'Insufficient windows for stability analysis'
    };
  }

  // Use first window as baseline
  const baselineWindow = windows[0];
  const psiByFeature = {};

  for (let j = 0; j < nFeatures; j++) {
    const featureName = featureNames[j];
    const baselineValues = baselineWindow.indices.map(i => X[i][j]);

    const windowPSIs = [];

    for (let w = 1; w < windows.length; w++) {
      const windowValues = windows[w].indices.map(i => X[i][j]);
      const psiResult = calculatePSI(baselineValues, windowValues);

      windowPSIs.push({
        windowIndex: w,
        startTs: windows[w].startTs,
        psi: psiResult.psi,
        status: psiResult.status
      });
    }

    // Calculate overall PSI (max PSI across windows)
    const maxPSI = Math.max(...windowPSIs.map(w => w.psi));
    const avgPSI = windowPSIs.reduce((sum, w) => sum + w.psi, 0) / windowPSIs.length;

    psiByFeature[featureName] = {
      maxPSI,
      avgPSI,
      windowPSIs,
      overallStatus: maxPSI < 0.1 ? 'STABLE' : maxPSI < 0.25 ? 'MODERATE_DRIFT' : 'SIGNIFICANT_DRIFT'
    };
  }

  return {
    windows: windows.map(w => ({
      startTs: w.startTs,
      endTs: w.endTs,
      sampleCount: w.sampleCount
    })),
    psiByFeature,
    baselineWindow: {
      startTs: baselineWindow.startTs,
      endTs: baselineWindow.endTs,
      sampleCount: baselineWindow.sampleCount
    }
  };
}

/**
 * Identify stable and unstable features
 * @param {Object} stabilityResult - Result from calculateFeatureStability
 * @param {number} threshold - PSI threshold (default 0.1)
 * @returns {Object} Stable and unstable feature lists
 */
export function categorizeFeaturesByStability(stabilityResult, threshold = 0.1) {
  const { psiByFeature } = stabilityResult;

  const stable = [];
  const unstable = [];

  for (const [name, result] of Object.entries(psiByFeature)) {
    if (result.maxPSI < threshold) {
      stable.push({ feature: name, maxPSI: result.maxPSI });
    } else {
      unstable.push({ feature: name, maxPSI: result.maxPSI, status: result.overallStatus });
    }
  }

  // Sort by PSI
  stable.sort((a, b) => a.maxPSI - b.maxPSI);
  unstable.sort((a, b) => b.maxPSI - a.maxPSI);

  return {
    stableFeatures: stable.map(f => f.feature),
    unstableFeatures: unstable.map(f => f.feature),
    stableDetails: stable,
    unstableDetails: unstable
  };
}

/**
 * Generate comprehensive stability report
 * @param {number[][]} X
 * @param {number[]} timestamps
 * @param {string[]} featureNames
 * @param {Object} options
 * @returns {Object} Complete stability analysis
 */
export function generateStabilityReport(X, timestamps, featureNames, options = {}) {
  const {
    windowMs = 86400000, // 1 day
    stableThreshold = 0.1,
    moderateThreshold = 0.25
  } = options;

  const stabilityResult = calculateFeatureStability(X, timestamps, featureNames, windowMs);
  const categorization = categorizeFeaturesByStability(stabilityResult, stableThreshold);

  // Count by status
  const statusCounts = {
    STABLE: 0,
    MODERATE_DRIFT: 0,
    SIGNIFICANT_DRIFT: 0
  };

  for (const result of Object.values(stabilityResult.psiByFeature)) {
    statusCounts[result.overallStatus]++;
  }

  // Generate recommendations
  const recommendations = [];

  if (categorization.unstableFeatures.length > 0) {
    recommendations.push(
      `${categorization.unstableFeatures.length} features show drift. Consider retraining model periodically.`
    );
  }

  if (statusCounts.SIGNIFICANT_DRIFT > featureNames.length / 4) {
    recommendations.push(
      'Many features show significant drift. Consider using time-aware validation.'
    );
  }

  return {
    ...stabilityResult,
    categorization,
    statusCounts,
    recommendations,
    summary: {
      totalFeatures: featureNames.length,
      stableFeatureCount: categorization.stableFeatures.length,
      unstableFeatureCount: categorization.unstableFeatures.length,
      windowCount: stabilityResult.windows.length,
      windowSizeMs: windowMs
    }
  };
}
