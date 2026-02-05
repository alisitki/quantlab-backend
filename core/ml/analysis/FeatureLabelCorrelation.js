/**
 * FeatureLabelCorrelation - Feature-label correlation analysis
 *
 * Measures how well each feature predicts the target label.
 * Uses point-biserial correlation for binary labels.
 */

import { calculatePearsonCorrelation } from './FeatureCorrelation.js';

/**
 * Calculate point-biserial correlation coefficient
 * Used when one variable is continuous and one is binary (0/1)
 * @param {number[]} continuousVar - Continuous feature values
 * @param {number[]} binaryVar - Binary label values (0 or 1)
 * @returns {number} Point-biserial correlation (-1 to 1)
 */
export function calculatePointBiserial(continuousVar, binaryVar) {
  if (continuousVar.length !== binaryVar.length || continuousVar.length === 0) {
    return NaN;
  }

  const n = continuousVar.length;

  // Separate into two groups based on binary variable
  const group0 = [];
  const group1 = [];

  for (let i = 0; i < n; i++) {
    if (binaryVar[i] === 0 || binaryVar[i] === -1) {
      group0.push(continuousVar[i]);
    } else {
      group1.push(continuousVar[i]);
    }
  }

  const n0 = group0.length;
  const n1 = group1.length;

  if (n0 === 0 || n1 === 0) return NaN;

  // Calculate means
  const mean0 = group0.reduce((a, b) => a + b, 0) / n0;
  const mean1 = group1.reduce((a, b) => a + b, 0) / n1;

  // Calculate overall standard deviation
  const overallMean = continuousVar.reduce((a, b) => a + b, 0) / n;
  const variance = continuousVar.reduce((sum, x) => sum + Math.pow(x - overallMean, 2), 0) / n;
  const std = Math.sqrt(variance);

  if (std === 0) return 0;

  // Point-biserial formula
  const rpb = ((mean1 - mean0) / std) * Math.sqrt((n0 * n1) / (n * n));

  return rpb;
}

/**
 * Calculate feature-label correlations for all features
 * @param {number[][]} X - Feature matrix (rows = samples, cols = features)
 * @param {number[]} y - Label array (binary: 0/1 or -1/1)
 * @param {string[]} featureNames - Feature names
 * @returns {Object} Correlation results for each feature
 */
export function calculateFeatureLabelCorrelation(X, y, featureNames) {
  const nFeatures = featureNames.length;
  const nSamples = X.length;

  if (nSamples === 0 || nFeatures === 0) {
    return { correlations: {}, rankings: [], summary: {} };
  }

  // Determine if labels are binary
  const uniqueLabels = [...new Set(y)];
  const isBinary = uniqueLabels.length === 2;

  const correlations = {};

  for (let j = 0; j < nFeatures; j++) {
    const featureCol = X.map(row => row[j]);
    const featureName = featureNames[j];

    // Calculate Pearson correlation
    const pearson = calculatePearsonCorrelation(featureCol, y);

    // Calculate point-biserial if binary
    const pointBiserial = isBinary ? calculatePointBiserial(featureCol, y) : null;

    // Use point-biserial for binary labels, Pearson otherwise
    const primaryCorr = isBinary ? pointBiserial : pearson;

    correlations[featureName] = {
      pearson,
      pointBiserial,
      primaryCorrelation: primaryCorr,
      absCorrelation: Math.abs(primaryCorr || 0),
      predictiveStrength: classifyPredictiveStrength(Math.abs(primaryCorr || 0))
    };
  }

  return {
    correlations,
    isBinaryLabel: isBinary,
    nSamples,
    nFeatures
  };
}

/**
 * Classify predictive strength based on absolute correlation
 * @param {number} absCorr - Absolute correlation value
 * @returns {string} Strength classification
 */
function classifyPredictiveStrength(absCorr) {
  if (absCorr >= 0.5) return 'STRONG';
  if (absCorr >= 0.3) return 'MODERATE';
  if (absCorr >= 0.1) return 'WEAK';
  return 'NEGLIGIBLE';
}

/**
 * Rank features by their correlation with the label
 * @param {Object} correlationResult - Result from calculateFeatureLabelCorrelation
 * @returns {Object[]} Sorted array of features by predictive power
 */
export function rankFeaturesByLabelCorrelation(correlationResult) {
  const { correlations } = correlationResult;

  const ranked = Object.entries(correlations)
    .map(([name, stats]) => ({
      feature: name,
      correlation: stats.primaryCorrelation,
      absCorrelation: stats.absCorrelation,
      strength: stats.predictiveStrength
    }))
    .sort((a, b) => b.absCorrelation - a.absCorrelation);

  // Add rank
  ranked.forEach((item, idx) => {
    item.rank = idx + 1;
  });

  return ranked;
}

/**
 * Get top N most predictive features
 * @param {Object} correlationResult
 * @param {number} n - Number of top features
 * @returns {string[]} Feature names
 */
export function getTopFeatures(correlationResult, n = 5) {
  const ranked = rankFeaturesByLabelCorrelation(correlationResult);
  return ranked.slice(0, n).map(r => r.feature);
}

/**
 * Get weak features (low predictive power)
 * @param {Object} correlationResult
 * @param {number} threshold - Correlation threshold below which features are weak
 * @returns {string[]} Feature names
 */
export function getWeakFeatures(correlationResult, threshold = 0.05) {
  const ranked = rankFeaturesByLabelCorrelation(correlationResult);
  return ranked
    .filter(r => r.absCorrelation < threshold)
    .map(r => r.feature);
}

/**
 * Analyze feature-label relationships comprehensively
 * @param {number[][]} X
 * @param {number[]} y
 * @param {string[]} featureNames
 * @param {Object} options
 * @returns {Object} Complete analysis
 */
export function analyzeFeatureLabelRelationships(X, y, featureNames, options = {}) {
  const {
    topN = 5,
    weakThreshold = 0.05
  } = options;

  const correlationResult = calculateFeatureLabelCorrelation(X, y, featureNames);
  const rankings = rankFeaturesByLabelCorrelation(correlationResult);
  const topFeatures = getTopFeatures(correlationResult, topN);
  const weakFeatures = getWeakFeatures(correlationResult, weakThreshold);

  // Count by strength category
  const strengthCounts = {
    STRONG: 0,
    MODERATE: 0,
    WEAK: 0,
    NEGLIGIBLE: 0
  };

  for (const r of rankings) {
    strengthCounts[r.strength]++;
  }

  // Calculate average correlation
  const avgCorrelation = rankings.length > 0
    ? rankings.reduce((sum, r) => sum + r.absCorrelation, 0) / rankings.length
    : 0;

  return {
    correlations: correlationResult.correlations,
    rankings,
    topFeatures,
    weakFeatures,
    isBinaryLabel: correlationResult.isBinaryLabel,
    nSamples: correlationResult.nSamples,
    nFeatures: correlationResult.nFeatures,
    summary: {
      avgAbsCorrelation: avgCorrelation.toFixed(4),
      strengthDistribution: strengthCounts,
      topFeatureCount: topFeatures.length,
      weakFeatureCount: weakFeatures.length,
      recommendation: generateRecommendation(rankings, weakFeatures)
    }
  };
}

/**
 * Generate feature selection recommendation
 * @param {Object[]} rankings
 * @param {string[]} weakFeatures
 * @returns {string}
 */
function generateRecommendation(rankings, weakFeatures) {
  if (weakFeatures.length === 0) {
    return 'All features have meaningful predictive power.';
  }

  if (weakFeatures.length >= rankings.length / 2) {
    return `WARNING: ${weakFeatures.length}/${rankings.length} features have negligible predictive power. Consider feature engineering.`;
  }

  return `Consider removing weak features: ${weakFeatures.slice(0, 3).join(', ')}${weakFeatures.length > 3 ? '...' : ''}`;
}
