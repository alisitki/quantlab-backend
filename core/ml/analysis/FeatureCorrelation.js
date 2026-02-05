/**
 * FeatureCorrelation - Feature correlation analysis module
 *
 * Calculates Pearson and Spearman correlations between features
 * to identify redundancy and multi-collinearity.
 */

/**
 * Calculate Pearson correlation coefficient between two arrays
 * @param {number[]} x
 * @param {number[]} y
 * @returns {number} Correlation coefficient (-1 to 1)
 */
export function calculatePearsonCorrelation(x, y) {
  if (x.length !== y.length || x.length === 0) return NaN;

  const n = x.length;
  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;

  for (let i = 0; i < n; i++) {
    sumX += x[i];
    sumY += y[i];
    sumXY += x[i] * y[i];
    sumX2 += x[i] * x[i];
    sumY2 += y[i] * y[i];
  }

  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

  if (denominator === 0) return 0;
  return numerator / denominator;
}

/**
 * Calculate Spearman rank correlation coefficient
 * @param {number[]} x
 * @param {number[]} y
 * @returns {number} Rank correlation coefficient (-1 to 1)
 */
export function calculateSpearmanCorrelation(x, y) {
  if (x.length !== y.length || x.length === 0) return NaN;

  const rankX = getRanks(x);
  const rankY = getRanks(y);

  return calculatePearsonCorrelation(rankX, rankY);
}

/**
 * Get ranks for an array (handles ties with average rank)
 * @param {number[]} arr
 * @returns {number[]} Ranks (1-based)
 */
function getRanks(arr) {
  const indexed = arr.map((val, idx) => ({ val, idx }));
  indexed.sort((a, b) => a.val - b.val);

  const ranks = new Array(arr.length);
  let i = 0;

  while (i < indexed.length) {
    let j = i;
    // Find all elements with the same value (ties)
    while (j < indexed.length && indexed[j].val === indexed[i].val) {
      j++;
    }
    // Assign average rank to ties
    const avgRank = (i + j + 1) / 2; // 1-based average
    for (let k = i; k < j; k++) {
      ranks[indexed[k].idx] = avgRank;
    }
    i = j;
  }

  return ranks;
}

/**
 * Calculate correlation matrix for all features
 * @param {number[][]} X - Feature matrix (rows = samples, cols = features)
 * @param {string[]} featureNames - Feature names
 * @param {string} method - 'pearson' or 'spearman'
 * @returns {Object} Correlation matrix and metadata
 */
export function calculateCorrelationMatrix(X, featureNames, method = 'pearson') {
  const nFeatures = featureNames.length;
  const nSamples = X.length;

  if (nSamples === 0 || nFeatures === 0) {
    return { matrix: [], featureNames: [], method };
  }

  // Extract columns
  const columns = [];
  for (let j = 0; j < nFeatures; j++) {
    columns.push(X.map(row => row[j]));
  }

  // Calculate correlation matrix
  const matrix = [];
  const correlationFn = method === 'spearman'
    ? calculateSpearmanCorrelation
    : calculatePearsonCorrelation;

  for (let i = 0; i < nFeatures; i++) {
    const row = [];
    for (let j = 0; j < nFeatures; j++) {
      if (i === j) {
        row.push(1.0);
      } else if (j < i) {
        // Symmetric matrix - reuse calculated value
        row.push(matrix[j][i]);
      } else {
        row.push(correlationFn(columns[i], columns[j]));
      }
    }
    matrix.push(row);
  }

  return {
    matrix,
    featureNames,
    method,
    nSamples
  };
}

/**
 * Find highly correlated feature pairs
 * @param {Object} correlationResult - Result from calculateCorrelationMatrix
 * @param {number} threshold - Correlation threshold (default 0.8)
 * @returns {Object[]} Array of highly correlated pairs
 */
export function findHighlyCorrelatedPairs(correlationResult, threshold = 0.8) {
  const { matrix, featureNames } = correlationResult;
  const pairs = [];

  for (let i = 0; i < matrix.length; i++) {
    for (let j = i + 1; j < matrix[i].length; j++) {
      const corr = matrix[i][j];
      if (Math.abs(corr) >= threshold) {
        pairs.push({
          feature_a: featureNames[i],
          feature_b: featureNames[j],
          correlation: corr,
          abs_correlation: Math.abs(corr)
        });
      }
    }
  }

  // Sort by absolute correlation descending
  pairs.sort((a, b) => b.abs_correlation - a.abs_correlation);

  return pairs;
}

/**
 * Calculate redundancy score (average of top correlations)
 * @param {Object} correlationResult - Result from calculateCorrelationMatrix
 * @param {number} topN - Number of top correlations to average
 * @returns {number} Redundancy score (0 to 1)
 */
export function calculateRedundancyScore(correlationResult, topN = 5) {
  const pairs = findHighlyCorrelatedPairs(correlationResult, 0);

  if (pairs.length === 0) return 0;

  const topCorrelations = pairs.slice(0, topN);
  const avgCorr = topCorrelations.reduce((sum, p) => sum + p.abs_correlation, 0) / topCorrelations.length;

  return avgCorr;
}

/**
 * Get feature clusters based on correlation
 * @param {Object} correlationResult
 * @param {number} threshold
 * @returns {string[][]} Clusters of correlated features
 */
export function getFeatureClusters(correlationResult, threshold = 0.8) {
  const { matrix, featureNames } = correlationResult;
  const n = featureNames.length;
  const visited = new Set();
  const clusters = [];

  for (let i = 0; i < n; i++) {
    if (visited.has(i)) continue;

    const cluster = [featureNames[i]];
    visited.add(i);

    for (let j = i + 1; j < n; j++) {
      if (visited.has(j)) continue;
      if (Math.abs(matrix[i][j]) >= threshold) {
        cluster.push(featureNames[j]);
        visited.add(j);
      }
    }

    if (cluster.length > 1) {
      clusters.push(cluster);
    }
  }

  return clusters;
}

/**
 * Generate full correlation analysis report
 * @param {number[][]} X - Feature matrix
 * @param {string[]} featureNames - Feature names
 * @param {Object} options - Analysis options
 * @returns {Object} Complete correlation analysis
 */
export function analyzeFeatureCorrelations(X, featureNames, options = {}) {
  const {
    method = 'pearson',
    highCorrThreshold = 0.8,
    clusterThreshold = 0.9
  } = options;

  const correlationResult = calculateCorrelationMatrix(X, featureNames, method);
  const highlyCorrelatedPairs = findHighlyCorrelatedPairs(correlationResult, highCorrThreshold);
  const redundancyScore = calculateRedundancyScore(correlationResult);
  const clusters = getFeatureClusters(correlationResult, clusterThreshold);

  // Identify features to potentially drop
  const dropCandidates = new Set();
  for (const pair of highlyCorrelatedPairs) {
    // Keep the first feature (alphabetically), drop the second
    if (pair.feature_a < pair.feature_b) {
      dropCandidates.add(pair.feature_b);
    } else {
      dropCandidates.add(pair.feature_a);
    }
  }

  return {
    matrix: correlationResult.matrix,
    featureNames,
    method,
    nSamples: correlationResult.nSamples,
    highlyCorrelatedPairs,
    redundancyScore,
    clusters,
    dropCandidates: Array.from(dropCandidates),
    summary: {
      totalFeatures: featureNames.length,
      highCorrPairs: highlyCorrelatedPairs.length,
      redundancyScore: redundancyScore.toFixed(3),
      clustersFound: clusters.length
    }
  };
}
