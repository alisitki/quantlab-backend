/**
 * PermutationImportance - Model-agnostic feature importance
 *
 * Measures feature importance by shuffling each feature
 * and measuring the drop in model performance.
 */

/**
 * Shuffle an array using Fisher-Yates algorithm
 * @param {any[]} arr - Array to shuffle
 * @param {number} seed - Random seed for reproducibility
 * @returns {any[]} Shuffled copy
 */
function shuffleArray(arr, seed = 42) {
  const shuffled = [...arr];
  let m = shuffled.length;
  let t, i;

  // Simple seeded random
  const random = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };

  while (m) {
    i = Math.floor(random() * m--);
    t = shuffled[m];
    shuffled[m] = shuffled[i];
    shuffled[i] = t;
  }

  return shuffled;
}

/**
 * Create a copy of X with one column shuffled
 * @param {number[][]} X - Feature matrix
 * @param {number} colIndex - Column index to shuffle
 * @param {number} seed - Random seed
 * @returns {number[][]} Matrix with shuffled column
 */
function shuffleColumn(X, colIndex, seed = 42) {
  // Extract column values
  const colValues = X.map(row => row[colIndex]);

  // Shuffle column values
  const shuffledCol = shuffleArray(colValues, seed);

  // Create new matrix with shuffled column
  return X.map((row, i) => {
    const newRow = [...row];
    newRow[colIndex] = shuffledCol[i];
    return newRow;
  });
}

/**
 * Calculate accuracy score
 * @param {number[]} yTrue - True labels
 * @param {number[]} yPred - Predicted labels
 * @returns {number} Accuracy (0 to 1)
 */
function calculateAccuracy(yTrue, yPred) {
  if (yTrue.length !== yPred.length || yTrue.length === 0) return 0;

  let correct = 0;
  for (let i = 0; i < yTrue.length; i++) {
    if (yTrue[i] === yPred[i]) correct++;
  }

  return correct / yTrue.length;
}

/**
 * Calculate permutation importance for all features
 * @param {Object} model - Model with predict(X) method
 * @param {number[][]} X - Test feature matrix
 * @param {number[]} y - Test labels
 * @param {string[]} featureNames - Feature names
 * @param {Object} options - Options
 * @returns {Object} Importance scores for each feature
 */
export async function calculatePermutationImportance(model, X, y, featureNames, options = {}) {
  const {
    nRepeats = 5,
    scoringFn = calculateAccuracy,
    seed = 42
  } = options;

  const nFeatures = featureNames.length;

  // Calculate baseline score
  const baselinePreds = model.predict(X);
  const baselineScore = scoringFn(y, baselinePreds);

  const importanceScores = {};

  for (let j = 0; j < nFeatures; j++) {
    const featureName = featureNames[j];
    const decreases = [];

    for (let r = 0; r < nRepeats; r++) {
      // Shuffle this feature
      const shuffledX = shuffleColumn(X, j, seed + r * 1000 + j);

      // Get predictions with shuffled feature
      const permutedPreds = model.predict(shuffledX);
      const permutedScore = scoringFn(y, permutedPreds);

      // Calculate decrease in performance
      const decrease = baselineScore - permutedScore;
      decreases.push(decrease);
    }

    // Calculate mean and std of decreases
    const meanDecrease = decreases.reduce((a, b) => a + b, 0) / nRepeats;
    const variance = decreases.reduce((sum, d) => sum + Math.pow(d - meanDecrease, 2), 0) / nRepeats;
    const stdDecrease = Math.sqrt(variance);

    importanceScores[featureName] = {
      meanDecrease,
      stdDecrease,
      decreases,
      isImportant: meanDecrease > stdDecrease // Simple significance test
    };
  }

  return {
    baselineAccuracy: baselineScore,
    importanceScores,
    nRepeats,
    nSamples: X.length
  };
}

/**
 * Rank features by permutation importance
 * @param {Object} importanceResult - Result from calculatePermutationImportance
 * @returns {Object[]} Sorted array of features
 */
export function rankFeaturesByImportance(importanceResult) {
  const { importanceScores } = importanceResult;

  const ranked = Object.entries(importanceScores)
    .map(([name, scores]) => ({
      feature: name,
      meanDecrease: scores.meanDecrease,
      stdDecrease: scores.stdDecrease,
      isImportant: scores.isImportant
    }))
    .sort((a, b) => b.meanDecrease - a.meanDecrease);

  // Add rank
  ranked.forEach((item, idx) => {
    item.rank = idx + 1;
  });

  return ranked;
}

/**
 * Get most important features
 * @param {Object} importanceResult
 * @param {number} n - Number of top features
 * @returns {string[]} Feature names
 */
export function getMostImportantFeatures(importanceResult, n = 5) {
  const ranked = rankFeaturesByImportance(importanceResult);
  return ranked
    .filter(r => r.meanDecrease > 0)
    .slice(0, n)
    .map(r => r.feature);
}

/**
 * Get least important features
 * @param {Object} importanceResult
 * @param {number} threshold - Maximum decrease to be considered unimportant
 * @returns {string[]} Feature names
 */
export function getLeastImportantFeatures(importanceResult, threshold = 0.005) {
  const ranked = rankFeaturesByImportance(importanceResult);
  return ranked
    .filter(r => r.meanDecrease <= threshold)
    .map(r => r.feature);
}

/**
 * Generate comprehensive importance analysis
 * @param {Object} model
 * @param {number[][]} X
 * @param {number[]} y
 * @param {string[]} featureNames
 * @param {Object} options
 * @returns {Object} Complete analysis
 */
export async function analyzeFeatureImportance(model, X, y, featureNames, options = {}) {
  const {
    nRepeats = 5,
    topN = 5,
    unimportantThreshold = 0.005
  } = options;

  const importanceResult = await calculatePermutationImportance(
    model, X, y, featureNames, { nRepeats }
  );

  const rankings = rankFeaturesByImportance(importanceResult);
  const mostImportant = getMostImportantFeatures(importanceResult, topN);
  const leastImportant = getLeastImportantFeatures(importanceResult, unimportantThreshold);

  // Normalize scores (0 to 1)
  const maxDecrease = Math.max(...rankings.map(r => r.meanDecrease), 0.001);
  const normalizedScores = {};

  for (const r of rankings) {
    normalizedScores[r.feature] = r.meanDecrease / maxDecrease;
  }

  // Count significant features
  const significantCount = rankings.filter(r => r.isImportant).length;

  return {
    baselineAccuracy: importanceResult.baselineAccuracy,
    importanceScores: importanceResult.importanceScores,
    normalizedScores,
    rankings,
    mostImportant,
    leastImportant,
    nRepeats,
    nSamples: importanceResult.nSamples,
    summary: {
      totalFeatures: featureNames.length,
      significantFeatures: significantCount,
      topFeature: rankings[0]?.feature || null,
      topFeatureImportance: rankings[0]?.meanDecrease?.toFixed(4) || 0,
      unimportantFeatureCount: leastImportant.length
    }
  };
}
