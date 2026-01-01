/**
 * applyDecision: Apply threshold to probability array to generate signals.
 */

/**
 * @typedef {Object} SignalResult
 * @property {boolean[]} signals - Signal array (true = long signal)
 * @property {number} pred_pos_count - Number of positive signals
 * @property {number} pred_pos_rate - Rate of positive signals
 * @property {number} thresholdUsed - Threshold that was applied
 * @property {string} probaSource - Source of probabilities
 */

/**
 * Apply decision threshold to probability array.
 * @param {number[]} probaArray - Array of probabilities [0, 1]
 * @param {Object} decisionConfig - Decision config with bestThreshold
 * @returns {SignalResult}
 */
export function applyDecision(probaArray, decisionConfig) {
  const threshold = decisionConfig.bestThreshold ?? 0.5;
  const probaSource = decisionConfig.probaSource || 'unknown';
  
  // Generate signals
  const signals = probaArray.map(p => p >= threshold);
  
  // Calculate stats
  const pred_pos_count = signals.filter(s => s).length;
  const pred_pos_rate = probaArray.length > 0 ? pred_pos_count / probaArray.length : 0;
  
  return {
    signals,
    pred_pos_count,
    pred_pos_rate,
    thresholdUsed: threshold,
    probaSource
  };
}

/**
 * Calculate probability statistics.
 * @param {number[]} probaArray
 * @returns {{min: number, mean: number, max: number}}
 */
export function getProbaStats(probaArray) {
  if (probaArray.length === 0) {
    return { min: 0, mean: 0, max: 0 };
  }
  
  const min = Math.min(...probaArray);
  const max = Math.max(...probaArray);
  const mean = probaArray.reduce((a, b) => a + b, 0) / probaArray.length;
  
  return { min, mean, max };
}
