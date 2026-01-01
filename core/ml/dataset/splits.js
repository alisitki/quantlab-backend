/**
 * Deterministic time-based splitting without shuffling.
 */
import { ML_CONFIG } from '../config.js';

/**
 * Split data into train, valid, test sets.
 * @param {Array<any>} X
 * @param {Array<any>} y
 * @returns {Object} { train: {X, y}, valid: {X, y}, test: {X, y} }
 */
export function splitDataset(X, y) {
  const total = X.length;
  const trainIdx = Math.floor(total * ML_CONFIG.splits.train);
  const validIdx = trainIdx + Math.floor(total * ML_CONFIG.splits.valid);

  return {
    train: {
      X: X.slice(0, trainIdx),
      y: y.slice(0, trainIdx)
    },
    valid: {
      X: X.slice(trainIdx, validIdx),
      y: y.slice(trainIdx, validIdx)
    },
    test: {
      X: X.slice(validIdx),
      y: y.slice(validIdx)
    }
  };
}
