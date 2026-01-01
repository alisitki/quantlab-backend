/**
 * evaluate.js: Calculates final performance metrics with strict stabilization guards.
 */
export function evaluateModel(model, testData) {
  const preds = model.predict(testData.X);
  const actuals = testData.y;
  const n = preds.length;

  // 1. Label Distribution
  let label_1_count = 0;
  let label_0_count = 0;
  
  for (let i = 0; i < n; i++) {
    if (actuals[i] === 1) label_1_count++;
    else label_0_count++;
  }

  const label_distribution = {
    label_1: label_1_count,
    label_0: label_0_count,
    total: n
  };

  // 2. Metrics Calculation (Detailed for Imbalance)
  let tp = 0;
  let fp = 0;
  let tn = 0;
  let fn = 0;
  
  for (let i = 0; i < n; i++) {
    const p = preds[i];
    const a = actuals[i];

    if (p === 1 && a === 1) tp++;
    else if (p === 1 && a !== 1) fp++;
    else if (p !== 1 && a === 1) fn++;
    else tn++; // p!=1, a!=1 (assuming binary task or "rest" is negative)
  }

  const pred_pos_count = tp + fp;
  const pred_pos_rate = n > 0 ? pred_pos_count / n : 0;
  
  // -- Precision (Precision of 1s) --
  // If no positive predictions, precision is undefined (null)
  const precision_pos = pred_pos_count > 0 ? tp / pred_pos_count : null;

  // -- Recall / TPR (Recall of 1s) --
  // If no positive labels, recall is undefined (null)
  const recall_pos = label_1_count > 0 ? tp / label_1_count : null;

  // -- F1 Score --
  let f1_pos = null;
  if (precision_pos !== null && recall_pos !== null) {
    const denom = precision_pos + recall_pos;
    f1_pos = denom > 0 ? (2 * precision_pos * recall_pos) / denom : 0;
  }

  // -- TNR (Specificity) --
  // If no negative labels, TNR is undefined (null)
  const tnr = label_0_count > 0 ? tn / label_0_count : null;

  // -- Balanced Accuracy --
  // (TPR + TNR) / 2. If one is missing, we can't fully calculate it.
  // Standard scikit-learn behavior: if a class is missing, warn. 
  // Here we return null if either is missing.
  let balancedAccuracy = null;
  if (recall_pos !== null && tnr !== null) {
    balancedAccuracy = (recall_pos + tnr) / 2;
  }

  // 3. Stabilization Guards & Status
  let evaluation_status = 'ok';
  let reason = 'nominal';

  // Legacy fields for backward compatibility
  // directionalHitRate IS precision_pos
  let directionalHitRate = precision_pos;
  const maxDrawdown = null; 

  // Guard: No Positive Labels (Dataset issue)
  if (label_1_count === 0) {
    evaluation_status = 'no_positive_labels';
    reason = 'validation set has no positive samples';
  }
  // Guard: No Positive Predictions
  else if (pred_pos_count === 0) {
    evaluation_status = 'no_positive_predictions';
    reason = 'model only predicts negative class (0)';
    // directionalHitRate stays null
  }

  // Safety override for directionalHitRate (ensure it's not null unless status implies it)
  if (directionalHitRate === null && evaluation_status === 'ok') {
     // Should not happen given logic above, but good specific safety
     directionalHitRate = 0; 
  }
  
  const accuracy = n > 0 ? (tp + tn) / n : 0;

  return {
    accuracy,
    balancedAccuracy,
    precision_pos,
    recall_pos,
    f1_pos,
    directionalHitRate,
    maxDrawdown,
    directionalSampleSize: pred_pos_count,
    pred_pos_count,
    pred_pos_rate,
    confusion_matrix: { tp, fp, tn, fn },
    label_distribution,
    evaluation_status,
    reason
  };
}

/**
 * Evaluate model using probability threshold for signal generation.
 * signal_long = (pred_proba >= threshold)
 * @param {Array<number>} probas - P(y=1) probabilities from model.predictProba()
 * @param {Array<number>} actuals - Ground truth labels
 * @param {number} threshold - Probability threshold for positive signal
 * @returns {Object} Threshold-aware metrics
 */
export function evaluateWithThreshold(probas, actuals, threshold) {
  const n = probas.length;
  
  // Generate binary predictions from threshold
  const preds = probas.map(p => p >= threshold ? 1 : 0);

  // Label distribution
  let label_1_count = 0;
  let label_0_count = 0;
  for (let i = 0; i < n; i++) {
    if (actuals[i] === 1) label_1_count++;
    else label_0_count++;
  }

  // Confusion matrix
  let tp = 0, fp = 0, tn = 0, fn = 0;
  for (let i = 0; i < n; i++) {
    const p = preds[i];
    const a = actuals[i];
    if (p === 1 && a === 1) tp++;
    else if (p === 1 && a !== 1) fp++;
    else if (p !== 1 && a === 1) fn++;
    else tn++;
  }

  const pred_pos_count = tp + fp;
  const pred_pos_rate = n > 0 ? pred_pos_count / n : 0;

  // Precision, Recall, F1
  const precision_pos = pred_pos_count > 0 ? tp / pred_pos_count : null;
  const recall_pos = label_1_count > 0 ? tp / label_1_count : null;
  
  let f1_pos = null;
  if (precision_pos !== null && recall_pos !== null) {
    const denom = precision_pos + recall_pos;
    f1_pos = denom > 0 ? (2 * precision_pos * recall_pos) / denom : 0;
  }

  // TNR and Balanced Accuracy
  const tnr = label_0_count > 0 ? tn / label_0_count : null;
  let balancedAccuracy = null;
  if (recall_pos !== null && tnr !== null) {
    balancedAccuracy = (recall_pos + tnr) / 2;
  }

  // Status
  let evaluation_status = 'ok';
  let reason = 'nominal';
  if (label_1_count === 0) {
    evaluation_status = 'no_positive_labels';
    reason = 'validation set has no positive samples';
  } else if (pred_pos_count === 0) {
    evaluation_status = 'no_positive_predictions';
    reason = 'threshold too high - no positive predictions';
  }

  return {
    pred_pos_count,
    pred_pos_rate,
    precision_pos,
    recall_pos,
    f1_pos,
    balancedAccuracy,
    confusion_matrix: { tp, fp, tn, fn },
    evaluation_status,
    reason
  };
}

/**
 * Evaluate model across a grid of probability thresholds.
 * @param {Object} model - Model with predictProba() and getProbaSource()
 * @param {Object} testData - { X, y }
 * @param {Array<number>} thresholds - Array of thresholds to evaluate
 * @returns {Object} Full metrics with threshold_results and best_threshold
 */
export function evaluateThresholdGrid(model, testData, thresholds = [0.50, 0.55, 0.60, 0.65, 0.70]) {
  const probas = model.predictProba(testData.X);
  const actuals = testData.y;
  const probaSource = model.getProbaSource();
  
  // Debug: proba stats
  const probaMin = Math.min(...probas);
  const probaMax = Math.max(...probas);
  const probaMean = probas.reduce((a, b) => a + b, 0) / probas.length;
  console.log(`[EvalGrid] proba stats: min=${probaMin.toFixed(4)}, mean=${probaMean.toFixed(4)}, max=${probaMax.toFixed(4)}`);
  
  // Sort thresholds ascending for deterministic ordering
  const sortedThresholds = [...thresholds].sort((a, b) => a - b);
  
  const threshold_results = {};
  let bestThreshold = null;
  let bestF1 = -1;

  for (const t of sortedThresholds) {
    const key = t.toFixed(2);
    const result = evaluateWithThreshold(probas, actuals, t);
    threshold_results[key] = result;
    
    // Debug: per-threshold pred_pos_count
    console.log(`[EvalGrid] t=${key} pred_pos=${result.pred_pos_count}`);
    
    // Track best by f1_pos (skip null)
    if (result.f1_pos !== null && result.f1_pos > bestF1) {
      bestF1 = result.f1_pos;
      bestThreshold = t;
    }
  }

  const proba_stats = {
    min: probaMin,
    max: probaMax,
    mean: probaMean
  };

  // Label distribution (compute once) - for TEST SPLIT only
  let label_1_count = 0;
  let label_0_count = 0;
  for (const a of actuals) {
    if (a === 1) label_1_count++;
    else label_0_count++;
  }

  // Legacy metrics using default threshold 0.50
  const defaultKey = '0.50';
  const defaultResult = threshold_results[defaultKey] || threshold_results[sortedThresholds[0].toFixed(2)];

  return {
    proba_source: probaSource,
    proba_stats,
    threshold_results,
    best_threshold: bestThreshold !== null 
      ? { by: 'f1_pos', value: bestThreshold, f1_pos: bestF1 }
      : { by: 'f1_pos', value: null, f1_pos: null },
    // Legacy fields for backward compatibility
    accuracy: defaultResult ? (defaultResult.confusion_matrix.tp + defaultResult.confusion_matrix.tn) / actuals.length : 0,
    balancedAccuracy: defaultResult?.balancedAccuracy ?? null,
    precision_pos: defaultResult?.precision_pos ?? null,
    recall_pos: defaultResult?.recall_pos ?? null,
    f1_pos: defaultResult?.f1_pos ?? null,
    directionalHitRate: defaultResult?.precision_pos ?? null,
    maxDrawdown: null,
    directionalSampleSize: defaultResult?.pred_pos_count ?? 0,
    pred_pos_count: defaultResult?.pred_pos_count ?? 0,
    pred_pos_rate: defaultResult?.pred_pos_rate ?? 0,
    label_distribution: { label_1: label_1_count, label_0: label_0_count, total: actuals.length },
    label_distribution_scope: 'test_split',
    evaluation_status: defaultResult?.evaluation_status ?? 'unknown',
    reason: defaultResult?.reason ?? 'unknown'
  };
}
