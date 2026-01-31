/**
 * XGBoostModel: Wrapper for XGBoost CPU.
 * Note: Deterministic linear softmax model (XGBoost replacement) for v1.
 */
import { ML_CONFIG } from '../config.js';

export class XGBoostModel {
  #config;
  #model = null;
  #probaSource = 'none';

  constructor(config = ML_CONFIG.xgb) {
    this.#config = config;
  }

  /**
   * Train the XGBoost model.
   * @param {Array<Array<number>>} X
   * @param {Array<number>} y
   */
  async train(X, y) {
    this.#probaSource = 'softmax';
    console.log(`[XGBoost] Training on ${X.length} rows...`);

    const seed = Number.isFinite(this.#config.seed) ? Number(this.#config.seed) : ML_CONFIG.RANDOM_SEED;
    const epochs = Number.isFinite(this.#config.epochs) ? Number(this.#config.epochs) : 50;
    const lr = Number.isFinite(this.#config.eta) ? Number(this.#config.eta) : 0.05;
    const l2 = Number.isFinite(this.#config.l2) ? Number(this.#config.l2) : 1e-4;

    const classes = 3; // -1,0,1 -> 0,1,2
    const dims = X[0]?.length || 0;
    const weights = initWeights(classes, dims + 1, seed);

    for (let epoch = 0; epoch < epochs; epoch++) {
      for (let i = 0; i < X.length; i++) {
        const features = X[i];
        const label = y[i] + 1;
        const logits = computeLogits(weights, features);
        const probs = softmax(logits);
        for (let c = 0; c < classes; c++) {
          const target = c === label ? 1 : 0;
          const grad = probs[c] - target;
          updateWeights(weights[c], features, grad, lr, l2);
        }
      }
    }

    this.#model = {
      trained: true,
      weights
    };

    console.log(`[XGBoost] Training complete.`);
  }

  /**
   * Predict labels for given features.
   * @param {Array<Array<number>>} X
   * @returns {Array<number>}
   */
  predict(X) {
    if (!this.#model) throw new Error('Model not trained.');
    const outputs = new Array(X.length);
    for (let i = 0; i < X.length; i++) {
      const logits = computeLogits(this.#model.weights, X[i]);
      const cls = argMax(logits);
      outputs[i] = cls - 1;
    }
    return outputs;
  }

  /**
   * Predict probabilities for positive class.
   * Uses heuristic detection to determine if model returns probs or logits.
   * @param {Array<Array<number>>} X
   * @returns {Array<number>} P(y=1) for each sample
   */
  predictProba(X) {
    // 1. Pseudo-proba fallback (highest priority if enabled)
    if (process.env.PSEUDO_PROBA === '1') {
      this.#probaSource = 'pseudo_sigmoid';
      return this.#generatePseudoProba(X);
    }

    if (!this.#model) throw new Error('Model not trained.');

    this.#probaSource = 'softmax';
    const probs = new Array(X.length);
    for (let i = 0; i < X.length; i++) {
      const logits = computeLogits(this.#model.weights, X[i]);
      const sm = softmax(logits);
      probs[i] = sm[2]; // class +1
    }
    return probs;
  }

  /**
   * Returns the source of probability predictions.
   * @returns {string}
   */
  getProbaSource() {
    return this.#probaSource;
  }

  /**
   * Sigmoid activation function.
   */
  #sigmoid(x) {
    return 1 / (1 + Math.exp(-x));
  }

  /**
   * Helper to generate old-style pseudo probabilities.
   */
  #generatePseudoProba(X) {
    const smallScaleIndices = [1, 2, 3, 5, 6, 7, 8, 9];
    return X.map(row => {
      let sum = 0;
      for (const i of smallScaleIndices) {
        if (i < row.length) sum += row[i];
      }
      const scale = 10;
      const score = Math.max(-5, Math.min(5, sum / scale));
      return this.#sigmoid(score);
    });
  }

  /**
   * Save model artifact.
   * @param {string} filePath 
   */
  async save(filePath) {
    console.log(`[XGBoost] Saving model to ${filePath}`);
    const fs = await import('fs');
    const payload = serializeModel(this.#model);
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
  }

  /**
   * Load model artifact.
   * @param {string} filePath 
   */
  async load(filePath) {
    console.log(`[XGBoost] Loading model from ${filePath}`);
    const fs = await import('fs');
    const data = fs.readFileSync(filePath, 'utf-8');
    this.#model = JSON.parse(data);
  }

  /**
   * For testing purposes only: Inject a mock model.
   */
  loadModelForTest(mockModel) {
    this.#model = mockModel;
    this.#probaSource = 'none';
  }

  /**
   * For testing purposes only.
   */
  getConfig() {
    return this.#config;
  }
}

function initWeights(classes, dims, seed) {
  const rng = new XorShift32(seed || 1);
  const weights = [];
  for (let c = 0; c < classes; c++) {
    const row = new Array(dims);
    for (let i = 0; i < dims; i++) {
      row[i] = (rng.next() - 0.5) * 0.01;
    }
    weights.push(row);
  }
  return weights;
}

function computeLogits(weights, features) {
  const dims = features.length;
  const logits = new Array(weights.length);
  for (let c = 0; c < weights.length; c++) {
    let sum = weights[c][0]; // bias
    for (let i = 0; i < dims; i++) {
      sum += weights[c][i + 1] * features[i];
    }
    logits[c] = sum;
  }
  return logits;
}

function softmax(logits) {
  let max = logits[0];
  for (let i = 1; i < logits.length; i++) {
    if (logits[i] > max) max = logits[i];
  }
  const exps = new Array(logits.length);
  let sum = 0;
  for (let i = 0; i < logits.length; i++) {
    const value = Math.exp(logits[i] - max);
    exps[i] = value;
    sum += value;
  }
  for (let i = 0; i < exps.length; i++) {
    exps[i] = exps[i] / sum;
  }
  return exps;
}

function updateWeights(weightsRow, features, grad, lr, l2) {
  weightsRow[0] -= lr * (grad + l2 * weightsRow[0]);
  for (let i = 0; i < features.length; i++) {
    const idx = i + 1;
    weightsRow[idx] -= lr * (grad * features[i] + l2 * weightsRow[idx]);
  }
}

function argMax(values) {
  let idx = 0;
  let max = values[0];
  for (let i = 1; i < values.length; i++) {
    if (values[i] > max) {
      max = values[i];
      idx = i;
    }
  }
  return idx;
}

class XorShift32 {
  constructor(seed) {
    let s = seed >>> 0;
    if (s === 0) s = 1;
    this.state = s;
  }

  next() {
    let x = this.state;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.state = x >>> 0;
    return this.state / 0xffffffff;
  }
}

function serializeModel(model) {
  if (!model || !model.weights) return model;
  const weights = model.weights.map((row) => row.map((v) => round(v)));
  return { trained: true, weights };
}

function round(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value * 1e12) / 1e12;
}
