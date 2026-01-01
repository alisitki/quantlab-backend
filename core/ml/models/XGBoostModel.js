/**
 * XGBoostModel: Wrapper for XGBoost CPU.
 * Note: v1 assumes an environment where xgboost can be executed or linked.
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
    this.#probaSource = 'none'; // Reset source on new training
    console.log(`[XGBoost] Training on ${X.length} rows...`);

    // 1. Calculate Imbalance Weights
    // "neg" here covers everything that is not label_1 (following evaluate.js logic)
    let pos = 0;
    let neg = 0;
    for (const label of y) {
      if (label === 1) pos++;
      else neg++;
    }

    const scale_pos_weight = neg / Math.max(pos, 1);
    const pos_rate = pos / X.length;
    
    console.log(`[XGBoost] Class Imbalance Stats:`);
    console.log(`  - Pos: ${pos}, Neg: ${neg}, Total: ${X.length}`);
    console.log(`  - Pos Rate: ${(pos_rate * 100).toFixed(2)}%`);
    console.log(`  - Scale Pos Weight: ${scale_pos_weight.toFixed(4)} (neg/pos)`);

    // 2. Update Config
    this.#config = {
      ...this.#config,
      scale_pos_weight,
      max_delta_step: 1 // Helps with extreme imbalance
    };
    
    // Convert -1, 0, 1 labels to 0, 1, 2 for multi:softmax if needed
    const shiftedY = y.map(label => label + 1);

    // Placeholder for real XGBoost training logic.
    // In a real environment, we'd use native bindings or a CLI wrapper.
    // For now, we simulate a model being created with a stub predict.
    this.#model = { 
      trained: true, 
      timestamp: Date.now(),
      predict: (samples) => samples.map(() => (Math.random() * 4) - 2) // Sim logits [-2, 2]
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
    
    // Simulate prediction: just return 0 (neutral) for now
    // until we have real bindings.
    return new Array(X.length).fill(0);
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

    // 2. Try native predictProba
    if (typeof this.#model.predictProba === 'function') {
      this.#probaSource = 'model_predictProba';
      return this.#model.predictProba(X);
    }

    // 3. Fallback to predict with heuristic range check
    if (typeof this.#model.predict === 'function') {
      const scores = this.#model.predict(X);
      
      // Heuristic: Check if all scores are in [0, 1] range
      const isProba = scores.every(s => s >= 0 && s <= 1);
      
      if (isProba) {
        this.#probaSource = 'model_predict_prob';
        return scores;
      } else {
        this.#probaSource = 'model_predict_logit_sigmoid';
        return scores.map(s => this.#sigmoid(s));
      }
    }

    throw new Error('No proba source available. Model does not support predict() or predictProba().');
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
    fs.writeFileSync(filePath, JSON.stringify(this.#model, null, 2));
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
