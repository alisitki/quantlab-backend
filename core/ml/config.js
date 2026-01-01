/**
 * ML Module Global Configuration
 */
export const ML_CONFIG = {
  // Model Parameters
  xgb: {
    seed: 42,
    booster: 'gbtree',
    objective: 'multi:softmax',
    num_class: 3, // -1, 0, 1 mapping to 0, 1, 2
    eta: 0.3,
    max_depth: 6,
    nround: 100,
    use_gpu: false, // CPU first
  },

  // Dataset Splits
  splits: {
    train: 0.70,
    valid: 0.15,
    test: 0.15
  },

  // Artifact Paths
  paths: {
    models: './ml/artifacts/models/',
    datasets: './ml/artifacts/datasets/'
  },

  // Seed for any random operations to ensure determinism
  RANDOM_SEED: 42
};
