/**
 * Scheduler Configuration
 * Centralized configuration for daily ML training jobs.
 */

export const SCHEDULER_CONFIG = {
  // Default symbols to train
  defaultSymbols: ['btcusdt', 'ethusdt', 'solusdt'],
  
  // Date range logic
  dateRange: {
    // Default: train on yesterday's data
    daysBack: 1,
    // For multi-day training windows
    windowSize: 1
  },
  
  // Model configuration
  model: {
    type: 'xgboost',
    params: {
      nround: 100,
      maxDepth: 6,
      eta: 0.1,
      objective: 'multi:softmax',
      numClass: 3
    },
    featureParams: {
      enabledFeatures: ['mid_price', 'spread', 'return_1', 'volatility_10']
    }
  },
  
  // GPU requirements
  gpu: {
    // Preferred GPU types (in order of preference)
    preferredTypes: ['RTX_3090', 'RTX_4090', 'A100', 'RTX 5090', 'RTX 5070 Ti', 'RTX PRO 6000'],
    // Maximum hourly cost in USD
    maxHourlyCost: 1.0,
    // Maximum runtime in minutes (safety cutoff)
    maxRuntimeMin: 60,
    // Minimum GPU memory in GB
    minGpuMemory: 16,
    // Minimum disk space in GB
    minDiskSpace: 20
  },
  
  // GitHub repo for cloning on GPU
  repo: {
    url: process.env.REPO_URL || 'https://github.com/alisitki/quantlab-backend.git',
    branch: process.env.REPO_BRANCH || 'main',
    commit: process.env.REPO_COMMIT || null
  },
  
  // S3 paths for artifacts
  s3: {
    artifactBucket: process.env.S3_ARTIFACTS_BUCKET || 'quantlab-artifacts',
    artifactEndpoint: process.env.S3_ARTIFACTS_ENDPOINT || process.env.S3_COMPACT_ENDPOINT,
    artifactPrefix: 'ml-artifacts',
    productionPrefix: 'models/production',
    promoteMode: 'off'
  }
};
