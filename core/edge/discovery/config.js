/**
 * Edge Discovery Configuration
 *
 * Constants and defaults for the edge discovery pipeline.
 */

export const DISCOVERY_CONFIG = {
  // Global seed for determinism
  seed: 42,

  // Feature configuration
  behaviorFeatures: [
    'liquidity_pressure',
    'return_momentum',
    'regime_stability',
    'spread_compression',
    'imbalance_acceleration',
    'micro_reversion',
    'quote_intensity',
    'behavior_divergence',
    'volatility_compression_score'
  ],

  regimeFeatures: [
    'volatility_ratio',
    'trend_strength',
    'spread_ratio'
  ],

  baseFeatures: [
    'mid_price',
    'spread',
    'return_1',
    'volatility'
  ],

  // Regime clustering
  regimeK: 4, // Number of regime clusters

  // Forward return horizons (in events)
  // 10 events ≈ 1-2 seconds, 50 events ≈ 5-10 seconds, 100 events ≈ 10-20 seconds
  forwardHorizons: [10, 50, 100],

  // Pattern scanning
  scanner: {
    minSupport: 30,              // Minimum pattern occurrence
    returnThreshold: 0.0005,     // 0.05% minimum mean return to consider
    thresholdLevels: [0.3, 0.5, 0.7], // Feature threshold scan levels
    quantileLevels: [0.1, 0.9],  // Quantile extremes to scan
    clusterK: 12,                // Number of micro-state clusters
    maxPatternsPerMethod: 200,   // Limit patterns per scan method
    maxCombinationDepth: 2       // Max features to combine in conditions
  },

  // Statistical testing
  tester: {
    minSampleSize: 30,           // Minimum occurrences to test
    pValueThreshold: 0.05,       // Maximum p-value for significance
    minSharpe: 0.5,              // Minimum Sharpe ratio
    permutationTestEnabled: process.env.DISCOVERY_PERMUTATION_TEST !== 'false', // DEFAULT: true (exact semantics)
    permutationN: 1000,          // Number of permutations for permutation test
    permutationMinHeapMB: 6144,  // Minimum heap required for permutation test (6 GB)
    multipleComparisonCorrection: true, // Apply Bonferroni correction
    minReturnMagnitude: 0.0003   // 0.03% minimum mean return magnitude
  },

  // Edge generation
  generator: {
    defaultTimeHorizon: 10000,   // 10 seconds default holding period
    maxEdgesPerRun: 20,          // Maximum edges to generate per discovery run
    minConfidenceScore: 0.6      // Minimum confidence score for generated edges
  }
};
