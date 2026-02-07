/**
 * Edge Validation Configuration
 *
 * Constants and defaults for edge validation pipeline.
 */

export const VALIDATION_CONFIG = {
  // Out-of-sample validation
  oos: {
    trainRatio: 0.7,           // 70% train
    testRatio: 0.3,            // 30% test
    minSharpeOOS: 0.5,         // Minimum out-of-sample Sharpe
    maxPerfDegradation: 0.5    // Max allowed (IS_sharpe - OOS_sharpe) / IS_sharpe
  },

  // Walk-forward analysis
  walkForward: {
    windowSize: 5000,          // Rows per window
    stepSize: 1000,            // Step between windows
    minWindowSharpe: 0,        // Min Sharpe per window (0 = allow negative)
    minPositiveWindows: 0.6    // Fraction of windows with positive Sharpe
  },

  // Decay detection
  decay: {
    windowSize: 1000,          // Window size for decay calculation
    maxDecayRate: -0.001,      // Maximum acceptable decay rate (negative slope)
    psiThreshold: 0.25         // PSI threshold for distribution shift
  },

  // Regime robustness
  regime: {
    minTradesPerRegime: 20,    // Min trades per regime to evaluate
    minRegimeSharpe: 0.3,      // Min Sharpe in target regimes
    selectivityThreshold: 0.2  // Min difference between target and other regimes
  },

  // Edge scoring
  scorer: {
    weights: {
      oos: 0.30,               // Out-of-sample performance
      walkForward: 0.25,       // Consistency over time
      decay: 0.20,             // Lack of decay
      regimeRobustness: 0.15,  // Regime specificity
      sampleSize: 0.10         // Sample size reliability
    },
    minScore: 0.5,             // Minimum score to validate
    weakThreshold: 0.4         // Below this = reject, above minScore = marginal
  }
};
