/**
 * Learning System Configuration
 *
 * Central configuration for closed-loop learning components:
 * - TradeOutcomeCollector: JSONL logging of trade outcomes
 * - EdgeConfidenceUpdater: Live confidence updates from performance
 * - EdgeRevalidationRunner: Drift-triggered re-validation
 * - LearningScheduler: Daily/weekly/monthly learning loops
 * - FeatureImportanceTracker: Feature importance analysis
 * - BehaviorRefinementEngine: Refinement proposals
 */

export const LEARNING_CONFIG = {
  /**
   * TradeOutcomeCollector Configuration
   */
  outcome: {
    // Directory for outcome JSONL files
    logDir: 'data/learning/outcomes',

    // Flush buffer interval (ms)
    flushIntervalMs: 5000,

    // Feature value precision (decimal places)
    featureDecimals: 6,

    // Max file size before rotation (bytes)
    maxFileSize: 50 * 1024 * 1024  // 50MB
  },

  /**
   * EdgeConfidenceUpdater Configuration
   */
  confidence: {
    // Minimum trades before updating confidence
    minSampleSize: 30,

    // EMA decay weight (alpha) - how much new trades affect confidence
    // 0.05 = 5% weight on new trade, 95% on existing
    decayWeight: 0.05,

    // Revalidation trigger thresholds
    revalidationTrigger: {
      // Confidence drop from baseline that triggers re-validation
      confidenceDrop: 0.15,  // 15% drop

      // Consecutive losses that trigger re-validation
      consecutiveLosses: 10,

      // Win rate drop from baseline that triggers re-validation
      winRateDrop: 0.10  // 10% drop
    }
  },

  /**
   * EdgeRevalidationRunner Configuration
   */
  revalidation: {
    // Minimum data rows required for re-validation
    minDataRows: 500,

    // Cooldown period before same edge can be re-validated (hours)
    cooldownHours: 24,

    // Maximum concurrent re-validations
    maxConcurrent: 3
  },

  /**
   * LearningScheduler Configuration
   */
  schedule: {
    // Daily run hour (UTC)
    dailyHourUTC: 4,  // 4 AM UTC

    // Weekly run day (0=Sunday, 6=Saturday)
    weeklyDayUTC: 0,  // Sunday

    // Monthly run day (1-28)
    monthlyDayUTC: 1,  // First day of month

    // Enable automatic re-validation on drift detection
    enableAutoRevalidation: true,

    // Output directory for refinement proposals
    refinementOutputDir: 'data/learning/refinements'
  },

  /**
   * FeatureImportanceTracker Configuration
   */
  importance: {
    // Rolling window size (number of analyses to keep)
    maxHistorySize: 10,

    // Minimum outcomes per edge for analysis
    minOutcomesPerEdge: 10
  },

  /**
   * BehaviorRefinementEngine Configuration
   */
  refinement: {
    // High importance threshold for WEIGHT_ADJUST proposals
    highImportanceThreshold: 0.6,

    // Low importance threshold for PRUNE_CANDIDATE proposals
    lowImportanceThreshold: 0.15,

    // Minimum edges showing low importance before suggesting prune
    minEdgesForPrune: 3,

    // Minimum correlation for NEW_FEATURE_SIGNAL proposals
    newFeatureCorrelation: 0.5
  }
};
