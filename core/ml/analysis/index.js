/**
 * Feature Analysis Module - Alpha Discovery & Feature Intelligence
 *
 * Provides comprehensive tools for analyzing feature quality,
 * correlations, importance, and alpha potential.
 */

// Correlation analysis
export {
  calculatePearsonCorrelation,
  calculateSpearmanCorrelation,
  calculateCorrelationMatrix,
  findHighlyCorrelatedPairs,
  calculateRedundancyScore,
  getFeatureClusters,
  analyzeFeatureCorrelations
} from './FeatureCorrelation.js';

// Feature-Label correlation
export {
  calculatePointBiserial,
  calculateFeatureLabelCorrelation,
  rankFeaturesByLabelCorrelation,
  getTopFeatures,
  getWeakFeatures,
  analyzeFeatureLabelRelationships
} from './FeatureLabelCorrelation.js';

// Label distribution
export {
  calculateLabelDistribution,
  calculateImbalanceRatio,
  calculateEntropy,
  calculateNormalizedEntropy,
  detectTemporalLabelDrift,
  analyzeSplitDistributions,
  analyzeLabelDistribution
} from './LabelDistribution.js';

// Permutation importance
export {
  calculatePermutationImportance,
  rankFeaturesByImportance,
  getMostImportantFeatures,
  getLeastImportantFeatures,
  analyzeFeatureImportance
} from './PermutationImportance.js';

// Feature distribution
export {
  calculateStats,
  detectOutliers,
  calculateKSStatistic,
  analyzeFeatureDistributions,
  generateFeatureDistributionReport
} from './FeatureDistribution.js';

// Feature stability
export {
  calculatePSI,
  calculateFeatureStability,
  categorizeFeaturesByStability,
  generateStabilityReport
} from './FeatureStability.js';

// Report generation
export {
  calculateAlphaScore,
  generateFeatureReport,
  formatReportAsMarkdown,
  formatReportAsJSON
} from './FeatureReportGenerator.js';
