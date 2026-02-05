/**
 * FeatureReportGenerator - Comprehensive feature analysis report
 *
 * Combines all analysis modules into a unified report with alpha scores.
 */

import { analyzeFeatureCorrelations } from './FeatureCorrelation.js';
import { analyzeFeatureLabelRelationships, rankFeaturesByLabelCorrelation } from './FeatureLabelCorrelation.js';
import { analyzeLabelDistribution } from './LabelDistribution.js';
import { generateFeatureDistributionReport } from './FeatureDistribution.js';
import { analyzeFeatureImportance, rankFeaturesByImportance } from './PermutationImportance.js';
import { generateStabilityReport } from './FeatureStability.js';

/**
 * Calculate alpha score for a feature
 * Combines importance, label correlation, and stability
 *
 * @param {number} normalizedImportance - 0 to 1
 * @param {number} absLabelCorrelation - 0 to 1
 * @param {number} psi - Population Stability Index (lower is better)
 * @returns {number} Alpha score (0 to 1)
 */
export function calculateAlphaScore(normalizedImportance, absLabelCorrelation, psi) {
  // Weights
  const importanceWeight = 0.4;
  const correlationWeight = 0.3;
  const stabilityWeight = 0.3;

  // Stability contribution: 1 - normalized PSI (capped at 1)
  const stabilityScore = Math.max(0, 1 - Math.min(psi / 0.5, 1));

  const alphaScore =
    importanceWeight * (normalizedImportance || 0) +
    correlationWeight * (absLabelCorrelation || 0) +
    stabilityWeight * stabilityScore;

  return alphaScore;
}

/**
 * Generate comprehensive feature report
 * @param {Object} params
 * @param {number[][]} params.X - Feature matrix
 * @param {number[]} params.y - Labels
 * @param {string[]} params.featureNames - Feature names
 * @param {Object} params.model - Trained model (optional, for importance)
 * @param {number[]} params.timestamps - Timestamps (optional, for stability)
 * @param {Object} params.options - Additional options
 * @returns {Object} Complete feature report
 */
export async function generateFeatureReport(params) {
  const {
    X,
    y,
    featureNames,
    model = null,
    timestamps = null,
    options = {}
  } = params;

  const report = {
    generatedAt: new Date().toISOString(),
    nSamples: X.length,
    nFeatures: featureNames.length,
    featureNames,
    sections: {}
  };

  // 1. Feature Correlations
  console.log('Analyzing feature correlations...');
  report.sections.correlations = analyzeFeatureCorrelations(X, featureNames, {
    method: options.correlationMethod || 'pearson',
    highCorrThreshold: options.highCorrThreshold || 0.8
  });

  // 2. Feature-Label Correlations
  console.log('Analyzing feature-label correlations...');
  report.sections.labelCorrelations = analyzeFeatureLabelRelationships(X, y, featureNames);

  // 3. Label Distribution
  console.log('Analyzing label distribution...');
  report.sections.labelDistribution = analyzeLabelDistribution(y, {
    timestamps,
    windowSize: options.labelWindowSize || 1000
  });

  // 4. Feature Distributions
  console.log('Analyzing feature distributions...');
  report.sections.featureDistributions = generateFeatureDistributionReport(X, featureNames, {
    outlierThreshold: options.outlierThreshold || 5.0
  });

  // 5. Permutation Importance (if model provided)
  if (model) {
    console.log('Calculating permutation importance...');
    report.sections.importance = await analyzeFeatureImportance(model, X, y, featureNames, {
      nRepeats: options.importanceRepeats || 5
    });
  }

  // 6. Feature Stability (if timestamps provided)
  if (timestamps) {
    console.log('Analyzing feature stability...');
    report.sections.stability = generateStabilityReport(X, timestamps, featureNames, {
      windowMs: options.stabilityWindowMs || 86400000
    });
  }

  // 7. Calculate Alpha Scores
  console.log('Calculating alpha scores...');
  report.alphaScores = calculateAlphaScores(report);

  // 8. Generate Summary and Recommendations
  report.summary = generateSummary(report);
  report.recommendations = generateRecommendations(report);

  return report;
}

/**
 * Calculate alpha scores for all features
 * @param {Object} report
 * @returns {Object} Alpha scores per feature
 */
function calculateAlphaScores(report) {
  const { featureNames, sections } = report;
  const alphaScores = {};

  // Get normalized importance scores
  let normalizedImportance = {};
  if (sections.importance?.normalizedScores) {
    normalizedImportance = sections.importance.normalizedScores;
  }

  // Get label correlations
  const labelCorrs = sections.labelCorrelations?.correlations || {};

  // Get stability PSI
  const psiByFeature = sections.stability?.psiByFeature || {};

  for (const name of featureNames) {
    const importance = normalizedImportance[name] || 0;
    const absLabelCorr = labelCorrs[name]?.absCorrelation || 0;
    const psi = psiByFeature[name]?.maxPSI || 0;

    alphaScores[name] = {
      score: calculateAlphaScore(importance, absLabelCorr, psi),
      components: {
        importance,
        labelCorrelation: absLabelCorr,
        stability: 1 - Math.min(psi / 0.5, 1)
      }
    };
  }

  // Rank by alpha score
  const ranked = Object.entries(alphaScores)
    .map(([name, data]) => ({ feature: name, ...data }))
    .sort((a, b) => b.score - a.score);

  ranked.forEach((item, idx) => {
    item.rank = idx + 1;
  });

  return {
    byFeature: alphaScores,
    ranked,
    topFeatures: ranked.slice(0, 5).map(r => r.feature),
    bottomFeatures: ranked.slice(-3).map(r => r.feature)
  };
}

/**
 * Generate report summary
 * @param {Object} report
 * @returns {Object} Summary
 */
function generateSummary(report) {
  const { sections, alphaScores, nFeatures, nSamples } = report;

  return {
    totalFeatures: nFeatures,
    totalSamples: nSamples,

    // Correlation summary
    redundantFeatures: sections.correlations?.dropCandidates?.length || 0,
    highCorrelationPairs: sections.correlations?.highlyCorrelatedPairs?.length || 0,

    // Label summary
    labelImbalance: sections.labelDistribution?.summary?.imbalanceRatio || 'N/A',
    labelBalance: sections.labelDistribution?.summary?.balanceStatus || 'N/A',

    // Feature quality
    weakPredictiveFeatures: sections.labelCorrelations?.weakFeatures?.length || 0,
    unstableFeatures: sections.stability?.categorization?.unstableFeatures?.length || 0,

    // Top alpha features
    topAlphaFeature: alphaScores?.ranked?.[0]?.feature || 'N/A',
    topAlphaScore: alphaScores?.ranked?.[0]?.score?.toFixed(3) || 'N/A',

    // Alerts count
    totalAlerts: sections.featureDistributions?.alerts?.length || 0
  };
}

/**
 * Generate actionable recommendations
 * @param {Object} report
 * @returns {string[]} Recommendations
 */
function generateRecommendations(report) {
  const recommendations = [];
  const { sections, summary } = report;

  // Redundant features
  if (summary.redundantFeatures > 0) {
    const dropList = sections.correlations.dropCandidates.slice(0, 3).join(', ');
    recommendations.push(`Consider removing ${summary.redundantFeatures} redundant features: ${dropList}`);
  }

  // Label imbalance
  if (sections.labelDistribution?.summary?.balanceStatus === 'SEVERELY_IMBALANCED') {
    recommendations.push('Severe label imbalance detected. Use class weights or oversampling.');
  }

  // Weak features
  if (summary.weakPredictiveFeatures > report.nFeatures / 3) {
    recommendations.push('Many features have weak predictive power. Consider feature engineering.');
  }

  // Unstable features
  if (summary.unstableFeatures > 0) {
    const unstable = sections.stability?.categorization?.unstableFeatures?.slice(0, 3).join(', ');
    recommendations.push(`Unstable features detected: ${unstable}. Consider time-aware training.`);
  }

  // Distribution alerts
  if (sections.featureDistributions?.shiftedFeatures?.length > 0) {
    recommendations.push('Distribution shift detected in some features. Verify train/test split.');
  }

  // Default
  if (recommendations.length === 0) {
    recommendations.push('Feature set looks healthy. Proceed with model training.');
  }

  return recommendations;
}

/**
 * Format report as markdown
 * @param {Object} report
 * @returns {string} Markdown string
 */
export function formatReportAsMarkdown(report) {
  const lines = [];

  lines.push('# Feature Analysis Report');
  lines.push(`\nGenerated: ${report.generatedAt}`);
  lines.push(`\n**Samples:** ${report.nSamples} | **Features:** ${report.nFeatures}`);

  // Summary
  lines.push('\n## Summary\n');
  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  for (const [key, value] of Object.entries(report.summary)) {
    lines.push(`| ${key} | ${value} |`);
  }

  // Top Alpha Features
  lines.push('\n## Top Alpha Features\n');
  lines.push(`| Rank | Feature | Alpha Score | Importance | Label Corr | Stability |`);
  lines.push(`|------|---------|-------------|------------|------------|-----------|`);
  for (const r of report.alphaScores.ranked.slice(0, 10)) {
    lines.push(`| ${r.rank} | ${r.feature} | ${r.score.toFixed(3)} | ${r.components.importance.toFixed(3)} | ${r.components.labelCorrelation.toFixed(3)} | ${r.components.stability.toFixed(3)} |`);
  }

  // Recommendations
  lines.push('\n## Recommendations\n');
  for (const rec of report.recommendations) {
    lines.push(`- ${rec}`);
  }

  // Highly Correlated Pairs
  if (report.sections.correlations?.highlyCorrelatedPairs?.length > 0) {
    lines.push('\n## Highly Correlated Feature Pairs\n');
    lines.push(`| Feature A | Feature B | Correlation |`);
    lines.push(`|-----------|-----------|-------------|`);
    for (const pair of report.sections.correlations.highlyCorrelatedPairs.slice(0, 10)) {
      lines.push(`| ${pair.feature_a} | ${pair.feature_b} | ${pair.correlation.toFixed(3)} |`);
    }
  }

  // Label Distribution
  lines.push('\n## Label Distribution\n');
  const labelDist = report.sections.labelDistribution?.overall;
  if (labelDist) {
    lines.push(`| Label | Count | Percentage |`);
    lines.push(`|-------|-------|------------|`);
    for (const [label, pct] of Object.entries(labelDist.percentages || {})) {
      lines.push(`| ${label} | ${labelDist.counts[label]} | ${pct.toFixed(1)}% |`);
    }
  }

  return lines.join('\n');
}

/**
 * Format report as JSON
 * @param {Object} report
 * @returns {string} JSON string
 */
export function formatReportAsJSON(report) {
  return JSON.stringify(report, null, 2);
}
