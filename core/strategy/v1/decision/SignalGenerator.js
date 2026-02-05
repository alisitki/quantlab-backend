/**
 * SignalGenerator - Dynamic Feature-Based Signal Generation
 *
 * Feature Analysis sonucunda en güçlü bulunan feature'lardan
 * dinamik sinyal üretir. Sabit indicator seti YASAK.
 */

import fs from 'fs';

/**
 * Signal direction constants
 */
export const SIGNAL_DIRECTION = {
  LONG: 1,
  SHORT: -1,
  NEUTRAL: 0
};

/**
 * Default signal generation config
 */
const DEFAULT_CONFIG = {
  topN: 5,
  minAlphaScore: 0.3,
  baseThreshold: 0.001,
  maxThreshold: 0.01,
  minThreshold: 0.0001
};

export class SignalGenerator {
  #config;
  #featureReport = null;
  #topFeatures = [];
  #signalConfigs = new Map();

  /**
   * @param {Object} config
   */
  constructor(config = {}) {
    this.#config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Load feature analysis report and select top features
   * @param {string} reportPath - Path to feature report JSON
   * @param {number} topN - Number of top features to use
   * @param {number} minAlphaScore - Minimum alpha score threshold
   */
  loadFromReport(reportPath, topN = this.#config.topN, minAlphaScore = this.#config.minAlphaScore) {
    try {
      const content = fs.readFileSync(reportPath, 'utf8');
      this.#featureReport = JSON.parse(content);
    } catch (err) {
      throw new Error(`Failed to load feature report: ${err.message}`);
    }

    // Extract features array from report
    let features = this.#featureReport.features ||
                   this.#featureReport.featureScores ||
                   [];

    // Handle sections-based report format
    if (features.length === 0 && this.#featureReport.sections) {
      const { sections, featureNames } = this.#featureReport;

      // Build features from sections
      features = featureNames.map((name) => {
        const labelCorrData = sections.labelCorrelations?.correlations?.[name];
        const labelCorr = labelCorrData?.primaryCorrelation || 0;
        const stabilityData = sections.stability?.scores?.[name];
        const stability = stabilityData?.psi !== undefined ? (1 - stabilityData.psi) : 1;
        const alphaScore = 0.4 * 0 + 0.3 * Math.abs(labelCorr) + 0.3 * stability;

        return {
          name,
          alphaScore,
          labelCorrelation: labelCorr,
          importance: 0,
          stability
        };
      });
    }

    if (features.length === 0) {
      throw new Error('No features found in report');
    }

    // Filter and sort by alpha score
    this.#topFeatures = features
      .filter(f => {
        const score = f.alphaScore ?? f.alpha_score ?? 0;
        return score >= minAlphaScore;
      })
      .sort((a, b) => {
        const scoreA = a.alphaScore ?? a.alpha_score ?? 0;
        const scoreB = b.alphaScore ?? b.alpha_score ?? 0;
        return scoreB - scoreA;
      })
      .slice(0, topN)
      .map(f => ({
        name: f.name || f.feature,
        alphaScore: f.alphaScore ?? f.alpha_score ?? 0,
        labelCorrelation: f.labelCorrelation ?? f.label_correlation ?? 0,
        importance: f.importance ?? f.permutation_importance ?? 0,
        stability: f.stability ?? f.psi ?? 1,
        direction: (f.labelCorrelation ?? f.label_correlation ?? 0) > 0 ? 'positive' : 'negative'
      }));

    // Build signal configs for each feature
    this.#signalConfigs.clear();
    for (const feature of this.#topFeatures) {
      this.#signalConfigs.set(feature.name, {
        threshold: this.#calculateThreshold(feature),
        weight: feature.alphaScore,
        direction: feature.direction,
        labelCorrelation: feature.labelCorrelation
      });
    }

    return this.#topFeatures;
  }

  /**
   * Load features from manual configuration (for testing)
   * @param {Array} features - Array of feature configs
   */
  loadFromConfig(features) {
    this.#topFeatures = features.map(f => ({
      name: f.name,
      alphaScore: f.alphaScore || 0.5,
      labelCorrelation: f.labelCorrelation || 0.1,
      direction: f.labelCorrelation > 0 ? 'positive' : 'negative'
    }));

    this.#signalConfigs.clear();
    for (const feature of this.#topFeatures) {
      this.#signalConfigs.set(feature.name, {
        threshold: this.#calculateThreshold(feature),
        weight: feature.alphaScore,
        direction: feature.direction,
        labelCorrelation: feature.labelCorrelation
      });
    }

    return this.#topFeatures;
  }

  /**
   * Calculate threshold for a feature based on label correlation
   * Stronger correlation = lower threshold (more sensitive)
   * @param {Object} feature
   * @returns {number}
   */
  #calculateThreshold(feature) {
    const { baseThreshold, maxThreshold, minThreshold } = this.#config;
    const correlation = Math.abs(feature.labelCorrelation);

    if (correlation < 0.01) {
      return maxThreshold; // Weak correlation = high threshold
    }

    // Inverse relationship: stronger correlation = lower threshold
    const threshold = baseThreshold / correlation;

    // Clamp to bounds
    return Math.min(maxThreshold, Math.max(minThreshold, threshold));
  }

  /**
   * Adjust threshold based on regime mode
   * @param {number} threshold
   * @param {Object} mode
   * @returns {number}
   */
  #adjustThresholdForRegime(threshold, mode) {
    if (!mode || !mode.combined) return threshold;

    const multiplier = mode.combined.thresholdMultiplier || 1.0;
    return threshold * multiplier;
  }

  /**
   * Generate signal for a single feature
   * @param {string} featureName
   * @param {number} value
   * @param {number} threshold
   * @param {number} weight
   * @param {Object} config
   * @returns {Object} Signal
   */
  #generateSignalForFeature(featureName, value, threshold, weight, config) {
    const { direction: featureDirection, labelCorrelation } = config;

    // Default: neutral
    let direction = SIGNAL_DIRECTION.NEUTRAL;
    let strength = 0;

    // Determine signal based on value vs threshold
    if (Math.abs(value) > threshold) {
      // For positive label correlation: positive value = LONG
      // For negative label correlation: positive value = SHORT
      if (featureDirection === 'positive') {
        direction = value > threshold ? SIGNAL_DIRECTION.LONG :
                    value < -threshold ? SIGNAL_DIRECTION.SHORT :
                    SIGNAL_DIRECTION.NEUTRAL;
      } else {
        // Negative correlation: inverse logic
        direction = value > threshold ? SIGNAL_DIRECTION.SHORT :
                    value < -threshold ? SIGNAL_DIRECTION.LONG :
                    SIGNAL_DIRECTION.NEUTRAL;
      }

      // Calculate strength based on how far past threshold
      const magnitude = Math.abs(value) / threshold;
      strength = Math.min(1.0, magnitude / 10) * weight;
    }

    return {
      name: featureName,
      direction,
      strength,
      feature: featureName,
      value,
      threshold,
      alphaScore: weight,
      labelCorrelation
    };
  }

  /**
   * Generate signals from all configured features
   * @param {Object} features - Feature values from FeatureBuilder
   * @param {Object} mode - Current regime mode (optional)
   * @returns {Object} { signals, consensus, activeFeatures }
   */
  generate(features, mode = null) {
    const signals = [];

    for (const [featureName, config] of this.#signalConfigs) {
      const value = features[featureName];

      // Skip if feature value not available
      if (value === undefined || value === null || isNaN(value)) {
        continue;
      }

      // Adjust threshold for current regime
      const adjustedThreshold = this.#adjustThresholdForRegime(config.threshold, mode);

      // Generate signal
      const signal = this.#generateSignalForFeature(
        featureName,
        value,
        adjustedThreshold,
        config.weight,
        config
      );

      // Only include non-neutral signals
      if (signal.direction !== SIGNAL_DIRECTION.NEUTRAL) {
        signals.push(signal);
      }
    }

    return {
      signals,
      consensus: this.#calculateConsensus(signals),
      activeFeatures: this.#topFeatures.map(f => f.name),
      mode: mode?.primary || null
    };
  }

  /**
   * Calculate consensus score from signals
   * @param {Array} signals
   * @returns {number} -1 to 1 (negative = SHORT consensus, positive = LONG consensus)
   */
  #calculateConsensus(signals) {
    if (signals.length === 0) return 0;

    let weightedSum = 0;
    let totalWeight = 0;

    for (const signal of signals) {
      weightedSum += signal.direction * signal.strength;
      totalWeight += signal.strength;
    }

    return totalWeight > 0 ? weightedSum / totalWeight : 0;
  }

  /**
   * Get top features
   * @returns {Array}
   */
  getTopFeatures() {
    return [...this.#topFeatures];
  }

  /**
   * Get signal config for a feature
   * @param {string} featureName
   * @returns {Object|null}
   */
  getSignalConfig(featureName) {
    return this.#signalConfigs.get(featureName) || null;
  }

  /**
   * Check if report is loaded
   * @returns {boolean}
   */
  isLoaded() {
    return this.#topFeatures.length > 0;
  }

  /**
   * Get report metadata
   * @returns {Object|null}
   */
  getReportMetadata() {
    if (!this.#featureReport) return null;

    return {
      generatedAt: this.#featureReport.generatedAt || this.#featureReport.generated_at,
      symbol: this.#featureReport.symbol,
      totalFeatures: this.#featureReport.features?.length || 0,
      selectedFeatures: this.#topFeatures.length
    };
  }

  /**
   * Reset generator state
   */
  reset() {
    this.#featureReport = null;
    this.#topFeatures = [];
    this.#signalConfigs.clear();
  }
}

export default SignalGenerator;
