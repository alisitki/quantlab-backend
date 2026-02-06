/**
 * ClusterRegimeFeature: Derived feature that uses RegimeCluster for prediction
 *
 * This is a DERIVED feature that depends on continuous regime features.
 * It uses a trained RegimeCluster model to predict the current regime.
 *
 * Must be configured with a trained RegimeCluster instance.
 *
 * Output: Cluster ID (0, 1, 2, ..., K-1) or null if features unavailable
 *
 * Example:
 *   const cluster = new RegimeCluster({ K: 4 });
 *   cluster.train(historicalData, ['volatility_ratio', 'trend_strength', 'spread_ratio']);
 *
 *   const feature = new ClusterRegimeFeature({ cluster, featureNames: [...] });
 */
export class ClusterRegimeFeature {
  static isDerived = true;
  static dependencies = []; // Set dynamically based on featureNames

  #cluster;
  #featureNames;
  #confidenceThreshold;

  /**
   * @param {Object} config
   * @param {RegimeCluster} config.cluster - Trained RegimeCluster instance
   * @param {Array<string>} config.featureNames - Feature names to use for prediction
   * @param {number} [config.confidenceThreshold] - Minimum confidence (default: 0.3)
   */
  constructor(config = {}) {
    if (!config.cluster) {
      throw new Error('ClusterRegimeFeature requires a trained cluster');
    }

    if (!config.featureNames || config.featureNames.length === 0) {
      throw new Error('ClusterRegimeFeature requires featureNames');
    }

    this.#cluster = config.cluster;
    this.#featureNames = config.featureNames;
    this.#confidenceThreshold = config.confidenceThreshold || 0.3;

    // Update static dependencies
    ClusterRegimeFeature.dependencies = config.featureNames;
  }

  /**
   * onEvent receives the full feature vector
   * @param {Object} features - Feature vector
   * @returns {number|null} Cluster ID or null
   */
  onEvent(features) {
    // Check if all required features are available
    for (const name of this.#featureNames) {
      if (features[name] === null || features[name] === undefined) {
        return null; // Features not ready
      }
    }

    // Predict regime
    const prediction = this.#cluster.predict(features);

    // Return cluster ID if confidence is sufficient
    if (prediction.cluster !== null && prediction.confidence >= this.#confidenceThreshold) {
      return prediction.cluster;
    }

    return null; // Low confidence or prediction failed
  }

  /**
   * Get prediction with confidence (for debugging/analysis)
   */
  getPredictionWithConfidence(features) {
    // Check if all required features are available
    for (const name of this.#featureNames) {
      if (features[name] === null || features[name] === undefined) {
        return { cluster: null, confidence: 0, reason: 'features_unavailable' };
      }
    }

    const prediction = this.#cluster.predict(features);

    return {
      cluster: prediction.cluster,
      confidence: prediction.confidence,
      distance: prediction.distance,
      meetsThreshold: prediction.confidence >= this.#confidenceThreshold
    };
  }

  reset() {
    // No internal state to reset (cluster is external)
  }
}
