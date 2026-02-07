/**
 * FeatureBuilder: Orchestrates multiple feature calculators for a single symbol.
 */
export class FeatureBuilder {
  #symbol;
  #features = {};
  #config;

  /**
   * @param {string} symbol
   * @param {Object} config
   * @param {Object} features - Map of featureName -> FeatureInstance
   */
  constructor(symbol, config, features) {
    this.#symbol = symbol;
    this.#config = config;
    this.#features = features;
  }

  /**
   * Process an event and return a flat feature vector.
   * Returns null until all required features are "warm".
   *
   * Supports two types of features:
   * 1. Event-based features: onEvent(event) -> value
   * 2. Derived features: onEvent(features) -> value (depends on other features)
   *
   * @param {Object} event
   * @returns {Object|null}
   */
  onEvent(event) {
    const vector = {};
    let allWarm = true;
    const nullFeatures = []; // Track which features are null

    // Pass 1: Calculate base features (event-based)
    for (const [name, feature] of Object.entries(this.#features)) {
      // Check if this is a derived feature (depends on other features)
      // Derived features declare: static isDerived = true
      const isDerived = feature.constructor.isDerived === true;

      if (!isDerived) {
        const val = feature.onEvent(event);
        if (val === null) {
          allWarm = false;
          nullFeatures.push(name);
        }
        vector[name] = val;
      }
    }

    // Pass 2: Calculate derived features (feature-based)
    for (const [name, feature] of Object.entries(this.#features)) {
      const isDerived = feature.constructor.isDerived === true;

      if (isDerived) {
        const val = feature.onEvent(vector);
        if (val === null) {
          allWarm = false;
          nullFeatures.push(name);
        }
        vector[name] = val;
      }
    }

    // Debug logging (only log first few times) - controlled by env
    if (!allWarm && !this._debugLogged && process.env.FEATURE_BUILDER_DEBUG === 'true') {
      this._debugLogCount = (this._debugLogCount || 0) + 1;
      if (this._debugLogCount <= 5 || this._debugLogCount === 100 || this._debugLogCount === 1000) {
        console.log(`[FeatureBuilder DEBUG ${this._debugLogCount}] Null features: ${nullFeatures.join(', ')}`);
      }
      if (this._debugLogCount >= 1000) {
        this._debugLogged = true;
      }
    }

    return allWarm ? vector : null;
  }

  /**
   * Reset all features.
   */
  reset() {
    for (const feature of Object.values(this.#features)) {
      feature.reset();
    }
  }
}
