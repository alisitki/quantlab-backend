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
   * @param {Object} event
   * @returns {Object|null}
   */
  onEvent(event) {
    const vector = {};
    let allWarm = true;

    for (const [name, feature] of Object.entries(this.#features)) {
      const val = feature.onEvent(event);
      if (val === null) {
        allWarm = false;
      }
      vector[name] = val;
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
