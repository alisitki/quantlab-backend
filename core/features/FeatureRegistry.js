import { FeatureBuilder } from './FeatureBuilder.js';
import { MidPriceFeature } from './builders/MidPriceFeature.js';
import { SpreadFeature } from './builders/SpreadFeature.js';
import { ReturnFeature } from './builders/ReturnFeature.js';
import { VolatilityFeature } from './builders/VolatilityFeature.js';

/**
 * FeatureRegistry: Manages feature instance creation.
 */
export class FeatureRegistry {
  static #builders = {
    mid_price: MidPriceFeature,
    spread: SpreadFeature,
    return_1: ReturnFeature,
    volatility: VolatilityFeature
  };

  /**
   * Create a FeatureBuilder instance for a symbol.
   * @param {string} symbol
   * @param {Object} config
   * @returns {FeatureBuilder}
   */
  static createFeatureBuilder(symbol, config = {}) {
    const enabledFeatures = config.enabledFeatures || ['mid_price', 'spread', 'return_1', 'volatility'];
    const featureInstances = {};

    for (const name of enabledFeatures) {
      const BuilderClass = this.#builders[name];
      if (BuilderClass) {
        featureInstances[name] = new BuilderClass(config[name] || {});
      }
    }

    return new FeatureBuilder(symbol, config, featureInstances);
  }
}
