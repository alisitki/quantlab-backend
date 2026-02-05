import { FeatureBuilder } from './FeatureBuilder.js';
import { MidPriceFeature } from './builders/MidPriceFeature.js';
import { SpreadFeature } from './builders/SpreadFeature.js';
import { ReturnFeature } from './builders/ReturnFeature.js';
import { VolatilityFeature } from './builders/VolatilityFeature.js';

// New technical indicators
import { EMAFeature } from './builders/EMAFeature.js';
import { RSIFeature } from './builders/RSIFeature.js';
import { ATRFeature } from './builders/ATRFeature.js';
import { ROCFeature } from './builders/ROCFeature.js';

// Regime detection
import { VolatilityRegimeFeature } from './builders/VolatilityRegimeFeature.js';
import { TrendRegimeFeature } from './builders/TrendRegimeFeature.js';
import { SpreadRegimeFeature } from './builders/SpreadRegimeFeature.js';

// Advanced indicators
import { MicropriceFeature } from './builders/MicropriceFeature.js';
import { ImbalanceEMAFeature } from './builders/ImbalanceEMAFeature.js';
import { EMASlopeFeature } from './builders/EMASlopeFeature.js';
import { BollingerPositionFeature } from './builders/BollingerPositionFeature.js';

/**
 * FeatureRegistry: Manages feature instance creation.
 */
export class FeatureRegistry {
  static #builders = {
    // Core features
    mid_price: MidPriceFeature,
    spread: SpreadFeature,
    return_1: ReturnFeature,
    volatility: VolatilityFeature,

    // Technical indicators
    ema: EMAFeature,
    rsi: RSIFeature,
    atr: ATRFeature,
    roc: ROCFeature,

    // Regime detection
    regime_volatility: VolatilityRegimeFeature,
    regime_trend: TrendRegimeFeature,
    regime_spread: SpreadRegimeFeature,

    // Advanced microstructure
    microprice: MicropriceFeature,
    imbalance_ema: ImbalanceEMAFeature,
    ema_slope: EMASlopeFeature,
    bollinger_pos: BollingerPositionFeature
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
