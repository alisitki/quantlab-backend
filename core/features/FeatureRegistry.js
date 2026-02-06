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

// Regime detection (categorical)
import { VolatilityRegimeFeature } from './builders/VolatilityRegimeFeature.js';
import { TrendRegimeFeature } from './builders/TrendRegimeFeature.js';
import { SpreadRegimeFeature } from './builders/SpreadRegimeFeature.js';

// Regime detection (continuous - Phase 3)
import { VolatilityRatioFeature } from './builders/regime/VolatilityRatioFeature.js';
import { TrendStrengthFeature } from './builders/regime/TrendStrengthFeature.js';
import { SpreadRatioFeature } from './builders/regime/SpreadRatioFeature.js';

// Advanced indicators
import { MicropriceFeature } from './builders/MicropriceFeature.js';
import { ImbalanceEMAFeature } from './builders/ImbalanceEMAFeature.js';
import { EMASlopeFeature } from './builders/EMASlopeFeature.js';
import { BollingerPositionFeature } from './builders/BollingerPositionFeature.js';

// Behavior features (Phase 2)
import { LiquidityPressureFeature } from './builders/behavior/LiquidityPressureFeature.js';
import { ReturnMomentumFeature } from './builders/behavior/ReturnMomentumFeature.js';
import { RegimeStabilityFeature } from './builders/behavior/RegimeStabilityFeature.js';
import { SpreadCompressionFeature } from './builders/behavior/SpreadCompressionFeature.js';
import { ImbalanceAccelerationFeature } from './builders/behavior/ImbalanceAccelerationFeature.js';
import { MicroReversionFeature } from './builders/behavior/MicroReversionFeature.js';
import { QuoteIntensityFeature } from './builders/behavior/QuoteIntensityFeature.js';
import { BehaviorDivergenceFeature } from './builders/behavior/BehaviorDivergenceFeature.js';
import { VolatilityCompressionScoreFeature } from './builders/behavior/VolatilityCompressionScoreFeature.js';

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

    // Regime detection (categorical)
    regime_volatility: VolatilityRegimeFeature,
    regime_trend: TrendRegimeFeature,
    regime_spread: SpreadRegimeFeature,

    // Regime detection (continuous - Phase 3)
    volatility_ratio: VolatilityRatioFeature,
    trend_strength: TrendStrengthFeature,
    spread_ratio: SpreadRatioFeature,

    // Advanced microstructure
    microprice: MicropriceFeature,
    imbalance_ema: ImbalanceEMAFeature,
    ema_slope: EMASlopeFeature,
    bollinger_pos: BollingerPositionFeature,

    // Behavior features (Phase 2)
    liquidity_pressure: LiquidityPressureFeature,
    return_momentum: ReturnMomentumFeature,
    regime_stability: RegimeStabilityFeature,
    spread_compression: SpreadCompressionFeature,
    imbalance_acceleration: ImbalanceAccelerationFeature,
    micro_reversion: MicroReversionFeature,
    quote_intensity: QuoteIntensityFeature,
    behavior_divergence: BehaviorDivergenceFeature,
    volatility_compression_score: VolatilityCompressionScoreFeature
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
