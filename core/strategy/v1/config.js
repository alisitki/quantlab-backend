/**
 * StrategyV1 Configuration Presets
 *
 * Three main presets:
 * - DEFAULT: Balanced approach
 * - HIGH_FREQUENCY: More trades, smaller positions
 * - QUALITY: Fewer trades, higher conviction
 */

import { COMBINE_MODE } from './decision/Combiner.js';

/**
 * Default configuration - Balanced approach
 */
export const DEFAULT_CONFIG = {
  // Feature Analysis rapor yolu (DİNAMİK FEATURE SEÇİMİ İÇİN ZORUNLU)
  featureReportPath: './reports/feature-report.json',

  // Dinamik feature seçim parametreleri
  topFeatureCount: 5,           // Kaç top feature kullanılacak
  minAlphaScore: 0.3,           // Minimum alpha score eşiği

  // Regime mode parameters (NOT filter!)
  regime: {
    modes: {
      vol_high: { positionScale: 0.5, thresholdMultiplier: 1.5 },
      vol_low: { positionScale: 1.0, thresholdMultiplier: 0.8 },
      vol_normal: { positionScale: 0.75, thresholdMultiplier: 1.0 }
    },
    trendPenalties: {
      longInDowntrend: 0.5,
      shortInUptrend: 0.5
    }
  },

  // Signal generation
  signals: {
    baseThreshold: 0.001,
    maxThreshold: 0.01,
    minThreshold: 0.0001
  },

  // Signal combination
  combiner: {
    mode: COMBINE_MODE.WEIGHTED,
    minStrength: 0.3,
    minSignals: 2,
    confidenceThreshold: 0.5
  },

  // Execution
  execution: {
    minConfidence: 0.5,
    positionSizing: 'alpha_scaled',  // fixed | confidence_scaled | alpha_scaled
    baseQuantity: 0.01,
    maxQuantity: 0.1,
    spreadThreshold: 0.0005      // Wide spread delay threshold
  },

  // Feature builder parameters
  featureParams: {
    ema: { period: 20 },
    roc: { period: 10 },
    bollinger: { period: 20 },
    volatility: { period: 20 },
    rsi: { period: 14 },
    atr: { period: 14 }
  },

  // Logging
  logging: {
    enabled: true,
    decisionLogging: true,
    regimeLogging: true
  }
};

/**
 * High frequency config - more trades, smaller positions
 */
export const HIGH_FREQUENCY_CONFIG = {
  ...DEFAULT_CONFIG,

  topFeatureCount: 3,           // Sadece en iyi 3 feature
  minAlphaScore: 0.4,           // Daha yüksek kalite threshold

  combiner: {
    mode: COMBINE_MODE.WEIGHTED,
    minStrength: 0.2,
    minSignals: 1,
    confidenceThreshold: 0.4
  },

  execution: {
    minConfidence: 0.4,
    positionSizing: 'alpha_scaled',
    baseQuantity: 0.005,        // Yarı pozisyon
    maxQuantity: 0.05,
    spreadThreshold: 0.0003
  }
};

/**
 * Quality focused config - fewer trades, higher conviction
 */
export const QUALITY_CONFIG = {
  ...DEFAULT_CONFIG,

  topFeatureCount: 7,
  minAlphaScore: 0.5,           // Çok yüksek alpha score

  combiner: {
    mode: COMBINE_MODE.UNANIMOUS,  // Tüm sinyaller aynı yönde
    minStrength: 0.5,
    minSignals: 3,
    confidenceThreshold: 0.7
  },

  execution: {
    minConfidence: 0.7,
    positionSizing: 'alpha_scaled',
    baseQuantity: 0.02,         // Daha büyük pozisyon
    maxQuantity: 0.2,
    spreadThreshold: 0.0005
  }
};

/**
 * Aggressive config - high risk, high reward
 */
export const AGGRESSIVE_CONFIG = {
  ...DEFAULT_CONFIG,

  topFeatureCount: 5,
  minAlphaScore: 0.25,          // Daha düşük threshold

  regime: {
    modes: {
      vol_high: { positionScale: 0.75, thresholdMultiplier: 1.2 },  // Daha az ceza
      vol_low: { positionScale: 1.2, thresholdMultiplier: 0.7 },   // Daha büyük pozisyon
      vol_normal: { positionScale: 1.0, thresholdMultiplier: 0.9 }
    }
  },

  combiner: {
    mode: COMBINE_MODE.MAJORITY,
    minStrength: 0.2,
    minSignals: 1,
    confidenceThreshold: 0.3
  },

  execution: {
    minConfidence: 0.3,
    positionSizing: 'confidence_scaled',
    baseQuantity: 0.02,
    maxQuantity: 0.15,
    spreadThreshold: 0.001
  }
};

/**
 * Conservative config - minimal risk
 */
export const CONSERVATIVE_CONFIG = {
  ...DEFAULT_CONFIG,

  topFeatureCount: 5,
  minAlphaScore: 0.6,           // En yüksek kalite

  regime: {
    modes: {
      vol_high: { positionScale: 0.25, thresholdMultiplier: 2.0 },  // Çok küçük pozisyon
      vol_low: { positionScale: 0.75, thresholdMultiplier: 1.0 },
      vol_normal: { positionScale: 0.5, thresholdMultiplier: 1.2 }
    }
  },

  combiner: {
    mode: COMBINE_MODE.UNANIMOUS,
    minStrength: 0.6,
    minSignals: 3,
    confidenceThreshold: 0.8
  },

  execution: {
    minConfidence: 0.8,
    positionSizing: 'fixed',
    baseQuantity: 0.005,
    maxQuantity: 0.05,
    spreadThreshold: 0.0003     // Sadece tight spread
  }
};

/**
 * Get config by name
 * @param {string} name - Config name
 * @returns {Object}
 */
export function getConfig(name) {
  const configs = {
    default: DEFAULT_CONFIG,
    high_frequency: HIGH_FREQUENCY_CONFIG,
    quality: QUALITY_CONFIG,
    aggressive: AGGRESSIVE_CONFIG,
    conservative: CONSERVATIVE_CONFIG
  };

  return configs[name.toLowerCase()] || DEFAULT_CONFIG;
}

/**
 * Merge user config with base config
 * @param {Object} base - Base config
 * @param {Object} overrides - User overrides
 * @returns {Object}
 */
export function mergeConfig(base, overrides) {
  return {
    ...base,
    ...overrides,
    regime: {
      ...base.regime,
      ...overrides?.regime,
      modes: {
        ...base.regime?.modes,
        ...overrides?.regime?.modes
      }
    },
    combiner: {
      ...base.combiner,
      ...overrides?.combiner
    },
    execution: {
      ...base.execution,
      ...overrides?.execution
    },
    featureParams: {
      ...base.featureParams,
      ...overrides?.featureParams
    }
  };
}

export default DEFAULT_CONFIG;
