/**
 * StrategyParameterMapper - Map edge properties to strategy parameters
 *
 * Derives strategy configuration from edge characteristics.
 */

import { FACTORY_CONFIG } from './config.js';

export class StrategyParameterMapper {
  /**
   * Map edge properties to strategy parameters
   * @param {Edge} edge
   * @param {string} templateType
   * @param {Object} edgeDefinition - Optional edge discovery definition
   * @returns {Object} Strategy parameters
   */
  map(edge, templateType, edgeDefinition = null) {
    const baseParams = {
      // Position sizing
      baseQuantity: this._deriveBaseQuantity(edge),
      maxQuantity: this._deriveMaxQuantity(edge),

      // Time management
      timeHorizon: edge.timeHorizon,
      cooldownMs: this._deriveCooldown(edge),

      // Features
      enabledFeatures: this._deriveEnabledFeatures(edge, edgeDefinition),

      // Gate configuration (optional - for future SignalGate integration)
      gateConfig: this._deriveGateConfig(edge),

      // Template type
      templateType
    };

    // Add template-specific parameters
    const templateDefaults = FACTORY_CONFIG.templateDefaults?.[templateType];
    if (templateDefaults) {
      Object.assign(baseParams, templateDefaults);
    }

    return baseParams;
  }

  _deriveBaseQuantity(edge) {
    // Scale by expected advantage
    const baseQty = FACTORY_CONFIG.parameters.baseQuantity;

    if (edge.expectedAdvantage && edge.expectedAdvantage.sharpe) {
      // Higher Sharpe = larger position
      const sharpeMultiplier = Math.min(2, 1 + edge.expectedAdvantage.sharpe / 2);
      return Math.round(baseQty * sharpeMultiplier);
    }

    return baseQty;
  }

  _deriveMaxQuantity(edge) {
    const maxQty = FACTORY_CONFIG.parameters.maxQuantity;

    // Conservative sizing for high-risk edges
    if (edge.riskProfile && edge.riskProfile.maxDrawdown > 0.05) {
      return Math.round(maxQty * 0.5);
    }

    return maxQty;
  }

  _deriveCooldown(edge) {
    // Use edge time horizon as cooldown
    return edge.timeHorizon || FACTORY_CONFIG.parameters.defaultCooldownMs;
  }

  _deriveEnabledFeatures(edge, edgeDefinition) {
    // Try to extract features from edge definition
    if (edgeDefinition && edgeDefinition.pattern && edgeDefinition.pattern.conditions) {
      const featureSet = new Set(['mid_price', 'spread']); // Always include base features

      // Extract features from pattern conditions
      for (const condition of edgeDefinition.pattern.conditions) {
        if (condition.feature) {
          featureSet.add(condition.feature);
        }
      }

      // Return unique features
      return Array.from(featureSet);
    }

    // Fallback: enable all behavior + regime features
    return [
      'mid_price',
      'spread',
      'liquidity_pressure',
      'return_momentum',
      'regime_stability',
      'spread_compression',
      'imbalance_acceleration',
      'micro_reversion',
      'quote_intensity',
      'behavior_divergence',
      'volatility_compression_score',
      'volatility_ratio',
      'trend_strength',
      'spread_ratio'
    ];
  }

  _deriveGateConfig(edge) {
    // Basic gate configuration
    return {
      minSignalScore: 0.6,
      cooldownMs: this._deriveCooldown(edge),
      maxSpreadNormalized: 0.001
    };
  }
}
