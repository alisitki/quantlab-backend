/**
 * StrategyTemplateSelector - Select appropriate template based on edge characteristics
 *
 * Analyzes edge pattern to determine which template fits best.
 */

import { MeanReversionTemplate } from './templates/MeanReversionTemplate.js';
import { MomentumTemplate } from './templates/MomentumTemplate.js';
import { BreakoutTemplate } from './templates/BreakoutTemplate.js';

export class StrategyTemplateSelector {
  /**
   * Select appropriate template based on edge characteristics
   * @param {Edge} edge
   * @returns {{ templateClass: typeof BaseTemplate, reason: string }}
   */
  select(edge) {
    // Analyze edge name and discovery method for clues
    const name = edge.name.toLowerCase();
    const discoveryMethod = edge.discoveryMethod || '';

    // Check for mean reversion indicators
    if (this._isMeanReversion(edge, name, discoveryMethod)) {
      return {
        templateClass: MeanReversionTemplate,
        reason: 'Mean reversion indicators detected'
      };
    }

    // Check for momentum indicators
    if (this._isMomentum(edge, name, discoveryMethod)) {
      return {
        templateClass: MomentumTemplate,
        reason: 'Momentum/trend indicators detected'
      };
    }

    // Check for breakout indicators
    if (this._isBreakout(edge, name, discoveryMethod)) {
      return {
        templateClass: BreakoutTemplate,
        reason: 'Volatility breakout indicators detected'
      };
    }

    // Default to mean reversion (safest)
    return {
      templateClass: MeanReversionTemplate,
      reason: 'Default template (mean reversion)'
    };
  }

  /**
   * Check if edge exhibits mean reversion characteristics
   */
  _isMeanReversion(edge, name, discoveryMethod) {
    // Check name for keywords
    if (name.includes('reversion') || name.includes('micro_reversion')) {
      return true;
    }

    // Check if edge name mentions mean reversion features
    if (name.includes('high micro') || name.includes('low micro')) {
      return true;
    }

    return false;
  }

  /**
   * Check if edge exhibits momentum characteristics
   */
  _isMomentum(edge, name, discoveryMethod) {
    // Check name for keywords
    if (name.includes('momentum') || name.includes('trend') || name.includes('continuation')) {
      return true;
    }

    // Check if edge name mentions momentum features
    if (name.includes('return momentum') || name.includes('liquidity pressure')) {
      return true;
    }

    return false;
  }

  /**
   * Check if edge exhibits breakout characteristics
   */
  _isBreakout(edge, name, discoveryMethod) {
    // Check name for keywords
    if (name.includes('breakout') || name.includes('compression') || name.includes('volatility')) {
      return true;
    }

    // Check if edge name mentions volatility features
    if (name.includes('compression score') || name.includes('spread compression')) {
      return true;
    }

    return false;
  }
}
