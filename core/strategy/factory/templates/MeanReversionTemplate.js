/**
 * MeanReversionTemplate - Strategy for mean-reversion edges
 *
 * Optimized for edges that work when prices revert to mean.
 * Features:
 * - Volatility-inverse position sizing (lower vol = larger position)
 * - Profit target exit (take profit on partial reversion)
 */

import { BaseTemplate } from './BaseTemplate.js';

export class MeanReversionTemplate extends BaseTemplate {
  constructor(params) {
    super(params);
    this.templateType = 'mean_reversion';

    // Mean reversion specific config
    this._profitTargetPct = params.config?.profitTargetPct || 0.0005; // 0.05% default
    this._maxVolatilityRatio = params.config?.maxVolatilityRatio || 2.0;

    // Track entry features for profit calculation
    this._entryFeatures = null;
  }

  /**
   * Override entry to track features and filter high volatility
   */
  async _handleEntry(event, features, regime, ctx) {
    // Block entry if volatility too high (reversion risk increases)
    if (features.volatility_ratio && features.volatility_ratio > this._maxVolatilityRatio) {
      return;
    }

    // Store features for profit target calculation
    this._entryFeatures = { ...features };

    // Call parent entry logic
    await super._handleEntry(event, features, regime, ctx);
  }

  /**
   * Override exit to add profit target logic
   */
  async _handleExit(event, features, regime, ctx) {
    // Check profit target first (mean reversion complete)
    if (this._position !== 'FLAT' && this._entryPrice && this._entryFeatures) {
      const currentPrice = features.mid_price;
      const pnl = this._position === 'LONG'
        ? (currentPrice - this._entryPrice) / this._entryPrice
        : (this._entryPrice - currentPrice) / this._entryPrice;

      // If profit target reached, exit early
      if (pnl >= this._profitTargetPct) {
        // Force exit by manually triggering close
        if (ctx.placeOrder) {
          const quantity = this.config.baseQuantity || 10;

          ctx.placeOrder({
            symbol: event.symbol,
            side: this._position === 'LONG' ? 'SELL' : 'BUY',
            type: 'MARKET',
            qty: quantity,
            ts_event: event.ts_event
          });

          // Record exit for outcome collector
          if (this._outcomeCollector) {
            const tradeId = `${this.getStrategyId()}_${this._tradeCount}`;
            this._outcomeCollector.recordExit(tradeId, {
              price: currentPrice,
              timestamp: event.ts_event,
              pnl,
              exitReason: 'profit_target'
            });
          }

          ctx.logger.info(`[${this.getStrategyId()}] EXIT ${this._position} @ ${currentPrice.toFixed(6)}, PnL: ${(pnl * 100).toFixed(3)}%, reason: profit_target`);

          this._position = 'FLAT';
          this._entryTime = null;
          this._entryPrice = null;
          this._entryFeatures = null;
          return;
        }
      }
    }

    // Otherwise use edge's exit logic
    await super._handleExit(event, features, regime, ctx);
  }

  /**
   * Override position sizing to scale inversely with volatility
   */
  _calculatePositionSize(confidence) {
    const baseQty = this.config.baseQuantity || 10;
    const maxQty = this.config.maxQuantity || 50;

    // Get volatility ratio (current vol / historical vol)
    const volRatio = this._entryFeatures?.volatility_ratio || 1.0;

    // Inverse volatility scaling: high vol = smaller position
    const volAdjustment = 1 / (1 + volRatio);

    // Confidence scaling
    const scaledQty = baseQty + (maxQty - baseQty) * (confidence || 0.5);

    // Apply volatility adjustment
    return Math.round(scaledQty * volAdjustment);
  }
}
