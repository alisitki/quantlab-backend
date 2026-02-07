/**
 * MomentumTemplate - Strategy for momentum/trend edges
 *
 * Optimized for edges that work when prices continue trending.
 * Features:
 * - Trailing stop (lock in profits as trend continues)
 * - Trend-scaled position sizing (stronger trend = larger position)
 */

import { BaseTemplate } from './BaseTemplate.js';

export class MomentumTemplate extends BaseTemplate {
  constructor(params) {
    super(params);
    this.templateType = 'momentum';

    // Momentum specific config
    this._trailingStopPct = params.config?.trailingStopPct || 0.015; // 1.5% default
    this._minTrendStrength = params.config?.minTrendStrength || 0.3;

    // Track maximum favorable excursion for trailing stop
    this._maxFavorablePrice = null;
    this._entryFeatures = null;
  }

  /**
   * Override entry to track features and filter weak trends
   */
  async _handleEntry(event, features, regime, ctx) {
    // Block entry if trend strength too weak
    if (features.trend_strength !== undefined && features.trend_strength < this._minTrendStrength) {
      return;
    }

    // Store features for position sizing
    this._entryFeatures = { ...features };

    // Reset trailing stop tracker
    this._maxFavorablePrice = null;

    // Call parent entry logic
    await super._handleEntry(event, features, regime, ctx);

    // Initialize trailing stop at entry
    if (this._position !== 'FLAT') {
      this._maxFavorablePrice = features.mid_price;
    }
  }

  /**
   * Override exit to add trailing stop logic
   */
  async _handleExit(event, features, regime, ctx) {
    if (this._position !== 'FLAT' && this._entryPrice) {
      const currentPrice = features.mid_price;

      // Update maximum favorable excursion
      if (this._position === 'LONG') {
        if (!this._maxFavorablePrice || currentPrice > this._maxFavorablePrice) {
          this._maxFavorablePrice = currentPrice;
        }

        // Check trailing stop: price retraced from peak
        const retraceFromPeak = (this._maxFavorablePrice - currentPrice) / this._maxFavorablePrice;
        if (retraceFromPeak >= this._trailingStopPct) {
          // Trailing stop hit
          if (ctx.placeOrder) {
            const quantity = this.config.baseQuantity || 10;
            const pnl = (currentPrice - this._entryPrice) / this._entryPrice;

            ctx.placeOrder({
              symbol: event.symbol,
              side: 'SELL',
              type: 'MARKET',
              qty: quantity,
              ts_event: event.ts_event
            });

            // Record exit
            if (this._outcomeCollector) {
              const tradeId = `${this.getStrategyId()}_${this._tradeCount}`;
              this._outcomeCollector.recordExit(tradeId, {
                price: currentPrice,
                timestamp: event.ts_event,
                pnl,
                exitReason: 'trailing_stop'
              });
            }

            ctx.logger.info(`[${this.getStrategyId()}] EXIT LONG @ ${currentPrice.toFixed(6)}, PnL: ${(pnl * 100).toFixed(3)}%, reason: trailing_stop (peak: ${this._maxFavorablePrice.toFixed(6)})`);

            this._position = 'FLAT';
            this._entryTime = null;
            this._entryPrice = null;
            this._maxFavorablePrice = null;
            this._entryFeatures = null;
            return;
          }
        }
      } else if (this._position === 'SHORT') {
        if (!this._maxFavorablePrice || currentPrice < this._maxFavorablePrice) {
          this._maxFavorablePrice = currentPrice;
        }

        // Check trailing stop: price retraced from peak (upward for SHORT)
        const retraceFromPeak = (currentPrice - this._maxFavorablePrice) / this._maxFavorablePrice;
        if (retraceFromPeak >= this._trailingStopPct) {
          // Trailing stop hit
          if (ctx.placeOrder) {
            const quantity = this.config.baseQuantity || 10;
            const pnl = (this._entryPrice - currentPrice) / this._entryPrice;

            ctx.placeOrder({
              symbol: event.symbol,
              side: 'BUY',
              type: 'MARKET',
              qty: quantity,
              ts_event: event.ts_event
            });

            // Record exit
            if (this._outcomeCollector) {
              const tradeId = `${this.getStrategyId()}_${this._tradeCount}`;
              this._outcomeCollector.recordExit(tradeId, {
                price: currentPrice,
                timestamp: event.ts_event,
                pnl,
                exitReason: 'trailing_stop'
              });
            }

            ctx.logger.info(`[${this.getStrategyId()}] EXIT SHORT @ ${currentPrice.toFixed(6)}, PnL: ${(pnl * 100).toFixed(3)}%, reason: trailing_stop (peak: ${this._maxFavorablePrice.toFixed(6)})`);

            this._position = 'FLAT';
            this._entryTime = null;
            this._entryPrice = null;
            this._maxFavorablePrice = null;
            this._entryFeatures = null;
            return;
          }
        }
      }
    }

    // Otherwise use edge's exit logic
    await super._handleExit(event, features, regime, ctx);
  }

  /**
   * Override position sizing to scale with trend strength
   */
  _calculatePositionSize(confidence) {
    const baseQty = this.config.baseQuantity || 10;
    const maxQty = this.config.maxQuantity || 50;

    // Get trend strength [0-1]
    const trendStrength = this._entryFeatures?.trend_strength || 0.5;

    // Scale position by trend strength: stronger trend = larger position
    const trendMultiplier = 0.5 + trendStrength; // [0.5 - 1.5]

    // Confidence scaling
    const scaledQty = baseQty + (maxQty - baseQty) * (confidence || 0.5);

    // Apply trend multiplier
    return Math.round(scaledQty * trendMultiplier);
  }
}
