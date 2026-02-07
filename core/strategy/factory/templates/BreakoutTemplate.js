/**
 * BreakoutTemplate - Strategy for volatility breakout edges
 *
 * Optimized for edges triggered by volatility compression/expansion.
 * Features:
 * - Activation delay (confirm breakout before entering)
 * - Time-based exit (failed breakout detection)
 * - Fixed position sizing (binary bet on breakout)
 */

import { BaseTemplate } from './BaseTemplate.js';

export class BreakoutTemplate extends BaseTemplate {
  constructor(params) {
    super(params);
    this.templateType = 'breakout';

    // Breakout specific config
    this._activationDelay = params.config?.activationDelay || 5; // events
    this._maxNoProgressEvents = params.config?.maxNoProgressEvents || 100;

    // Breakout confirmation tracking
    this._breakoutSignalTime = null;
    this._breakoutSignalPrice = null;
    this._breakoutDirection = null;
    this._breakoutConfirmed = false;
    this._eventsSinceEntry = 0;
  }

  /**
   * Override entry to add breakout confirmation delay
   */
  async _handleEntry(event, features, regime, ctx) {
    const entryEval = this.edge.evaluateEntry(features, regime);

    if (!entryEval.active) {
      // Reset if signal disappears
      if (this._breakoutSignalTime) {
        this._breakoutSignalTime = null;
        this._breakoutSignalPrice = null;
        this._breakoutDirection = null;
      }
      return;
    }

    // Check cooldown
    if (this._entryTime && (event.ts_event - this._entryTime) < this.config.cooldownMs) {
      return;
    }

    // First signal detection
    if (!this._breakoutSignalTime) {
      this._breakoutSignalTime = event.ts_event;
      this._breakoutSignalPrice = features.mid_price;
      this._breakoutDirection = entryEval.direction;
      this._breakoutConfirmed = false;
      ctx.logger.info(`[${this.getStrategyId()}] BREAKOUT SIGNAL detected ${entryEval.direction} @ ${features.mid_price.toFixed(6)}, waiting ${this._activationDelay} events...`);
      return;
    }

    // Check if enough events passed since signal
    const eventsPassed = Math.floor((event.ts_event - this._breakoutSignalTime) / 100); // Rough event count

    if (eventsPassed < this._activationDelay) {
      return; // Still waiting
    }

    // Confirm breakout: price should continue in signal direction
    const currentPrice = features.mid_price;
    const priceMove = this._breakoutDirection === 'LONG'
      ? (currentPrice - this._breakoutSignalPrice) / this._breakoutSignalPrice
      : (this._breakoutSignalPrice - currentPrice) / this._breakoutSignalPrice;

    // If price moved in expected direction (or at least didn't reverse), confirm
    if (priceMove >= -0.0001) { // Allow tiny reversal
      // Confirmed - enter position
      if (ctx.placeOrder) {
        const quantity = this._calculatePositionSize(entryEval.confidence);

        ctx.placeOrder({
          symbol: event.symbol,
          side: entryEval.direction === 'LONG' ? 'BUY' : 'SELL',
          type: 'MARKET',
          qty: quantity,
          ts_event: event.ts_event
        });

        this._position = entryEval.direction;
        this._entryTime = event.ts_event;
        this._entryPrice = features.mid_price;
        this._tradeCount++;
        this._breakoutConfirmed = true;
        this._eventsSinceEntry = 0;

        // Store features/regime for outcome collection
        this._lastFeatures = features;
        this._lastRegime = regime;

        // Record trade entry
        if (this._outcomeCollector) {
          const tradeId = `${this.getStrategyId()}_${this._tradeCount}`;
          this._outcomeCollector.recordEntry(tradeId, {
            features,
            regime,
            edgeId: this.edge.id,
            direction: entryEval.direction,
            price: features.mid_price,
            timestamp: event.ts_event
          });
        }

        ctx.logger.info(`[${this.getStrategyId()}] BREAKOUT CONFIRMED - ENTRY ${entryEval.direction} @ ${features.mid_price.toFixed(6)}, confidence: ${entryEval.confidence?.toFixed(2)}`);

        // Reset signal tracking
        this._breakoutSignalTime = null;
        this._breakoutSignalPrice = null;
        this._breakoutDirection = null;
      }
    } else {
      // False breakout - reset
      ctx.logger.info(`[${this.getStrategyId()}] BREAKOUT FAILED (price reversed), resetting signal`);
      this._breakoutSignalTime = null;
      this._breakoutSignalPrice = null;
      this._breakoutDirection = null;
    }
  }

  /**
   * Override exit to add time-based failed breakout detection
   */
  async _handleExit(event, features, regime, ctx) {
    if (this._position !== 'FLAT') {
      this._eventsSinceEntry++;

      // Check if no progress after max events (failed breakout)
      if (this._eventsSinceEntry >= this._maxNoProgressEvents) {
        const currentPrice = features.mid_price;
        const pnl = this._position === 'LONG'
          ? (currentPrice - this._entryPrice) / this._entryPrice
          : (this._entryPrice - currentPrice) / this._entryPrice;

        // If not profitable, exit early (breakout didn't materialize)
        if (pnl < 0.0005) { // Less than 0.05% profit
          if (ctx.placeOrder) {
            const quantity = this.config.baseQuantity || 10;

            ctx.placeOrder({
              symbol: event.symbol,
              side: this._position === 'LONG' ? 'SELL' : 'BUY',
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
                exitReason: 'no_progress'
              });
            }

            ctx.logger.info(`[${this.getStrategyId()}] EXIT ${this._position} @ ${currentPrice.toFixed(6)}, PnL: ${(pnl * 100).toFixed(3)}%, reason: no_progress (failed breakout)`);

            this._position = 'FLAT';
            this._entryTime = null;
            this._entryPrice = null;
            this._eventsSinceEntry = 0;
            return;
          }
        }
      }
    }

    // Otherwise use edge's exit logic
    await super._handleExit(event, features, regime, ctx);
  }

  /**
   * Breakout strategies use fixed position sizing (not scaled by confidence)
   * This is a binary bet - either breakout happens or it doesn't
   */
  _calculatePositionSize(confidence) {
    return this.config.baseQuantity || 10;
  }
}
