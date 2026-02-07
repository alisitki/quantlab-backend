/**
 * BaseTemplate - Abstract base for all strategy templates
 *
 * All templates follow the standard strategy interface:
 * - onStart(ctx)
 * - onEvent(event, ctx)
 * - onEnd(ctx)
 *
 * Templates are parameterized by Edge properties.
 */

import { FeatureRegistry } from '../../../features/FeatureRegistry.js';

export class BaseTemplate {
  /**
   * @param {Object} params - Parameters derived from edge
   * @param {Edge} params.edge - The edge this strategy expresses
   * @param {Object} params.config - Strategy-specific parameters
   */
  constructor(params) {
    if (!params.edge) {
      throw new Error('BaseTemplate: edge is required');
    }

    this.edge = params.edge;
    this.config = params.config || {};
    this.templateType = 'base';

    // Outcome collector for closed-loop learning (optional)
    this._outcomeCollector = params.outcomeCollector || null;

    // Internal state (protected)
    this._featureBuilder = null;
    this._position = 'FLAT';
    this._tradeCount = 0;
    this._entryTime = null;
    this._entryPrice = null;
    this._lastFeatures = null;
    this._lastRegime = null;
  }

  /**
   * Template identifier
   * @returns {string}
   */
  getTemplateType() {
    return this.templateType;
  }

  /**
   * Generate strategy ID from edge ID
   * @returns {string}
   */
  getStrategyId() {
    return `strat_${this.edge.id}_${this.templateType}_v1`;
  }

  /**
   * Initialize strategy (called once at start)
   */
  async onStart(ctx) {
    // Build feature builder with features needed by edge
    const featureNames = this._extractRequiredFeatures();

    this._featureBuilder = FeatureRegistry.createFeatureBuilder(ctx.symbol || 'UNKNOWN', {
      enabledFeatures: ['mid_price', 'spread', ...featureNames]
    });

    this._position = 'FLAT';
    this._tradeCount = 0;
    this._entryTime = null;
    this._entryPrice = null;

    ctx.logger.info(`[${this.getStrategyId()}] Strategy started with edge: ${this.edge.name}`);
  }

  /**
   * Process event (called for each market event)
   */
  async onEvent(event, ctx) {
    // Extract features
    const features = this._featureBuilder.onEvent(event);

    if (!features) {
      return; // Warmup period
    }

    // Extract regime (if edge uses regime-based features)
    const regime = this._extractRegime(features);

    // Handle position logic
    if (this._position === 'FLAT') {
      await this._handleEntry(event, features, regime, ctx);
    } else {
      await this._handleExit(event, features, regime, ctx);
    }
  }

  /**
   * Finalize strategy (called once at end)
   */
  async onEnd(ctx) {
    ctx.logger.info(`[${this.getStrategyId()}] Strategy ended. Trades: ${this._tradeCount}, Final position: ${this._position}`);
  }

  /**
   * Handle entry logic (override in subclasses if needed)
   */
  async _handleEntry(event, features, regime, ctx) {
    const entryEval = this.edge.evaluateEntry(features, regime);

    if (!entryEval.active) {
      return;
    }

    // Check cooldown
    if (this._entryTime && (event.ts_event - this._entryTime) < this.config.cooldownMs) {
      return;
    }

    // Place order
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

      // Store features/regime for outcome collection
      this._lastFeatures = features;
      this._lastRegime = regime;

      // Record trade entry for closed-loop learning
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

      ctx.logger.info(`[${this.getStrategyId()}] ENTRY ${entryEval.direction} @ ${features.mid_price.toFixed(6)}, confidence: ${entryEval.confidence?.toFixed(2)}`);
    }
  }

  /**
   * Handle exit logic (override in subclasses if needed)
   */
  async _handleExit(event, features, regime, ctx) {
    const exitEval = this.edge.evaluateExit(features, regime, this._entryTime, event.ts_event);

    if (!exitEval.exit) {
      return;
    }

    // Close position
    if (ctx.placeOrder) {
      const quantity = this.config.baseQuantity || 10;

      ctx.placeOrder({
        symbol: event.symbol,
        side: this._position === 'LONG' ? 'SELL' : 'BUY',
        type: 'MARKET',
        qty: quantity,
        ts_event: event.ts_event
      });

      const exitPrice = features.mid_price;
      const pnl = this._position === 'LONG'
        ? (exitPrice - this._entryPrice) / this._entryPrice
        : (this._entryPrice - exitPrice) / this._entryPrice;

      // Record trade exit for closed-loop learning
      if (this._outcomeCollector) {
        const tradeId = `${this.getStrategyId()}_${this._tradeCount}`;
        this._outcomeCollector.recordExit(tradeId, {
          price: exitPrice,
          timestamp: event.ts_event,
          pnl,
          exitReason: exitEval.reason || 'unknown'
        });
      }

      ctx.logger.info(`[${this.getStrategyId()}] EXIT ${this._position} @ ${exitPrice.toFixed(6)}, PnL: ${(pnl * 100).toFixed(3)}%, reason: ${exitEval.reason}`);

      this._position = 'FLAT';
      this._entryTime = null;
      this._entryPrice = null;
    }
  }

  /**
   * Extract required features from edge conditions
   * @returns {string[]}
   */
  _extractRequiredFeatures() {
    // Get features from edge's discovery definition (if available)
    // For now, return all behavior features
    return [
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

  /**
   * Extract regime from features
   * @param {Object} features
   * @returns {number}
   */
  _extractRegime(features) {
    // Simple heuristic: use regime_volatility or default to 0
    return features.regime_volatility || 0;
  }

  /**
   * Calculate position size based on confidence
   * @param {number} confidence - Entry confidence [0-1]
   * @returns {number}
   */
  _calculatePositionSize(confidence) {
    const baseQty = this.config.baseQuantity || 10;
    const maxQty = this.config.maxQuantity || 50;

    // Scale position by confidence
    const scaledQty = baseQty + (maxQty - baseQty) * (confidence || 0.5);

    return Math.round(scaledQty);
  }
}
