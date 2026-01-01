/**
 * QuantLab Baseline Strategy v1 — Main Strategy Class
 * 
 * Implements the Strategy interface for replay-driven backtesting.
 * Uses features → decision pipeline for clean separation.
 * 
 * Architecture:
 * - config.js: Parameters
 * - features.js: Feature extraction
 * - decision.js: Trading logic
 * - BaselineStrategy.js: Orchestration + execution
 * 
 * Risk Guards (strategy-level):
 * - Max 1 position at a time
 * - Cooldown after each trade
 * - No pyramiding
 */

import { createConfig } from './config.js';
import { decide } from './decision.js';
import { FeatureRegistry } from '../../features/FeatureRegistry.js';

/**
 * @typedef {import('../types.js').RunnerContext} RunnerContext
 * @typedef {import('./config.js').BaselineConfig} BaselineConfig
 */

export class BaselineStrategy {
  /** @type {BaselineConfig} */
  #config;
  
  /** @type {import('../../features/FeatureBuilder.js').FeatureBuilder} */
  #featureBuilder;
  
  /** @type {'LONG' | 'SHORT' | 'FLAT' | 'EXIT_LONG' | 'EXIT_SHORT'} */
  #position = 'FLAT';
  
  /** @type {number} */
  #cooldownRemaining = 0;
  
  /** @type {number} */
  #tradeCount = 0;
  
  /** @type {number} */
  #signalCount = 0;

  /**
   * @param {Partial<BaselineConfig>} [userConfig={}]
   */
  constructor(userConfig = {}) {
    this.#config = createConfig(userConfig);
    this.#featureBuilder = FeatureRegistry.createFeatureBuilder(this.#config.symbol, {
      enabledFeatures: ['mid_price', 'spread', 'return_1', 'volatility']
    });
  }

  /**
   * Called before replay starts
   * @param {RunnerContext} ctx
   */
  async onStart(ctx) {
    ctx.logger.info('=== BaselineStrategy v1 (FeatureBuilder) ===');
    ctx.logger.info(`Config: symbol=${this.#config.symbol} qty=${this.#config.orderQty}`);
    ctx.logger.info(`        cooldown=${this.#config.cooldownEvents} threshold=${this.#config.momentumThreshold}`);
    ctx.logger.info(`        spreadMaxBps=${this.#config.spreadMaxBps}`);
    
    if (!ctx.placeOrder) {
      ctx.logger.warn('No execution engine attached - signals only mode');
    }
  }

  /**
   * Process each BBO event
   * @param {Object} event
   * @param {RunnerContext} ctx
   */
  async onEvent(event, ctx) {
    // Skip non-matching symbols
    const eventSymbol = (event.symbol || '').toLowerCase();
    if (eventSymbol && eventSymbol !== this.#config.symbol.toLowerCase()) {
      return;
    }

    // Compute features using FeatureBuilder
    const features = this.#featureBuilder.onEvent(event);
    
    // Skip until warm (FeatureBuilder returns null until all features are valid)
    if (!features) {
      return;
    }

    // Check cooldown
    if (this.#cooldownRemaining > 0) {
      this.#cooldownRemaining--;
      return;
    }

    // Get decision from decision engine
    const decision = decide({
      mid_price: features.mid_price,
      spread: features.spread,
      return_1: features.return_1,
      volatility: features.volatility, // Added for future use
      position: this.#position,
      config: this.#config
    });

    // Execute decision
    if (decision.action !== 'FLAT') {
      this.#signalCount++;
      this.#executeDecision(decision, event, ctx);
    }
  }

  /**
   * Execute a trading decision
   * @param {import('./decision.js').Decision} decision
   * @param {Object} event
   * @param {RunnerContext} ctx
   */
  #executeDecision(decision, event, ctx) {
    if (!ctx.placeOrder) {
      return; // No execution engine
    }

    const symbol = event.symbol || this.#config.symbol.toUpperCase();
    const ts_event = event.ts_event;

    switch (decision.action) {
      case 'LONG':
        ctx.placeOrder({
          symbol,
          side: 'BUY',
          qty: this.#config.orderQty,
          ts_event
        });
        this.#position = 'LONG';
        this.#tradeCount++;
        this.#cooldownRemaining = this.#config.cooldownEvents;
        break;

      case 'SHORT':
        ctx.placeOrder({
          symbol,
          side: 'SELL',
          qty: this.#config.orderQty,
          ts_event
        });
        this.#position = 'SHORT';
        this.#tradeCount++;
        this.#cooldownRemaining = this.#config.cooldownEvents;
        break;

      case 'EXIT_LONG':
        ctx.placeOrder({
          symbol,
          side: 'SELL',
          qty: this.#config.orderQty,
          ts_event
        });
        this.#position = 'FLAT';
        this.#tradeCount++;
        this.#cooldownRemaining = this.#config.cooldownEvents;
        break;

      case 'EXIT_SHORT':
        ctx.placeOrder({
          symbol,
          side: 'BUY',
          qty: this.#config.orderQty,
          ts_event
        });
        this.#position = 'FLAT';
        this.#tradeCount++;
        this.#cooldownRemaining = this.#config.cooldownEvents;
        break;
    }
  }

  /**
   * Called after replay ends
   * @param {RunnerContext} ctx
   */
  async onEnd(ctx) {
    ctx.logger.info('\n=== BaselineStrategy Summary ===');
    ctx.logger.info(`Total signals: ${this.#signalCount}`);
    ctx.logger.info(`Total trades:  ${this.#tradeCount}`);
    ctx.logger.info(`Final position: ${this.#position}`);
    ctx.logger.info(`Events processed: ${ctx.stats.processed}`);
  }

  /**
   * Get strategy stats (for testing)
   */
  getStats() {
    return {
      tradeCount: this.#tradeCount,
      signalCount: this.#signalCount,
      position: this.#position,
      config: { ...this.#config }
    };
  }
}
