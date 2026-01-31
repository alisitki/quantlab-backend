/**
 * SignalEngine — EMA Cross Strategy
 * 
 * Stateful engine that maintains EMA buffers and generates signals.
 * Output format matches ExecutionEngine.onOrder() expectations.
 */

/**
 * @typedef {Object} Signal
 * @property {'BUY'|'SELL'} side
 * @property {string} symbol
 * @property {number} qty
 * @property {bigint|number} ts_event
 */

/**
 * @typedef {Object} StrategyConfig
 * @property {number} fastPeriod - Fast EMA period (default: 9)
 * @property {number} slowPeriod - Slow EMA period (default: 21)
 * @property {number} positionSize - Order quantity (default: 0.1)
 */

import { createHash } from 'node:crypto';

export class SignalEngine {
  /** @type {StrategyConfig} */
  #config;
  
  /** @type {string} SHA256 of the config */
  #configHash;

  
  /** @type {number} */
  #fastEma = 0;
  
  /** @type {number} */
  #slowEma = 0;
  
  /** @type {number} */
  #eventCount = 0;
  
  /** @type {'FLAT'|'LONG'|'SHORT'} */
  #position = 'FLAT';
  
  /** @type {number} Fast EMA multiplier */
  #fastK;
  
  /** @type {number} Slow EMA multiplier */
  #slowK;

  /**
   * @param {StrategyConfig} [config]
   */
  constructor(config = {}) {
    this.#config = {
      fastPeriod: config.fastPeriod || 9,
      slowPeriod: config.slowPeriod || 21,
      positionSize: config.positionSize || 0.1
    };
    
    // EMA smoothing factors
    this.#fastK = 2 / (this.#config.fastPeriod + 1);
    this.#slowK = 2 / (this.#config.slowPeriod + 1);

    // Hash config for manifest
    this.#configHash = createHash('sha256')
      .update(JSON.stringify(this.#config))
      .digest('hex');
  }

  /**
   * Get config hash
   */
  getConfigHash() {
    return this.#configHash;
  }


  /**
   * Process event and generate signal if conditions met
   * @param {Object} event - Market event from replayd
   * @returns {Signal|null}
   */
  onEvent(event) {
    // Need mid price for EMA calculation
    const bid = Number(event.bid_price || event.payload?.bid_price);
    const ask = Number(event.ask_price || event.payload?.ask_price);
    
    if (!bid || !ask || bid <= 0 || ask <= 0) {
      return null; // Skip non-BBO events
    }
    
    const midPrice = (bid + ask) / 2;
    const symbol = event.symbol || event.payload?.symbol;
    const ts_event = event.ts_event || event.payload?.ts_event;
    
    this.#eventCount++;
    
    // Initialize EMAs with first price
    if (this.#eventCount === 1) {
      this.#fastEma = midPrice;
      this.#slowEma = midPrice;
      return null;
    }
    
    // Update EMAs
    const prevFast = this.#fastEma;
    const prevSlow = this.#slowEma;
    
    this.#fastEma = midPrice * this.#fastK + this.#fastEma * (1 - this.#fastK);
    this.#slowEma = midPrice * this.#slowK + this.#slowEma * (1 - this.#slowK);
    
    // Wait for warmup period
    if (this.#eventCount < this.#config.slowPeriod) {
      return null;
    }
    
    // Detect crossover
    const wasFastAbove = prevFast > prevSlow;
    const isFastAbove = this.#fastEma > this.#slowEma;
    
    // Bullish crossover: fast crosses above slow → BUY
    if (!wasFastAbove && isFastAbove && this.#position !== 'LONG') {
      this.#position = 'LONG';
      return {
        side: 'BUY',
        symbol,
        qty: this.#config.positionSize,
        ts_event
      };
    }
    
    // Bearish crossover: fast crosses below slow → SELL
    if (wasFastAbove && !isFastAbove && this.#position !== 'SHORT') {
      this.#position = 'SHORT';
      return {
        side: 'SELL',
        symbol,
        qty: this.#config.positionSize,
        ts_event
      };
    }
    
    return null;
  }

  /**
   * Get current state for debugging/snapshotting
   */
  snapshot() {
    return {
      fastEma: this.#fastEma,
      slowEma: this.#slowEma,
      position: this.#position,
      eventCount: this.#eventCount,
      fastK: this.#fastK,
      slowK: this.#slowK
    };
  }

  /**
   * Reset engine state
   */
  reset() {
    this.#fastEma = 0;
    this.#slowEma = 0;
    this.#eventCount = 0;
    this.#position = 'FLAT';
  }
}
