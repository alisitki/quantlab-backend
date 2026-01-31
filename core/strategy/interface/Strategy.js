/**
 * QuantLab Strategy Runtime — Base Strategy Class
 * 
 * PHASE 6: Legacy Adapter
 * 
 * Base class for Strategy v2 implementations.
 * Provides default implementations and documentation.
 * 
 * @module core/strategy/interface/Strategy
 */

/**
 * @typedef {import('./types.js').RuntimeContext} RuntimeContext
 * @typedef {import('./types.js').ReplayEvent} ReplayEvent
 */

/**
 * Base class for Strategy v2 implementations.
 * Extend this class to create deterministic, snapshot-compatible strategies.
 * 
 * @abstract
 */
export class Strategy {
  /** @type {string} */
  id = 'base-strategy';
  
  /** @type {string} */
  version = '1.0.0';
  
  /**
   * Create a strategy instance.
   * 
   * @param {Object} [config] - Strategy configuration
   */
  constructor(config = {}) {
    this.config = Object.freeze({ ...config });
  }
  
  /**
   * Called once before replay starts.
   * Use for initialization, state setup, indicator warmup.
   * 
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onInit(ctx) {
    ctx.logger.debug(`Strategy ${this.id} v${this.version} initialized`);
  }
  
  /**
   * Called for each event during replay.
   * This is where your trading logic goes.
   * 
   * @abstract
   * @param {ReplayEvent} event - Replay event
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onEvent(event, ctx) {
    throw new Error('Strategy.onEvent() must be implemented');
  }
  
  /**
   * Called after replay ends.
   * Use for cleanup, final calculations, reporting.
   * 
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onFinalize(ctx) {
    ctx.logger.debug(`Strategy ${this.id} finalized`);
  }
  
  /**
   * Return current internal state for snapshotting.
   * Override to expose strategy-specific state.
   * 
   * @returns {Object} Current state
   */
  getState() {
    return {};
  }
  
  /**
   * Restore internal state from a snapshot.
   * Override to restore strategy-specific state.
   * 
   * @param {Object} state - State to restore
   */
  setState(state) {
    // Default: no-op
  }
}

export default Strategy;
