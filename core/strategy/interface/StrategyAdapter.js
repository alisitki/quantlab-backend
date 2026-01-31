/**
 * QuantLab Strategy Runtime — Strategy Adapter
 * 
 * PHASE 6: Legacy Adapter
 * 
 * Wraps legacy v1 strategies to the v2 interface.
 * Enables existing strategies to run on the new runtime without modification.
 * 
 * @module core/strategy/interface/StrategyAdapter
 */

/**
 * @typedef {import('./types.js').StrategyV1} StrategyV1
 * @typedef {import('./types.js').StrategyV2} StrategyV2
 * @typedef {import('./types.js').RuntimeContext} RuntimeContext
 * @typedef {import('./types.js').ReplayEvent} ReplayEvent
 */

/**
 * Adapter that wraps a v1 strategy to conform to the v2 interface.
 * 
 * V1 Interface:
 * - onStart(ctx)
 * - onEvent(event, ctx)
 * - onEnd(ctx)
 * 
 * V2 Interface:
 * - onInit(ctx)
 * - onEvent(event, ctx)
 * - onFinalize(ctx)
 * - getState()
 * - setState(state)
 * 
 * @implements {StrategyV2}
 */
export class StrategyAdapter {
  /** @type {StrategyV1} */
  #wrapped;
  
  /** @type {string} */
  id;
  
  /** @type {string} */
  version;
  
  /** @type {boolean} */
  #warned;
  
  /**
   * Create a strategy adapter.
   * 
   * @param {StrategyV1} strategy - Legacy v1 strategy to wrap
   * @param {Object} [options] - Adapter options
   * @param {string} [options.id] - Override strategy ID
   * @param {string} [options.version] - Override strategy version
   */
  constructor(strategy, { id, version } = {}) {
    this.#wrapped = strategy;
    this.#warned = false;
    
    // Extract ID and version from wrapped strategy or use defaults
    this.id = id ?? strategy.id ?? strategy.constructor?.name ?? 'adapted-strategy';
    this.version = version ?? strategy.version ?? '1.0.0-adapted';
  }
  
  /**
   * V2 onInit — calls v1 onStart if available.
   * 
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onInit(ctx) {
    this.#logAdapterWarning(ctx);
    
    if (this.#wrapped.onStart) {
      await this.#wrapped.onStart(ctx);
    }
  }
  
  /**
   * V2 onEvent — delegates to v1 onEvent.
   * 
   * @param {ReplayEvent} event - Replay event
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onEvent(event, ctx) {
    await this.#wrapped.onEvent(event, ctx);
  }
  
  /**
   * V2 onFinalize — calls v1 onEnd if available.
   * 
   * @param {RuntimeContext} ctx - Runtime context
   * @returns {Promise<void>}
   */
  async onFinalize(ctx) {
    if (this.#wrapped.onEnd) {
      await this.#wrapped.onEnd(ctx);
    }
  }
  
  /**
   * V2 getState — returns empty object for legacy strategies.
   * Legacy strategies don't expose internal state.
   * 
   * @returns {Object} Empty state (legacy limitation)
   */
  getState() {
    // Check if wrapped strategy has its own getState
    if (typeof this.#wrapped.getState === 'function') {
      return this.#wrapped.getState();
    }
    
    // Legacy strategies don't expose state
    return { __adapted: true, __warning: 'Legacy strategy - no snapshot support' };
  }
  
  /**
   * V2 setState — no-op for legacy strategies.
   * Legacy strategies don't support state restoration.
   * 
   * @param {Object} state - State to restore (ignored)
   */
  setState(state) {
    // Check if wrapped strategy has its own setState
    if (typeof this.#wrapped.setState === 'function') {
      this.#wrapped.setState(state);
      return;
    }
    
    // Legacy strategies can't restore state
    console.warn('[StrategyAdapter] setState called on legacy strategy - no state restoration available');
  }
  
  /**
   * Log a one-time warning about adapter limitations.
   * 
   * @param {RuntimeContext} ctx
   */
  #logAdapterWarning(ctx) {
    if (this.#warned) return;
    this.#warned = true;
    
    ctx.logger.warn(
      `Running legacy v1 strategy "${this.id}" via adapter. ` +
      'Snapshot/resume not fully supported.'
    );
  }
  
  /**
   * Get the wrapped strategy instance.
   * Useful for debugging or direct access.
   * 
   * @returns {StrategyV1} Wrapped strategy
   */
  getWrapped() {
    return this.#wrapped;
  }
  
  /**
   * Check if a strategy needs adaptation.
   * 
   * @param {Object} strategy - Strategy to check
   * @returns {boolean} True if v1 (needs adapter)
   */
  static needsAdapter(strategy) {
    // V2 strategies have all required methods
    const hasV2Methods = 
      typeof strategy.onInit === 'function' &&
      typeof strategy.onEvent === 'function' &&
      typeof strategy.onFinalize === 'function' &&
      typeof strategy.getState === 'function' &&
      typeof strategy.setState === 'function';
    
    // If it has V2 methods, no adapter needed
    if (hasV2Methods) return false;
    
    // If it has onEvent, it's a strategy but needs adaptation
    return typeof strategy.onEvent === 'function';
  }
  
  /**
   * Conditionally wrap a strategy.
   * Returns the strategy as-is if it's v2, or wraps with adapter if v1.
   * 
   * @param {Object} strategy - Strategy to potentially wrap
   * @param {Object} [options] - Adapter options
   * @returns {StrategyV2} V2-compatible strategy
   */
  static adapt(strategy, options = {}) {
    if (StrategyAdapter.needsAdapter(strategy)) {
      return new StrategyAdapter(strategy, options);
    }
    return strategy;
  }
}

export default StrategyAdapter;
