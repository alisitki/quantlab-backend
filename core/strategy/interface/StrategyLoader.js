/**
 * QuantLab Strategy Runtime — Strategy Loader
 * 
 * PHASE 6: Legacy Adapter
 * 
 * Dynamic strategy loading with automatic v1/v2 detection.
 * Supports file paths, modules, and class instantiation.
 * 
 * @module core/strategy/interface/StrategyLoader
 */

import { pathToFileURL } from 'node:url';
import { StrategyAdapter } from './StrategyAdapter.js';

/**
 * Load and optionally adapt a strategy from various sources.
 */
export class StrategyLoader {
  /**
   * Load a strategy from a file path.
   * Automatically wraps v1 strategies with adapter.
   * 
   * @param {string} filePath - Path to strategy file
   * @param {Object} [options] - Load options
   * @param {Object} [options.config] - Strategy configuration
   * @param {string} [options.exportName] - Named export (default: 'default')
   * @param {boolean} [options.autoAdapt=true] - Automatically adapt v1 strategies
   * @returns {Promise<Object>} Loaded strategy
   */
  static async loadFromFile(filePath, { config = {}, exportName = 'default', autoAdapt = true } = {}) {
    // Convert to file URL for ESM import
    const fileUrl = filePath.startsWith('file://') 
      ? filePath 
      : pathToFileURL(filePath).href;
    
    // Dynamic import
    const module = await import(fileUrl);
    
    // Get the strategy export - handle both default and named exports
    let StrategyExport = null;
    
    if (exportName === 'default') {
      // Try default export first
      if (module.default) {
        StrategyExport = module.default;
      } else {
        // No default export - look for a strategy class in named exports
        // Strategy classes typically end with 'Strategy' or have onEvent in prototype
        for (const [name, exp] of Object.entries(module)) {
          if (typeof exp === 'function' && exp.prototype) {
            // Check if it looks like a strategy class
            const hasStrategyMethods = 
              typeof exp.prototype.onEvent === 'function' ||
              typeof exp.prototype.onStart === 'function';
            
            if (hasStrategyMethods || name.endsWith('Strategy')) {
              StrategyExport = exp;
              break;
            }
          }
        }
        
        // Fallback: if still not found, try first class export
        if (!StrategyExport) {
          for (const exp of Object.values(module)) {
            if (typeof exp === 'function' && exp.prototype) {
              StrategyExport = exp;
              break;
            }
          }
        }
      }
    } else {
      // Explicit named export requested
      StrategyExport = module[exportName];
    }
    
    if (!StrategyExport) {
      throw new Error(`LOAD_ERROR: No strategy export found in ${filePath}`);
    }
    
    // Instantiate if it's a class
    let strategy;
    if (typeof StrategyExport === 'function' && StrategyExport.prototype) {
      // It's a class - instantiate it
      strategy = new StrategyExport(config);
    } else if (typeof StrategyExport === 'object') {
      // It's already an instance
      strategy = StrategyExport;
    } else {
      throw new Error(`LOAD_ERROR: Invalid strategy export in ${filePath}`);
    }
    
    // IMPORTANT: Adapt BEFORE validation!
    // v1 strategies have onEvent but need adaptation for v2 interface
    if (autoAdapt && StrategyLoader.#isV1Strategy(strategy)) {
      strategy = StrategyAdapter.adapt(strategy);
    }
    
    // Now validate the (potentially adapted) strategy
    if (typeof strategy.onEvent !== 'function') {
      throw new Error(`LOAD_ERROR: Strategy must have onEvent method: ${filePath}`);
    }
    
    return strategy;
  }
  
  /**
   * Check if strategy is v1 (has onEvent but lacks full v2 interface).
   * @private
   */
  static #isV1Strategy(strategy) {
    const hasOnEvent = typeof strategy.onEvent === 'function';
    if (!hasOnEvent) return false;
    
    // v1 indicators: has onStart/onEnd instead of onInit/onFinalize
    const hasV1Lifecycle = 
      typeof strategy.onStart === 'function' ||
      typeof strategy.onEnd === 'function';
    
    // v2 requires all of: onInit, onFinalize, getState, setState
    const hasV2Interface = 
      typeof strategy.onInit === 'function' &&
      typeof strategy.onFinalize === 'function' &&
      typeof strategy.getState === 'function' &&
      typeof strategy.setState === 'function';
    
    // It's v1 if it has v1 lifecycle OR lacks full v2 interface
    return hasV1Lifecycle || !hasV2Interface;
  }
  
  /**
   * Load a strategy from a class.
   * 
   * @param {Function} StrategyClass - Strategy class
   * @param {Object} [options] - Load options
   * @param {Object} [options.config] - Strategy configuration
   * @param {boolean} [options.autoAdapt=true] - Automatically adapt v1 strategies
   * @returns {Object} Strategy instance
   */
  static loadFromClass(StrategyClass, { config = {}, autoAdapt = true } = {}) {
    const strategy = new StrategyClass(config);
    
    if (typeof strategy.onEvent !== 'function') {
      throw new Error('LOAD_ERROR: Strategy must have onEvent method');
    }
    
    if (autoAdapt && StrategyAdapter.needsAdapter(strategy)) {
      return StrategyAdapter.adapt(strategy);
    }
    
    return strategy;
  }
  
  /**
   * Wrap an existing strategy instance if needed.
   * 
   * @param {Object} strategy - Strategy instance
   * @param {Object} [options] - Options
   * @param {boolean} [options.autoAdapt=true] - Automatically adapt v1 strategies
   * @returns {Object} V2-compatible strategy
   */
  static wrap(strategy, { autoAdapt = true } = {}) {
    if (typeof strategy.onEvent !== 'function') {
      throw new Error('LOAD_ERROR: Strategy must have onEvent method');
    }
    
    if (autoAdapt && StrategyAdapter.needsAdapter(strategy)) {
      return StrategyAdapter.adapt(strategy);
    }
    
    return strategy;
  }
  
  /**
   * Detect strategy version (v1 or v2).
   * 
   * @param {Object} strategy - Strategy to check
   * @returns {'v1'|'v2'|'unknown'} Version string
   */
  static detectVersion(strategy) {
    const hasOnEvent = typeof strategy.onEvent === 'function';
    
    if (!hasOnEvent) return 'unknown';
    
    const hasV2Methods = 
      typeof strategy.onInit === 'function' &&
      typeof strategy.onFinalize === 'function' &&
      typeof strategy.getState === 'function' &&
      typeof strategy.setState === 'function';
    
    return hasV2Methods ? 'v2' : 'v1';
  }
  
  /**
   * Validate a strategy has required methods.
   * 
   * @param {Object} strategy - Strategy to validate
   * @returns {{valid: boolean, errors: string[]}} Validation result
   */
  static validate(strategy) {
    const errors = [];
    
    if (!strategy) {
      return { valid: false, errors: ['Strategy is null or undefined'] };
    }
    
    if (typeof strategy.onEvent !== 'function') {
      errors.push('Missing required method: onEvent');
    }
    
    return { valid: errors.length === 0, errors };
  }
}

export default StrategyLoader;
