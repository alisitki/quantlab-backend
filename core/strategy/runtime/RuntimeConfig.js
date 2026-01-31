/**
 * QuantLab Strategy Runtime — Runtime Configuration
 * 
 * PHASE 3: Lifecycle & Runtime
 * 
 * Configuration schema and validation for StrategyRuntime.
 * Immutable after creation — changes require new runtime instance.
 * 
 * @module core/strategy/runtime/RuntimeConfig
 */

import { computeHash } from '../safety/DeterminismValidator.js';
import { ErrorPolicy, OrderingMode } from '../interface/types.js';

/**
 * @typedef {import('../interface/types.js').RuntimeConfig} RuntimeConfigType
 * @typedef {import('../interface/types.js').DatasetInfo} DatasetInfo
 */

/**
 * Default configuration values
 */
const DEFAULTS = Object.freeze({
  batchSize: 10000,
  errorPolicy: ErrorPolicy.FAIL_FAST,
  orderingMode: OrderingMode.STRICT,
  enableMetrics: true,
  enableCheckpoints: false,
  checkpointInterval: 100000,
  seed: ''
});

/**
 * Runtime configuration with validation and hashing.
 */
export class RuntimeConfig {
  /** @type {DatasetInfo} */
  #dataset;
  
  /** @type {Object} */
  #strategy;
  
  /** @type {Object} */
  #strategyConfig;
  
  /** @type {Object} */
  #executionConfig;
  
  /** @type {number} */
  #batchSize;
  
  /** @type {Object|null} */
  #clock;
  
  /** @type {string} */
  #seed;
  
  /** @type {string} */
  #errorPolicy;
  
  /** @type {string} */
  #orderingMode;
  
  /** @type {boolean} */
  #enableMetrics;
  
  /** @type {boolean} */
  #enableCheckpoints;
  
  /** @type {number} */
  #checkpointInterval;
  
  /** @type {string} */
  #configHash;
  
  /**
   * Create a runtime configuration.
   * 
   * @param {RuntimeConfigType} config - Configuration object
   * @throws {Error} If required fields are missing
   */
  constructor(config) {
    // Validate required fields
    this.#validateRequired(config);
    
    // Set values with defaults
    this.#dataset = Object.freeze({ ...config.dataset });
    this.#strategy = config.strategy;
    this.#strategyConfig = Object.freeze({ ...config.strategyConfig || {} });
    this.#executionConfig = Object.freeze({ ...config.executionConfig || {} });
    this.#batchSize = config.batchSize ?? DEFAULTS.batchSize;
    this.#clock = config.clock ?? null;
    this.#seed = config.seed ?? DEFAULTS.seed;
    this.#errorPolicy = config.errorPolicy ?? DEFAULTS.errorPolicy;
    this.#orderingMode = config.orderingMode ?? DEFAULTS.orderingMode;
    this.#enableMetrics = config.enableMetrics ?? DEFAULTS.enableMetrics;
    this.#enableCheckpoints = config.enableCheckpoints ?? DEFAULTS.enableCheckpoints;
    this.#checkpointInterval = config.checkpointInterval ?? DEFAULTS.checkpointInterval;
    
    // Validate enum values
    this.#validateEnums();
    
    // Compute config hash for deterministic run_id
    this.#configHash = this.#computeConfigHash();
    
    // Freeze this instance
    Object.freeze(this);
  }
  
  /**
   * Validate required configuration fields.
   * 
   * @param {RuntimeConfigType} config
   * @throws {Error} If validation fails
   */
  #validateRequired(config) {
    if (!config) {
      throw new Error('CONFIG_ERROR: Configuration object is required');
    }
    
    if (!config.dataset) {
      throw new Error('CONFIG_ERROR: dataset is required');
    }
    
    if (!config.dataset.parquet) {
      throw new Error('CONFIG_ERROR: dataset.parquet is required');
    }
    
    if (!config.dataset.meta) {
      throw new Error('CONFIG_ERROR: dataset.meta is required');
    }
    
    if (!config.strategy) {
      throw new Error('CONFIG_ERROR: strategy is required');
    }
  }
  
  /**
   * Validate enum configuration values.
   * 
   * @throws {Error} If enum value is invalid
   */
  #validateEnums() {
    const validErrorPolicies = Object.values(ErrorPolicy);
    if (!validErrorPolicies.includes(this.#errorPolicy)) {
      throw new Error(`CONFIG_ERROR: Invalid errorPolicy: ${this.#errorPolicy}`);
    }
    
    const validOrderingModes = Object.values(OrderingMode);
    if (!validOrderingModes.includes(this.#orderingMode)) {
      throw new Error(`CONFIG_ERROR: Invalid orderingMode: ${this.#orderingMode}`);
    }
  }
  
  /**
   * Compute hash of configuration for deterministic run_id.
   * 
   * @returns {string} Configuration hash
   */
  #computeConfigHash() {
    const hashInput = {
      dataset: this.#dataset,
      strategyConfig: this.#strategyConfig,
      executionConfig: this.#executionConfig,
      batchSize: this.#batchSize,
      seed: this.#seed,
      errorPolicy: this.#errorPolicy,
      orderingMode: this.#orderingMode
    };
    return computeHash(hashInput);
  }
  
  // ============================================================================
  // GETTERS (all readonly)
  // ============================================================================
  
  /** @returns {DatasetInfo} */
  get dataset() { return this.#dataset; }
  
  /** @returns {Object} */
  get strategy() { return this.#strategy; }
  
  /** @returns {Object} */
  get strategyConfig() { return this.#strategyConfig; }
  
  /** @returns {Object} */
  get executionConfig() { return this.#executionConfig; }
  
  /** @returns {number} */
  get batchSize() { return this.#batchSize; }
  
  /** @returns {Object|null} */
  get clock() { return this.#clock; }
  
  /** @returns {string} */
  get seed() { return this.#seed; }
  
  /** @returns {string} */
  get errorPolicy() { return this.#errorPolicy; }
  
  /** @returns {string} */
  get orderingMode() { return this.#orderingMode; }
  
  /** @returns {boolean} */
  get enableMetrics() { return this.#enableMetrics; }
  
  /** @returns {boolean} */
  get enableCheckpoints() { return this.#enableCheckpoints; }
  
  /** @returns {number} */
  get checkpointInterval() { return this.#checkpointInterval; }
  
  /** @returns {string} */
  get configHash() { return this.#configHash; }
  
  // ============================================================================
  // METHODS
  // ============================================================================
  
  /**
   * Get configuration as plain object (for logging/debugging).
   * 
   * @returns {Object} Configuration object
   */
  toObject() {
    return {
      dataset: { ...this.#dataset },
      strategyConfig: { ...this.#strategyConfig },
      executionConfig: { ...this.#executionConfig },
      batchSize: this.#batchSize,
      seed: this.#seed,
      errorPolicy: this.#errorPolicy,
      orderingMode: this.#orderingMode,
      enableMetrics: this.#enableMetrics,
      enableCheckpoints: this.#enableCheckpoints,
      checkpointInterval: this.#checkpointInterval,
      configHash: this.#configHash
    };
  }
  
  /**
   * Get short hash for display.
   * 
   * @returns {string} First 8 characters of config hash
   */
  getShortHash() {
    return this.#configHash.substring(0, 8);
  }
}

/**
 * Create RuntimeConfig with validation.
 * 
 * @param {RuntimeConfigType} config - Configuration object
 * @returns {RuntimeConfig} Validated configuration
 */
export function createRuntimeConfig(config) {
  return new RuntimeConfig(config);
}

export default RuntimeConfig;
