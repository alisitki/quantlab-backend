/**
 * QuantLab Strategy Runtime — Runtime Context
 * 
 * PHASE 3: Lifecycle & Runtime
 * 
 * Enhanced context passed to strategies during execution.
 * Contains all resources needed for event processing.
 * 
 * @module core/strategy/runtime/RuntimeContext
 */

/**
 * @typedef {import('../interface/types.js').RuntimeContext} RuntimeContextType
 * @typedef {import('../interface/types.js').CursorInfo} CursorInfo
 * @typedef {import('../interface/types.js').Logger} Logger
 * @typedef {import('./RuntimeConfig.js').RuntimeConfig} RuntimeConfig
 */

/**
 * Create a structured logger with run_id correlation.
 * 
 * @param {string} runId - Run identifier for correlation
 * @returns {Logger} Structured logger
 */
function createLogger(runId) {
  const prefix = `[${runId}]`;
  
  return {
    info: (...args) => console.log(prefix, '[INFO]', ...args),
    warn: (...args) => console.warn(prefix, '[WARN]', ...args),
    error: (...args) => console.error(prefix, '[ERROR]', ...args),
    debug: (...args) => {
      if (process.env.DEBUG) {
        console.log(prefix, '[DEBUG]', ...args);
      }
    }
  };
}

/**
 * Runtime context factory.
 * Creates the context object passed to strategy methods.
 */
export class RuntimeContext {
  /** @type {string} */
  #runId;
  
  /** @type {Object} */
  #dataset;
  
  /** @type {CursorInfo} */
  #cursor;
  
  /** @type {Object|null} */
  #metrics;
  
  /** @type {Logger} */
  #logger;
  
  /** @type {function|null} */
  #placeOrder;
  
  /** @type {function|null} */
  #getExecutionState;
  
  /** @type {string} */
  #status;
  
  /** @type {RuntimeConfig} */
  #config;
  
  /** @type {number} */
  #processedCount;
  
  /**
   * Create a runtime context.
   * 
   * @param {Object} options - Context options
   * @param {string} options.runId - Deterministic run identifier
   * @param {Object} options.dataset - Dataset information
   * @param {RuntimeConfig} options.config - Runtime configuration
   * @param {Object} [options.metrics] - Metrics registry
   * @param {function} [options.placeOrder] - Order placement function
   * @param {function} [options.getExecutionState] - Execution state getter
   */
  constructor({
    runId,
    dataset,
    config,
    metrics = null,
    placeOrder = null,
    getExecutionState = null
  }) {
    this.#runId = runId;
    this.#dataset = Object.freeze({ ...dataset });
    this.#config = config;
    this.#metrics = metrics;
    this.#logger = createLogger(runId);
    this.#placeOrder = placeOrder;
    this.#getExecutionState = getExecutionState;
    this.#status = 'CREATED';
    
    // Initialize cursor as empty
    this.#cursor = {
      ts_event: null,
      seq: null,
      encoded: null
    };
    
    this.#processedCount = 0;
  }
  
  // ============================================================================
  // GETTERS
  // ============================================================================
  
  /** @returns {string} */
  get runId() { return this.#runId; }
  
  /** @returns {Object} */
  get dataset() { return this.#dataset; }
  
  /** @returns {CursorInfo} */
  get cursor() { return { ...this.#cursor }; }
  
  /** @returns {Object|null} */
  get metrics() { return this.#metrics; }
  
  /** @returns {Logger} */
  get logger() { return this.#logger; }
  
  /** @returns {string} */
  get status() { return this.#status; }
  
  /** @returns {RuntimeConfig} */
  get config() { return this.#config; }
  
  /** 
   * @returns {Object} Legacy stats compatibility
   */
  get stats() {
    return {
      processed: this.#processedCount
    };
  }
  
  /**
   * Increment processed count (called by runtime).
   * @private
   */
  incrementProcessed() {
    this.#processedCount++;
  }
  
  // ============================================================================
  // ORDER PLACEMENT
  // ============================================================================
  
  /**
   * Place an order through the execution engine.
   * 
   * @param {Object} orderIntent - Order intent
   * @returns {Object} Fill result
   * @throws {Error} If no execution engine attached
   */
  placeOrder(orderIntent) {
    if (!this.#placeOrder) {
      throw new Error('CONTEXT_ERROR: No execution engine attached - cannot place orders');
    }
    return this.#placeOrder(orderIntent);
  }
  
  /**
   * Check if order placement is available.
   * 
   * @returns {boolean} True if execution engine is attached
   */
  canPlaceOrders() {
    return this.#placeOrder !== null;
  }
  
  // ============================================================================
  // EXECUTION STATE
  // ============================================================================
  
  /**
   * Get current execution state snapshot.
   *
   * @returns {Object|null} Execution state or null if not available
   */
  getExecutionState() {
    if (!this.#getExecutionState) {
      return null;
    }
    return this.#getExecutionState();
  }

  /**
   * Execution engine compatibility interface for RiskManager.
   * Provides snapshot() method expected by risk rules.
   *
   * @returns {Object|null} Execution interface with snapshot() or null
   */
  get execution() {
    if (!this.#getExecutionState) {
      return null;
    }
    const getState = this.#getExecutionState;
    return {
      snapshot: () => getState()
    };
  }

  // ============================================================================
  // CURSOR MANAGEMENT
  // ============================================================================
  
  /**
   * Update cursor position (called by runtime after each event).
   * 
   * @param {Object} cursorInfo - New cursor position
   */
  updateCursor(cursorInfo) {
    this.#cursor = {
      ts_event: cursorInfo.ts_event ?? this.#cursor.ts_event,
      seq: cursorInfo.seq ?? this.#cursor.seq,
      encoded: cursorInfo.encoded ?? this.#cursor.encoded
    };
  }
  
  // ============================================================================
  // STATUS MANAGEMENT
  // ============================================================================
  
  /**
   * Update runtime status (called by runtime).
   * 
   * @param {string} status - New status
   */
  setStatus(status) {
    this.#status = status;
  }
  
  // ============================================================================
  // METRICS HELPERS
  // ============================================================================
  
  /**
   * Increment a metric counter.
   * 
   * @param {string} name - Metric name
   * @param {number} [value=1] - Value to add
   */
  incrementMetric(name, value = 1) {
    if (this.#metrics && this.#metrics.increment) {
      this.#metrics.increment(name, value);
    }
  }
  
  /**
   * Set a metric gauge value.
   * 
   * @param {string} name - Metric name
   * @param {number} value - Value to set
   */
  setMetric(name, value) {
    if (this.#metrics && this.#metrics.set) {
      this.#metrics.set(name, value);
    }
  }
  
  // ============================================================================
  // SNAPSHOT
  // ============================================================================
  
  /**
   * Get context snapshot for logging/debugging.
   * 
   * @returns {Object} Context snapshot
   */
  snapshot() {
    return {
      runId: this.#runId,
      dataset: { ...this.#dataset },
      cursor: { ...this.#cursor },
      status: this.#status,
      hasExecution: this.#placeOrder !== null,
      hasMetrics: this.#metrics !== null
    };
  }
}

/**
 * Create a runtime context.
 * 
 * @param {Object} options - Context options
 * @returns {RuntimeContext} Runtime context
 */
export function createRuntimeContext(options) {
  return new RuntimeContext(options);
}

export default RuntimeContext;
