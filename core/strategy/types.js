/**
 * @typedef {Object} RunnerContext
 * @property {string} runId - Unique ID for this run
 * @property {Object} dataset - Dataset information
 * @property {string} dataset.parquet - S3/Local path to parquet
 * @property {string} dataset.meta - S3/Local path to meta.json
 * @property {Object} stats - Runtime stats
 * @property {number} stats.processed - Count of processed events
 * @property {Object} logger - Simple console wrapper
 * @property {Function} logger.info
 * @property {Function} logger.error
 * @property {Function} logger.warn
 * @property {import('../execution/engine.js').ExecutionEngine} [execution] - Execution engine instance (optional)
 * @property {function(Object): import('../execution/fill.js').FillResult} [placeOrder] - Order placement function
 */

/**
 * @typedef {Object} Strategy
 * @property {function(RunnerContext): Promise<void>|void} [onStart] - Called before replay starts
 * @property {function(Object, RunnerContext): Promise<void>} onEvent - Called for each event. MUST be async or return Promise for backpressure.
 * @property {function(RunnerContext): Promise<void>|void} [onEnd] - Called after replay ends
 */

// This file is for JSDoc type definitions
export {};
