/**
 * QuantLab Strategy Runtime — Type Definitions
 * 
 * PHASE 1: Determinism Foundation
 * 
 * This file defines all interfaces and types for the Strategy Runtime v2.
 * NO LOGIC — types only.
 * 
 * @module core/strategy/interface/types
 */

// Re-export execution types for convenience
// (These are stable v1 contracts)

/**
 * @typedef {import('../../execution/fill.js').FillResult} FillResult
 */

/**
 * @typedef {import('../../execution/order.js').Order} Order
 */

/**
 * @typedef {import('../../execution/state.js').ExecutionStateSnapshot} ExecutionStateSnapshot
 */

// ============================================================================
// ENUMS
// ============================================================================

/**
 * Run lifecycle status enum
 * @readonly
 * @enum {string}
 */
export const RunLifecycleStatus = Object.freeze({
  CREATED: 'CREATED',
  INITIALIZING: 'INITIALIZING',
  READY: 'READY',
  RUNNING: 'RUNNING',
  PAUSED: 'PAUSED',
  FINALIZING: 'FINALIZING',
  DONE: 'DONE',
  FAILED: 'FAILED',
  CANCELED: 'CANCELED'
});

/**
 * Error containment policy enum
 * @readonly
 * @enum {string}
 */
export const ErrorPolicy = Object.freeze({
  FAIL_FAST: 'FAIL_FAST',       // Stop on first error
  SKIP_AND_LOG: 'SKIP_AND_LOG', // Skip event, log error, continue
  QUARANTINE: 'QUARANTINE'      // Move to quarantine, continue
});

/**
 * Ordering guard mode enum
 * @readonly
 * @enum {string}
 */
export const OrderingMode = Object.freeze({
  STRICT: 'STRICT',   // Fail on any ordering violation
  WARN: 'WARN'        // Log warning, continue
});

// ============================================================================
// CORE INTERFACES — STRATEGY V2
// ============================================================================

/**
 * Strategy v2 interface — production-grade, deterministic
 * 
 * @typedef {Object} StrategyV2
 * @property {string} id - Unique strategy identifier (e.g., 'baseline-v1')
 * @property {string} version - Semantic version (e.g., '1.0.0')
 * @property {function(RuntimeContext): Promise<void>} onInit - Called once before replay starts
 * @property {function(ReplayEvent, RuntimeContext): Promise<void>} onEvent - Called for each event
 * @property {function(RuntimeContext): Promise<void>} onFinalize - Called after replay ends
 * @property {function(): Object} getState - Return current internal state for snapshotting
 * @property {function(Object): void} setState - Restore from a previous snapshot
 */

/**
 * Legacy Strategy v1 interface (for backward compatibility)
 * 
 * @typedef {Object} StrategyV1
 * @property {function(RunnerContext): Promise<void>|void} [onStart] - Called before replay starts
 * @property {function(Object, RunnerContext): Promise<void>} onEvent - Called for each event
 * @property {function(RunnerContext): Promise<void>|void} [onEnd] - Called after replay ends
 */

// ============================================================================
// RUNTIME CONTEXT
// ============================================================================

/**
 * Enhanced runtime context for Strategy v2
 * 
 * @typedef {Object} RuntimeContext
 * @property {string} runId - Deterministic run identifier (hash-based)
 * @property {DatasetInfo} dataset - Dataset metadata
 * @property {CursorInfo} cursor - Current cursor position
 * @property {RuntimeMetricsInterface} metrics - Metrics counters and gauges
 * @property {Logger} logger - Structured logger with run_id correlation
 * @property {function(OrderIntent): FillResult} placeOrder - Execute order through ExecutionEngine
 * @property {function(): ExecutionStateSnapshot} getExecutionState - Get current execution state
 * @property {string} status - Current lifecycle status
 * @property {RuntimeConfig} config - Runtime configuration (immutable)
 */

/**
 * Dataset information
 * 
 * @typedef {Object} DatasetInfo
 * @property {string} parquet - Path to parquet file (S3 or local)
 * @property {string} meta - Path to meta.json file
 * @property {string} [stream] - Stream type (e.g., 'bbo')
 * @property {string} [date] - Partition date (YYYY-MM-DD)
 * @property {string} [symbol] - Trading symbol
 */

/**
 * Cursor position information
 * 
 * @typedef {Object} CursorInfo
 * @property {bigint|null} ts_event - Last processed event timestamp (nanoseconds)
 * @property {bigint|null} seq - Last processed sequence number
 * @property {string|null} encoded - Base64-encoded cursor string for resume
 */

/**
 * Order intent from strategy
 * 
 * @typedef {Object} OrderIntent
 * @property {string} symbol - Trading symbol
 * @property {'BUY'|'SELL'} side - Order side
 * @property {number} qty - Order quantity
 * @property {bigint} [ts_event] - Event timestamp for order
 */

// ============================================================================
// REPLAY EVENT
// ============================================================================

/**
 * Replay event structure (from ReplayEngine)
 * 
 * @typedef {Object} ReplayEvent
 * @property {bigint} ts_event - Event timestamp in nanoseconds
 * @property {bigint} seq - Sequence number for ordering
 * @property {string} [symbol] - Trading symbol
 * @property {number} [bid_price] - Best bid price (BBO stream)
 * @property {number} [ask_price] - Best ask price (BBO stream)
 * @property {number} [bid_size] - Best bid size
 * @property {number} [ask_size] - Best ask size
 * @property {string} [cursor] - Cursor for this event
 */

// ============================================================================
// RUNTIME STATE
// ============================================================================

/**
 * Unified runtime state snapshot
 * 
 * @typedef {Object} RuntimeStateSnapshot
 * @property {string} runId - Run identifier
 * @property {Object} cursor - Last processed cursor {ts_event, seq}
 * @property {ExecutionStateSnapshot} executionState - From ExecutionEngine.snapshot()
 * @property {Object} strategyState - From strategy.getState()
 * @property {Object} metrics - Metric counters snapshot
 * @property {string} stateHash - SHA256 of combined state
 * @property {string} fillsHash - SHA256 of fills sequence
 * @property {string} timestamp - ISO timestamp of snapshot
 */

/**
 * Run manifest (persisted after run completes)
 * 
 * @typedef {Object} RunManifest
 * @property {string} run_id - Unique run identifier
 * @property {string} started_at - ISO timestamp
 * @property {string} ended_at - ISO timestamp
 * @property {string} ended_reason - 'finished' | 'kill' | 'error' | 'canceled'
 * @property {Object} input - Input configuration
 * @property {Object} output - Run results including hashes
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

/**
 * Runtime configuration
 * 
 * @typedef {Object} RuntimeConfig
 * @property {DatasetInfo} dataset - Dataset to replay
 * @property {StrategyV2|StrategyV1} strategy - Strategy instance
 * @property {Object} [strategyConfig] - Strategy-specific configuration
 * @property {Object} [executionConfig] - Execution engine configuration
 * @property {number} [batchSize=10000] - Replay batch size
 * @property {Object} [clock] - Clock implementation (AsapClock, etc.)
 * @property {string} [seed] - Seed for deterministic run_id generation
 * @property {ErrorPolicy} [errorPolicy='FAIL_FAST'] - Error handling policy
 * @property {OrderingMode} [orderingMode='STRICT'] - Ordering check mode
 * @property {boolean} [enableMetrics=true] - Enable metrics collection
 * @property {boolean} [enableCheckpoints=false] - Enable periodic checkpoints
 * @property {number} [checkpointInterval=100000] - Events between checkpoints
 */

// ============================================================================
// LOGGING
// ============================================================================

/**
 * Structured logger interface
 * 
 * @typedef {Object} Logger
 * @property {function(...args): void} info - Info level log
 * @property {function(...args): void} warn - Warning level log
 * @property {function(...args): void} error - Error level log
 * @property {function(...args): void} debug - Debug level log
 */

// ============================================================================
// METRICS
// ============================================================================

/**
 * Metrics interface for runtime
 * 
 * @typedef {Object} RuntimeMetricsInterface
 * @property {function(string, number=): void} increment - Increment counter
 * @property {function(string, number): void} set - Set gauge value
 * @property {function(): Object} snapshot - Get metrics snapshot
 * @property {function(): string} render - Render Prometheus format
 */

// ============================================================================
// CHECKPOINT
// ============================================================================

/**
 * Checkpoint data structure
 * 
 * @typedef {Object} Checkpoint
 * @property {RuntimeStateSnapshot} state - Full state snapshot
 * @property {number} eventIndex - Event index at checkpoint
 * @property {string} checkpointId - Unique checkpoint identifier
 * @property {string} createdAt - ISO timestamp
 */

// ============================================================================
// ERROR TYPES
// ============================================================================

/**
 * Strategy error wrapper
 * 
 * @typedef {Object} StrategyError
 * @property {string} type - Error type (e.g., 'STRATEGY_ERROR', 'ORDERING_ERROR')
 * @property {string} message - Error message
 * @property {number} eventIndex - Event index where error occurred
 * @property {Object} [event] - Event that caused the error
 * @property {Object} [context] - Additional context
 */

// Export nothing - this file is for JSDoc type definitions only
export {};
