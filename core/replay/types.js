/**
 * QuantLab Replay Engine v1 — Type Definitions
 * JSDoc types for meta.json, replay options, and row data.
 */

/**
 * @typedef {Object} MetaData
 * @property {number} schema_version - Must be 1 for v1 replay
 * @property {number} rows - Total row count in parquet
 * @property {number} ts_event_min - Minimum ts_event timestamp (nanoseconds)
 * @property {number} ts_event_max - Maximum ts_event timestamp (nanoseconds)
 * @property {number} [source_files] - Number of source files merged
 */

/**
 * @typedef {Object} ReplayOptions
 * @property {number} [startTs] - Filter: minimum ts_event (inclusive)
 * @property {number} [endTs] - Filter: maximum ts_event (inclusive)
 * @property {number} [batchSize=10000] - Rows per batch (default: 10000)
 */

/**
 * @typedef {Object.<string, any>} Row
 * Row from parquet file. Schema depends on stream type.
 * Common fields: ts_event, symbol, etc.
 */

/**
 * @typedef {Object} ReplayStats
 * @property {number} rowsEmitted - Total rows yielded
 * @property {number} batchesProcessed - Number of batches completed
 * @property {number} elapsedMs - Total replay time in milliseconds
 */

export {};
