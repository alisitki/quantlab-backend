/**
 * QuantLab Replay Engine v1
 * Deterministic event replay from compact parquet + meta.json
 */

export { ReplayEngine } from './ReplayEngine.js';
export { loadMeta } from './MetaLoader.js';
export { validateSchemaVersion, assertRowCount, validateAll } from './SchemaValidator.js';
export { ParquetReader } from './ParquetReader.js';

// Re-export types for JSDoc consumers
/** @typedef {import('./types.js').MetaData} MetaData */
/** @typedef {import('./types.js').ReplayOptions} ReplayOptions */
/** @typedef {import('./types.js').Row} Row */
/** @typedef {import('./types.js').ReplayStats} ReplayStats */
