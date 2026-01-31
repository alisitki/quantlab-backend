/**
 * QuantLab Replay Engine v1.2
 * Deterministic event replay from compact parquet + meta.json
 */

export { ReplayEngine } from './ReplayEngine.js';
export { loadMeta, loadMultiMeta } from './MetaLoader.js';
export { validateSchemaVersion, assertRowCount, validateAll } from './SchemaValidator.js';
export { ParquetReader } from './ParquetReader.js';
export { encodeCursor, decodeCursor, createCursor } from './CursorCodec.js';
export { 
  ORDERING_COLUMNS, 
  SQL_ORDER_CLAUSE, 
  ORDERING_VERSION,
  buildCursorWhereClause 
} from './ORDERING_CONTRACT.js';

// Clock exports
export { default as AsapClock } from './clock/AsapClock.js';
export { default as RealtimeClock } from './clock/RealtimeClock.js';
export { default as ScaledClock } from './clock/ScaledClock.js';

// Re-export types for JSDoc consumers
/** @typedef {import('./types.js').MetaData} MetaData */
/** @typedef {import('./types.js').ReplayOptions} ReplayOptions */
/** @typedef {import('./types.js').Row} Row */
/** @typedef {import('./types.js').ReplayStats} ReplayStats */
/** @typedef {import('./types.js').ReplayCursorV1} ReplayCursorV1 */
