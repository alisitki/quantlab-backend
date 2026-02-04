/**
 * QuantLab Replay Engine v1 — Schema Validator
 * Validates schema version and row count assertions.
 */

import { ORDERING_COLUMNS } from './ORDERING_CONTRACT.js';

/** Supported schema versions */
const SUPPORTED_VERSIONS = [1];

/**
 * Validate schema version is supported
 * @param {number} version - schema_version from meta.json
 * @throws {Error} If version not supported
 */
export function validateSchemaVersion(version) {
  if (!SUPPORTED_VERSIONS.includes(version)) {
    throw new Error(
      `SCHEMA_VERSION_UNSUPPORTED: Got ${version}, supported: [${SUPPORTED_VERSIONS.join(', ')}]`
    );
  }
}

/**
 * Assert parquet row count matches meta.rows
 * @param {number} expected - rows from meta.json
 * @param {number} actual - row count from parquet
 * @throws {Error} If counts don't match
 */
export function assertRowCount(expected, actual) {
  if (expected !== actual) {
    throw new Error(
      `ROW_COUNT_MISMATCH: meta.rows=${expected}, parquet_rows=${actual}`
    );
  }
}

/**
 * Validate ordering_columns match ORDERING_CONTRACT and include required fields
 * @param {import('./types.js').MetaData} meta
 */
export function validateOrderingColumns(meta) {
  const cols = meta.ordering_columns;
  if (!Array.isArray(cols) || cols.length === 0) {
    throw new Error('ORDERING_COLUMNS_MISSING: meta.ordering_columns is required');
  }

  const expected = ORDERING_COLUMNS;
  const sameLength = cols.length === expected.length;
  const sameOrder = sameLength && cols.every((c, i) => c === expected[i]);
  if (!sameOrder) {
    throw new Error(`ORDERING_COLUMNS_MISMATCH: meta=${JSON.stringify(cols)} expected=${JSON.stringify(expected)}`);
  }

  for (const required of ['ts_event', 'seq']) {
    if (!cols.includes(required)) {
      throw new Error(`ORDERING_COLUMNS_INVALID: missing required column '${required}'`);
    }
  }
}

/**
 * Full validation suite
 * @param {import('./types.js').MetaData} meta
 * @param {number} parquetRowCount
 */
export function validateAll(meta, parquetRowCount) {
  validateSchemaVersion(meta.schema_version);
  validateOrderingColumns(meta);
  assertRowCount(meta.rows, parquetRowCount);
}
