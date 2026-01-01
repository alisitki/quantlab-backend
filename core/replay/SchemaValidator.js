/**
 * QuantLab Replay Engine v1 — Schema Validator
 * Validates schema version and row count assertions.
 */

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
 * Full validation suite
 * @param {import('./types.js').MetaData} meta
 * @param {number} parquetRowCount
 */
export function validateAll(meta, parquetRowCount) {
  validateSchemaVersion(meta.schema_version);
  assertRowCount(meta.rows, parquetRowCount);
}
