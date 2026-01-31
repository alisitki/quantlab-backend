/**
 * QuantLab Replay Engine — Cursor Codec v1
 * 
 * Encodes/decodes replay cursors to/from portable base64 strings.
 * Cursor represents the LAST PROCESSED event for resume capability.
 */

import { ORDERING_COLUMNS, ORDERING_VERSION } from './ORDERING_CONTRACT.js';

/**
 * @typedef {Object} ReplayCursorV1
 * @property {1} v - Cursor version (always 1 for this codec)
 * @property {number} ts_event - Last processed ts_event (nanoseconds)
 * @property {number} seq - Last processed seq
 */

/**
 * Encode a cursor object into a portable base64 string.
 * 
 * @param {Object} cursor - Cursor object (must contain v and ORDERING_COLUMNS)
 * @returns {string} Base64 encoded cursor string
 */
export function encodeCursor(cursor) {
  if (!cursor) return null;
  
  // Ensure we only encode what's necessary (version + ordering columns)
  const toEncode = { v: cursor.v || ORDERING_VERSION };
  for (const col of ORDERING_COLUMNS) {
    const val = cursor[col];
    // Always stringify for JSON precision safety
    toEncode[col] = val !== undefined ? val.toString() : val;
  }

  const json = JSON.stringify(toEncode);
  return Buffer.from(json, 'utf-8').toString('base64');
}


/**
 * Decode a base64 cursor string into cursor object.
 * Validates version and required fields.
 * 
 * @param {string} base64Cursor - Base64 encoded cursor string
 * @returns {ReplayCursorV1} Decoded cursor object
 * @throws {Error} If cursor is invalid or version mismatch
 */
export function decodeCursor(base64Cursor) {
  if (!base64Cursor || typeof base64Cursor !== 'string') {
    throw new Error('CURSOR_INVALID: Cursor must be a non-empty string');
  }

  let json;
  try {
    json = Buffer.from(base64Cursor, 'base64').toString('utf-8');
  } catch (err) {
    throw new Error(`CURSOR_DECODE_FAILED: Invalid base64: ${err.message}`);
  }

  let cursor;
  try {
    cursor = JSON.parse(json);
  } catch (err) {
    throw new Error(`CURSOR_PARSE_FAILED: Invalid JSON: ${err.message}`);
  }

  // Validate version
  if (cursor.v !== ORDERING_VERSION) {
    throw new Error(
      `CURSOR_VERSION_MISMATCH: Got v${cursor.v}, expected v${ORDERING_VERSION}`
    );
  }

  // Parse and validate required fields from ORDERING_COLUMNS
  for (const col of ORDERING_COLUMNS) {
    if (cursor[col] === undefined || cursor[col] === null) {
      throw new Error(`CURSOR_MISSING_FIELD: Required field '${col}' is missing`);
    }
    // Attempt to parse as BigInt
    try {
      cursor[col] = BigInt(cursor[col]);
    } catch (err) {
      throw new Error(`CURSOR_INVALID_TYPE: Field '${col}' must be a valid number string`);
    }
  }

  return cursor;
}

/**
 * Create a cursor object from event row data (for internal use).
 * Picks all columns defined in ORDERING_COLUMNS.
 * 
 * @param {Object} row - Event row
 * @returns {Object} Cursor object with version and ordering fields
 */
export function createCursor(row) {
  const cursor = { v: ORDERING_VERSION };
  for (const col of ORDERING_COLUMNS) {
    const val = row[col];
    if (val === undefined || val === null) {
      throw new Error(`CREATE_CURSOR_FAILED: Row is missing ordering column '${col}'`);
    }
    // High-precision preservation
    cursor[col] = val;
  }
  return cursor;
}

