/**
 * QuantLab Replay Engine — ORDERING CONTRACT v1
 * 
 * This file is the SINGLE SOURCE OF TRUTH for event ordering rules.
 * Any change to ordering logic MUST be made here and reflected in all consumers.
 * 
 * ═══════════════════════════════════════════════════════════════════════════════
 * RULE 1: Global event order is: ts_event ASC, seq ASC
 * ═══════════════════════════════════════════════════════════════════════════════
 * All replay operations MUST use this ordering. No exceptions.
 * 
 * ═══════════════════════════════════════════════════════════════════════════════
 * RULE 2: (ts_event, seq) MUST be unique per dataset
 * ═══════════════════════════════════════════════════════════════════════════════
 * The combination of ts_event and seq must uniquely identify each event.
 * This is a HARD REQUIREMENT for deterministic replay.
 * 
 * ═══════════════════════════════════════════════════════════════════════════════
 * RULE 3: If uniqueness cannot be guaranteed, a TIE-BREAKER is MANDATORY
 * ═══════════════════════════════════════════════════════════════════════════════
 * If the data source cannot guarantee (ts_event, seq) uniqueness:
 *   - Add a tie-breaker field (e.g., source_id, event_id, row_hash)
 *   - Include tie-breaker in ORDERING_COLUMNS
 *   - Include tie-breaker in SQL_ORDER_CLAUSE
 *   - Include tie-breaker in cursor format
 * 
 * Example with tie-breaker:
 *   ORDERING_COLUMNS = ['ts_event', 'seq', 'source_id']
 *   SQL_ORDER_CLAUSE = 'ORDER BY ts_event ASC, seq ASC, source_id ASC'
 * 
 * ═══════════════════════════════════════════════════════════════════════════════
 * VIOLATION: Non-unique (ts_event, seq) = UNDEFINED BEHAVIOR
 * ═══════════════════════════════════════════════════════════════════════════════
 * If two events share the same (ts_event, seq):
 *   - Replay order becomes non-deterministic
 *   - Resume from cursor may skip or duplicate events
 *   - Hash verification will FAIL across runs
 * 
 * ═══════════════════════════════════════════════════════════════════════════════
 * CURSOR CONTRACT
 * ═══════════════════════════════════════════════════════════════════════════════
 * Cursor fields MUST exactly match ORDERING_COLUMNS.
 * Cursor represents the LAST PROCESSED event.
 * Resume starts from the NEXT event (exclusive).
 * 
 * SQL implementation:
 *   WHERE (ts_event > cursor.ts_event) 
 *      OR (ts_event = cursor.ts_event AND seq > cursor.seq)
 */

/** 
 * Ordered list of columns that define global event order.
 * Cursor must include all these fields.
 * @type {readonly string[]}
 */
export const ORDERING_COLUMNS = Object.freeze(['ts_event', 'seq']);

/**
 * SQL ORDER BY clause for deterministic replay.
 * Must match ORDERING_COLUMNS exactly.
 * @type {string}
 */
export const SQL_ORDER_CLAUSE = 'ORDER BY ts_event ASC, seq ASC';

/**
 * Current ordering contract version.
 * Increment if ordering rules change.
 * @type {number}
 */
export const ORDERING_VERSION = 1;

/**
 * Compare two ordering tuples based on ORDERING_COLUMNS.
 * Returns -1 if a < b, 0 if equal, 1 if a > b.
 * @param {Object} a
 * @param {Object} b
 * @returns {number}
 */
export function compareOrdering(a, b) {
  for (const col of ORDERING_COLUMNS) {
    let av;
    let bv;
    try {
      av = BigInt(a[col]);
      bv = BigInt(b[col]);
    } catch (err) {
      throw new Error(`ORDERING_COMPARE_FAILED: '${col}' must be a valid integer`);
    }
    if (av < bv) return -1;
    if (av > bv) return 1;
  }
  return 0;
}

/**
 * Validate monotonic progress between previous and current rows.
 * Throws on duplicate or out-of-order.
 * @param {Object|null} prev
 * @param {Object} curr
 */
export function enforceOrderingProgress(prev, curr) {
  if (!prev) return;
  const cmp = compareOrdering(prev, curr);
  const formatTuple = (obj) =>
    ORDERING_COLUMNS.map((col) => `${col}=${obj[col]}`).join(',');
  if (cmp === 0) {
    throw new Error(`ORDERING_VIOLATION_DUPLICATE: duplicate ordering tuple ${formatTuple(curr)}`);
  }
  if (cmp > 0) {
    throw new Error(`ORDERING_VIOLATION_OUT_OF_ORDER: ordering tuple decreased prev=${formatTuple(prev)} curr=${formatTuple(curr)}`);
  }
}

/**
 * Build SQL WHERE clause for exclusive cursor resume.
 * Returns clause that selects events AFTER the given cursor position.
 * Dynamically builds the clause based on ORDERING_COLUMNS.
 * 
 * Logic for (C1, C2, C3) > (V1, V2, V3):
 * (C1 > V1) OR (C1 = V1 AND (C2 > V2 OR (C2 = V2 AND (C3 > V3))))
 * 
 * @param {Object} cursor - Cursor object containing all ORDERING_COLUMNS
 * @returns {string} SQL WHERE clause component
 */
export function buildCursorWhereClause(cursor) {
  if (!cursor) return '';

  /**
   * Recursive helper to build nested OR/AND logic
   * @param {number} index 
   * @returns {string}
   */
  function buildRecursive(index) {
    const col = ORDERING_COLUMNS[index];
    const val = cursor[col].toString();

    // Use CAST to ensure BigInt/UBIGINT precision in DuckDB
    const castedVal = `CAST('${val}' AS UBIGINT)`;

    // If it's the last column, just C > V
    if (index === ORDERING_COLUMNS.length - 1) {
      return `${col} > ${castedVal}`;
    }

    // Otherwise: (C > V) OR (C = V AND (recursive...))
    return `(${col} > ${castedVal}) OR (${col} = ${castedVal} AND (${buildRecursive(index + 1)}))`;
  }


  return `(${buildRecursive(0)})`;
}
