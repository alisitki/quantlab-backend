/**
 * QuantLab Strategy Runtime — Determinism Validator
 * 
 * PHASE 1: Determinism Foundation
 * 
 * Provides hash computation and determinism verification utilities.
 * All functions are PURE — no side effects, no I/O, no time dependency.
 * 
 * Critical for:
 * - Twin-run verification (same input → same hash)
 * - State fingerprinting
 * - Run ID generation
 * 
 * @module core/strategy/safety/DeterminismValidator
 */

import { createHash } from 'node:crypto';
import { canonicalStringify } from '../state/StateSerializer.js';

// ============================================================================
// HASH COMPUTATION
// ============================================================================

/**
 * Compute SHA256 hash of any value using canonical serialization.
 * 
 * @param {any} value - Value to hash
 * @returns {string} SHA256 hex digest
 */
export function computeHash(value) {
  const canonical = canonicalStringify(value);
  return createHash('sha256').update(canonical).digest('hex');
}

/**
 * Compute state hash for runtime state snapshot.
 * Produces deterministic fingerprint of entire runtime state.
 * 
 * @param {Object} state - State object with cursor, execution, strategy
 * @returns {string} SHA256 hex digest
 */
export function computeStateHash(state) {
  // Ensure consistent structure for hashing
  const normalized = {
    cursor: state.cursor || null,
    executionState: state.executionState || null,
    strategyState: state.strategyState || null
  };
  return computeHash(normalized);
}

/**
 * Compute fills hash for trade sequence verification.
 * Produces deterministic fingerprint of all fills.
 * 
 * @param {Array<Object>} fills - Array of fill objects
 * @returns {string} SHA256 hex digest
 */
export function computeFillsHash(fills) {
  if (!Array.isArray(fills) || fills.length === 0) {
    return computeHash([]);
  }
  
  // Normalize fills to ensure consistent hashing
  const normalized = fills.map(fill => ({
    id: fill.id,
    side: fill.side,
    price: fill.fillPrice ?? fill.price,
    qty: fill.qty,
    ts: String(fill.ts_event ?? fill.ts)
  }));
  
  return computeHash(normalized);
}

/**
 * Compute deterministic run ID from input parameters.
 * 
 * Run ID is a hash of:
 * - Dataset identity (parquet path, meta path)
 * - Strategy configuration
 * - Optional seed for reproducibility
 * 
 * @param {Object} params - Run parameters
 * @param {Object} params.dataset - Dataset info {parquet, meta, stream, date, symbol}
 * @param {Object} [params.config] - Strategy configuration
 * @param {string} [params.seed] - Optional seed for additional entropy
 * @returns {string} Deterministic run ID (format: run_<8-char-hash>)
 */
export function computeRunId({ dataset, config = {}, seed = '' }) {
  const input = {
    dataset: {
      parquet: dataset.parquet,
      meta: dataset.meta,
      stream: dataset.stream || null,
      date: dataset.date || null,
      symbol: dataset.symbol || null
    },
    config,
    seed
  };
  
  const hash = computeHash(input);
  return `run_${hash.substring(0, 16)}`;
}

// ============================================================================
// ORDERING VALIDATION
// ============================================================================

/**
 * Compare two events for ordering validation.
 * Uses (ts_event, seq) tuple for total ordering.
 * 
 * @param {Object} prev - Previous event
 * @param {Object} curr - Current event
 * @returns {{ok: boolean, error?: string}} Validation result
 */
export function compareEventOrder(prev, curr) {
  if (!prev || !curr) {
    return { ok: true };
  }
  
  const prevTs = BigInt(prev.ts_event ?? 0);
  const prevSeq = BigInt(prev.seq ?? 0);
  const currTs = BigInt(curr.ts_event ?? 0);
  const currSeq = BigInt(curr.seq ?? 0);
  
  // Valid ordering: curr > prev in (ts_event, seq) tuple
  if (currTs > prevTs) {
    return { ok: true };
  }
  
  if (currTs === prevTs && currSeq > prevSeq) {
    return { ok: true };
  }
  
  // Invalid: curr <= prev
  return {
    ok: false,
    error: `ORDERING_VIOLATION: prev=(${prevTs},${prevSeq}) curr=(${currTs},${currSeq})`
  };
}

/**
 * Assert monotonic ordering — throws if violated.
 * 
 * @param {Object} prev - Previous event
 * @param {Object} curr - Current event
 * @throws {Error} If ordering is violated
 */
export function assertMonotonic(prev, curr) {
  const result = compareEventOrder(prev, curr);
  if (!result.ok) {
    throw new Error(result.error);
  }
}

// ============================================================================
// TWIN-RUN VERIFICATION
// ============================================================================

/**
 * Compare two run results for determinism verification.
 * 
 * @param {Object} run1 - First run result
 * @param {Object} run2 - Second run result
 * @returns {{match: boolean, details: Object}} Comparison result
 */
export function compareTwinRuns(run1, run2) {
  const stateMatch = run1.stateHash === run2.stateHash;
  const fillsMatch = run1.fillsHash === run2.fillsHash;
  const eventCountMatch = run1.eventCount === run2.eventCount;
  
  return {
    match: stateMatch && fillsMatch && eventCountMatch,
    details: {
      stateHash: {
        run1: run1.stateHash,
        run2: run2.stateHash,
        match: stateMatch
      },
      fillsHash: {
        run1: run1.fillsHash,
        run2: run2.fillsHash,
        match: fillsMatch
      },
      eventCount: {
        run1: run1.eventCount,
        run2: run2.eventCount,
        match: eventCountMatch
      }
    }
  };
}

/**
 * Generate determinism report for a run.
 * 
 * @param {Object} runResult - Run result containing state and fills
 * @returns {Object} Determinism fingerprint report
 */
export function generateDeterminismReport(runResult) {
  return {
    runId: runResult.runId,
    stateHash: runResult.stateHash || computeStateHash(runResult),
    fillsHash: runResult.fillsHash || computeFillsHash(runResult.fills || []),
    eventCount: runResult.eventCount || 0,
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  };
}

// ============================================================================
// CHECKSUM UTILITIES
// ============================================================================

/**
 * Compute incremental hash update (for streaming).
 * Returns a new hasher that can be updated incrementally.
 * 
 * @returns {Object} Hasher with update() and digest() methods
 */
export function createIncrementalHasher() {
  const hash = createHash('sha256');
  
  return {
    /**
     * Update hash with new data
     * @param {any} value - Value to add to hash
     */
    update(value) {
      const canonical = canonicalStringify(value);
      hash.update(canonical);
    },
    
    /**
     * Get final hash digest
     * @returns {string} SHA256 hex digest
     */
    digest() {
      return hash.digest('hex');
    }
  };
}

/**
 * Verify hash matches expected value.
 * 
 * @param {any} value - Value to hash
 * @param {string} expectedHash - Expected hash value
 * @returns {boolean} True if hash matches
 */
export function verifyHash(value, expectedHash) {
  const actualHash = computeHash(value);
  return actualHash === expectedHash;
}

// Default export for convenience
export default {
  computeHash,
  computeStateHash,
  computeFillsHash,
  computeRunId,
  compareEventOrder,
  assertMonotonic,
  compareTwinRuns,
  generateDeterminismReport,
  createIncrementalHasher,
  verifyHash
};
