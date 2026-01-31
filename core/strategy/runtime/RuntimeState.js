/**
 * QuantLab Strategy Runtime — Runtime State
 * 
 * PHASE 3: Lifecycle & Runtime
 * 
 * Unified state container combining:
 * - Cursor position
 * - Execution state (positions, fills, PnL)
 * - Strategy state
 * - Metrics snapshot
 * 
 * Provides atomic snapshots for checkpointing and determinism verification.
 * 
 * @module core/strategy/runtime/RuntimeState
 */

import { computeStateHash, computeFillsHash, computeHash } from '../safety/DeterminismValidator.js';
import { canonicalClone, immutableSnapshot } from '../state/StateSerializer.js';

/**
 * @typedef {import('../interface/types.js').RuntimeStateSnapshot} RuntimeStateSnapshot
 * @typedef {import('../interface/types.js').CursorInfo} CursorInfo
 */

/**
 * Unified runtime state container.
 */
export class RuntimeState {
  /** @type {string} */
  #runId;
  
  /** @type {CursorInfo} */
  #cursor;
  
  /** @type {Object|null} */
  #executionState;
  
  /** @type {Object} */
  #strategyState;
  
  /** @type {Object} */
  #metricsSnapshot;
  
  /** @type {number} */
  #eventCount;
  
  /** @type {Array} */
  #fills;
  
  /**
   * Create a runtime state container.
   * 
   * @param {Object} options - Initial state options
   * @param {string} options.runId - Run identifier
   */
  constructor({ runId }) {
    this.#runId = runId;
    this.#cursor = { ts_event: null, seq: null, encoded: null };
    this.#executionState = null;
    this.#strategyState = {};
    this.#metricsSnapshot = {};
    this.#eventCount = 0;
    this.#fills = [];
  }
  
  // ============================================================================
  // SETTERS
  // ============================================================================
  
  /**
   * Update cursor position.
   * 
   * @param {CursorInfo} cursor - New cursor position
   */
  updateCursor(cursor) {
    this.#cursor = {
      ts_event: cursor.ts_event ?? this.#cursor.ts_event,
      seq: cursor.seq ?? this.#cursor.seq,
      encoded: cursor.encoded ?? this.#cursor.encoded
    };
  }
  
  /**
   * Update execution state from ExecutionEngine.snapshot().
   * 
   * @param {Object} state - Execution state snapshot
   */
  updateExecutionState(state) {
    this.#executionState = canonicalClone(state);
    
    // Extract fills for separate hashing
    if (state && state.fills) {
      this.#fills = canonicalClone(state.fills);
    }
  }
  
  /**
   * Update strategy state from strategy.getState().
   * 
   * @param {Object} state - Strategy state
   */
  updateStrategyState(state) {
    this.#strategyState = canonicalClone(state ?? {});
  }
  
  /**
   * Update metrics snapshot.
   * 
   * @param {Object} metrics - Metrics snapshot
   */
  updateMetrics(metrics) {
    this.#metricsSnapshot = canonicalClone(metrics ?? {});
  }
  
  /**
   * Increment event counter.
   * 
   * @param {number} [count=1] - Number to add
   */
  incrementEventCount(count = 1) {
    this.#eventCount += count;
  }
  
  /**
   * Add a fill to the fills array.
   * 
   * @param {Object} fill - Fill to add
   */
  addFill(fill) {
    this.#fills.push(canonicalClone(fill));
  }
  
  // ============================================================================
  // GETTERS
  // ============================================================================
  
  /** @returns {string} */
  get runId() { return this.#runId; }
  
  /** @returns {CursorInfo} */
  get cursor() { return { ...this.#cursor }; }
  
  /** @returns {Object|null} */
  get executionState() { return this.#executionState ? canonicalClone(this.#executionState) : null; }
  
  /** @returns {Object} */
  get strategyState() { return canonicalClone(this.#strategyState); }
  
  /** @returns {Object} */
  get metricsSnapshot() { return canonicalClone(this.#metricsSnapshot); }
  
  /** @returns {number} */
  get eventCount() { return this.#eventCount; }
  
  /** @returns {Array} */
  get fills() { return canonicalClone(this.#fills); }
  
  // ============================================================================
  // HASH COMPUTATION
  // ============================================================================
  
  /**
   * Compute state hash for determinism verification.
   * 
   * @returns {string} SHA256 hash of combined state
   */
  computeStateHash() {
    return computeStateHash({
      cursor: this.#cursor,
      executionState: this.#executionState,
      strategyState: this.#strategyState
    });
  }
  
  /**
   * Compute fills hash for trade sequence verification.
   * 
   * @returns {string} SHA256 hash of fills
   */
  computeFillsHash() {
    return computeFillsHash(this.#fills);
  }
  
  // ============================================================================
  // SNAPSHOT
  // ============================================================================
  
  /**
   * Create a full runtime state snapshot.
   * This is the primary method for checkpointing.
   * 
   * @returns {RuntimeStateSnapshot} Immutable state snapshot
   */
  snapshot() {
    const stateHash = this.computeStateHash();
    const fillsHash = this.computeFillsHash();
    
    return immutableSnapshot({
      runId: this.#runId,
      cursor: this.#cursor,
      executionState: this.#executionState,
      strategyState: this.#strategyState,
      metrics: this.#metricsSnapshot,
      eventCount: this.#eventCount,
      fillsCount: this.#fills.length,
      stateHash,
      fillsHash,
      timestamp: new Date().toISOString()
    });
  }
  
  /**
   * Create a minimal snapshot for quick status checks.
   * 
   * @returns {Object} Minimal status snapshot
   */
  statusSnapshot() {
    return {
      runId: this.#runId,
      eventCount: this.#eventCount,
      fillsCount: this.#fills.length,
      cursorTs: this.#cursor.ts_event?.toString() ?? null,
      stateHash: this.computeStateHash().substring(0, 8),
      fillsHash: this.computeFillsHash().substring(0, 8)
    };
  }
  
  // ============================================================================
  // RESTORE
  // ============================================================================
  
  /**
   * Restore state from a snapshot.
   * 
   * @param {RuntimeStateSnapshot} snapshot - Snapshot to restore from
   * @throws {Error} If snapshot is invalid or hash mismatch
   */
  restore(snapshot) {
    if (!snapshot || !snapshot.runId) {
      throw new Error('RESTORE_ERROR: Invalid snapshot - missing runId');
    }
    
    // Verify run ID matches
    if (snapshot.runId !== this.#runId) {
      throw new Error(
        `RESTORE_ERROR: Run ID mismatch - expected=${this.#runId} got=${snapshot.runId}`
      );
    }
    
    // Verify state hash if present
    if (snapshot.stateHash) {
      const expectedHash = computeStateHash({
        cursor: snapshot.cursor,
        executionState: snapshot.executionState,
        strategyState: snapshot.strategyState
      });
      
      if (expectedHash !== snapshot.stateHash) {
        throw new Error(
          `RESTORE_ERROR: State hash mismatch - expected=${snapshot.stateHash.substring(0, 8)} actual=${expectedHash.substring(0, 8)}`
        );
      }
    }
    
    // Restore state
    this.#cursor = { ...snapshot.cursor };
    this.#executionState = snapshot.executionState ? canonicalClone(snapshot.executionState) : null;
    this.#strategyState = canonicalClone(snapshot.strategyState ?? {});
    this.#metricsSnapshot = canonicalClone(snapshot.metrics ?? {});
    this.#eventCount = snapshot.eventCount ?? 0;
    
    // Note: fills are not restored directly - they come from execution state
    if (snapshot.executionState?.fills) {
      this.#fills = canonicalClone(snapshot.executionState.fills);
    }
  }
  
  // ============================================================================
  // COMPARISON
  // ============================================================================
  
  /**
   * Compare this state with another for determinism verification.
   * 
   * @param {RuntimeState|RuntimeStateSnapshot} other - Other state to compare
   * @returns {Object} Comparison result
   */
  compare(other) {
    const otherSnapshot = other.snapshot ? other.snapshot() : other;
    const thisSnapshot = this.snapshot();
    
    const stateMatch = thisSnapshot.stateHash === otherSnapshot.stateHash;
    const fillsMatch = thisSnapshot.fillsHash === otherSnapshot.fillsHash;
    const eventCountMatch = thisSnapshot.eventCount === otherSnapshot.eventCount;
    
    return {
      match: stateMatch && fillsMatch && eventCountMatch,
      details: {
        stateHash: { this: thisSnapshot.stateHash, other: otherSnapshot.stateHash, match: stateMatch },
        fillsHash: { this: thisSnapshot.fillsHash, other: otherSnapshot.fillsHash, match: fillsMatch },
        eventCount: { this: thisSnapshot.eventCount, other: otherSnapshot.eventCount, match: eventCountMatch }
      }
    };
  }
}

/**
 * Create a runtime state container.
 * 
 * @param {Object} options - State options
 * @returns {RuntimeState} Runtime state
 */
export function createRuntimeState(options) {
  return new RuntimeState(options);
}

export default RuntimeState;
