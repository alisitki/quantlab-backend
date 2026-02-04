/**
 * QuantLab Strategy Runtime — Strategy State Container
 * 
 * PHASE 2: State & Snapshot
 * 
 * Base class for managing strategy internal state in a deterministic,
 * snapshot-compatible way.
 * 
 * Features:
 * - Type-safe state management
 * - Immutable snapshots for checkpointing
 * - State restoration from snapshots
 * - Hash computation for determinism verification
 * 
 * @module core/strategy/state/StrategyStateContainer
 */

import { canonicalClone, immutableSnapshot } from './StateSerializer.js';
import { computeHash } from '../safety/DeterminismValidator.js';

/**
 * Base state container for strategy internal state.
 * Strategies should extend or compose this for snapshot support.
 * 
 * @template T - State type
 */
export class StrategyStateContainer {
  /** @type {T} */
  #state;
  
  /** @type {T} */
  #initialState;
  
  /** @type {number} */
  #version = 0;
  
  /**
   * Create a new state container.
   * 
   * @param {T} [initialState={}] - Initial state
   */
  constructor(initialState = {}) {
    this.#initialState = canonicalClone(initialState);
    this.#state = canonicalClone(initialState);
    this.#version = 0;
  }
  
  /**
   * Get the current state (clone to prevent mutation).
   * 
   * @returns {T} Cloned current state
   */
  get() {
    return canonicalClone(this.#state);
  }
  
  /**
   * Get a specific value from state.
   * 
   * @param {string} key - State key
   * @returns {any} Value at key
   */
  getValue(key) {
    return this.#state[key];
  }
  
  /**
   * Set the entire state (replaces current state).
   * 
   * @param {T} newState - New state to set
   */
  set(newState) {
    this.#state = canonicalClone(newState);
    this.#version++;
  }
  
  /**
   * Update specific values in state (merge).
   * 
   * @param {Partial<T>} updates - Values to merge into state
   */
  update(updates) {
    this.#state = { ...this.#state, ...canonicalClone(updates) };
    this.#version++;
  }
  
  /**
   * Set a specific value in state.
   * 
   * @param {string} key - State key
   * @param {any} value - Value to set
   */
  setValue(key, value) {
    this.#state[key] = canonicalClone(value);
    this.#version++;
  }
  
  /**
   * Increment a numeric value in state.
   * 
   * @param {string} key - State key
   * @param {number} [amount=1] - Amount to increment
   */
  increment(key, amount = 1) {
    const current = this.#state[key] ?? 0;
    this.#state[key] = current + amount;
    this.#version++;
  }
  
  /**
   * Create an immutable snapshot of current state.
   * This is the primary method for checkpointing.
   * 
   * @returns {Object} Immutable state snapshot with metadata
   */
  snapshot() {
    const stateClone = immutableSnapshot(this.#state);
    const hash = computeHash(stateClone);
    
    return Object.freeze({
      state: stateClone,
      version: this.#version,
      hash,
      timestamp: Date.now()
    });
  }
  
  /**
   * Get only the state data (for embedding in larger snapshots).
   * 
   * @returns {T} Immutable state data
   */
  getState() {
    return immutableSnapshot(this.#state);
  }
  
  /**
   * Restore state from a snapshot.
   * 
   * @param {Object} snapshot - Snapshot to restore from
   * @throws {Error} If snapshot is invalid
   */
  restore(snapshot) {
    if (!snapshot || !snapshot.state) {
      throw new Error('RESTORE_ERROR: Invalid snapshot - missing state');
    }
    
    // Verify hash if present
    if (snapshot.hash) {
      const actualHash = computeHash(snapshot.state);
      if (actualHash !== snapshot.hash) {
        throw new Error(
          `RESTORE_ERROR: Hash mismatch - expected=${snapshot.hash.substring(0, 8)} actual=${actualHash.substring(0, 8)}`
        );
      }
    }
    
    this.#state = canonicalClone(snapshot.state);
    this.#version = snapshot.version ?? this.#version + 1;
  }
  
  /**
   * Restore from raw state data (without metadata).
   * 
   * @param {T} state - State data to restore
   */
  setState(state) {
    this.#state = canonicalClone(state);
    this.#version++;
  }
  
  /**
   * Reset state to initial values.
   */
  reset() {
    this.#state = canonicalClone(this.#initialState);
    this.#version = 0;
  }
  
  /**
   * Get current version number.
   * 
   * @returns {number} Version number
   */
  getVersion() {
    return this.#version;
  }
  
  /**
   * Compute hash of current state.
   * 
   * @returns {string} SHA256 hex digest
   */
  computeHash() {
    return computeHash(this.#state);
  }
  
  /**
   * Check if current state equals a snapshot.
   * 
   * @param {Object} snapshot - Snapshot to compare
   * @returns {boolean} True if equal
   */
  equals(snapshot) {
    if (!snapshot || !snapshot.state) return false;
    return this.computeHash() === computeHash(snapshot.state);
  }
}

export default StrategyStateContainer;
