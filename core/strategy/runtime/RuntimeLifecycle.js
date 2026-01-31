/**
 * QuantLab Strategy Runtime — Runtime Lifecycle
 * 
 * PHASE 3: Lifecycle & Runtime
 * 
 * State machine for run lifecycle management.
 * Enforces valid transitions and emits state change events.
 * 
 * State flow:
 *   CREATED → INITIALIZING → READY → RUNNING → [PAUSED] → FINALIZING → DONE
 *                                                                    ↘ FAILED
 *                                                                    ↘ CANCELED
 * 
 * @module core/strategy/runtime/RuntimeLifecycle
 */

import { EventEmitter } from 'node:events';
import { RunLifecycleStatus } from '../interface/types.js';

/**
 * Valid state transitions.
 * Key = current state, Value = array of valid next states.
 */
const TRANSITIONS = Object.freeze({
  [RunLifecycleStatus.CREATED]: [
    RunLifecycleStatus.INITIALIZING,
    RunLifecycleStatus.CANCELED
  ],
  [RunLifecycleStatus.INITIALIZING]: [
    RunLifecycleStatus.READY,
    RunLifecycleStatus.FAILED
  ],
  [RunLifecycleStatus.READY]: [
    RunLifecycleStatus.RUNNING,
    RunLifecycleStatus.CANCELED
  ],
  [RunLifecycleStatus.RUNNING]: [
    RunLifecycleStatus.PAUSED,
    RunLifecycleStatus.FINALIZING,
    RunLifecycleStatus.FAILED
  ],
  [RunLifecycleStatus.PAUSED]: [
    RunLifecycleStatus.RUNNING,
    RunLifecycleStatus.FINALIZING,
    RunLifecycleStatus.CANCELED
  ],
  [RunLifecycleStatus.FINALIZING]: [
    RunLifecycleStatus.DONE,
    RunLifecycleStatus.FAILED
  ],
  // Terminal states - no transitions allowed
  [RunLifecycleStatus.DONE]: [],
  [RunLifecycleStatus.FAILED]: [],
  [RunLifecycleStatus.CANCELED]: []
});

/**
 * Terminal states that cannot transition further.
 */
const TERMINAL_STATES = Object.freeze([
  RunLifecycleStatus.DONE,
  RunLifecycleStatus.FAILED,
  RunLifecycleStatus.CANCELED
]);

/**
 * Runtime lifecycle state machine.
 * 
 * @extends EventEmitter
 */
export class RuntimeLifecycle extends EventEmitter {
  /** @type {string} */
  #status;
  
  /** @type {Array<{from: string, to: string, timestamp: number}>} */
  #history;
  
  /** @type {Error|null} */
  #lastError;
  
  /** @type {number} */
  #createdAt;
  
  /** @type {number|null} */
  #startedAt;
  
  /** @type {number|null} */
  #endedAt;
  
  /**
   * Create a new lifecycle manager.
   */
  constructor() {
    super();
    this.#status = RunLifecycleStatus.CREATED;
    this.#history = [];
    this.#lastError = null;
    this.#createdAt = Date.now();
    this.#startedAt = null;
    this.#endedAt = null;
  }
  
  // ============================================================================
  // GETTERS
  // ============================================================================
  
  /** @returns {string} Current status */
  get status() { return this.#status; }
  
  /** @returns {Error|null} Last error if in FAILED state */
  get lastError() { return this.#lastError; }
  
  /** @returns {boolean} True if in terminal state */
  get isTerminal() { return TERMINAL_STATES.includes(this.#status); }
  
  /** @returns {boolean} True if running */
  get isRunning() { return this.#status === RunLifecycleStatus.RUNNING; }
  
  /** @returns {boolean} True if paused */
  get isPaused() { return this.#status === RunLifecycleStatus.PAUSED; }
  
  /** @returns {boolean} True if can start running */
  get canStart() { return this.#status === RunLifecycleStatus.READY; }
  
  /** @returns {boolean} True if can process events */
  get canProcess() { 
    return this.#status === RunLifecycleStatus.RUNNING;
  }
  
  /** @returns {number} Creation timestamp */
  get createdAt() { return this.#createdAt; }
  
  /** @returns {number|null} Start timestamp */
  get startedAt() { return this.#startedAt; }
  
  /** @returns {number|null} End timestamp */
  get endedAt() { return this.#endedAt; }
  
  /** @returns {number|null} Duration in milliseconds */
  get durationMs() {
    if (!this.#startedAt) return null;
    const end = this.#endedAt ?? Date.now();
    return end - this.#startedAt;
  }
  
  // ============================================================================
  // TRANSITIONS
  // ============================================================================
  
  /**
   * Attempt a state transition.
   * 
   * @param {string} targetStatus - Target status
   * @param {Object} [options] - Transition options
   * @param {Error} [options.error] - Error if transitioning to FAILED
   * @throws {Error} If transition is invalid
   */
  transition(targetStatus, { error } = {}) {
    // Validate target status
    if (!Object.values(RunLifecycleStatus).includes(targetStatus)) {
      throw new Error(`LIFECYCLE_ERROR: Invalid status: ${targetStatus}`);
    }
    
    // Check if transition is allowed
    const allowedTransitions = TRANSITIONS[this.#status] || [];
    if (!allowedTransitions.includes(targetStatus)) {
      throw new Error(
        `LIFECYCLE_ERROR: Invalid transition from ${this.#status} to ${targetStatus}. ` +
        `Allowed: [${allowedTransitions.join(', ')}]`
      );
    }
    
    const previousStatus = this.#status;
    const timestamp = Date.now();
    
    // Record history
    this.#history.push({
      from: previousStatus,
      to: targetStatus,
      timestamp
    });
    
    // Update status
    this.#status = targetStatus;
    
    // Track timestamps
    if (targetStatus === RunLifecycleStatus.RUNNING && !this.#startedAt) {
      this.#startedAt = timestamp;
    }
    
    if (TERMINAL_STATES.includes(targetStatus)) {
      this.#endedAt = timestamp;
    }
    
    // Store error if transitioning to FAILED
    if (targetStatus === RunLifecycleStatus.FAILED && error) {
      this.#lastError = error;
    }
    
    // Emit state change event
    this.emit('transition', {
      from: previousStatus,
      to: targetStatus,
      timestamp,
      error
    });
  }
  
  // ============================================================================
  // CONVENIENCE METHODS
  // ============================================================================
  
  /**
   * Transition to INITIALIZING state.
   */
  initialize() {
    this.transition(RunLifecycleStatus.INITIALIZING);
  }
  
  /**
   * Transition to READY state.
   */
  ready() {
    this.transition(RunLifecycleStatus.READY);
  }
  
  /**
   * Transition to RUNNING state.
   */
  start() {
    this.transition(RunLifecycleStatus.RUNNING);
  }
  
  /**
   * Transition to PAUSED state.
   */
  pause() {
    this.transition(RunLifecycleStatus.PAUSED);
  }
  
  /**
   * Resume from PAUSED to RUNNING.
   */
  resume() {
    if (this.#status !== RunLifecycleStatus.PAUSED) {
      throw new Error(`LIFECYCLE_ERROR: Cannot resume from ${this.#status}, must be PAUSED`);
    }
    this.transition(RunLifecycleStatus.RUNNING);
  }
  
  /**
   * Transition to FINALIZING state.
   */
  finalize() {
    this.transition(RunLifecycleStatus.FINALIZING);
  }
  
  /**
   * Transition to DONE state.
   */
  complete() {
    this.transition(RunLifecycleStatus.DONE);
  }
  
  /**
   * Transition to FAILED state.
   * 
   * @param {Error} error - Error that caused failure
   */
  fail(error) {
    this.transition(RunLifecycleStatus.FAILED, { error });
  }
  
  /**
   * Transition to CANCELED state.
   */
  cancel() {
    this.transition(RunLifecycleStatus.CANCELED);
  }
  
  // ============================================================================
  // GUARDS
  // ============================================================================
  
  /**
   * Assert that we're in an expected state.
   * 
   * @param {string|string[]} expected - Expected status or array of statuses
   * @throws {Error} If not in expected state
   */
  assertStatus(expected) {
    const allowed = Array.isArray(expected) ? expected : [expected];
    if (!allowed.includes(this.#status)) {
      throw new Error(
        `LIFECYCLE_ERROR: Expected status [${allowed.join(', ')}], got ${this.#status}`
      );
    }
  }
  
  /**
   * Assert that we can process events.
   * 
   * @throws {Error} If not in RUNNING state
   */
  assertCanProcess() {
    if (!this.canProcess) {
      throw new Error(
        `LIFECYCLE_ERROR: Cannot process events in ${this.#status} state`
      );
    }
  }
  
  // ============================================================================
  // SNAPSHOT
  // ============================================================================
  
  /**
   * Get lifecycle snapshot for logging/persistence.
   * 
   * @returns {Object} Lifecycle snapshot
   */
  snapshot() {
    return {
      status: this.#status,
      isTerminal: this.isTerminal,
      createdAt: this.#createdAt,
      startedAt: this.#startedAt,
      endedAt: this.#endedAt,
      durationMs: this.durationMs,
      historyLength: this.#history.length,
      lastError: this.#lastError ? this.#lastError.message : null
    };
  }
  
  /**
   * Get full transition history.
   * 
   * @returns {Array} Transition history
   */
  getHistory() {
    return [...this.#history];
  }
}

export default RuntimeLifecycle;
