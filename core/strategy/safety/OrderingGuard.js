/**
 * QuantLab Strategy Runtime — Ordering Guard
 * 
 * PHASE 4: Safety & Error Containment
 * 
 * Enforces monotonic event ordering (ts_event, seq).
 * Prevents out-of-order event processing which would break determinism.
 * 
 * @module core/strategy/safety/OrderingGuard
 */

import { compareEventOrder } from './DeterminismValidator.js';
import { OrderingMode } from '../interface/types.js';

/**
 * Ordering guard for event monotonicity enforcement.
 */
export class OrderingGuard {
  /** @type {string} */
  #mode;
  
  /** @type {Object|null} */
  #lastEvent;
  
  /** @type {number} */
  #violationCount;
  
  /** @type {Array} */
  #violations;
  
  /** @type {number} */
  #maxViolationsToTrack;
  
  /**
   * Create an ordering guard.
   * 
   * @param {Object} [options] - Configuration options
   * @param {string} [options.mode='STRICT'] - Ordering mode (STRICT or WARN)
   * @param {number} [options.maxViolationsToTrack=10] - Max violations to store
   */
  constructor({ mode = OrderingMode.STRICT, maxViolationsToTrack = 10 } = {}) {
    this.#mode = mode;
    this.#lastEvent = null;
    this.#violationCount = 0;
    this.#violations = [];
    this.#maxViolationsToTrack = maxViolationsToTrack;
  }
  
  /**
   * Check event ordering against the previous event.
   * 
   * @param {Object|null} prevEvent - Previous event (can be null for first event)
   * @param {Object} currEvent - Current event to check
   * @returns {{ok: boolean, error?: string}} Check result
   * @throws {Error} In STRICT mode if ordering is violated
   */
  check(prevEvent, currEvent) {
    // Use internal state if prevEvent not provided
    const prev = prevEvent ?? this.#lastEvent;
    
    const result = compareEventOrder(prev, currEvent);
    
    if (!result.ok) {
      this.#violationCount++;
      
      // Track violation details
      if (this.#violations.length < this.#maxViolationsToTrack) {
        this.#violations.push({
          prevTs: prev?.ts_event?.toString(),
          prevSeq: prev?.seq?.toString(),
          currTs: currEvent?.ts_event?.toString(),
          currSeq: currEvent?.seq?.toString(),
          timestamp: Date.now()
        });
      }
      
      if (this.#mode === OrderingMode.STRICT) {
        throw new Error(result.error);
      } else {
        // WARN mode - log but continue
        console.warn(`[OrderingGuard] ${result.error}`);
      }
    }
    
    // Update internal state
    this.#lastEvent = currEvent;
    
    return result;
  }
  
  /**
   * Validate an event against the last tracked event.
   * Convenience method that uses internal state.
   * 
   * @param {Object} event - Event to validate
   * @returns {{ok: boolean, error?: string}} Validation result
   */
  validate(event) {
    return this.check(null, event);
  }
  
  /**
   * Reset the guard state.
   * Call this when starting a new sequence.
   */
  reset() {
    this.#lastEvent = null;
    this.#violationCount = 0;
    this.#violations = [];
  }
  
  /**
   * Reset to a specific event (for resume from checkpoint).
   * 
   * @param {Object} event - Event to reset to
   */
  resetTo(event) {
    this.#lastEvent = event;
  }
  
  /**
   * Get the last processed event.
   * 
   * @returns {Object|null} Last event
   */
  getLastEvent() {
    return this.#lastEvent;
  }
  
  /**
   * Get violation statistics.
   * 
   * @returns {Object} Violation stats
   */
  getStats() {
    return {
      violationCount: this.#violationCount,
      violations: [...this.#violations],
      mode: this.#mode
    };
  }
  
  /**
   * Check if any violations have occurred.
   * 
   * @returns {boolean} True if violations occurred
   */
  hasViolations() {
    return this.#violationCount > 0;
  }
}

/**
 * Create an ordering guard.
 * 
 * @param {Object} [options] - Configuration options
 * @returns {OrderingGuard} Ordering guard instance
 */
export function createOrderingGuard(options) {
  return new OrderingGuard(options);
}

export default OrderingGuard;
