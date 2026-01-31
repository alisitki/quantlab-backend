/**
 * QuantLab Strategy Runtime — Error Containment
 * 
 * PHASE 4: Safety & Error Containment
 * 
 * Wraps strategy event processing with error handling policies.
 * Prevents single event errors from crashing the entire run.
 * 
 * @module core/strategy/safety/ErrorContainment
 */

import { ErrorPolicy } from '../interface/types.js';

/**
 * @typedef {Object} ContainmentResult
 * @property {boolean} ok - True if execution succeeded
 * @property {boolean} skipped - True if event was skipped due to error
 * @property {Error|null} error - Error if one occurred
 * @property {number} [retryCount] - Number of retries attempted
 */

/**
 * Error containment wrapper for strategy event processing.
 */
export class ErrorContainment {
  /** @type {string} */
  #policy;
  
  /** @type {number} */
  #errorCount;
  
  /** @type {number} */
  #skippedCount;
  
  /** @type {number} */
  #maxErrors;
  
  /** @type {Array} */
  #errorLog;
  
  /** @type {number} */
  #maxErrorsToTrack;
  
  /** @type {function|null} */
  #onError;
  
  /**
   * Create an error containment wrapper.
   * 
   * @param {Object} [options] - Configuration options
   * @param {string} [options.policy='FAIL_FAST'] - Error policy
   * @param {number} [options.maxErrors=100] - Max errors before forced stop
   * @param {number} [options.maxErrorsToTrack=50] - Max errors to store
   * @param {function} [options.onError] - Error callback
   */
  constructor({
    policy = ErrorPolicy.FAIL_FAST,
    maxErrors = 100,
    maxErrorsToTrack = 50,
    onError = null
  } = {}) {
    this.#policy = policy;
    this.#errorCount = 0;
    this.#skippedCount = 0;
    this.#maxErrors = maxErrors;
    this.#errorLog = [];
    this.#maxErrorsToTrack = maxErrorsToTrack;
    this.#onError = onError;
  }
  
  /**
   * Wrap an async function with error containment.
   * 
   * @param {function} fn - Async function to wrap
   * @param {Object} [context] - Context for error logging
   * @returns {Promise<ContainmentResult>} Result of execution
   */
  async wrap(fn, context = {}) {
    try {
      await fn();
      return { ok: true, skipped: false, error: null };
    } catch (error) {
      return this.#handleError(error, context);
    }
  }
  
  /**
   * Wrap a sync function with error containment.
   * 
   * @param {function} fn - Sync function to wrap
   * @param {Object} [context] - Context for error logging
   * @returns {ContainmentResult} Result of execution
   */
  wrapSync(fn, context = {}) {
    try {
      fn();
      return { ok: true, skipped: false, error: null };
    } catch (error) {
      return this.#handleError(error, context);
    }
  }
  
  /**
   * Handle an error according to the policy.
   * 
   * @param {Error} error - Error that occurred
   * @param {Object} context - Context for logging
   * @returns {ContainmentResult} Result based on policy
   */
  #handleError(error, context) {
    this.#errorCount++;
    
    // Track error details
    if (this.#errorLog.length < this.#maxErrorsToTrack) {
      this.#errorLog.push({
        message: error.message,
        type: error.constructor.name,
        timestamp: Date.now(),
        context: { ...context }
      });
    }
    
    // Call error callback if provided
    if (this.#onError) {
      try {
        this.#onError(error, context);
      } catch {
        // Ignore errors in error handler
      }
    }
    
    // Check if we've hit the error limit
    if (this.#errorCount >= this.#maxErrors) {
      throw new Error(
        `ERROR_LIMIT_EXCEEDED: ${this.#errorCount} errors reached max limit of ${this.#maxErrors}`
      );
    }
    
    // Apply policy
    switch (this.#policy) {
      case ErrorPolicy.FAIL_FAST:
        throw error;
        
      case ErrorPolicy.SKIP_AND_LOG:
        this.#skippedCount++;
        console.warn(`[ErrorContainment] Skipped event due to error: ${error.message}`);
        return { ok: false, skipped: true, error };
        
      case ErrorPolicy.QUARANTINE:
        this.#skippedCount++;
        // In quarantine mode, we could write to a separate file/queue
        // For now, just log with QUARANTINE prefix
        console.warn(`[ErrorContainment] QUARANTINE: ${error.message}`, context);
        return { ok: false, skipped: true, error };
        
      default:
        throw error;
    }
  }
  
  /**
   * Get error statistics.
   * 
   * @returns {Object} Error statistics
   */
  getStats() {
    return {
      policy: this.#policy,
      errorCount: this.#errorCount,
      skippedCount: this.#skippedCount,
      maxErrors: this.#maxErrors,
      errorLog: [...this.#errorLog]
    };
  }
  
  /**
   * Check if any errors have occurred.
   * 
   * @returns {boolean} True if errors occurred
   */
  hasErrors() {
    return this.#errorCount > 0;
  }
  
  /**
   * Get the number of errors.
   * 
   * @returns {number} Error count
   */
  get errorCount() {
    return this.#errorCount;
  }
  
  /**
   * Get the number of skipped events.
   * 
   * @returns {number} Skipped count
   */
  get skippedCount() {
    return this.#skippedCount;
  }
  
  /**
   * Reset error statistics.
   */
  reset() {
    this.#errorCount = 0;
    this.#skippedCount = 0;
    this.#errorLog = [];
  }
}

/**
 * Create an error containment wrapper.
 * 
 * @param {Object} [options] - Configuration options
 * @returns {ErrorContainment} Error containment instance
 */
export function createErrorContainment(options) {
  return new ErrorContainment(options);
}

export default ErrorContainment;
