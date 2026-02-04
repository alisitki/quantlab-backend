/**
 * ApprovalManager — Human approval gate for live trading.
 *
 * Ensures human review and explicit approval before:
 * - Transitioning from canary to live trading
 * - Starting live runs with real money
 *
 * States:
 * - PENDING: Canary completed, awaiting human review
 * - APPROVED: Human approved, can proceed
 * - REJECTED: Human rejected, cannot proceed
 * - EXPIRED: Timeout without decision
 *
 * Phase 4 Safety: No autonomous live trading without human approval.
 */

import crypto from 'node:crypto';
import { emitAudit } from '../audit/AuditWriter.js';
import { sendAlert, AlertType, AlertSeverity } from '../alerts/index.js';

/** Approval states */
export const ApprovalState = {
  PENDING: 'PENDING',
  APPROVED: 'APPROVED',
  REJECTED: 'REJECTED',
  EXPIRED: 'EXPIRED'
};

/** Default timeout: 1 hour */
const DEFAULT_TIMEOUT_MS = 60 * 60 * 1000;

/** Expiration warning: 15 minutes before */
const EXPIRATION_WARNING_MS = 15 * 60 * 1000;

/**
 * @typedef {Object} CanaryResult
 * @property {string} canary_run_id - Canary run ID
 * @property {string} exchange - Exchange used
 * @property {string[]} symbols - Symbols tested
 * @property {string} strategy_path - Strategy file path
 * @property {number} duration_ms - Run duration
 * @property {number} decision_count - Number of decisions
 * @property {string} decision_hash - Decision hash for parity
 * @property {Object} stats - Run statistics
 * @property {boolean} guards_passed - All guards passed
 * @property {string|null} guard_failure - Failed guard name if any
 */

/**
 * @typedef {Object} ApprovalRequest
 * @property {string} approval_id - Unique approval ID
 * @property {string} state - Current state
 * @property {CanaryResult} canary_result - Canary run results
 * @property {number} created_at - Creation timestamp
 * @property {number} expires_at - Expiration timestamp
 * @property {string|null} decided_by - Actor who made decision
 * @property {number|null} decided_at - Decision timestamp
 * @property {string|null} decision_reason - Reason for decision
 * @property {boolean} warning_sent - Expiration warning sent
 */

class ApprovalManager {
  /** @type {Map<string, ApprovalRequest>} */
  #requests = new Map();

  /** @type {Map<string, ApprovalRequest>} - By canary_run_id for lookup */
  #byCanaryRunId = new Map();

  /** @type {number} */
  #timeoutMs;

  /** @type {NodeJS.Timeout|null} */
  #expirationChecker = null;

  /** @type {Set<function>} */
  #listeners = new Set();

  constructor(options = {}) {
    this.#timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT_MS;

    // Start expiration checker
    this.#startExpirationChecker();
  }

  /**
   * Create a new approval request after canary run
   * @param {CanaryResult} canaryResult - Canary run results
   * @param {Object} [options] - Options
   * @param {number} [options.timeoutMs] - Custom timeout
   * @returns {ApprovalRequest}
   */
  createRequest(canaryResult, options = {}) {
    const approvalId = `approval_${crypto.randomUUID()}`;
    const now = Date.now();
    const timeoutMs = options.timeoutMs || this.#timeoutMs;

    const request = {
      approval_id: approvalId,
      state: ApprovalState.PENDING,
      canary_result: canaryResult,
      created_at: now,
      expires_at: now + timeoutMs,
      decided_by: null,
      decided_at: null,
      decision_reason: null,
      warning_sent: false
    };

    this.#requests.set(approvalId, request);
    this.#byCanaryRunId.set(canaryResult.canary_run_id, request);

    // Emit audit
    emitAudit({
      actor: 'system',
      action: 'APPROVAL_REQUESTED',
      target_type: 'approval',
      target_id: approvalId,
      reason: 'Canary run completed, awaiting human approval',
      metadata: {
        canary_run_id: canaryResult.canary_run_id,
        exchange: canaryResult.exchange,
        symbols: canaryResult.symbols,
        decision_count: canaryResult.decision_count,
        guards_passed: canaryResult.guards_passed,
        expires_at: new Date(request.expires_at).toISOString()
      }
    });

    console.log(JSON.stringify({
      event: 'approval_requested',
      approval_id: approvalId,
      canary_run_id: canaryResult.canary_run_id,
      expires_at: new Date(request.expires_at).toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('request_created', request);

    return request;
  }

  /**
   * Approve a request
   * @param {string} approvalId - Approval ID
   * @param {Object} options
   * @param {string} options.actor - Who is approving
   * @param {string} options.reason - Reason for approval
   * @returns {{success: boolean, request?: ApprovalRequest, error?: string}}
   */
  approve(approvalId, { actor, reason }) {
    const request = this.#requests.get(approvalId);

    if (!request) {
      return { success: false, error: 'APPROVAL_NOT_FOUND' };
    }

    if (request.state !== ApprovalState.PENDING) {
      return {
        success: false,
        error: 'INVALID_STATE',
        message: `Cannot approve request in state: ${request.state}`
      };
    }

    if (Date.now() > request.expires_at) {
      request.state = ApprovalState.EXPIRED;
      return { success: false, error: 'APPROVAL_EXPIRED' };
    }

    // Update state
    request.state = ApprovalState.APPROVED;
    request.decided_by = actor;
    request.decided_at = Date.now();
    request.decision_reason = reason;

    // Emit audit
    emitAudit({
      actor,
      action: 'APPROVAL_APPROVED',
      target_type: 'approval',
      target_id: approvalId,
      reason,
      metadata: {
        canary_run_id: request.canary_result.canary_run_id,
        exchange: request.canary_result.exchange,
        symbols: request.canary_result.symbols
      }
    });

    console.log(JSON.stringify({
      event: 'approval_approved',
      approval_id: approvalId,
      actor,
      reason
    }));

    // Notify listeners
    this.#notifyListeners('approved', request);

    return { success: true, request };
  }

  /**
   * Reject a request
   * @param {string} approvalId - Approval ID
   * @param {Object} options
   * @param {string} options.actor - Who is rejecting
   * @param {string} options.reason - Reason for rejection
   * @returns {{success: boolean, request?: ApprovalRequest, error?: string}}
   */
  reject(approvalId, { actor, reason }) {
    const request = this.#requests.get(approvalId);

    if (!request) {
      return { success: false, error: 'APPROVAL_NOT_FOUND' };
    }

    if (request.state !== ApprovalState.PENDING) {
      return {
        success: false,
        error: 'INVALID_STATE',
        message: `Cannot reject request in state: ${request.state}`
      };
    }

    // Update state
    request.state = ApprovalState.REJECTED;
    request.decided_by = actor;
    request.decided_at = Date.now();
    request.decision_reason = reason;

    // Emit audit
    emitAudit({
      actor,
      action: 'APPROVAL_REJECTED',
      target_type: 'approval',
      target_id: approvalId,
      reason,
      metadata: {
        canary_run_id: request.canary_result.canary_run_id,
        exchange: request.canary_result.exchange,
        symbols: request.canary_result.symbols
      }
    });

    console.log(JSON.stringify({
      event: 'approval_rejected',
      approval_id: approvalId,
      actor,
      reason
    }));

    // Notify listeners
    this.#notifyListeners('rejected', request);

    return { success: true, request };
  }

  /**
   * Get approval request by ID
   * @param {string} approvalId
   * @returns {ApprovalRequest|null}
   */
  getRequest(approvalId) {
    return this.#requests.get(approvalId) || null;
  }

  /**
   * Get approval request by canary run ID
   * @param {string} canaryRunId
   * @returns {ApprovalRequest|null}
   */
  getByCanaryRunId(canaryRunId) {
    return this.#byCanaryRunId.get(canaryRunId) || null;
  }

  /**
   * List pending approvals
   * @returns {ApprovalRequest[]}
   */
  listPending() {
    return Array.from(this.#requests.values())
      .filter(r => r.state === ApprovalState.PENDING);
  }

  /**
   * List all approvals (with optional filter)
   * @param {Object} [options]
   * @param {string} [options.state] - Filter by state
   * @param {number} [options.limit] - Max results
   * @returns {ApprovalRequest[]}
   */
  listAll(options = {}) {
    let results = Array.from(this.#requests.values());

    if (options.state) {
      results = results.filter(r => r.state === options.state);
    }

    // Sort by created_at descending
    results.sort((a, b) => b.created_at - a.created_at);

    if (options.limit) {
      results = results.slice(0, options.limit);
    }

    return results;
  }

  /**
   * Check if there's a valid (approved, not expired) approval for given config
   * @param {Object} config
   * @param {string} config.exchange
   * @param {string[]} config.symbols
   * @param {string} config.strategy_path
   * @returns {{valid: boolean, approval?: ApprovalRequest, reason?: string}}
   */
  checkApproval(config) {
    // Find approved requests matching config
    const matching = Array.from(this.#requests.values())
      .filter(r => {
        if (r.state !== ApprovalState.APPROVED) return false;
        if (r.canary_result.exchange !== config.exchange) return false;

        // Check symbols match (all requested symbols must be in approved symbols)
        const approvedSymbols = new Set(r.canary_result.symbols);
        const allSymbolsApproved = config.symbols.every(s => approvedSymbols.has(s));
        if (!allSymbolsApproved) return false;

        // Check strategy path matches
        if (r.canary_result.strategy_path !== config.strategy_path) return false;

        return true;
      })
      .sort((a, b) => b.decided_at - a.decided_at);

    if (matching.length === 0) {
      return {
        valid: false,
        reason: 'NO_MATCHING_APPROVAL'
      };
    }

    const approval = matching[0];

    // Check if approval is still within validity window (24 hours from approval)
    const validityWindowMs = 24 * 60 * 60 * 1000;
    if (Date.now() > approval.decided_at + validityWindowMs) {
      return {
        valid: false,
        reason: 'APPROVAL_VALIDITY_EXPIRED',
        approval
      };
    }

    return { valid: true, approval };
  }

  /**
   * Check if approval gate is required
   * @returns {boolean}
   */
  isApprovalRequired() {
    // Can be made configurable via env var
    return process.env.APPROVAL_GATE_ENABLED !== '0';
  }

  /**
   * Add listener for approval events
   * @param {function} listener - Called with (event, request)
   */
  addListener(listener) {
    this.#listeners.add(listener);
  }

  /**
   * Remove listener
   * @param {function} listener
   */
  removeListener(listener) {
    this.#listeners.delete(listener);
  }

  /**
   * Start expiration checker interval
   */
  #startExpirationChecker() {
    if (this.#expirationChecker) return;

    this.#expirationChecker = setInterval(() => {
      this.#checkExpirations();
    }, 60 * 1000); // Check every minute

    // Don't block process exit
    this.#expirationChecker.unref();
  }

  /**
   * Check for expired approvals
   */
  #checkExpirations() {
    const now = Date.now();

    for (const request of this.#requests.values()) {
      if (request.state !== ApprovalState.PENDING) continue;

      // Check for expiration
      if (now > request.expires_at) {
        request.state = ApprovalState.EXPIRED;

        emitAudit({
          actor: 'system',
          action: 'APPROVAL_EXPIRED',
          target_type: 'approval',
          target_id: request.approval_id,
          reason: 'Approval request expired without decision',
          metadata: {
            canary_run_id: request.canary_result.canary_run_id,
            created_at: new Date(request.created_at).toISOString(),
            expires_at: new Date(request.expires_at).toISOString()
          }
        });

        console.warn(JSON.stringify({
          event: 'approval_expired',
          approval_id: request.approval_id,
          canary_run_id: request.canary_result.canary_run_id
        }));

        // Send Slack alert for expired approval
        sendAlert({
          type: AlertType.APPROVAL_EXPIRED,
          severity: AlertSeverity.ERROR,
          message: `Approval EXPIRED without decision! ${request.canary_result.exchange} ${request.canary_result.symbols.join(', ')} - New canary run required`,
          source: 'ApprovalManager',
          metadata: {
            approval_id: request.approval_id,
            canary_run_id: request.canary_result.canary_run_id,
            exchange: request.canary_result.exchange,
            symbols: request.canary_result.symbols,
            created_at: new Date(request.created_at).toISOString()
          }
        }).catch(err => console.error('[ALERT] Failed:', err.message));

        this.#notifyListeners('expired', request);
        continue;
      }

      // Check for expiration warning (15 min before)
      if (!request.warning_sent && now > request.expires_at - EXPIRATION_WARNING_MS) {
        request.warning_sent = true;

        const expiresInMin = Math.round((request.expires_at - now) / 60000);

        console.warn(JSON.stringify({
          event: 'approval_expiring_soon',
          approval_id: request.approval_id,
          canary_run_id: request.canary_result.canary_run_id,
          expires_in_ms: request.expires_at - now
        }));

        // Send Slack alert for expiring approval
        sendAlert({
          type: AlertType.APPROVAL_EXPIRING,
          severity: AlertSeverity.WARNING,
          message: `Approval expiring in ${expiresInMin} minutes! Review required for ${request.canary_result.exchange} ${request.canary_result.symbols.join(', ')}`,
          source: 'ApprovalManager',
          metadata: {
            approval_id: request.approval_id,
            canary_run_id: request.canary_result.canary_run_id,
            exchange: request.canary_result.exchange,
            symbols: request.canary_result.symbols,
            expires_in_minutes: expiresInMin
          }
        }).catch(err => console.error('[ALERT] Failed:', err.message));

        this.#notifyListeners('expiring_soon', request);
      }
    }
  }

  /**
   * Notify listeners
   */
  #notifyListeners(event, request) {
    for (const listener of this.#listeners) {
      try {
        listener(event, request);
      } catch (err) {
        console.error(JSON.stringify({
          event: 'approval_listener_error',
          error: err.message
        }));
      }
    }
  }

  /**
   * Stop expiration checker
   */
  stop() {
    if (this.#expirationChecker) {
      clearInterval(this.#expirationChecker);
      this.#expirationChecker = null;
    }
  }

  /**
   * Get statistics
   */
  getStats() {
    const all = Array.from(this.#requests.values());
    return {
      total: all.length,
      pending: all.filter(r => r.state === ApprovalState.PENDING).length,
      approved: all.filter(r => r.state === ApprovalState.APPROVED).length,
      rejected: all.filter(r => r.state === ApprovalState.REJECTED).length,
      expired: all.filter(r => r.state === ApprovalState.EXPIRED).length
    };
  }
}

// Singleton instance
let instance = null;

/**
 * Get the singleton ApprovalManager instance
 * @returns {ApprovalManager}
 */
export function getApprovalManager() {
  if (!instance) {
    instance = new ApprovalManager();
  }
  return instance;
}

/**
 * Reset the singleton (for testing only)
 */
export function resetApprovalManager() {
  if (instance) {
    instance.stop();
  }
  instance = null;
}

export { ApprovalManager };
export default getApprovalManager;
