/**
 * Approval Routes for strategyd
 *
 * Human approval gate for live trading.
 * Ensures explicit human review before transitioning from canary to live.
 *
 * GET  /v1/approval/pending     - List pending approval requests
 * GET  /v1/approval/stats       - Get approval statistics
 * GET  /v1/approval/history     - Get recent approval decisions
 * GET  /v1/approval/:id         - Get approval details
 * POST /v1/approval/:id/approve - Approve a request
 * POST /v1/approval/:id/reject  - Reject a request
 * POST /v1/approval/request     - Create approval request (internal use)
 */

import {
  getApprovalManager,
  ApprovalState
} from '../../../core/approval/ApprovalManager.js';
import { emitAudit } from '../../../core/audit/AuditWriter.js';
import { sendAlert, AlertType, AlertSeverity } from '../../../core/alerts/index.js';

export default async function approvalRoutes(fastify, options) {

  /**
   * GET /v1/approval/pending - List pending approval requests
   */
  fastify.get('/v1/approval/pending', async (request, reply) => {
    const approvalManager = getApprovalManager();
    const pending = approvalManager.listPending();

    return {
      count: pending.length,
      requests: pending.map(formatApprovalResponse)
    };
  });

  /**
   * GET /v1/approval/stats - Get approval statistics
   */
  fastify.get('/v1/approval/stats', async (request, reply) => {
    const approvalManager = getApprovalManager();
    const stats = approvalManager.getStats();

    return {
      ...stats,
      approval_required: approvalManager.isApprovalRequired(),
      timestamp: new Date().toISOString()
    };
  });

  /**
   * GET /v1/approval/history - Get recent approval decisions
   */
  fastify.get('/v1/approval/history', async (request, reply) => {
    const { limit = 20, state } = request.query;
    const approvalManager = getApprovalManager();

    const requests = approvalManager.listAll({
      state: state || undefined,
      limit: Math.min(parseInt(limit) || 20, 100)
    });

    return {
      count: requests.length,
      requests: requests.map(formatApprovalResponse)
    };
  });

  /**
   * GET /v1/approval/:id - Get approval details
   */
  fastify.get('/v1/approval/:id', async (request, reply) => {
    const { id } = request.params;
    const approvalManager = getApprovalManager();

    const approval = approvalManager.getRequest(id);
    if (!approval) {
      return reply.code(404).send({
        error: 'APPROVAL_NOT_FOUND',
        approval_id: id
      });
    }

    return formatApprovalResponse(approval, { includeCanaryDetails: true });
  });

  /**
   * POST /v1/approval/:id/approve - Approve a request
   *
   * Body:
   * - reason: string (required) - Reason for approval
   */
  fastify.post('/v1/approval/:id/approve', async (request, reply) => {
    const { id } = request.params;
    const { reason } = request.body || {};

    if (!reason || typeof reason !== 'string' || reason.trim().length === 0) {
      return reply.code(400).send({
        error: 'REASON_REQUIRED',
        message: 'Approval reason is required'
      });
    }

    const approvalManager = getApprovalManager();
    const actor = `http:${request.ip}`;

    const result = approvalManager.approve(id, {
      actor,
      reason: reason.trim()
    });

    if (!result.success) {
      const statusCode = result.error === 'APPROVAL_NOT_FOUND' ? 404 : 400;
      return reply.code(statusCode).send({
        error: result.error,
        message: result.message || result.error
      });
    }

    // Send alert
    sendAlert({
      type: AlertType.APPROVAL_GRANTED,
      severity: AlertSeverity.INFO,
      message: `Live trading approved for ${result.request.canary_result.exchange} ${result.request.canary_result.symbols.join(', ')}`,
      source: 'strategyd',
      metadata: {
        approval_id: id,
        actor,
        reason: reason.trim(),
        exchange: result.request.canary_result.exchange,
        symbols: result.request.canary_result.symbols
      }
    }).catch(err => console.error('[ALERT] Failed:', err.message));

    return {
      status: 'APPROVED',
      approval: formatApprovalResponse(result.request)
    };
  });

  /**
   * POST /v1/approval/:id/reject - Reject a request
   *
   * Body:
   * - reason: string (required) - Reason for rejection
   */
  fastify.post('/v1/approval/:id/reject', async (request, reply) => {
    const { id } = request.params;
    const { reason } = request.body || {};

    if (!reason || typeof reason !== 'string' || reason.trim().length === 0) {
      return reply.code(400).send({
        error: 'REASON_REQUIRED',
        message: 'Rejection reason is required'
      });
    }

    const approvalManager = getApprovalManager();
    const actor = `http:${request.ip}`;

    const result = approvalManager.reject(id, {
      actor,
      reason: reason.trim()
    });

    if (!result.success) {
      const statusCode = result.error === 'APPROVAL_NOT_FOUND' ? 404 : 400;
      return reply.code(statusCode).send({
        error: result.error,
        message: result.message || result.error
      });
    }

    // Send alert
    sendAlert({
      type: AlertType.APPROVAL_REJECTED,
      severity: AlertSeverity.WARNING,
      message: `Live trading rejected for ${result.request.canary_result.exchange} ${result.request.canary_result.symbols.join(', ')}`,
      source: 'strategyd',
      metadata: {
        approval_id: id,
        actor,
        reason: reason.trim(),
        exchange: result.request.canary_result.exchange,
        symbols: result.request.canary_result.symbols
      }
    }).catch(err => console.error('[ALERT] Failed:', err.message));

    return {
      status: 'REJECTED',
      approval: formatApprovalResponse(result.request)
    };
  });

  /**
   * POST /v1/approval/request - Create new approval request
   *
   * Internal use: Called after canary run completes successfully.
   *
   * Body:
   * - canary_run_id: string (required)
   * - exchange: string (required)
   * - symbols: string[] (required)
   * - strategy_path: string (required)
   * - duration_ms: number
   * - decision_count: number
   * - decision_hash: string
   * - stats: object
   * - guards_passed: boolean
   * - guard_failure: string|null
   */
  fastify.post('/v1/approval/request', async (request, reply) => {
    const {
      canary_run_id,
      exchange,
      symbols,
      strategy_path,
      duration_ms,
      decision_count,
      decision_hash,
      stats,
      guards_passed,
      guard_failure
    } = request.body || {};

    // Validation
    if (!canary_run_id) {
      return reply.code(400).send({ error: 'CANARY_RUN_ID_REQUIRED' });
    }
    if (!exchange) {
      return reply.code(400).send({ error: 'EXCHANGE_REQUIRED' });
    }
    if (!symbols || !Array.isArray(symbols) || symbols.length === 0) {
      return reply.code(400).send({ error: 'SYMBOLS_REQUIRED' });
    }
    if (!strategy_path) {
      return reply.code(400).send({ error: 'STRATEGY_PATH_REQUIRED' });
    }

    const approvalManager = getApprovalManager();

    // Check if approval already exists for this canary run
    const existing = approvalManager.getByCanaryRunId(canary_run_id);
    if (existing) {
      return reply.code(409).send({
        error: 'APPROVAL_ALREADY_EXISTS',
        approval_id: existing.approval_id,
        state: existing.state
      });
    }

    const canaryResult = {
      canary_run_id,
      exchange,
      symbols,
      strategy_path,
      duration_ms: duration_ms || 0,
      decision_count: decision_count || 0,
      decision_hash: decision_hash || null,
      stats: stats || {},
      guards_passed: guards_passed !== false,
      guard_failure: guard_failure || null
    };

    const approval = approvalManager.createRequest(canaryResult);

    // Send alert for new approval request
    sendAlert({
      type: AlertType.APPROVAL_PENDING,
      severity: AlertSeverity.WARNING,
      message: `Approval required for live trading: ${exchange} ${symbols.join(', ')}`,
      source: 'strategyd',
      metadata: {
        approval_id: approval.approval_id,
        canary_run_id,
        exchange,
        symbols,
        decision_count,
        guards_passed,
        expires_at: new Date(approval.expires_at).toISOString()
      }
    }).catch(err => console.error('[ALERT] Failed:', err.message));

    return reply.code(201).send({
      status: 'PENDING',
      approval: formatApprovalResponse(approval)
    });
  });

  /**
   * GET /v1/approval/check - Check if valid approval exists
   *
   * Query params:
   * - exchange: string (required)
   * - symbols: string (comma-separated, required)
   * - strategy_path: string (required)
   */
  fastify.get('/v1/approval/check', async (request, reply) => {
    const { exchange, symbols, strategy_path } = request.query;

    if (!exchange || !symbols || !strategy_path) {
      return reply.code(400).send({
        error: 'MISSING_PARAMETERS',
        required: ['exchange', 'symbols', 'strategy_path']
      });
    }

    const approvalManager = getApprovalManager();

    // Check if approval is required
    if (!approvalManager.isApprovalRequired()) {
      return {
        valid: true,
        approval_required: false,
        message: 'Approval gate is disabled'
      };
    }

    const symbolsArray = symbols.split(',').map(s => s.trim()).filter(Boolean);

    const result = approvalManager.checkApproval({
      exchange,
      symbols: symbolsArray,
      strategy_path
    });

    return {
      valid: result.valid,
      approval_required: true,
      reason: result.reason || null,
      approval: result.approval ? formatApprovalResponse(result.approval) : null
    };
  });
}

/**
 * Format approval response
 */
function formatApprovalResponse(approval, options = {}) {
  const response = {
    approval_id: approval.approval_id,
    state: approval.state,
    created_at: new Date(approval.created_at).toISOString(),
    expires_at: new Date(approval.expires_at).toISOString(),
    decided_by: approval.decided_by,
    decided_at: approval.decided_at ? new Date(approval.decided_at).toISOString() : null,
    decision_reason: approval.decision_reason,
    canary_summary: {
      canary_run_id: approval.canary_result.canary_run_id,
      exchange: approval.canary_result.exchange,
      symbols: approval.canary_result.symbols,
      strategy_path: approval.canary_result.strategy_path,
      decision_count: approval.canary_result.decision_count,
      guards_passed: approval.canary_result.guards_passed,
      guard_failure: approval.canary_result.guard_failure
    }
  };

  // Calculate time remaining for pending requests
  if (approval.state === ApprovalState.PENDING) {
    const remaining = approval.expires_at - Date.now();
    response.expires_in_ms = Math.max(0, remaining);
    response.expires_in_minutes = Math.max(0, Math.round(remaining / 60000));
  }

  // Include full canary details if requested
  if (options.includeCanaryDetails) {
    response.canary_details = {
      ...approval.canary_result,
      duration_ms: approval.canary_result.duration_ms,
      decision_hash: approval.canary_result.decision_hash,
      stats: approval.canary_result.stats
    };
  }

  return response;
}
