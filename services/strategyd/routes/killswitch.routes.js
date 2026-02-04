/**
 * Kill Switch Routes for strategyd
 *
 * Centralized kill switch management for live trading safety.
 * Uses KillSwitchManager singleton for system-wide coordination.
 *
 * GET  /v1/kill-switch/status     - Get current kill switch status
 * POST /v1/kill-switch/activate   - Activate global or symbol kill switch
 * POST /v1/kill-switch/deactivate - Deactivate kill switch
 * POST /v1/kill-switch/emergency  - Emergency stop all trading immediately
 */

import { getKillSwitchManager } from '../../../core/futures/KillSwitchManager.js';
import { emitAudit } from '../../../core/audit/AuditWriter.js';
import { sendAlert, AlertType, AlertSeverity } from '../../../core/alerts/index.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';

export default async function killswitchRoutes(fastify, options) {

  /**
   * GET /v1/kill-switch/status - Get current kill switch status
   */
  fastify.get('/v1/kill-switch/status', async (request, reply) => {
    const killSwitchManager = getKillSwitchManager();
    const status = killSwitchManager.getStatus();
    const health = observerRegistry.getHealth();

    return {
      ...status,
      active_runs: health.active_runs,
      timestamp: new Date().toISOString()
    };
  });

  /**
   * POST /v1/kill-switch/activate - Activate kill switch
   *
   * Body:
   * - type: 'global' | 'symbol' (required)
   * - reason: string (required)
   * - symbols: string[] (required if type='symbol')
   * - stopAllRuns: boolean (default true, only for global)
   */
  fastify.post('/v1/kill-switch/activate', async (request, reply) => {
    const { type, reason, symbols, stopAllRuns = true } = request.body || {};

    // Validation
    if (!type || !['global', 'symbol'].includes(type)) {
      return reply.code(400).send({
        error: 'INVALID_TYPE',
        message: "type must be 'global' or 'symbol'"
      });
    }

    if (!reason || typeof reason !== 'string' || reason.trim().length === 0) {
      return reply.code(400).send({
        error: 'REASON_REQUIRED',
        message: 'reason is required and must be a non-empty string'
      });
    }

    if (type === 'symbol') {
      if (!symbols || !Array.isArray(symbols) || symbols.length === 0) {
        return reply.code(400).send({
          error: 'SYMBOLS_REQUIRED',
          message: 'symbols array is required for symbol kill switch'
        });
      }
    }

    const killSwitchManager = getKillSwitchManager();
    const actor = `http:${request.ip}`;

    let result;
    if (type === 'global') {
      result = killSwitchManager.activateGlobal({
        reason: reason.trim(),
        actor,
        stopAllRuns
      });

      // Send critical alert
      sendAlert({
        type: AlertType.KILL_SWITCH_ACTIVATED,
        severity: AlertSeverity.CRITICAL,
        message: `GLOBAL KILL SWITCH ACTIVATED: ${reason}`,
        source: 'strategyd',
        metadata: {
          type: 'global',
          reason,
          activated_by: actor,
          stopped_runs: stopAllRuns
        }
      }).catch(err => console.error('[ALERT] Failed:', err.message));

    } else {
      result = killSwitchManager.activateSymbols({
        symbols,
        reason: reason.trim(),
        actor
      });

      // Send warning alert for symbol kill
      if (result.added.length > 0) {
        sendAlert({
          type: AlertType.KILL_SWITCH_ACTIVATED,
          severity: AlertSeverity.WARNING,
          message: `Symbol kill switch activated for: ${result.added.join(', ')}`,
          source: 'strategyd',
          metadata: {
            type: 'symbol',
            symbols: result.added,
            reason,
            activated_by: actor
          }
        }).catch(err => console.error('[ALERT] Failed:', err.message));
      }
    }

    return {
      status: 'ACTIVATED',
      type,
      ...result,
      current_state: killSwitchManager.getStatus()
    };
  });

  /**
   * POST /v1/kill-switch/deactivate - Deactivate kill switch
   *
   * Body:
   * - type: 'global' | 'symbol' | 'all' (required)
   * - symbols: string[] (required if type='symbol')
   */
  fastify.post('/v1/kill-switch/deactivate', async (request, reply) => {
    const { type, symbols } = request.body || {};

    // Validation
    if (!type || !['global', 'symbol', 'all'].includes(type)) {
      return reply.code(400).send({
        error: 'INVALID_TYPE',
        message: "type must be 'global', 'symbol', or 'all'"
      });
    }

    if (type === 'symbol') {
      if (!symbols || !Array.isArray(symbols) || symbols.length === 0) {
        return reply.code(400).send({
          error: 'SYMBOLS_REQUIRED',
          message: 'symbols array is required for symbol deactivation'
        });
      }
    }

    const killSwitchManager = getKillSwitchManager();
    const actor = `http:${request.ip}`;

    let result;
    if (type === 'global') {
      result = killSwitchManager.deactivateGlobal({ actor });
    } else if (type === 'symbol') {
      result = killSwitchManager.deactivateSymbols({ symbols, actor });
    } else {
      result = killSwitchManager.deactivateAll({ actor });
    }

    // Send info alert
    sendAlert({
      type: AlertType.KILL_SWITCH_DEACTIVATED,
      severity: AlertSeverity.INFO,
      message: `Kill switch deactivated (type: ${type})`,
      source: 'strategyd',
      metadata: {
        type,
        deactivated_by: actor,
        ...result
      }
    }).catch(err => console.error('[ALERT] Failed:', err.message));

    return {
      status: 'DEACTIVATED',
      type,
      ...result,
      current_state: killSwitchManager.getStatus()
    };
  });

  /**
   * POST /v1/kill-switch/emergency - Emergency stop all trading
   *
   * Immediately activates global kill switch and stops ALL runs.
   * Use in critical situations only.
   *
   * Body:
   * - reason: string (required)
   */
  fastify.post('/v1/kill-switch/emergency', async (request, reply) => {
    const { reason } = request.body || {};

    if (!reason || typeof reason !== 'string' || reason.trim().length === 0) {
      return reply.code(400).send({
        error: 'REASON_REQUIRED',
        message: 'reason is required for emergency stop'
      });
    }

    const killSwitchManager = getKillSwitchManager();
    const actor = `http:${request.ip}`;

    // Log emergency stop with high visibility
    console.error('='.repeat(60));
    console.error('EMERGENCY STOP TRIGGERED');
    console.error(`Reason: ${reason}`);
    console.error(`Actor: ${actor}`);
    console.error(`Time: ${new Date().toISOString()}`);
    console.error('='.repeat(60));

    const result = killSwitchManager.emergencyStop({
      reason: reason.trim(),
      actor
    });

    // Emit special audit for emergency
    emitAudit({
      actor,
      action: 'EMERGENCY_STOP',
      target_type: 'system',
      target_id: 'all_trading',
      reason: reason.trim(),
      metadata: {
        stopped_runs: result.stopped_runs,
        timestamp: new Date().toISOString()
      }
    });

    // Send critical alert
    sendAlert({
      type: AlertType.EMERGENCY_STOP,
      severity: AlertSeverity.CRITICAL,
      message: `EMERGENCY STOP: ${reason}`,
      source: 'strategyd',
      metadata: {
        reason,
        actor,
        stopped_runs: result.stopped_runs
      }
    }).catch(err => console.error('[ALERT] Failed:', err.message));

    return {
      status: 'EMERGENCY_STOP_EXECUTED',
      ...result,
      current_state: killSwitchManager.getStatus(),
      message: 'All trading has been stopped. Manual intervention required to resume.'
    };
  });

  /**
   * GET /v1/kill-switch/history - Get kill switch activation history
   *
   * Returns recent kill switch events from audit log.
   */
  fastify.get('/v1/kill-switch/history', async (request, reply) => {
    const { limit = 20 } = request.query;

    // For now, return current state info
    // Full history would require reading audit log
    const killSwitchManager = getKillSwitchManager();
    const status = killSwitchManager.getStatus();

    return {
      current_state: status,
      note: 'Full history available in audit log',
      audit_actions: [
        'KILL_SWITCH_ACTIVATE',
        'KILL_SWITCH_DEACTIVATE',
        'EMERGENCY_STOP'
      ]
    };
  });
}
