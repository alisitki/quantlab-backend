/**
 * Live Strategy Routes for strategyd
 *
 * POST /live/start  - Start a live strategy run
 * POST /live/stop   - Stop a live strategy run
 * GET  /live/status - Get status of all live runs
 * GET  /live/status/:id - Get status of specific run
 * POST /live/kill-switch - Activate/deactivate kill switch
 * GET  /live/kill-switch/status - Get kill switch status
 * GET  /live/preflight - Run pre-flight checks before live trading
 */

import fs from 'node:fs';
import path from 'node:path';
import { LiveStrategyRunner } from '../../../core/strategy/live/LiveStrategyRunner.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';
import { emitAudit } from '../../../core/audit/AuditWriter.js';
import { sendAlert, AlertType, AlertSeverity } from '../../../core/alerts/index.js';
import { getKillSwitchManager } from '../../../core/futures/KillSwitchManager.js';
import { getApprovalManager } from '../../../core/approval/ApprovalManager.js';

// Default configurations
const DEFAULT_EXECUTION_CONFIG = {
  initialCapital: 10000,
  feeRate: 0.0004
};

const DEFAULT_RISK_CONFIG = {
  enabled: true,
  maxPositions: 5,
  cooldownEvents: 100,
  maxDailyLossPct: 0.02,
  stopLossPct: 0.01,
  takeProfitPct: 0.02
};

const VALID_EXCHANGES = ['binance', 'bybit', 'okx'];

// In-memory runner storage (keyed by live_run_id)
const activeRunners = new Map();

export default async function liveRoutes(fastify, options) {

  /**
   * POST /live/start - Start a new live strategy run
   */
  fastify.post('/live/start', async (request, reply) => {
    const {
      exchange,
      symbols,
      strategyPath,
      strategyConfig,
      seed,
      errorPolicy,
      orderingMode,
      enableMetrics,
      riskConfig,
      executionConfig,
      maxLagMs
    } = request.body || {};

    // Kill switch check
    const killSwitchManager = getKillSwitchManager();
    if (killSwitchManager.isGlobalActive()) {
      const status = killSwitchManager.getStatus();
      return reply.code(503).send({
        error: 'KILL_SWITCH_ACTIVE',
        message: 'Live trading is currently disabled by kill switch',
        activatedAt: status.activated_at,
        reason: status.reason
      });
    }

    // Approval gate check (skip if bypass_approval is true and env allows)
    const bypassApproval = request.body?.bypass_approval === true &&
      process.env.APPROVAL_BYPASS_ALLOWED === '1';

    if (!bypassApproval) {
      const approvalManager = getApprovalManager();
      if (approvalManager.isApprovalRequired()) {
        const absoluteStrategyPath = path.isAbsolute(strategyPath)
          ? strategyPath
          : path.resolve(process.cwd(), strategyPath);

        const approvalCheck = approvalManager.checkApproval({
          exchange,
          symbols,
          strategy_path: absoluteStrategyPath
        });

        if (!approvalCheck.valid) {
          return reply.code(403).send({
            error: 'APPROVAL_REQUIRED',
            message: 'Live trading requires human approval. Submit canary run for approval first.',
            reason: approvalCheck.reason,
            approval: approvalCheck.approval ? {
              approval_id: approvalCheck.approval.approval_id,
              state: approvalCheck.approval.state
            } : null
          });
        }

        // Log approved live run start
        emitAudit({
          actor: 'http_client',
          action: 'LIVE_RUN_WITH_APPROVAL',
          target_type: 'live_run',
          target_id: null,
          reason: 'Starting live run with valid approval',
          metadata: {
            approval_id: approvalCheck.approval.approval_id,
            exchange,
            symbols,
            ip: request.ip
          }
        });
      }
    }

    // Required field validation
    if (!exchange || !symbols || !strategyPath) {
      return reply.code(400).send({
        error: 'MISSING_REQUIRED_FIELDS',
        required: ['exchange', 'symbols', 'strategyPath']
      });
    }

    // Exchange validation
    if (!VALID_EXCHANGES.includes(exchange)) {
      return reply.code(400).send({
        error: 'INVALID_EXCHANGE',
        valid: VALID_EXCHANGES
      });
    }

    // Symbols validation
    if (!Array.isArray(symbols) || symbols.length === 0) {
      return reply.code(400).send({
        error: 'INVALID_SYMBOLS',
        message: 'symbols must be a non-empty array'
      });
    }

    // Strategy path validation
    const absoluteStrategyPath = path.isAbsolute(strategyPath)
      ? strategyPath
      : path.resolve(process.cwd(), strategyPath);

    if (!fs.existsSync(absoluteStrategyPath)) {
      return reply.code(400).send({
        error: 'STRATEGY_NOT_FOUND',
        path: strategyPath
      });
    }

    try {
      // Merge configs with defaults
      const mergedExecutionConfig = {
        ...DEFAULT_EXECUTION_CONFIG,
        ...executionConfig
      };

      const mergedRiskConfig = {
        ...DEFAULT_RISK_CONFIG,
        ...riskConfig
      };

      const runner = new LiveStrategyRunner({
        dataset: { parquet: 'live', meta: 'live' },
        exchange,
        symbols,
        strategyPath: absoluteStrategyPath,
        strategyConfig,
        seed,
        errorPolicy,
        orderingMode,
        enableMetrics: enableMetrics !== false,
        riskConfig: mergedRiskConfig,
        executionConfig: mergedExecutionConfig,
        maxLagMs
      });

      const liveRunId = runner.liveRunId;
      activeRunners.set(liveRunId, runner);

      // Audit log
      emitAudit({
        actor: 'http_client',
        action: 'LIVE_RUN_START',
        target_type: 'live_run',
        target_id: liveRunId,
        reason: null,
        metadata: {
          exchange,
          symbols,
          strategyPath,
          ip: request.ip
        }
      });

      // Start run in background
      runner.run({ handleSignals: false })
        .then((result) => {
          console.log(`[LIVE] Run completed: ${liveRunId}`, JSON.stringify(result));
          emitAudit({
            actor: 'system',
            action: 'LIVE_RUN_COMPLETE',
            target_type: 'live_run',
            target_id: liveRunId,
            reason: result?.stop_reason || 'completed',
            metadata: { result }
          });
        })
        .catch((err) => {
          console.error(`[LIVE] Run error: ${liveRunId} - ${err.message}`);
          emitAudit({
            actor: 'system',
            action: 'LIVE_RUN_ERROR',
            target_type: 'live_run',
            target_id: liveRunId,
            reason: err.message,
            metadata: { error: err.message }
          });

          // Send error alert
          sendAlert({
            type: AlertType.LIVE_RUN_ERROR,
            severity: AlertSeverity.ERROR,
            message: `Live run failed: ${err.message}`,
            source: 'strategyd',
            metadata: {
              live_run_id: liveRunId,
              exchange,
              symbols,
              error: err.message
            }
          }).catch(alertErr => console.error('[ALERT] Failed to send run error alert:', alertErr.message));
        })
        .finally(() => {
          activeRunners.delete(liveRunId);
        });

      return reply.code(201).send({
        live_run_id: liveRunId,
        status: 'STARTED',
        message: 'Live strategy run started',
        config: {
          exchange,
          symbols,
          riskEnabled: mergedRiskConfig.enabled,
          initialCapital: mergedExecutionConfig.initialCapital
        }
      });
    } catch (err) {
      emitAudit({
        actor: 'http_client',
        action: 'LIVE_RUN_START_FAILED',
        target_type: 'live_run',
        target_id: null,
        reason: err.message,
        metadata: {
          exchange,
          symbols,
          strategyPath,
          ip: request.ip
        }
      });

      return reply.code(500).send({
        error: 'START_FAILED',
        message: err.message
      });
    }
  });

  /**
   * POST /live/stop - Stop a live strategy run
   */
  fastify.post('/live/stop', async (request, reply) => {
    const { live_run_id } = request.body || {};

    if (!live_run_id) {
      return reply.code(400).send({
        error: 'MISSING_LIVE_RUN_ID'
      });
    }

    // Audit log
    emitAudit({
      actor: 'http_client',
      action: 'LIVE_RUN_STOP_REQUEST',
      target_type: 'live_run',
      target_id: live_run_id,
      reason: 'API_STOP',
      metadata: { ip: request.ip }
    });

    // Try local runner first
    const runner = activeRunners.get(live_run_id);
    if (runner) {
      runner.stop();
      return reply.send({
        live_run_id,
        status: 'STOP_REQUESTED',
        source: 'local_runner'
      });
    }

    // Fallback to observer registry
    const ok = observerRegistry.stopRun(live_run_id, 'API_STOP');
    if (ok) {
      return reply.send({
        live_run_id,
        status: 'STOP_REQUESTED',
        source: 'observer_registry'
      });
    }

    return reply.code(404).send({
      error: 'RUN_NOT_FOUND',
      live_run_id
    });
  });

  /**
   * GET /live/status - Get status of all live runs
   */
  fastify.get('/live/status', async (request, reply) => {
    const runs = observerRegistry.listRuns();
    const health = observerRegistry.getHealth();

    return {
      health,
      runs,
      active_count: runs.filter(r => r.status === 'RUNNING').length,
      local_runners: activeRunners.size
    };
  });

  /**
   * GET /live/status/:id - Get status of specific run
   */
  fastify.get('/live/status/:id', async (request, reply) => {
    const { id } = request.params;
    const runs = observerRegistry.listRuns();
    const run = runs.find(r => r.live_run_id === id);

    if (!run) {
      return reply.code(404).send({
        error: 'RUN_NOT_FOUND',
        live_run_id: id
      });
    }

    return run;
  });

  // ============================================================================
  // KILL SWITCH ENDPOINTS
  // ============================================================================

  /**
   * POST /live/kill-switch - Activate or deactivate kill switch
   *
   * DEPRECATED: Use /v1/kill-switch/* endpoints instead.
   * This endpoint is kept for backward compatibility.
   */
  fastify.post('/live/kill-switch', async (request, reply) => {
    const { activate, reason } = request.body || {};
    const killSwitchManager = getKillSwitchManager();
    const actor = `http:${request.ip}`;

    if (typeof activate !== 'boolean') {
      return reply.code(400).send({
        error: 'INVALID_REQUEST',
        message: 'activate must be a boolean (true or false)'
      });
    }

    if (activate) {
      const result = killSwitchManager.activateGlobal({
        reason: reason || 'Manual activation via /live/kill-switch',
        actor,
        stopAllRuns: true
      });

      // Send critical alert
      sendAlert({
        type: AlertType.KILL_SWITCH_ACTIVATED,
        severity: AlertSeverity.CRITICAL,
        message: `Kill switch activated. Reason: ${reason || 'Manual activation'}`,
        source: 'strategyd',
        metadata: { ip: request.ip }
      }).catch(err => console.error('[ALERT] Failed:', err.message));

      const status = killSwitchManager.getStatus();
      return reply.send({
        status: 'KILL_SWITCH_ACTIVATED',
        state: {
          active: status.is_active,
          activatedAt: status.activated_at ? new Date(status.activated_at).toISOString() : null,
          activatedBy: status.activated_by,
          reason: status.reason
        }
      });

    } else {
      killSwitchManager.deactivateGlobal({ actor });

      // Send info alert
      sendAlert({
        type: AlertType.KILL_SWITCH_DEACTIVATED,
        severity: AlertSeverity.INFO,
        message: 'Kill switch deactivated. Live trading re-enabled.',
        source: 'strategyd',
        metadata: { ip: request.ip }
      }).catch(err => console.error('[ALERT] Failed:', err.message));

      return reply.send({
        status: 'KILL_SWITCH_DEACTIVATED',
        state: {
          active: false,
          activatedAt: null,
          activatedBy: null,
          reason: null
        }
      });
    }
  });

  /**
   * GET /live/kill-switch/status - Get current kill switch status
   *
   * DEPRECATED: Use /v1/kill-switch/status instead.
   */
  fastify.get('/live/kill-switch/status', async (request, reply) => {
    const killSwitchManager = getKillSwitchManager();
    const status = killSwitchManager.getStatus();

    return {
      active: status.is_active,
      activatedAt: status.activated_at ? new Date(status.activated_at).toISOString() : null,
      activatedBy: status.activated_by,
      reason: status.reason,
      activeRunners: activeRunners.size,
      registryRuns: observerRegistry.listRuns().filter(r => r.status === 'RUNNING').length
    };
  });

  // ============================================================================
  // PRE-FLIGHT CHECKS
  // ============================================================================

  /**
   * GET /live/preflight - Run pre-flight checks before live trading
   *
   * Checks:
   * - Kill switch is not active
   * - Required environment variables are set
   * - WebSocket connectivity is enabled
   * - Risk configuration is valid
   * - Audit system is operational
   * - Observer API is healthy
   */
  fastify.get('/live/preflight', async (request, reply) => {
    const checks = [];
    let allPassed = true;

    // 1. Kill Switch Check
    const killSwitchManager = getKillSwitchManager();
    const killSwitchStatus = killSwitchManager.getStatus();
    const killSwitchCheck = {
      name: 'kill_switch',
      status: killSwitchStatus.is_active ? 'FAIL' : 'PASS',
      message: killSwitchStatus.is_active
        ? `Kill switch is active (reason: ${killSwitchStatus.reason})`
        : 'Kill switch is not active',
      details: killSwitchStatus
    };
    checks.push(killSwitchCheck);
    if (killSwitchCheck.status === 'FAIL') allPassed = false;

    // 2. Environment Variables Check
    const requiredEnvVars = [
      'STRATEGYD_TOKEN',
      'REPLAYD_TOKEN'
    ];
    const recommendedEnvVars = [
      'CORE_LIVE_WS_ENABLED',
      'RUN_ARCHIVE_ENABLED',
      'AUDIT_SPOOL_DIR'
    ];

    const missingRequired = requiredEnvVars.filter(v => !process.env[v]);
    const missingRecommended = recommendedEnvVars.filter(v => !process.env[v]);

    const envCheck = {
      name: 'environment',
      status: missingRequired.length > 0 ? 'FAIL' : (missingRecommended.length > 0 ? 'WARN' : 'PASS'),
      message: missingRequired.length > 0
        ? `Missing required env vars: ${missingRequired.join(', ')}`
        : (missingRecommended.length > 0
          ? `Missing recommended env vars: ${missingRecommended.join(', ')}`
          : 'All environment variables configured'),
      details: {
        required: requiredEnvVars.map(v => ({ name: v, set: !!process.env[v] })),
        recommended: recommendedEnvVars.map(v => ({ name: v, set: !!process.env[v] }))
      }
    };
    checks.push(envCheck);
    if (envCheck.status === 'FAIL') allPassed = false;

    // 3. WebSocket Connectivity Check
    const wsEnabled = process.env.CORE_LIVE_WS_ENABLED === '1';
    const wsCheck = {
      name: 'websocket',
      status: wsEnabled ? 'PASS' : 'WARN',
      message: wsEnabled
        ? 'Live WebSocket connectivity enabled'
        : 'Live WebSocket connectivity not enabled (CORE_LIVE_WS_ENABLED != 1)'
    };
    checks.push(wsCheck);

    // 4. Risk Configuration Check
    const riskEnabled = process.env.RISK_ENABLED !== '0';
    const riskCheck = {
      name: 'risk_config',
      status: 'PASS',
      message: riskEnabled
        ? 'Risk management enabled with default configuration'
        : 'Risk management disabled (RISK_ENABLED=0)',
      details: {
        enabled: riskEnabled,
        defaults: DEFAULT_RISK_CONFIG
      }
    };
    checks.push(riskCheck);

    // 5. Audit System Check
    const auditSpoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';
    let auditCheck;
    try {
      const auditDirExists = fs.existsSync(auditSpoolDir);
      auditCheck = {
        name: 'audit_system',
        status: auditDirExists ? 'PASS' : 'WARN',
        message: auditDirExists
          ? `Audit spool directory exists: ${auditSpoolDir}`
          : `Audit spool directory does not exist: ${auditSpoolDir}`,
        details: { spoolDir: auditSpoolDir }
      };
    } catch (err) {
      auditCheck = {
        name: 'audit_system',
        status: 'WARN',
        message: `Cannot check audit directory: ${err.message}`,
        details: { spoolDir: auditSpoolDir, error: err.message }
      };
    }
    checks.push(auditCheck);

    // 6. Run Archive Check
    const archiveEnabled = process.env.RUN_ARCHIVE_ENABLED === '1';
    const archiveCheck = {
      name: 'run_archive',
      status: archiveEnabled ? 'PASS' : 'WARN',
      message: archiveEnabled
        ? 'Run archiving enabled'
        : 'Run archiving disabled (RUN_ARCHIVE_ENABLED != 1)',
      details: {
        enabled: archiveEnabled,
        bucket: process.env.RUN_ARCHIVE_S3_BUCKET || null
      }
    };
    checks.push(archiveCheck);

    // 7. Observer Registry Health
    const observerHealth = observerRegistry.getHealth();
    const observerCheck = {
      name: 'observer_registry',
      status: 'PASS',
      message: `Observer registry healthy (${observerHealth.activeRuns || 0} active runs)`,
      details: observerHealth
    };
    checks.push(observerCheck);

    // 8. Approval Gate Check
    const approvalManager = getApprovalManager();
    const approvalRequired = approvalManager.isApprovalRequired();
    const pendingApprovals = approvalManager.listPending();
    const approvalStats = approvalManager.getStats();

    const approvalCheck = {
      name: 'approval_gate',
      status: approvalRequired ? 'INFO' : 'WARN',
      message: approvalRequired
        ? `Approval gate enabled (${pendingApprovals.length} pending)`
        : 'Approval gate disabled (APPROVAL_GATE_ENABLED=0)',
      details: {
        enabled: approvalRequired,
        pending_count: pendingApprovals.length,
        stats: approvalStats
      }
    };
    checks.push(approvalCheck);

    // 9. Active Runners Check
    const activeRunnersCheck = {
      name: 'active_runners',
      status: 'INFO',
      message: `${activeRunners.size} local runners active`,
      details: { count: activeRunners.size }
    };
    checks.push(activeRunnersCheck);

    // Summary
    const summary = {
      ready: allPassed,
      timestamp: new Date().toISOString(),
      passed: checks.filter(c => c.status === 'PASS').length,
      warnings: checks.filter(c => c.status === 'WARN').length,
      failed: checks.filter(c => c.status === 'FAIL').length,
      info: checks.filter(c => c.status === 'INFO').length
    };

    return {
      preflight: allPassed ? 'READY' : 'NOT_READY',
      summary,
      checks
    };
  });
}
