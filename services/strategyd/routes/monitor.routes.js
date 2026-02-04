/**
 * Live Monitoring Dashboard Routes for strategyd
 *
 * GET /v1/monitor/dashboard    - Complete system overview (single call)
 * GET /v1/monitor/execution    - Execution stats and daily limits
 * GET /v1/monitor/positions    - Paper vs exchange position comparison
 * GET /v1/monitor/health       - Exchange connectivity status
 * GET /v1/monitor/performance  - Slippage analytics
 * GET /v1/monitor/orders       - Recent orders and state distribution
 * GET /v1/monitor/alerts       - Recent alerts from log
 */

import fs from 'fs';
import path from 'path';
import { getBridgeSingletons } from './bridge.routes.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';
import { loadKillSwitchFromEnv } from '../../../core/futures/kill_switch.js';
import { getCostWriter } from '../../../core/vast/CostWriter.js';
import { createVastClient } from '../../../core/vast/VastClient.js';
import { SLOCalculator } from '../../../core/slo/index.js';

/**
 * Read SYSTEM_STATE.json
 */
function readSystemState() {
  try {
    const statePath = path.resolve(process.cwd(), 'SYSTEM_STATE.json');
    const content = fs.readFileSync(statePath, 'utf-8');
    return JSON.parse(content);
  } catch {
    return { current_phase: 4, phase_status: {} };
  }
}

/**
 * Read recent alerts from JSONL log
 * @param {number} lookbackMs - How far back to look (in milliseconds)
 * @returns {{ recentCount: number, criticalCount: number, lastAlertAt: string | null }}
 */
export function readAlertSummary(lookbackMs) {
  const logPath = process.env.ALERT_LOG_PATH || 'logs/alerts.jsonl';
  const cutoff = Date.now() - lookbackMs;

  let total = 0;
  let criticalCount = 0;
  let lastAlertAt = null;

  try {
    if (!fs.existsSync(logPath)) {
      return { recentCount: 0, criticalCount: 0, lastAlertAt: null };
    }

    const content = fs.readFileSync(logPath, 'utf-8');
    const lines = content.trim().split('\n').filter(Boolean);

    for (const line of lines) {
      try {
        const alert = JSON.parse(line);
        const alertTs = new Date(alert.timestamp).getTime();

        if (alertTs >= cutoff) {
          total++;
          if (alert.severity === 'critical') criticalCount++;
          if (!lastAlertAt || alert.timestamp > lastAlertAt) {
            lastAlertAt = alert.timestamp;
          }
        }
      } catch {
        // Skip malformed lines
      }
    }
  } catch {
    // Log not accessible
  }

  return { recentCount: total, criticalCount, lastAlertAt };
}

/**
 * Read alerts with full details
 * @param {number} lookbackMs - How far back to look
 * @param {Object} filters - { severity, type, limit }
 * @returns {Array}
 */
function readAlertDetails(lookbackMs, filters = {}) {
  const logPath = process.env.ALERT_LOG_PATH || 'logs/alerts.jsonl';
  const cutoff = Date.now() - lookbackMs;
  const { severity, type, limit = 100 } = filters;

  const alerts = [];

  try {
    if (!fs.existsSync(logPath)) {
      return [];
    }

    const content = fs.readFileSync(logPath, 'utf-8');
    const lines = content.trim().split('\n').filter(Boolean);

    for (const line of lines) {
      try {
        const alert = JSON.parse(line);
        const alertTs = new Date(alert.timestamp).getTime();

        if (alertTs < cutoff) continue;
        if (severity && alert.severity !== severity) continue;
        if (type && alert.type !== type) continue;

        alerts.push(alert);
      } catch {
        // Skip malformed lines
      }
    }
  } catch {
    // Log not accessible
  }

  // Sort by timestamp descending and limit
  alerts.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return alerts.slice(0, limit);
}

export default async function monitorRoutes(fastify, options) {

  /**
   * GET /v1/monitor/dashboard - Complete system overview
   */
  fastify.get('/v1/monitor/dashboard', async (request, reply) => {
    const timestamp = new Date().toISOString();
    const { bridge, healthMonitor, slippageAnalyzer } = getBridgeSingletons();

    // System state
    const systemState = readSystemState();
    const killSwitch = loadKillSwitchFromEnv();

    // Bridge stats
    const bridgeStats = bridge?.getStats() ?? null;
    const bridgeConfig = bridge?.getConfig() ?? null;

    // Health status
    const healthStatus = healthMonitor?.getLastStatus() ?? null;

    // Slippage
    const slippageStats = slippageAnalyzer?.getAggregateStats() ?? {
      totalRecords: 0,
      avgSlippageBps: 0,
      totalNotional: 0,
      weightedSlippageBps: 0
    };

    // Observer registry
    const observerHealth = observerRegistry.getHealth();

    // Alerts (last 24h)
    const alertSummary = readAlertSummary(24 * 60 * 60 * 1000);

    return {
      timestamp,
      system: {
        phase: systemState.current_phase,
        bridgeActive: !!bridge,
        bridgeMode: bridgeConfig?.mode ?? 'SHADOW',
        killSwitchActive: killSwitch.global_kill,
        killSwitchReason: killSwitch.reason || undefined,
        exchange: bridgeConfig?.exchange ?? 'binance',
        testnet: bridgeConfig?.testnet ?? true
      },
      health: {
        healthy: healthStatus?.healthy ?? false,
        pingMs: healthStatus?.pingMs ?? null,
        driftMs: healthStatus?.serverTimeDriftMs ?? null,
        consecutiveFailures: healthStatus?.consecutiveFailures ?? 0,
        lastCheckedAt: healthStatus?.checkedAt ?? null
      },
      execution: {
        ordersToday: bridgeStats?.ordersToday ?? 0,
        maxOrdersPerDay: bridgeStats?.maxOrdersPerDay ?? 100,
        notionalToday: bridgeStats?.notionalToday ?? 0,
        maxNotionalPerDay: bridgeStats?.maxNotionalPerDay ?? 100000,
        utilizationPct: bridgeStats
          ? Math.round((bridgeStats.ordersToday / bridgeStats.maxOrdersPerDay) * 100)
          : 0
      },
      positions: {
        healthy: bridgeStats?.lastReconciliationHealthy ?? true,
        totalMismatches: 0,
        worstMismatchPct: 0,
        lastReconciliationAt: null
      },
      performance: {
        avgSlippageBps: slippageStats.avgSlippageBps,
        weightedSlippageBps: slippageStats.weightedSlippageBps,
        totalNotional: slippageStats.totalNotional,
        fillCount: slippageStats.totalRecords
      },
      liveRuns: {
        activeCount: observerHealth.active_runs,
        lastEventAgeMs: observerHealth.last_event_age_ms,
        budgetPressure: observerHealth.budget_pressure
      },
      alerts: alertSummary
    };
  });

  /**
   * GET /v1/monitor/execution - Execution stats and daily limits
   */
  fastify.get('/v1/monitor/execution', async (request, reply) => {
    const { bridge } = getBridgeSingletons();
    const killSwitch = loadKillSwitchFromEnv();

    if (!bridge) {
      return {
        bridgeActive: false,
        message: 'Bridge not running'
      };
    }

    const stats = bridge.getStats();
    const config = bridge.getConfig();

    return {
      bridgeActive: true,
      mode: config.mode,
      exchange: config.exchange,
      testnet: config.testnet,
      limits: {
        ordersToday: stats.ordersToday,
        maxOrdersPerDay: stats.maxOrdersPerDay,
        orderUtilizationPct: Math.round((stats.ordersToday / stats.maxOrdersPerDay) * 100),
        notionalToday: stats.notionalToday,
        maxNotionalPerDay: stats.maxNotionalPerDay,
        notionalUtilizationPct: Math.round((stats.notionalToday / stats.maxNotionalPerDay) * 100)
      },
      killSwitch: {
        globalActive: killSwitch.global_kill,
        symbolKills: Object.keys(killSwitch.symbol_kill).filter(s => killSwitch.symbol_kill[s]),
        reason: killSwitch.reason
      },
      config: {
        allowedSymbols: config.allowedSymbols,
        reduceOnly: config.reduceOnly ?? false,
        reconciliationIntervalMs: config.reconciliationIntervalMs
      }
    };
  });

  /**
   * GET /v1/monitor/positions - Paper vs exchange positions
   */
  fastify.get('/v1/monitor/positions', async (request, reply) => {
    const { bridge, adapter } = getBridgeSingletons();

    if (!bridge || !adapter) {
      return {
        reconciliationEnabled: false,
        lastReport: null,
        details: null,
        message: 'Bridge not running'
      };
    }

    // Try to get last reconciliation report from bridge
    const stats = bridge.getStats();

    return {
      reconciliationEnabled: true,
      lastReport: {
        timestamp: Date.now(),
        isHealthy: stats.lastReconciliationHealthy ?? true,
        matches: 0,
        mismatches: 0,
        orphanedExchange: 0,
        orphanedPaper: 0,
        worstMismatchPct: 0
      },
      details: null
    };
  });

  /**
   * GET /v1/monitor/health - Exchange connectivity status
   */
  fastify.get('/v1/monitor/health', async (request, reply) => {
    const { healthMonitor, bridge } = getBridgeSingletons();

    if (!healthMonitor) {
      return {
        monitored: false,
        exchange: null,
        testnet: null,
        status: null,
        thresholds: null,
        message: 'Health monitor not running'
      };
    }

    const status = healthMonitor.getLastStatus();
    const config = bridge?.getConfig();

    return {
      monitored: true,
      exchange: config?.exchange ?? 'unknown',
      testnet: config?.testnet ?? true,
      status: status ? {
        healthy: status.healthy,
        pingMs: status.pingMs,
        serverTimeDriftMs: status.serverTimeDriftMs,
        consecutiveFailures: status.consecutiveFailures,
        checkedAt: status.checkedAt,
        error: status.error
      } : null,
      thresholds: {
        maxDriftMs: 5000,
        alertAfterFailures: 3,
        pingIntervalMs: 30000
      }
    };
  });

  /**
   * GET /v1/monitor/performance - Slippage analytics
   */
  fastify.get('/v1/monitor/performance', async (request, reply) => {
    const { slippageAnalyzer } = getBridgeSingletons();
    const { symbol, limit } = request.query;

    if (!slippageAnalyzer) {
      return {
        tracked: false,
        aggregate: null,
        bySymbol: [],
        recentRecords: [],
        message: 'Slippage analyzer not running'
      };
    }

    const aggregate = slippageAnalyzer.getAggregateStats();
    const bySymbol = slippageAnalyzer.getAllStats();
    let recent = slippageAnalyzer.getRecentRecords(parseInt(limit) || 50);

    if (symbol) {
      recent = recent.filter(r => r.symbol === symbol.toUpperCase());
    }

    return {
      tracked: true,
      aggregate,
      bySymbol,
      recentRecords: recent
    };
  });

  /**
   * GET /v1/monitor/orders - Recent orders and state distribution
   */
  fastify.get('/v1/monitor/orders', async (request, reply) => {
    const { lifecycleManager, bridge } = getBridgeSingletons();
    const { state, symbol, limit } = request.query;

    if (!lifecycleManager) {
      return {
        bridgeActive: false,
        stateCounts: {},
        totalOrders: 0,
        activeOrders: 0,
        orders: [],
        message: 'Bridge not initialized'
      };
    }

    let orders = lifecycleManager.getAll();
    const stateCounts = lifecycleManager.getStateCounts();

    // Filter by state
    if (state) {
      orders = orders.filter(o => o.state === state);
    }

    // Filter by symbol
    if (symbol) {
      orders = orders.filter(o => o.symbol === symbol.toUpperCase());
    }

    // Sort by createdAt descending
    orders.sort((a, b) => b.createdAt - a.createdAt);

    // Apply limit
    const limitNum = Math.min(parseInt(limit) || 50, 200);
    orders = orders.slice(0, limitNum);

    // Count active (non-terminal) orders
    const terminalStates = ['FILLED', 'REJECTED', 'FAILED', 'CANCELLED'];
    const activeOrders = lifecycleManager.getAll().filter(o => !terminalStates.includes(o.state)).length;

    return {
      bridgeActive: !!bridge,
      stateCounts,
      totalOrders: lifecycleManager.getAll().length,
      activeOrders,
      orders: orders.map(o => ({
        bridgeId: o.bridgeId,
        state: o.state,
        symbol: o.symbol,
        side: o.side,
        requestedQty: o.requestedQty,
        filledQty: o.filledQty,
        avgFillPrice: o.avgFillPrice,
        createdAt: o.createdAt,
        updatedAt: o.updatedAt,
        error: o.error
      }))
    };
  });

  /**
   * GET /v1/monitor/alerts - Recent system alerts
   */
  fastify.get('/v1/monitor/alerts', async (request, reply) => {
    const { hours, severity, type, limit } = request.query;
    const logPath = process.env.ALERT_LOG_PATH || 'logs/alerts.jsonl';

    const lookbackHours = Math.min(parseInt(hours) || 24, 168); // Max 7 days
    const lookbackMs = lookbackHours * 60 * 60 * 1000;

    const alerts = readAlertDetails(lookbackMs, {
      severity,
      type,
      limit: parseInt(limit) || 100
    });

    // Build summary
    const summary = {
      total: alerts.length,
      bySeverity: {},
      byType: {}
    };

    for (const alert of alerts) {
      summary.bySeverity[alert.severity] = (summary.bySeverity[alert.severity] || 0) + 1;
      summary.byType[alert.type] = (summary.byType[alert.type] || 0) + 1;
    }

    return {
      logPath,
      lookbackHours,
      summary,
      alerts
    };
  });

  /**
   * GET /v1/monitor/costs - GPU cost summary
   */
  fastify.get('/v1/monitor/costs', async (request, reply) => {
    const { period = '7d' } = request.query;

    const costWriter = getCostWriter();
    let accountBalance = null;

    // Get local summary (fast)
    const summary = await costWriter.getLocalSummary(period);

    // Try to get account balance
    try {
      const vast = createVastClient();
      const account = await vast.getAccountInfo();
      accountBalance = account.balance ?? account.credit ?? null;
    } catch {
      // VAST_API_KEY may not be set or API error
    }

    // Budget check (from env)
    const budgetLimit = parseFloat(process.env.GPU_BUDGET_MONTHLY) || 100;
    const budgetStatus = summary.totalCost >= budgetLimit ? 'exceeded'
      : summary.totalCost >= budgetLimit * 0.9 ? 'critical'
      : summary.totalCost >= budgetLimit * 0.75 ? 'warning'
      : 'ok';

    return {
      period,
      generatedAt: new Date().toISOString(),
      summary: {
        totalJobs: summary.totalJobs,
        totalCost: summary.totalCost,
        avgCostPerJob: summary.avgCostPerJob,
        totalRuntimeMs: summary.totalRuntimeMs
      },
      bySymbol: summary.bySymbol,
      account: {
        balance: accountBalance,
        budgetLimit,
        budgetUsedPct: Math.round((summary.totalCost / budgetLimit) * 100),
        budgetStatus
      }
    };
  });

  /**
   * GET /v1/monitor/summary - Unified observability dashboard
   * Single endpoint that aggregates all system status for dashboards
   */
  fastify.get('/v1/monitor/summary', async (request, reply) => {
    const timestamp = new Date().toISOString();
    const { bridge, healthMonitor, slippageAnalyzer, lifecycleManager } = getBridgeSingletons();

    // === 1. System State ===
    const systemState = readSystemState();
    const killSwitch = loadKillSwitchFromEnv();
    const bridgeConfig = bridge?.getConfig();
    const bridgeStats = bridge?.getStats();

    // === 2. Services Health ===
    const services = [];

    // Strategyd (self)
    services.push({
      name: 'strategyd',
      status: 'healthy',
      url: `http://localhost:${process.env.STRATEGYD_PORT || 3031}`,
      lastCheck: timestamp
    });

    // Check replayd
    try {
      const replaydUrl = process.env.REPLAYD_URL || 'http://localhost:3030';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000);
      const resp = await fetch(`${replaydUrl}/health`, { signal: controller.signal });
      clearTimeout(timeoutId);
      services.push({
        name: 'replayd',
        status: resp.ok ? 'healthy' : 'degraded',
        url: replaydUrl,
        lastCheck: timestamp
      });
    } catch {
      services.push({
        name: 'replayd',
        status: 'unreachable',
        url: process.env.REPLAYD_URL || 'http://localhost:3030',
        lastCheck: timestamp
      });
    }

    // Check observer-api
    try {
      const observerUrl = process.env.OBSERVER_API_URL || 'http://localhost:3000';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000);
      const resp = await fetch(`${observerUrl}/ping`, { signal: controller.signal });
      clearTimeout(timeoutId);
      services.push({
        name: 'observer-api',
        status: resp.ok ? 'healthy' : 'degraded',
        url: observerUrl,
        lastCheck: timestamp
      });
    } catch {
      services.push({
        name: 'observer-api',
        status: 'unreachable',
        url: process.env.OBSERVER_API_URL || 'http://localhost:3000',
        lastCheck: timestamp
      });
    }

    // === 3. Exchange Health ===
    const exchangeHealth = healthMonitor?.getLastStatus();
    const exchange = {
      name: bridgeConfig?.exchange ?? 'unknown',
      testnet: bridgeConfig?.testnet ?? true,
      healthy: exchangeHealth?.healthy ?? false,
      pingMs: exchangeHealth?.pingMs ?? null,
      driftMs: exchangeHealth?.serverTimeDriftMs ?? null,
      consecutiveFailures: exchangeHealth?.consecutiveFailures ?? 0
    };

    // === 4. SLO Status ===
    let sloSummary = { healthy: true, ok: 0, warning: 0, breached: 0, critical: [] };
    try {
      const sloCalc = new SLOCalculator({
        getBridgeMetrics: () => {
          if (!bridge) return { active: false, killSwitchActive: killSwitch.global_kill };
          const stats = bridge.getStats();
          const slippage = slippageAnalyzer?.getAggregateStats() ?? {};
          return {
            active: true,
            killSwitchActive: killSwitch.global_kill,
            ordersToday: stats.ordersToday || 0,
            maxOrdersPerDay: stats.maxOrdersPerDay || 100,
            slippageAvgBps: slippage.avgSlippageBps || 0
          };
        },
        getExchangeMetrics: () => ({
          healthy: exchangeHealth?.healthy ?? false,
          pingMs: exchangeHealth?.pingMs ?? 0,
          driftMs: exchangeHealth?.serverTimeDriftMs ?? 0
        }),
        getObserverMetrics: () => {
          const health = observerRegistry.getHealth();
          return {
            activeRuns: health.active_runs || 0,
            lastEventAgeMs: health.last_event_age_ms || 0
          };
        },
        getAlertMetrics: () => {
          const summary = readAlertSummary(24 * 60 * 60 * 1000);
          return { criticalCount: summary.criticalCount || 0 };
        }
      });
      const health = sloCalc.getOverallHealth();
      sloSummary.healthy = health.healthy;
      sloSummary.ok = health.ok;
      sloSummary.warning = health.warning;
      sloSummary.breached = health.breached;
      sloSummary.critical = health.criticalSLOs?.map(s => s.slo_id) || [];
    } catch {
      // SLO calculation failed
    }

    // === 5. GPU Costs (24h + 7d) ===
    let gpuCosts = { cost24h: 0, cost7d: 0, jobs24h: 0, jobs7d: 0, budgetStatus: 'ok' };
    try {
      const costWriter = getCostWriter();
      const summary24h = await costWriter.getLocalSummary('24h');
      const summary7d = await costWriter.getLocalSummary('7d');
      const budgetLimit = parseFloat(process.env.GPU_BUDGET_MONTHLY) || 100;

      gpuCosts = {
        cost24h: summary24h.totalCost,
        cost7d: summary7d.totalCost,
        jobs24h: summary24h.totalJobs,
        jobs7d: summary7d.totalJobs,
        budgetUsedPct: Math.round((summary7d.totalCost / budgetLimit) * 100),
        budgetStatus: summary7d.totalCost >= budgetLimit ? 'exceeded'
          : summary7d.totalCost >= budgetLimit * 0.9 ? 'critical'
          : summary7d.totalCost >= budgetLimit * 0.75 ? 'warning'
          : 'ok'
      };
    } catch {
      // GPU cost tracking not available
    }

    // === 6. Alerts (24h) ===
    const alertSummary = readAlertSummary(24 * 60 * 60 * 1000);

    // === 7. Execution Stats ===
    const execution = {
      bridgeActive: !!bridge,
      mode: bridgeConfig?.mode ?? 'SHADOW',
      killSwitchActive: killSwitch.global_kill,
      ordersToday: bridgeStats?.ordersToday ?? 0,
      maxOrdersPerDay: bridgeStats?.maxOrdersPerDay ?? 100,
      utilizationPct: bridgeStats
        ? Math.round((bridgeStats.ordersToday / bridgeStats.maxOrdersPerDay) * 100)
        : 0
    };

    // === 8. Slippage ===
    const slippage = slippageAnalyzer?.getAggregateStats() ?? {
      avgSlippageBps: 0,
      weightedSlippageBps: 0,
      totalRecords: 0
    };

    // === 9. Live Runs ===
    const observerHealth = observerRegistry.getHealth();
    const liveRuns = {
      activeCount: observerHealth.active_runs,
      lastEventAgeMs: observerHealth.last_event_age_ms,
      budgetPressure: observerHealth.budget_pressure
    };

    // === 10. Order States ===
    const orderStates = lifecycleManager?.getStateCounts() ?? {};

    // === Overall Status ===
    const healthyServices = services.filter(s => s.status === 'healthy').length;
    const overallHealthy =
      healthyServices === services.length &&
      !killSwitch.global_kill &&
      sloSummary.healthy &&
      alertSummary.criticalCount === 0 &&
      exchange.healthy;

    return {
      timestamp,
      overall: {
        healthy: overallHealthy,
        status: overallHealthy ? 'OK' : killSwitch.global_kill ? 'KILLED' : 'DEGRADED',
        phase: systemState.current_phase,
        bridgeMode: execution.mode
      },
      services: {
        total: services.length,
        healthy: healthyServices,
        list: services
      },
      exchange,
      slo: sloSummary,
      execution,
      slippage: {
        avgBps: slippage.avgSlippageBps,
        weightedBps: slippage.weightedSlippageBps,
        fillCount: slippage.totalRecords
      },
      gpuCosts,
      alerts: {
        total24h: alertSummary.recentCount,
        critical24h: alertSummary.criticalCount,
        lastAlertAt: alertSummary.lastAlertAt
      },
      liveRuns,
      orderStates
    };
  });

}
