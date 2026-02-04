/**
 * Strategyd Metrics Route
 * GET /metrics - Prometheus format metrics
 *
 * Includes:
 * - Runner metrics (events, equity, signals)
 * - Bridge execution metrics (orders, notional, mode)
 * - Exchange health (ping, drift, failures)
 * - Slippage analytics
 * - Order state distribution
 * - Kill switch status
 * - Live run metrics
 * - Alert summary (24h)
 */

import { getBridgeSingletons } from './bridge.routes.js';
import { readAlertSummary } from './monitor.routes.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';
import { loadKillSwitchFromEnv } from '../../../core/futures/kill_switch.js';
import { SLOCalculator, SLO_DEFINITIONS } from '../../../core/slo/index.js';

/**
 * Render bridge and system metrics in Prometheus format
 */
function renderBridgeMetrics() {
  const { bridge, healthMonitor, slippageAnalyzer, lifecycleManager } = getBridgeSingletons();
  const killSwitch = loadKillSwitchFromEnv();
  const observerHealth = observerRegistry.getHealth();
  const alertSummary = readAlertSummary(24 * 60 * 60 * 1000); // 24h

  const lines = [];

  // Bridge Active
  lines.push('# HELP strategyd_bridge_active Whether the execution bridge is running');
  lines.push('# TYPE strategyd_bridge_active gauge');
  lines.push(`strategyd_bridge_active ${bridge ? 1 : 0}`);
  lines.push('');

  if (bridge) {
    const stats = bridge.getStats();
    const config = bridge.getConfig();

    // Bridge Mode
    lines.push('# HELP strategyd_bridge_mode Current bridge execution mode');
    lines.push('# TYPE strategyd_bridge_mode gauge');
    lines.push(`strategyd_bridge_mode{mode="SHADOW"} ${config.mode === 'SHADOW' ? 1 : 0}`);
    lines.push(`strategyd_bridge_mode{mode="CANARY"} ${config.mode === 'CANARY' ? 1 : 0}`);
    lines.push(`strategyd_bridge_mode{mode="LIVE"} ${config.mode === 'LIVE' ? 1 : 0}`);
    lines.push('');

    // Orders Today
    lines.push('# HELP strategyd_bridge_orders_today Number of orders placed today');
    lines.push('# TYPE strategyd_bridge_orders_today gauge');
    lines.push(`strategyd_bridge_orders_today ${stats.ordersToday || 0}`);
    lines.push('');

    // Notional Today
    lines.push('# HELP strategyd_bridge_notional_today Total notional value traded today');
    lines.push('# TYPE strategyd_bridge_notional_today gauge');
    lines.push(`strategyd_bridge_notional_today ${stats.notionalToday || 0}`);
    lines.push('');

    // Max limits
    lines.push('# HELP strategyd_bridge_max_orders_per_day Maximum orders allowed per day');
    lines.push('# TYPE strategyd_bridge_max_orders_per_day gauge');
    lines.push(`strategyd_bridge_max_orders_per_day ${stats.maxOrdersPerDay || 100}`);
    lines.push('');

    lines.push('# HELP strategyd_bridge_max_notional_per_day Maximum notional allowed per day');
    lines.push('# TYPE strategyd_bridge_max_notional_per_day gauge');
    lines.push(`strategyd_bridge_max_notional_per_day ${stats.maxNotionalPerDay || 100000}`);
    lines.push('');

    // Testnet flag
    lines.push('# HELP strategyd_bridge_testnet Whether bridge is in testnet mode');
    lines.push('# TYPE strategyd_bridge_testnet gauge');
    lines.push(`strategyd_bridge_testnet ${config.testnet ? 1 : 0}`);
    lines.push('');
  }

  // Exchange Health
  if (healthMonitor) {
    const status = healthMonitor.getLastStatus();

    lines.push('# HELP strategyd_exchange_healthy Whether exchange is healthy');
    lines.push('# TYPE strategyd_exchange_healthy gauge');
    lines.push(`strategyd_exchange_healthy ${status?.healthy ? 1 : 0}`);
    lines.push('');

    lines.push('# HELP strategyd_exchange_ping_ms Exchange ping latency in milliseconds');
    lines.push('# TYPE strategyd_exchange_ping_ms gauge');
    lines.push(`strategyd_exchange_ping_ms ${status?.pingMs || 0}`);
    lines.push('');

    lines.push('# HELP strategyd_exchange_drift_ms Server time drift in milliseconds');
    lines.push('# TYPE strategyd_exchange_drift_ms gauge');
    lines.push(`strategyd_exchange_drift_ms ${status?.serverTimeDriftMs || 0}`);
    lines.push('');

    lines.push('# HELP strategyd_exchange_failures_consecutive Consecutive health check failures');
    lines.push('# TYPE strategyd_exchange_failures_consecutive gauge');
    lines.push(`strategyd_exchange_failures_consecutive ${status?.consecutiveFailures || 0}`);
    lines.push('');
  }

  // Slippage
  if (slippageAnalyzer) {
    const slippage = slippageAnalyzer.getAggregateStats();

    lines.push('# HELP strategyd_slippage_avg_bps Average slippage in basis points');
    lines.push('# TYPE strategyd_slippage_avg_bps gauge');
    lines.push(`strategyd_slippage_avg_bps ${slippage.avgSlippageBps || 0}`);
    lines.push('');

    lines.push('# HELP strategyd_slippage_weighted_bps Notional-weighted slippage in basis points');
    lines.push('# TYPE strategyd_slippage_weighted_bps gauge');
    lines.push(`strategyd_slippage_weighted_bps ${slippage.weightedSlippageBps || 0}`);
    lines.push('');

    lines.push('# HELP strategyd_slippage_total_notional Total notional tracked for slippage');
    lines.push('# TYPE strategyd_slippage_total_notional counter');
    lines.push(`strategyd_slippage_total_notional ${slippage.totalNotional || 0}`);
    lines.push('');

    lines.push('# HELP strategyd_slippage_fill_count Total fills tracked');
    lines.push('# TYPE strategyd_slippage_fill_count counter');
    lines.push(`strategyd_slippage_fill_count ${slippage.totalRecords || 0}`);
    lines.push('');
  }

  // Order States
  if (lifecycleManager) {
    const stateCounts = lifecycleManager.getStateCounts();

    lines.push('# HELP strategyd_orders_total Orders by state');
    lines.push('# TYPE strategyd_orders_total gauge');
    for (const [state, count] of Object.entries(stateCounts)) {
      lines.push(`strategyd_orders_total{state="${state}"} ${count}`);
    }
    lines.push('');
  }

  // Kill Switch
  lines.push('# HELP strategyd_kill_switch_active Whether global kill switch is active');
  lines.push('# TYPE strategyd_kill_switch_active gauge');
  lines.push(`strategyd_kill_switch_active ${killSwitch.global_kill ? 1 : 0}`);
  lines.push('');

  // Symbol kills count
  const symbolKillCount = Object.values(killSwitch.symbol_kill || {}).filter(v => v).length;
  lines.push('# HELP strategyd_symbol_kills_active Number of symbols with kill switch active');
  lines.push('# TYPE strategyd_symbol_kills_active gauge');
  lines.push(`strategyd_symbol_kills_active ${symbolKillCount}`);
  lines.push('');

  // Live Runs
  lines.push('# HELP strategyd_live_runs_active Number of active live strategy runs');
  lines.push('# TYPE strategyd_live_runs_active gauge');
  lines.push(`strategyd_live_runs_active ${observerHealth.active_runs || 0}`);
  lines.push('');

  lines.push('# HELP strategyd_live_run_last_event_age_ms Age of last event in milliseconds');
  lines.push('# TYPE strategyd_live_run_last_event_age_ms gauge');
  lines.push(`strategyd_live_run_last_event_age_ms ${observerHealth.last_event_age_ms || 0}`);
  lines.push('');

  lines.push('# HELP strategyd_live_run_budget_pressure Budget pressure ratio (0-1)');
  lines.push('# TYPE strategyd_live_run_budget_pressure gauge');
  lines.push(`strategyd_live_run_budget_pressure ${observerHealth.budget_pressure || 0}`);
  lines.push('');

  // Alerts (24h)
  lines.push('# HELP strategyd_alerts_24h_total Alerts in the last 24 hours');
  lines.push('# TYPE strategyd_alerts_24h_total gauge');
  lines.push(`strategyd_alerts_24h_total ${alertSummary.recentCount || 0}`);
  lines.push('');

  lines.push('# HELP strategyd_alerts_24h_critical Critical alerts in the last 24 hours');
  lines.push('# TYPE strategyd_alerts_24h_critical gauge');
  lines.push(`strategyd_alerts_24h_critical ${alertSummary.criticalCount || 0}`);

  return lines.join('\n');
}

/**
 * Create metrics provider for SLO calculator
 */
function createSLOMetricsProvider() {
  return {
    getBridgeMetrics() {
      const { bridge, slippageAnalyzer } = getBridgeSingletons();
      const killSwitch = loadKillSwitchFromEnv();
      if (!bridge) {
        return { active: false, killSwitchActive: killSwitch.global_kill };
      }
      const stats = bridge.getStats();
      const slippage = slippageAnalyzer?.getAggregateStats() ?? {};
      return {
        active: true,
        killSwitchActive: killSwitch.global_kill,
        ordersToday: stats.ordersToday || 0,
        maxOrdersPerDay: stats.maxOrdersPerDay || 100,
        notionalToday: stats.notionalToday || 0,
        maxNotionalPerDay: stats.maxNotionalPerDay || 100000,
        slippageAvgBps: slippage.avgSlippageBps || 0,
        slippageWeightedBps: slippage.weightedSlippageBps || 0
      };
    },
    getExchangeMetrics() {
      const { healthMonitor } = getBridgeSingletons();
      const status = healthMonitor?.getLastStatus();
      return {
        healthy: status?.healthy ?? false,
        pingMs: status?.pingMs ?? 0,
        driftMs: status?.serverTimeDriftMs ?? 0
      };
    },
    getObserverMetrics() {
      const health = observerRegistry.getHealth();
      return {
        activeRuns: health.active_runs || 0,
        lastEventAgeMs: health.last_event_age_ms || 0,
        budgetPressure: health.budget_pressure || 0
      };
    },
    getAlertMetrics() {
      const summary = readAlertSummary(24 * 60 * 60 * 1000);
      return {
        recentCount: summary.recentCount || 0,
        criticalCount: summary.criticalCount || 0
      };
    }
  };
}

// Singleton SLO calculator
let sloCalculator = null;
function getSLOCalculator() {
  if (!sloCalculator) {
    sloCalculator = new SLOCalculator(createSLOMetricsProvider());
  }
  return sloCalculator;
}

/**
 * Render SLO metrics in Prometheus format
 */
function renderSLOMetrics() {
  const calc = getSLOCalculator();
  const statuses = calc.evaluateAll();
  const lines = [];

  // SLO Status (1=OK, 0.5=WARNING, 0=BREACHED)
  lines.push('# HELP strategyd_slo_status SLO status (1=OK, 0.5=WARNING, 0=BREACHED)');
  lines.push('# TYPE strategyd_slo_status gauge');
  for (const s of statuses) {
    const value = s.status === 'OK' ? 1 : s.status === 'WARNING' ? 0.5 : 0;
    lines.push(`strategyd_slo_status{slo="${s.slo_id}",tier="${s.tier}"} ${value}`);
  }
  lines.push('');

  // SLO Target
  lines.push('# HELP strategyd_slo_target SLO target value');
  lines.push('# TYPE strategyd_slo_target gauge');
  for (const s of statuses) {
    lines.push(`strategyd_slo_target{slo="${s.slo_id}"} ${s.target}`);
  }
  lines.push('');

  // SLO Current Value
  lines.push('# HELP strategyd_slo_current Current SLO metric value');
  lines.push('# TYPE strategyd_slo_current gauge');
  for (const s of statuses) {
    if (s.current_value !== null) {
      lines.push(`strategyd_slo_current{slo="${s.slo_id}"} ${s.current_value}`);
    }
  }
  lines.push('');

  // Error Budget (for availability SLOs)
  lines.push('# HELP strategyd_slo_error_budget_remaining Remaining error budget (ratio)');
  lines.push('# TYPE strategyd_slo_error_budget_remaining gauge');
  for (const s of statuses) {
    if (SLO_DEFINITIONS[s.slo_id]?.unit === 'ratio') {
      lines.push(`strategyd_slo_error_budget_remaining{slo="${s.slo_id}"} ${s.error_budget_remaining}`);
    }
  }
  lines.push('');

  // Error Budget Consumed Percentage
  lines.push('# HELP strategyd_slo_error_budget_consumed_pct Error budget consumed percentage');
  lines.push('# TYPE strategyd_slo_error_budget_consumed_pct gauge');
  for (const s of statuses) {
    if (SLO_DEFINITIONS[s.slo_id]?.unit === 'ratio') {
      lines.push(`strategyd_slo_error_budget_consumed_pct{slo="${s.slo_id}"} ${s.error_budget_consumed_pct}`);
    }
  }
  lines.push('');

  // Overall Health
  const health = calc.getOverallHealth();
  lines.push('# HELP strategyd_slo_overall_healthy Overall system SLO health (1=healthy, 0=degraded)');
  lines.push('# TYPE strategyd_slo_overall_healthy gauge');
  lines.push(`strategyd_slo_overall_healthy ${health.healthy ? 1 : 0}`);
  lines.push('');

  lines.push('# HELP strategyd_slo_count_by_status Count of SLOs by status');
  lines.push('# TYPE strategyd_slo_count_by_status gauge');
  lines.push(`strategyd_slo_count_by_status{status="ok"} ${health.ok}`);
  lines.push(`strategyd_slo_count_by_status{status="warning"} ${health.warning}`);
  lines.push(`strategyd_slo_count_by_status{status="breached"} ${health.breached}`);

  return lines.join('\n');
}

export default async function metricsRoutes(fastify, options) {
  const { runner } = options;

  fastify.get('/metrics', async (request, reply) => {
    reply.type('text/plain; version=0.0.4; charset=utf-8');

    // Runner metrics (events, equity, signals, etc.)
    const runnerMetrics = runner.renderMetrics();

    // Bridge and system metrics
    const bridgeMetrics = renderBridgeMetrics();

    // SLO metrics
    const sloMetrics = renderSLOMetrics();

    return runnerMetrics + '\n\n' + bridgeMetrics + '\n\n' + sloMetrics;
  });
}
