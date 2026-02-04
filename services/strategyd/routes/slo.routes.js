/**
 * SLO Routes for strategyd
 *
 * GET /v1/slo/status           - All SLO statuses
 * GET /v1/slo/status/:sloId    - Specific SLO detail
 * GET /v1/slo/budget           - Error budgets summary
 * GET /v1/slo/compliance       - Compliance report
 * GET /v1/slo/health           - Overall system health
 */

import { SLOCalculator, SLO_DEFINITIONS, getAllSLOIds } from '../../../core/slo/index.js';
import { getBridgeSingletons } from './bridge.routes.js';
import { readAlertSummary } from './monitor.routes.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';
import { loadKillSwitchFromEnv } from '../../../core/futures/kill_switch.js';

// Singleton calculator instance
let calculator = null;

/**
 * Create metrics provider that fetches current system metrics
 */
function createMetricsProvider() {
  return {
    getBridgeMetrics() {
      const { bridge, slippageAnalyzer } = getBridgeSingletons();
      const killSwitch = loadKillSwitchFromEnv();

      if (!bridge) {
        return {
          active: false,
          killSwitchActive: killSwitch.global_kill,
          ordersToday: 0,
          maxOrdersPerDay: 100,
          notionalToday: 0,
          maxNotionalPerDay: 100000,
          slippageAvgBps: 0,
          slippageWeightedBps: 0
        };
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
        driftMs: status?.serverTimeDriftMs ?? 0,
        consecutiveFailures: status?.consecutiveFailures ?? 0
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
      const summary = readAlertSummary(24 * 60 * 60 * 1000); // 24h

      return {
        recentCount: summary.recentCount || 0,
        criticalCount: summary.criticalCount || 0,
        lastAlertAt: summary.lastAlertAt
      };
    }
  };
}

/**
 * Get or create calculator singleton
 */
function getCalculator() {
  if (!calculator) {
    calculator = new SLOCalculator(createMetricsProvider());
  }
  return calculator;
}

export default async function sloRoutes(fastify, options) {

  /**
   * GET /v1/slo/status - All SLO statuses
   */
  fastify.get('/v1/slo/status', async (request, reply) => {
    const calc = getCalculator();
    const { tier, status } = request.query;

    let statuses = calc.evaluateAll();

    // Filter by tier
    if (tier) {
      const tierNum = parseInt(tier);
      if ([1, 2, 3].includes(tierNum)) {
        statuses = statuses.filter(s => s.tier === tierNum);
      }
    }

    // Filter by status
    if (status) {
      const statusUpper = status.toUpperCase();
      statuses = statuses.filter(s => s.status === statusUpper);
    }

    return {
      count: statuses.length,
      evaluated_at: Date.now(),
      slos: statuses
    };
  });

  /**
   * GET /v1/slo/status/:sloId - Specific SLO detail
   */
  fastify.get('/v1/slo/status/:sloId', async (request, reply) => {
    const { sloId } = request.params;
    const calc = getCalculator();

    const definition = SLO_DEFINITIONS[sloId];
    if (!definition) {
      return reply.code(404).send({
        error: 'SLO_NOT_FOUND',
        message: `SLO '${sloId}' not found`,
        available: getAllSLOIds()
      });
    }

    const status = calc.evaluate(sloId);

    return {
      ...status,
      definition: {
        description: definition.description,
        target: definition.target,
        unit: definition.unit,
        window: definition.window,
        tier: definition.tier,
        warningThreshold: definition.warningThreshold,
        comparison: definition.comparison
      }
    };
  });

  /**
   * GET /v1/slo/budget - Error budgets summary
   */
  fastify.get('/v1/slo/budget', async (request, reply) => {
    const calc = getCalculator();
    const statuses = calc.evaluateAll();

    // Only availability SLOs have meaningful error budgets
    const budgets = statuses
      .filter(s => SLO_DEFINITIONS[s.slo_id]?.unit === 'ratio' &&
                   SLO_DEFINITIONS[s.slo_id]?.comparison === 'gte')
      .map(s => ({
        slo_id: s.slo_id,
        name: s.name,
        tier: s.tier,
        target: s.target,
        current_value: s.current_value,
        error_budget_remaining: s.error_budget_remaining,
        error_budget_consumed_pct: s.error_budget_consumed_pct,
        status: s.status
      }));

    // Sort by consumed percentage (highest first)
    budgets.sort((a, b) => b.error_budget_consumed_pct - a.error_budget_consumed_pct);

    return {
      count: budgets.length,
      evaluated_at: Date.now(),
      summary: {
        total_budgets: budgets.length,
        critical_consumed: budgets.filter(b => b.error_budget_consumed_pct >= 80).length,
        healthy: budgets.filter(b => b.error_budget_consumed_pct < 50).length
      },
      budgets
    };
  });

  /**
   * GET /v1/slo/compliance - Compliance report
   */
  fastify.get('/v1/slo/compliance', async (request, reply) => {
    const calc = getCalculator();
    const { window } = request.query;

    // Note: Real compliance would track historical data
    // This provides current snapshot as approximation
    const summary = calc.getComplianceSummary(window || '30d');
    const health = calc.getOverallHealth();

    return {
      window: summary.window,
      evaluated_at: summary.evaluated_at,
      overall_compliance: {
        ...summary.overall,
        healthy: health.healthy
      },
      by_tier: summary.by_tier,
      breakdown: {
        ok: health.ok,
        warning: health.warning,
        breached: health.breached,
        unknown: health.unknown
      }
    };
  });

  /**
   * GET /v1/slo/health - Overall system health
   */
  fastify.get('/v1/slo/health', async (request, reply) => {
    const calc = getCalculator();
    const health = calc.getOverallHealth();
    const statuses = calc.evaluateAll();

    // Get most critical issues
    const criticalIssues = statuses
      .filter(s => s.status === 'BREACHED' && s.tier === 1)
      .map(s => ({ slo_id: s.slo_id, name: s.name, current_value: s.current_value, target: s.target }));

    const warnings = statuses
      .filter(s => s.status === 'WARNING' || (s.status === 'BREACHED' && s.tier > 1))
      .map(s => ({ slo_id: s.slo_id, name: s.name, status: s.status }));

    return {
      healthy: health.healthy,
      status: health.breached > 0 ? 'DEGRADED' : health.warning > 0 ? 'WARNING' : 'HEALTHY',
      summary: {
        total: health.total,
        ok: health.ok,
        warning: health.warning,
        breached: health.breached
      },
      critical_issues: criticalIssues,
      warnings: warnings.slice(0, 5),
      evaluated_at: Date.now()
    };
  });

  /**
   * GET /v1/slo/definitions - List all SLO definitions
   */
  fastify.get('/v1/slo/definitions', async (request, reply) => {
    const { tier } = request.query;

    let definitions = Object.entries(SLO_DEFINITIONS).map(([id, def]) => ({
      slo_id: id,
      name: def.name,
      description: def.description,
      target: def.target,
      unit: def.unit,
      tier: def.tier,
      window: def.window
    }));

    if (tier) {
      const tierNum = parseInt(tier);
      definitions = definitions.filter(d => d.tier === tierNum);
    }

    // Sort by tier
    definitions.sort((a, b) => a.tier - b.tier);

    return {
      count: definitions.length,
      definitions
    };
  });

}
