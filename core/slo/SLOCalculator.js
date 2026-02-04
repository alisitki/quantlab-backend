/**
 * SLO Calculator
 *
 * Evaluates current metrics against SLO definitions.
 * Calculates status (OK/WARNING/BREACHED) and error budgets.
 */

import { SLO_DEFINITIONS, getSLODefinition, getAllSLOIds } from './definitions.js';

/**
 * @typedef {Object} SLOStatus
 * @property {string} slo_id - SLO identifier
 * @property {string} name - Human readable name
 * @property {number} current_value - Current metric value
 * @property {number} target - SLO target value
 * @property {string} unit - Unit of measurement
 * @property {string} status - 'OK' | 'WARNING' | 'BREACHED'
 * @property {number} error_budget_remaining - Remaining error budget (for availability SLOs)
 * @property {number} error_budget_consumed_pct - Percentage of error budget consumed
 * @property {number} tier - SLO tier
 * @property {string} window - Evaluation window
 * @property {number} evaluated_at - Evaluation timestamp
 */

/**
 * Status enum
 */
export const SLO_STATUS = {
  OK: 'OK',
  WARNING: 'WARNING',
  BREACHED: 'BREACHED',
  UNKNOWN: 'UNKNOWN'
};

export class SLOCalculator {
  #metricsProvider;

  /**
   * @param {Object} metricsProvider - Object with methods to fetch current metrics
   * @param {Function} metricsProvider.getBridgeMetrics - Returns bridge stats
   * @param {Function} metricsProvider.getExchangeMetrics - Returns exchange health
   * @param {Function} metricsProvider.getObserverMetrics - Returns observer health
   * @param {Function} metricsProvider.getAlertMetrics - Returns alert counts
   */
  constructor(metricsProvider) {
    this.#metricsProvider = metricsProvider;
  }

  /**
   * Fetch current value for a metric
   * @param {string} source - Metric source
   * @param {string} key - Metric key
   * @returns {number|null} Current value or null if unavailable
   */
  #fetchMetricValue(source, key) {
    try {
      switch (source) {
        case 'bridge': {
          const metrics = this.#metricsProvider.getBridgeMetrics();
          return this.#extractBridgeMetric(metrics, key);
        }
        case 'exchange': {
          const metrics = this.#metricsProvider.getExchangeMetrics();
          return this.#extractExchangeMetric(metrics, key);
        }
        case 'observer': {
          const metrics = this.#metricsProvider.getObserverMetrics();
          return this.#extractObserverMetric(metrics, key);
        }
        case 'alerts': {
          const metrics = this.#metricsProvider.getAlertMetrics();
          return this.#extractAlertMetric(metrics, key);
        }
        default:
          return null;
      }
    } catch {
      return null;
    }
  }

  #extractBridgeMetric(metrics, key) {
    if (!metrics) return null;
    switch (key) {
      case 'active':
        return metrics.active ? 1 : 0;
      case 'killSwitchInactive':
        return metrics.killSwitchActive ? 0 : 1;
      case 'slippageAvgBps':
        return metrics.slippageAvgBps ?? null;
      case 'slippageWeightedBps':
        return metrics.slippageWeightedBps ?? null;
      case 'orderUtilization':
        if (metrics.ordersToday != null && metrics.maxOrdersPerDay > 0) {
          return metrics.ordersToday / metrics.maxOrdersPerDay;
        }
        return null;
      case 'notionalUtilization':
        if (metrics.notionalToday != null && metrics.maxNotionalPerDay > 0) {
          return metrics.notionalToday / metrics.maxNotionalPerDay;
        }
        return null;
      default:
        return null;
    }
  }

  #extractExchangeMetric(metrics, key) {
    if (!metrics) return null;
    switch (key) {
      case 'healthy':
        return metrics.healthy ? 1 : 0;
      case 'pingMs':
        return metrics.pingMs ?? null;
      case 'driftMs':
        return Math.abs(metrics.driftMs ?? 0);
      default:
        return null;
    }
  }

  #extractObserverMetric(metrics, key) {
    if (!metrics) return null;
    switch (key) {
      case 'lastEventAgeMs':
        return metrics.lastEventAgeMs ?? null;
      case 'activeRuns':
        return metrics.activeRuns ?? 0;
      case 'budgetPressure':
        return metrics.budgetPressure ?? 0;
      default:
        return null;
    }
  }

  #extractAlertMetric(metrics, key) {
    if (!metrics) return null;
    switch (key) {
      case 'criticalCount24h':
        return metrics.criticalCount ?? 0;
      case 'totalCount24h':
        return metrics.recentCount ?? 0;
      default:
        return null;
    }
  }

  /**
   * Determine SLO status based on current value and definition
   * @param {number} currentValue - Current metric value
   * @param {Object} definition - SLO definition
   * @returns {string} Status ('OK', 'WARNING', 'BREACHED')
   */
  #determineStatus(currentValue, definition) {
    if (currentValue === null) return SLO_STATUS.UNKNOWN;

    const { target, warningThreshold, comparison } = definition;

    if (comparison === 'gte') {
      // For availability: higher is better
      if (currentValue >= target) return SLO_STATUS.OK;
      if (currentValue >= warningThreshold) return SLO_STATUS.WARNING;
      return SLO_STATUS.BREACHED;
    } else {
      // For latency/errors: lower is better
      if (currentValue <= target) return SLO_STATUS.OK;
      if (currentValue <= warningThreshold) return SLO_STATUS.WARNING;
      return SLO_STATUS.BREACHED;
    }
  }

  /**
   * Calculate error budget for availability SLOs
   * @param {number} currentValue - Current availability ratio
   * @param {number} target - Target availability ratio
   * @returns {{ remaining: number, consumedPct: number }} Error budget info
   */
  #calculateErrorBudget(currentValue, target) {
    if (currentValue === null || target >= 1) {
      return { remaining: 0, consumedPct: 100 };
    }

    // Error budget = 1 - target (e.g., 0.001 for 99.9% target)
    const totalBudget = 1 - target;
    if (totalBudget <= 0) {
      return { remaining: 0, consumedPct: 100 };
    }

    // Consumed = target - current (if current < target)
    const consumed = Math.max(0, target - currentValue);
    const remaining = Math.max(0, totalBudget - consumed);
    const consumedPct = Math.min(100, (consumed / totalBudget) * 100);

    return {
      remaining: Number(remaining.toFixed(6)),
      consumedPct: Number(consumedPct.toFixed(2))
    };
  }

  /**
   * Evaluate a single SLO
   * @param {string} sloId - SLO identifier
   * @returns {SLOStatus} SLO status object
   */
  evaluate(sloId) {
    const definition = getSLODefinition(sloId);
    if (!definition) {
      return {
        slo_id: sloId,
        name: 'Unknown',
        current_value: null,
        target: 0,
        unit: 'unknown',
        status: SLO_STATUS.UNKNOWN,
        error_budget_remaining: 0,
        error_budget_consumed_pct: 100,
        tier: 0,
        window: 'unknown',
        evaluated_at: Date.now()
      };
    }

    const currentValue = this.#fetchMetricValue(definition.metricSource, definition.metricKey);
    const status = this.#determineStatus(currentValue, definition);

    // Calculate error budget for ratio-based SLOs
    let errorBudget = { remaining: 0, consumedPct: 0 };
    if (definition.unit === 'ratio' && definition.comparison === 'gte') {
      errorBudget = this.#calculateErrorBudget(currentValue, definition.target);
    }

    return {
      slo_id: sloId,
      name: definition.name,
      description: definition.description,
      current_value: currentValue,
      target: definition.target,
      unit: definition.unit,
      status,
      error_budget_remaining: errorBudget.remaining,
      error_budget_consumed_pct: errorBudget.consumedPct,
      tier: definition.tier,
      window: definition.window,
      evaluated_at: Date.now()
    };
  }

  /**
   * Evaluate all SLOs
   * @returns {SLOStatus[]} Array of all SLO statuses
   */
  evaluateAll() {
    return getAllSLOIds().map(id => this.evaluate(id));
  }

  /**
   * Get SLOs by status
   * @param {string} status - Status to filter by
   * @returns {SLOStatus[]} Filtered SLO statuses
   */
  getByStatus(status) {
    return this.evaluateAll().filter(s => s.status === status);
  }

  /**
   * Get SLOs by tier
   * @param {number} tier - Tier to filter by
   * @returns {SLOStatus[]} Filtered SLO statuses
   */
  getByTier(tier) {
    return this.evaluateAll().filter(s => s.tier === tier);
  }

  /**
   * Get overall system health based on SLOs
   * @returns {{ healthy: boolean, breached: number, warning: number, ok: number, unknown: number }}
   */
  getOverallHealth() {
    const all = this.evaluateAll();
    const counts = {
      breached: 0,
      warning: 0,
      ok: 0,
      unknown: 0
    };

    for (const slo of all) {
      switch (slo.status) {
        case SLO_STATUS.BREACHED:
          counts.breached++;
          break;
        case SLO_STATUS.WARNING:
          counts.warning++;
          break;
        case SLO_STATUS.OK:
          counts.ok++;
          break;
        default:
          counts.unknown++;
      }
    }

    return {
      healthy: counts.breached === 0,
      ...counts,
      total: all.length
    };
  }

  /**
   * Get compliance summary for a time window
   * @param {string} window - Time window ('24h', '7d', '30d')
   * @returns {Object} Compliance summary
   */
  getComplianceSummary(window = '30d') {
    const all = this.evaluateAll();
    const byTier = {
      1: { total: 0, ok: 0, warning: 0, breached: 0 },
      2: { total: 0, ok: 0, warning: 0, breached: 0 },
      3: { total: 0, ok: 0, warning: 0, breached: 0 }
    };

    for (const slo of all) {
      const tier = byTier[slo.tier];
      if (!tier) continue;
      tier.total++;
      if (slo.status === SLO_STATUS.OK) tier.ok++;
      else if (slo.status === SLO_STATUS.WARNING) tier.warning++;
      else if (slo.status === SLO_STATUS.BREACHED) tier.breached++;
    }

    // Calculate compliance percentages
    for (const tier of Object.values(byTier)) {
      tier.compliance_pct = tier.total > 0
        ? Number(((tier.ok / tier.total) * 100).toFixed(1))
        : 100;
    }

    return {
      window,
      evaluated_at: Date.now(),
      overall: {
        total: all.length,
        ok: all.filter(s => s.status === SLO_STATUS.OK).length,
        compliance_pct: all.length > 0
          ? Number(((all.filter(s => s.status === SLO_STATUS.OK).length / all.length) * 100).toFixed(1))
          : 100
      },
      by_tier: byTier
    };
  }
}
