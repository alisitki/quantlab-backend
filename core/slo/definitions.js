/**
 * SLO Definitions
 *
 * Defines Service Level Objectives for the QuantLab system.
 * Each SLO has a target, metric source, evaluation window, and tier.
 *
 * Tiers:
 * - Tier 1: Critical (99.9% target) - System must be operational
 * - Tier 2: High (99.5% target) - Performance degradation unacceptable
 * - Tier 3: Standard (99% target) - Best effort, monitored
 */

/**
 * @typedef {Object} SLODefinition
 * @property {string} name - Human readable name
 * @property {string} description - What this SLO measures
 * @property {number} target - Target value (ratio for availability, absolute for latency/slippage)
 * @property {string} unit - Unit of measurement (ratio, ms, bps, percent)
 * @property {string} metricSource - How to fetch current value ('bridge', 'exchange', 'observer', 'alerts')
 * @property {string} metricKey - Key to extract from source
 * @property {string} window - Evaluation window (24h, 7d, 30d)
 * @property {number} tier - Priority tier (1=critical, 2=high, 3=standard)
 * @property {number} warningThreshold - Threshold for WARNING status (as ratio of target)
 * @property {string} comparison - How to compare ('gte' for availability, 'lte' for latency/errors)
 */

export const SLO_DEFINITIONS = {
  // ============================================================================
  // TIER 1 - CRITICAL (99.9% availability targets)
  // ============================================================================

  exchange_availability: {
    name: 'Exchange Availability',
    description: 'Exchange API must be reachable and responsive',
    target: 0.999, // 99.9%
    unit: 'ratio',
    metricSource: 'exchange',
    metricKey: 'healthy',
    window: '30d',
    tier: 1,
    warningThreshold: 0.9995, // Warn at 99.95%
    comparison: 'gte'
  },

  bridge_availability: {
    name: 'Bridge Availability',
    description: 'Execution bridge must be running when expected',
    target: 0.999, // 99.9%
    unit: 'ratio',
    metricSource: 'bridge',
    metricKey: 'active',
    window: '30d',
    tier: 1,
    warningThreshold: 0.9995,
    comparison: 'gte'
  },

  kill_switch_inactive: {
    name: 'Kill Switch Inactive',
    description: 'Global kill switch should not be triggered',
    target: 1.0, // Never triggered
    unit: 'ratio',
    metricSource: 'bridge',
    metricKey: 'killSwitchInactive',
    window: '30d',
    tier: 1,
    warningThreshold: 1.0, // Any activation is warning
    comparison: 'gte'
  },

  // ============================================================================
  // TIER 2 - HIGH (Performance targets)
  // ============================================================================

  exchange_latency_p99: {
    name: 'Exchange Latency P99',
    description: 'Exchange API response time (99th percentile)',
    target: 500, // 500ms
    unit: 'ms',
    metricSource: 'exchange',
    metricKey: 'pingMs',
    window: '24h',
    tier: 2,
    warningThreshold: 250, // Warn at 250ms
    comparison: 'lte'
  },

  exchange_time_drift: {
    name: 'Exchange Time Drift',
    description: 'Server time synchronization drift',
    target: 1000, // 1000ms max drift
    unit: 'ms',
    metricSource: 'exchange',
    metricKey: 'driftMs',
    window: '24h',
    tier: 2,
    warningThreshold: 500,
    comparison: 'lte'
  },

  slippage_average: {
    name: 'Slippage Average',
    description: 'Average execution slippage',
    target: 50, // 50 bps (0.5%)
    unit: 'bps',
    metricSource: 'bridge',
    metricKey: 'slippageAvgBps',
    window: '24h',
    tier: 2,
    warningThreshold: 30, // Warn at 30 bps
    comparison: 'lte'
  },

  slippage_weighted: {
    name: 'Slippage Weighted',
    description: 'Notional-weighted execution slippage',
    target: 100, // 100 bps (1%)
    unit: 'bps',
    metricSource: 'bridge',
    metricKey: 'slippageWeightedBps',
    window: '24h',
    tier: 2,
    warningThreshold: 75,
    comparison: 'lte'
  },

  // ============================================================================
  // TIER 3 - STANDARD (Operational targets)
  // ============================================================================

  daily_order_utilization: {
    name: 'Daily Order Utilization',
    description: 'Daily order limit should not be exhausted',
    target: 0.80, // Max 80% utilization
    unit: 'ratio',
    metricSource: 'bridge',
    metricKey: 'orderUtilization',
    window: '24h',
    tier: 3,
    warningThreshold: 0.70, // Warn at 70%
    comparison: 'lte'
  },

  daily_notional_utilization: {
    name: 'Daily Notional Utilization',
    description: 'Daily notional limit should not be exhausted',
    target: 0.80, // Max 80% utilization
    unit: 'ratio',
    metricSource: 'bridge',
    metricKey: 'notionalUtilization',
    window: '24h',
    tier: 3,
    warningThreshold: 0.70,
    comparison: 'lte'
  },

  alerts_critical_24h: {
    name: 'Critical Alerts (24h)',
    description: 'Critical alerts in the last 24 hours',
    target: 0, // Zero critical alerts
    unit: 'count',
    metricSource: 'alerts',
    metricKey: 'criticalCount24h',
    window: '24h',
    tier: 3,
    warningThreshold: 1, // Warn at 1 alert
    comparison: 'lte'
  },

  live_runs_healthy: {
    name: 'Live Runs Healthy',
    description: 'Active live runs should be receiving events',
    target: 60000, // Max 60s since last event
    unit: 'ms',
    metricSource: 'observer',
    metricKey: 'lastEventAgeMs',
    window: '1h',
    tier: 3,
    warningThreshold: 30000, // Warn at 30s
    comparison: 'lte'
  }
};

/**
 * Get SLO definitions by tier
 * @param {number} tier - Tier number (1, 2, or 3)
 * @returns {Object} SLO definitions for that tier
 */
export function getSLOsByTier(tier) {
  return Object.fromEntries(
    Object.entries(SLO_DEFINITIONS).filter(([_, def]) => def.tier === tier)
  );
}

/**
 * Get all SLO IDs
 * @returns {string[]} Array of SLO IDs
 */
export function getAllSLOIds() {
  return Object.keys(SLO_DEFINITIONS);
}

/**
 * Get a specific SLO definition
 * @param {string} sloId - SLO ID
 * @returns {SLODefinition|null} SLO definition or null
 */
export function getSLODefinition(sloId) {
  return SLO_DEFINITIONS[sloId] || null;
}
