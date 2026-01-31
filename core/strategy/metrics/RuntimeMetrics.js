/**
 * QuantLab Strategy Runtime — Runtime Metrics
 * 
 * PHASE 5: Metrics & Observability
 * 
 * Defines metric types and constants for runtime observability.
 * Metrics are observation-only and do NOT affect determinism.
 * 
 * @module core/strategy/metrics/RuntimeMetrics
 */

/**
 * Metric type enum
 * @readonly
 * @enum {string}
 */
export const MetricType = Object.freeze({
  COUNTER: 'counter',
  GAUGE: 'gauge',
  HISTOGRAM: 'histogram'
});

/**
 * Core runtime metric definitions.
 * Each metric has a name, type, description, and optional labels.
 */
export const RuntimeMetricDefs = Object.freeze({
  // Counters
  EVENTS_TOTAL: {
    name: 'strategy_events_total',
    type: MetricType.COUNTER,
    description: 'Total number of events processed',
    labels: ['run_id']
  },
  
  SIGNALS_TOTAL: {
    name: 'strategy_signals_total',
    type: MetricType.COUNTER,
    description: 'Total number of signals generated',
    labels: ['run_id', 'signal_type']
  },
  
  FILLS_TOTAL: {
    name: 'strategy_fills_total',
    type: MetricType.COUNTER,
    description: 'Total number of fills executed',
    labels: ['run_id', 'side']
  },
  
  ERRORS_TOTAL: {
    name: 'strategy_errors_total',
    type: MetricType.COUNTER,
    description: 'Total number of errors encountered',
    labels: ['run_id', 'error_type']
  },
  
  SKIPPED_EVENTS_TOTAL: {
    name: 'strategy_skipped_events_total',
    type: MetricType.COUNTER,
    description: 'Total number of events skipped due to errors',
    labels: ['run_id']
  },
  
  CHECKPOINTS_TOTAL: {
    name: 'strategy_checkpoints_total',
    type: MetricType.COUNTER,
    description: 'Total number of checkpoints saved',
    labels: ['run_id']
  },
  
  // Gauges
  EQUITY: {
    name: 'strategy_equity',
    type: MetricType.GAUGE,
    description: 'Current equity value',
    labels: ['run_id']
  },
  
  POSITION_VALUE: {
    name: 'strategy_position_value',
    type: MetricType.GAUGE,
    description: 'Current position value',
    labels: ['run_id']
  },
  
  UNREALIZED_PNL: {
    name: 'strategy_unrealized_pnl',
    type: MetricType.GAUGE,
    description: 'Current unrealized PnL',
    labels: ['run_id']
  },
  
  REALIZED_PNL: {
    name: 'strategy_realized_pnl',
    type: MetricType.GAUGE,
    description: 'Total realized PnL',
    labels: ['run_id']
  },
  
  QUEUE_SIZE: {
    name: 'strategy_queue_size',
    type: MetricType.GAUGE,
    description: 'Current event queue size',
    labels: ['run_id']
  },
  
  LAST_EVENT_TS: {
    name: 'strategy_last_event_ts',
    type: MetricType.GAUGE,
    description: 'Timestamp of last processed event (epoch ms)',
    labels: ['run_id']
  },
  
  // Histograms (optional, for latency tracking)
  EVENT_LATENCY_MS: {
    name: 'strategy_event_latency_ms',
    type: MetricType.HISTOGRAM,
    description: 'Event processing latency in milliseconds',
    labels: ['run_id'],
    buckets: [0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000]
  }
});

/**
 * Get all metric names as an array.
 * 
 * @returns {string[]} Array of metric names
 */
export function getMetricNames() {
  return Object.values(RuntimeMetricDefs).map(m => m.name);
}

/**
 * Get metric definition by name.
 * 
 * @param {string} name - Metric name
 * @returns {Object|undefined} Metric definition
 */
export function getMetricDef(name) {
  return Object.values(RuntimeMetricDefs).find(m => m.name === name);
}

export default RuntimeMetricDefs;
