/**
 * QuantLab Strategy Runtime — Metrics Registry
 * 
 * PHASE 5: Metrics & Observability
 * 
 * Per-run metrics collection with Prometheus-compatible output.
 * Metrics are observation-only and do NOT affect determinism.
 * 
 * @module core/strategy/metrics/MetricsRegistry
 */

import { MetricType, RuntimeMetricDefs } from './RuntimeMetrics.js';
import { canonicalClone } from '../state/StateSerializer.js';

/**
 * Per-run metrics registry.
 * Collects counters and gauges for observability.
 */
export class MetricsRegistry {
  /** @type {string} */
  #runId;
  
  /** @type {Map<string, number>} */
  #counters;
  
  /** @type {Map<string, number>} */
  #gauges;
  
  /** @type {Map<string, Array<number>>} */
  #histograms;
  
  /** @type {number} */
  #createdAt;
  
  /**
   * Create a metrics registry.
   * 
   * @param {Object} [options] - Configuration options
   * @param {string} [options.runId] - Run identifier for labels
   */
  constructor({ runId = 'unknown' } = {}) {
    this.#runId = runId;
    this.#counters = new Map();
    this.#gauges = new Map();
    this.#histograms = new Map();
    this.#createdAt = Date.now();
    
    // Initialize standard metrics
    this.#initializeMetrics();
  }
  
  /**
   * Initialize standard metrics with zero values.
   */
  #initializeMetrics() {
    // Initialize counters
    this.#counters.set('events_total', 0);
    this.#counters.set('signals_total', 0);
    this.#counters.set('fills_total', 0);
    this.#counters.set('errors_total', 0);
    this.#counters.set('skipped_events_total', 0);
    this.#counters.set('checkpoints_total', 0);
    
    // Initialize gauges
    this.#gauges.set('equity', 0);
    this.#gauges.set('position_value', 0);
    this.#gauges.set('unrealized_pnl', 0);
    this.#gauges.set('realized_pnl', 0);
    this.#gauges.set('queue_size', 0);
    this.#gauges.set('last_event_ts', 0);
  }
  
  /**
   * Increment a counter.
   * 
   * @param {string} name - Counter name
   * @param {number} [value=1] - Value to add
   */
  increment(name, value = 1) {
    const current = this.#counters.get(name) ?? 0;
    this.#counters.set(name, current + value);
  }
  
  /**
   * Set a gauge value.
   * 
   * @param {string} name - Gauge name
   * @param {number} value - Value to set
   */
  set(name, value) {
    this.#gauges.set(name, value);
  }
  
  /**
   * Record a histogram observation.
   * 
   * @param {string} name - Histogram name
   * @param {number} value - Value to observe
   */
  observe(name, value) {
    if (!this.#histograms.has(name)) {
      this.#histograms.set(name, []);
    }
    this.#histograms.get(name).push(value);
  }
  
  /**
   * Get a counter value.
   * 
   * @param {string} name - Counter name
   * @returns {number} Counter value
   */
  getCounter(name) {
    return this.#counters.get(name) ?? 0;
  }
  
  /**
   * Get a gauge value.
   * 
   * @param {string} name - Gauge name
   * @returns {number} Gauge value
   */
  getGauge(name) {
    return this.#gauges.get(name) ?? 0;
  }
  
  /**
   * Get histogram statistics.
   * 
   * @param {string} name - Histogram name
   * @returns {Object|null} Histogram stats
   */
  getHistogramStats(name) {
    const values = this.#histograms.get(name);
    if (!values || values.length === 0) return null;
    
    const sorted = [...values].sort((a, b) => a - b);
    const sum = values.reduce((a, b) => a + b, 0);
    
    return {
      count: values.length,
      sum,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: sum / values.length,
      p50: sorted[Math.floor(sorted.length * 0.5)],
      p95: sorted[Math.floor(sorted.length * 0.95)],
      p99: sorted[Math.floor(sorted.length * 0.99)]
    };
  }
  
  /**
   * Get all metrics as a snapshot.
   * 
   * @returns {Object} Metrics snapshot
   */
  snapshot() {
    const counters = {};
    for (const [key, value] of this.#counters) {
      counters[key] = value;
    }
    
    const gauges = {};
    for (const [key, value] of this.#gauges) {
      gauges[key] = value;
    }
    
    const histograms = {};
    for (const [key] of this.#histograms) {
      histograms[key] = this.getHistogramStats(key);
    }
    
    return {
      runId: this.#runId,
      counters,
      gauges,
      histograms,
      uptimeMs: Date.now() - this.#createdAt
    };
  }
  
  /**
   * Render metrics in Prometheus format.
   * 
   * @returns {string} Prometheus-formatted metrics
   */
  render() {
    const lines = [];
    const labels = `run_id="${this.#runId}"`;
    
    // Counters
    for (const [name, value] of this.#counters) {
      lines.push(`# TYPE strategy_${name} counter`);
      lines.push(`strategy_${name}{${labels}} ${value}`);
    }
    
    // Gauges
    for (const [name, value] of this.#gauges) {
      lines.push(`# TYPE strategy_${name} gauge`);
      lines.push(`strategy_${name}{${labels}} ${value}`);
    }
    
    // Histograms (summary style)
    for (const [name] of this.#histograms) {
      const stats = this.getHistogramStats(name);
      if (stats) {
        lines.push(`# TYPE strategy_${name} summary`);
        lines.push(`strategy_${name}_count{${labels}} ${stats.count}`);
        lines.push(`strategy_${name}_sum{${labels}} ${stats.sum}`);
        lines.push(`strategy_${name}{${labels},quantile="0.5"} ${stats.p50}`);
        lines.push(`strategy_${name}{${labels},quantile="0.95"} ${stats.p95}`);
        lines.push(`strategy_${name}{${labels},quantile="0.99"} ${stats.p99}`);
      }
    }
    
    return lines.join('\n');
  }
  
  /**
   * Reset all metrics to initial values.
   */
  reset() {
    this.#counters.clear();
    this.#gauges.clear();
    this.#histograms.clear();
    this.#initializeMetrics();
  }
  
  /**
   * Merge metrics from another registry.
   * Useful for aggregating metrics from multiple runs.
   * 
   * @param {MetricsRegistry} other - Other registry to merge from
   */
  merge(other) {
    const otherSnapshot = other.snapshot();
    
    // Add counters
    for (const [key, value] of Object.entries(otherSnapshot.counters)) {
      this.increment(key, value);
    }
    
    // For gauges, take the latest value
    for (const [key, value] of Object.entries(otherSnapshot.gauges)) {
      this.set(key, value);
    }
  }
}

/**
 * Create a metrics registry.
 * 
 * @param {Object} [options] - Configuration options
 * @returns {MetricsRegistry} Metrics registry
 */
export function createMetricsRegistry(options) {
  return new MetricsRegistry(options);
}

export default MetricsRegistry;
