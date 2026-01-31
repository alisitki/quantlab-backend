#!/usr/bin/env node
/**
 * Metrics Module Test Suite
 * 
 * PHASE 5: Metrics & Observability — Verification
 * 
 * Tests:
 * 1. MetricsRegistry counter/gauge operations
 * 2. Histogram statistics
 * 3. Prometheus format output
 * 4. Snapshot and reset
 */

import assert from 'node:assert';
import { MetricsRegistry } from './MetricsRegistry.js';
import { getMetricNames, getMetricDef, MetricType } from './RuntimeMetrics.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// RuntimeMetrics Tests
// ============================================================================

test('getMetricNames returns all metric names', () => {
  const names = getMetricNames();
  
  assert.ok(Array.isArray(names));
  assert.ok(names.length > 0);
  assert.ok(names.includes('strategy_events_total'));
  assert.ok(names.includes('strategy_equity'));
});

test('getMetricDef returns definition by name', () => {
  const def = getMetricDef('strategy_events_total');
  
  assert.ok(def);
  assert.strictEqual(def.type, MetricType.COUNTER);
  assert.ok(def.description);
});

test('getMetricDef returns undefined for unknown', () => {
  const def = getMetricDef('unknown_metric');
  assert.strictEqual(def, undefined);
});

// ============================================================================
// MetricsRegistry Counter Tests
// ============================================================================

test('MetricsRegistry increments counters', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.increment('events_total');
  registry.increment('events_total');
  registry.increment('events_total', 5);
  
  assert.strictEqual(registry.getCounter('events_total'), 7);
});

test('MetricsRegistry initializes standard counters to 0', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  assert.strictEqual(registry.getCounter('events_total'), 0);
  assert.strictEqual(registry.getCounter('fills_total'), 0);
  assert.strictEqual(registry.getCounter('errors_total'), 0);
});

test('MetricsRegistry handles custom counters', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.increment('custom_counter');
  registry.increment('custom_counter', 10);
  
  assert.strictEqual(registry.getCounter('custom_counter'), 11);
});

// ============================================================================
// MetricsRegistry Gauge Tests
// ============================================================================

test('MetricsRegistry sets gauges', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.set('equity', 10500.25);
  
  assert.strictEqual(registry.getGauge('equity'), 10500.25);
});

test('MetricsRegistry overwrites gauge values', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.set('equity', 100);
  registry.set('equity', 200);
  registry.set('equity', 150);
  
  assert.strictEqual(registry.getGauge('equity'), 150);
});

// ============================================================================
// MetricsRegistry Histogram Tests
// ============================================================================

test('MetricsRegistry records histogram observations', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.observe('latency', 10);
  registry.observe('latency', 20);
  registry.observe('latency', 30);
  
  const stats = registry.getHistogramStats('latency');
  
  assert.strictEqual(stats.count, 3);
  assert.strictEqual(stats.sum, 60);
  assert.strictEqual(stats.min, 10);
  assert.strictEqual(stats.max, 30);
  assert.strictEqual(stats.avg, 20);
});

test('MetricsRegistry returns null for empty histogram', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  const stats = registry.getHistogramStats('nonexistent');
  assert.strictEqual(stats, null);
});

test('MetricsRegistry calculates percentiles', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  // Add 100 values from 1 to 100
  for (let i = 1; i <= 100; i++) {
    registry.observe('latency', i);
  }
  
  const stats = registry.getHistogramStats('latency');
  
  // Percentiles may vary slightly based on indexing method
  assert.ok(stats.p50 >= 49 && stats.p50 <= 51, `p50 expected ~50, got ${stats.p50}`);
  assert.ok(stats.p95 >= 94 && stats.p95 <= 96, `p95 expected ~95, got ${stats.p95}`);
  assert.ok(stats.p99 >= 98 && stats.p99 <= 100, `p99 expected ~99, got ${stats.p99}`);
});

// ============================================================================
// MetricsRegistry Snapshot Tests
// ============================================================================

test('MetricsRegistry snapshot returns all metrics', () => {
  const registry = new MetricsRegistry({ runId: 'test_run' });
  
  registry.increment('events_total', 100);
  registry.set('equity', 1234.56);
  registry.observe('latency', 5);
  
  const snapshot = registry.snapshot();
  
  assert.strictEqual(snapshot.runId, 'test_run');
  assert.strictEqual(snapshot.counters.events_total, 100);
  assert.strictEqual(snapshot.gauges.equity, 1234.56);
  assert.ok(snapshot.histograms.latency);
  assert.ok(snapshot.uptimeMs >= 0);
});

// ============================================================================
// MetricsRegistry Prometheus Output Tests
// ============================================================================

test('MetricsRegistry renders Prometheus format', () => {
  const registry = new MetricsRegistry({ runId: 'prom_test' });
  
  registry.increment('events_total', 500);
  registry.set('equity', 9999);
  
  const output = registry.render();
  
  assert.ok(output.includes('strategy_events_total{run_id="prom_test"} 500'));
  assert.ok(output.includes('strategy_equity{run_id="prom_test"} 9999'));
  assert.ok(output.includes('# TYPE strategy_events_total counter'));
  assert.ok(output.includes('# TYPE strategy_equity gauge'));
});

test('MetricsRegistry renders histogram summaries', () => {
  const registry = new MetricsRegistry({ runId: 'hist_test' });
  
  for (let i = 1; i <= 100; i++) {
    registry.observe('latency', i);
  }
  
  const output = registry.render();
  
  assert.ok(output.includes('strategy_latency_count'));
  assert.ok(output.includes('strategy_latency_sum'));
  assert.ok(output.includes('quantile="0.5"'));
  assert.ok(output.includes('quantile="0.95"'));
});

// ============================================================================
// MetricsRegistry Reset Tests
// ============================================================================

test('MetricsRegistry reset clears all metrics', () => {
  const registry = new MetricsRegistry({ runId: 'test' });
  
  registry.increment('events_total', 100);
  registry.set('equity', 5000);
  registry.observe('latency', 10);
  
  registry.reset();
  
  assert.strictEqual(registry.getCounter('events_total'), 0);
  assert.strictEqual(registry.getGauge('equity'), 0);
  assert.strictEqual(registry.getHistogramStats('latency'), null);
});

// ============================================================================
// MetricsRegistry Merge Tests
// ============================================================================

test('MetricsRegistry merge combines metrics', () => {
  const registry1 = new MetricsRegistry({ runId: 'run1' });
  const registry2 = new MetricsRegistry({ runId: 'run2' });
  
  registry1.increment('events_total', 100);
  registry2.increment('events_total', 200);
  registry2.set('equity', 5000);
  
  registry1.merge(registry2);
  
  assert.strictEqual(registry1.getCounter('events_total'), 300);
  assert.strictEqual(registry1.getGauge('equity'), 5000);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== Metrics Module Test Suite ===\n');
  
  let passed = 0;
  let failed = 0;
  
  for (const { name, fn } of tests) {
    try {
      await fn();
      console.log(`✓ ${name}`);
      passed++;
    } catch (err) {
      console.error(`✗ ${name}`);
      console.error(`  ${err.message}`);
      failed++;
    }
  }
  
  console.log(`\n--- Results ---`);
  console.log(`Passed: ${passed}/${tests.length}`);
  console.log(`Failed: ${failed}/${tests.length}`);
  
  if (failed > 0) {
    console.log('\nRESULT: FAIL');
    process.exit(1);
  } else {
    console.log('\nRESULT: PASS');
    process.exit(0);
  }
}

run();
