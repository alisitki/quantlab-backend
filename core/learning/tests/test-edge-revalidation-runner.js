/**
 * Edge Revalidation Runner Tests
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeRevalidationRunner } from '../EdgeRevalidationRunner.js';
import { EdgeRegistry } from '../../edge/EdgeRegistry.js';
import { Edge } from '../../edge/Edge.js';

// Mock validation pipeline
class MockValidationPipeline {
  constructor() {
    this.revalidateCalls = [];
  }

  async revalidate(edge, dataset) {
    this.revalidateCalls.push({ edgeId: edge.id, dataset });

    // Simulate validation result
    return {
      newStatus: 'VALIDATED',
      score: 0.75,
      validationResults: {}
    };
  }
}

describe('EdgeRevalidationRunner', () => {
  let registry;
  let validationPipeline;
  let runner;
  let edge1, edge2, edge3;

  // Create synthetic dataset
  const createDataset = (size = 500) => {
    return {
      rows: Array(size).fill(null).map((_, i) => ({
        timestamp: Date.now() + i * 1000,
        features: { test: Math.random() }
      }))
    };
  };

  beforeEach(() => {
    registry = new EdgeRegistry();
    validationPipeline = new MockValidationPipeline();

    edge1 = new Edge({
      id: 'test_edge_1',
      name: 'Test Edge 1',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge2 = new Edge({
      id: 'test_edge_2',
      name: 'Test Edge 2',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge3 = new Edge({
      id: 'test_edge_3',
      name: 'Test Edge 3',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'CANDIDATE'
    });

    registry.register(edge1);
    registry.register(edge2);
    registry.register(edge3);

    runner = new EdgeRevalidationRunner({
      edgeRegistry: registry,
      validationPipeline
    });
  });

  it('should process single alert and trigger revalidation', async () => {
    const dataset = createDataset();

    const alerts = [
      [{
        type: 'CONFIDENCE_DROP',
        edgeId: 'test_edge_1',
        drop: 0.2
      }]
    ];

    const results = await runner.processAlerts(alerts, dataset);

    assert.equal(results.length, 1);
    assert.equal(results[0].edgeId, 'test_edge_1');
    assert.equal(results[0].newStatus, 'VALIDATED');
    assert.equal(results[0].trigger, 'CONFIDENCE_DROP');
    assert.ok(results[0].revalidatedAt);

    // Check validation pipeline was called
    assert.equal(validationPipeline.revalidateCalls.length, 1);
    assert.equal(validationPipeline.revalidateCalls[0].edgeId, 'test_edge_1');
  });

  it('should process multiple alerts', async () => {
    const dataset = createDataset();

    const alerts = [
      [{
        type: 'CONFIDENCE_DROP',
        edgeId: 'test_edge_1',
        drop: 0.2
      }],
      [{
        type: 'CONSECUTIVE_LOSSES',
        edgeId: 'test_edge_2',
        count: 10
      }]
    ];

    const results = await runner.processAlerts(alerts, dataset);

    assert.equal(results.length, 2);
    assert.equal(results[0].edgeId, 'test_edge_1');
    assert.equal(results[1].edgeId, 'test_edge_2');

    assert.equal(validationPipeline.revalidateCalls.length, 2);
  });

  it('should enforce cooldown period', async () => {
    const dataset = createDataset();

    const alert = [{
      type: 'CONFIDENCE_DROP',
      edgeId: 'test_edge_1',
      drop: 0.2
    }];

    // First revalidation
    const results1 = await runner.processAlerts([alert], dataset);
    assert.equal(results1.length, 1);
    assert.equal(results1[0].edgeId, 'test_edge_1');
    assert.notEqual(results1[0].status, 'SKIPPED');

    // Second revalidation (should be skipped due to cooldown)
    const results2 = await runner.processAlerts([alert], dataset);
    assert.equal(results2.length, 1);
    assert.equal(results2[0].status, 'SKIPPED');
    assert.ok(results2[0].reason.includes('COOLDOWN'));
  });

  it('should allow revalidation after cooldown', async () => {
    const dataset = createDataset();

    const alert = [{
      type: 'CONFIDENCE_DROP',
      edgeId: 'test_edge_1',
      drop: 0.2
    }];

    // First revalidation
    await runner.processAlerts([alert], dataset);

    // Should be skipped immediately
    let results = await runner.processAlerts([alert], dataset);
    assert.equal(results[0].status, 'SKIPPED');

    // Manually clear cooldown (simulating time passing)
    runner.clearCooldown('test_edge_1');

    // Second revalidation (should succeed)
    results = await runner.processAlerts([alert], dataset);
    assert.equal(results.length, 1);
    assert.notEqual(results[0].status, 'SKIPPED');
  });

  it('should enforce max concurrent revalidations', async () => {
    const dataset = createDataset();

    // Override config for low concurrency
    runner.config.maxConcurrent = 1;

    // Create slow validation pipeline
    const slowPipeline = {
      async revalidate(edge, dataset) {
        await new Promise(resolve => setTimeout(resolve, 100));
        return { newStatus: 'VALIDATED', score: 0.75 };
      }
    };

    runner.validationPipeline = slowPipeline;

    const alerts = [
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }],
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_2' }]
    ];

    // Start both in parallel
    const resultsPromise = runner.processAlerts(alerts, dataset);

    // Check running count during execution
    await new Promise(resolve => setTimeout(resolve, 50));
    const summary = runner.getSummary();
    assert.ok(summary.currentlyRunning <= 1);

    await resultsPromise;
  });

  it('should skip if edge not found', async () => {
    const dataset = createDataset();

    const alert = [{
      type: 'CONFIDENCE_DROP',
      edgeId: 'nonexistent_edge'
    }];

    const results = await runner.processAlerts([alert], dataset);

    assert.equal(results.length, 1);
    assert.equal(results[0].status, 'NOT_FOUND');
  });

  it('should skip if dataset too small', async () => {
    const smallDataset = createDataset(100);  // < 500 minDataRows

    const alert = [{
      type: 'CONFIDENCE_DROP',
      edgeId: 'test_edge_1'
    }];

    const results = await runner.processAlerts([alert], smallDataset);

    assert.equal(results.length, 0);
  });

  it('should revalidate all VALIDATED edges', async () => {
    const dataset = createDataset();

    const results = await runner.revalidateAll(dataset);

    // Only edge1 and edge2 are VALIDATED (edge3 is CANDIDATE)
    assert.equal(results.length, 2);

    const edgeIds = results.map(r => r.edgeId).sort();
    assert.deepEqual(edgeIds, ['test_edge_1', 'test_edge_2']);

    results.forEach(result => {
      assert.equal(result.trigger, 'SCHEDULED');
    });
  });

  it('should track revalidation history', async () => {
    const dataset = createDataset();

    const alert1 = [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }];
    const alert2 = [{ type: 'CONSECUTIVE_LOSSES', edgeId: 'test_edge_2' }];

    await runner.processAlerts([alert1], dataset);

    // Clear cooldown for second run
    runner.clearCooldown('test_edge_1');
    runner.clearCooldown('test_edge_2');

    await runner.processAlerts([alert2], dataset);

    const history = runner.getHistory();
    assert.equal(history.length, 2);
    assert.equal(history[0].edgeId, 'test_edge_1');
    assert.equal(history[1].edgeId, 'test_edge_2');
  });

  it('should filter history by edgeId', async () => {
    const dataset = createDataset();

    await runner.processAlerts([
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }]
    ], dataset);

    runner.clearCooldown('test_edge_2');

    await runner.processAlerts([
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_2' }]
    ], dataset);

    const history = runner.getHistory({ edgeId: 'test_edge_1' });
    assert.equal(history.length, 1);
    assert.equal(history[0].edgeId, 'test_edge_1');
  });

  it('should filter history by time', async () => {
    const dataset = createDataset();

    const beforeTime = new Date().toISOString();

    await runner.processAlerts([
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }]
    ], dataset);

    const history = runner.getHistory({ since: beforeTime });
    assert.equal(history.length, 1);
  });

  it('should limit history results', async () => {
    const dataset = createDataset();

    // Create 5 revalidations
    for (let i = 0; i < 5; i++) {
      const edgeId = i % 2 === 0 ? 'test_edge_1' : 'test_edge_2';
      runner.clearCooldown(edgeId);

      await runner.processAlerts([
        [{ type: 'CONFIDENCE_DROP', edgeId }]
      ], dataset);
    }

    const history = runner.getHistory({ limit: 2 });
    assert.equal(history.length, 2);
  });

  it('should provide summary', async () => {
    const dataset = createDataset();

    await runner.processAlerts([
      [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }]
    ], dataset);

    const summary = runner.getSummary();

    assert.equal(summary.totalRevalidations, 1);
    assert.equal(summary.currentlyRunning, 0);
    assert.equal(summary.trackedEdges, 1);
    assert.ok(Array.isArray(summary.recentRevalidations));
  });

  it('should handle validation errors', async () => {
    const dataset = createDataset();

    // Create failing validation pipeline
    const failingPipeline = {
      async revalidate() {
        throw new Error('Validation failed');
      }
    };

    runner.validationPipeline = failingPipeline;

    const alert = [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }];

    const results = await runner.processAlerts([alert], dataset);

    assert.equal(results.length, 1);
    assert.equal(results[0].status, 'ERROR');
    assert.ok(results[0].error);
  });

  it('should clear cooldown manually', async () => {
    const dataset = createDataset();

    const alert = [{ type: 'CONFIDENCE_DROP', edgeId: 'test_edge_1' }];

    // First revalidation
    await runner.processAlerts([alert], dataset);

    // Should be skipped due to cooldown
    let results = await runner.processAlerts([alert], dataset);
    assert.equal(results[0].status, 'SKIPPED');

    // Clear cooldown
    runner.clearCooldown('test_edge_1');

    // Should succeed now
    results = await runner.processAlerts([alert], dataset);
    assert.notEqual(results[0].status, 'SKIPPED');
  });
});
