/**
 * Tests for EdgeSerializer
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { EdgeSerializer } from '../EdgeSerializer.js';
import { EdgeRegistry } from '../EdgeRegistry.js';
import { Edge } from '../Edge.js';

const TEST_FILE = '/tmp/test-edge-serializer.json';

describe('EdgeSerializer', () => {
  let serializer;

  beforeEach(() => {
    serializer = new EdgeSerializer();
  });

  afterEach(async () => {
    try {
      await fs.unlink(TEST_FILE);
    } catch (err) {
      // Ignore
    }
  });

  it('should serialize empty registry', () => {
    const registry = new EdgeRegistry();
    const serialized = serializer.serialize(registry);

    assert.equal(serialized.version, 1);
    assert.ok(serialized.timestamp);
    assert.ok(Array.isArray(serialized.edges));
    assert.equal(serialized.edges.length, 0);
  });

  it('should round-trip serialize and deserialize', () => {
    const registry = new EdgeRegistry();

    // Create test edge with definition
    const edge = new Edge({
      id: 'test_edge_1',
      name: 'Test Edge',
      entryCondition: (features) => ({ active: features.liquidity_pressure > 0.5, direction: 'LONG' }),
      exitCondition: () => ({ exit: false }),
      regimes: [0, 1],
      status: 'CANDIDATE',
      expectedAdvantage: { mean: 0.001, std: 0.003, sharpe: 1.2, winRate: 0.55 }
    });

    const definition = {
      pattern: {
        id: '1',
        type: 'threshold',
        conditions: [{ feature: 'liquidity_pressure', operator: '>', value: 0.5 }],
        regimes: [0, 1],
        direction: 'LONG',
        horizon: 10,
        support: 100,
        forwardReturns: { mean: 0.001, std: 0.003 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.75,
        tests: { sharpeTest: { sharpe: 1.2 } }
      }
    };

    registry.register(edge, definition);

    // Serialize
    const serialized = serializer.serialize(registry);

    // Deserialize
    const restoredRegistry = serializer.deserialize(serialized);

    // Verify
    assert.equal(restoredRegistry.size(), 1);

    const restoredEdge = restoredRegistry.get('discovered_threshold_1'); // EdgeCandidateGenerator creates new ID
    assert.ok(restoredEdge);
    assert.equal(restoredEdge.status, 'CANDIDATE');
    assert.ok(restoredEdge.entryCondition);
    assert.ok(restoredEdge.exitCondition);
  });

  it('should reconstruct working closures', () => {
    const registry = new EdgeRegistry();

    const edge = new Edge({
      id: 'test_edge_2',
      name: 'High Liquidity',
      entryCondition: (features) => ({ active: features.liquidity_pressure > 0.6, direction: 'LONG' }),
      exitCondition: () => ({ exit: false }),
      status: 'CANDIDATE'
    });

    const definition = {
      pattern: {
        id: '2',
        type: 'threshold',
        conditions: [{ feature: 'liquidity_pressure', operator: '>', value: 0.6 }],
        regimes: null,
        direction: 'LONG',
        horizon: 10,
        support: 50,
        forwardReturns: { mean: 0.002, std: 0.004 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.8,
        tests: { sharpeTest: { sharpe: 1.5 } }
      }
    };

    registry.register(edge, definition);

    // Round-trip
    const serialized = serializer.serialize(registry);
    const restored = serializer.deserialize(serialized);

    const restoredEdge = restored.get('discovered_threshold_2');

    // Test entry condition
    const entryResult = restoredEdge.entryCondition({ liquidity_pressure: 0.7 }, 0);
    assert.equal(entryResult.active, true);
    assert.equal(entryResult.direction, 'LONG');

    const entryResult2 = restoredEdge.entryCondition({ liquidity_pressure: 0.5 }, 0);
    assert.equal(entryResult2.active, false);

    // Test exit condition
    const exitResult = restoredEdge.exitCondition({ liquidity_pressure: 0.7 }, 0, Date.now());
    assert.equal(exitResult.exit, false);
  });

  it('should save and load from file', async () => {
    const registry = new EdgeRegistry();

    const edge = new Edge({
      id: 'test_edge_3',
      name: 'File Test',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    const definition = {
      pattern: {
        id: '3',
        type: 'cluster',
        conditions: [{ feature: 'cluster', operator: '==', value: 2 }],
        regimes: [0],
        direction: 'SHORT',
        horizon: 50,
        support: 80,
        forwardReturns: { mean: -0.001, std: 0.002 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.7,
        tests: { sharpeTest: { sharpe: 1.0 } }
      }
    };

    registry.register(edge, definition);

    // Save
    await serializer.saveToFile(TEST_FILE, registry);

    // Load
    const loadedRegistry = await serializer.loadFromFile(TEST_FILE);

    assert.equal(loadedRegistry.size(), 1);

    const loadedEdge = loadedRegistry.get('discovered_cluster_3');
    assert.ok(loadedEdge);
    assert.equal(loadedEdge.status, 'VALIDATED');
  });

  it('should handle missing definitions gracefully', () => {
    const serialized = {
      version: 1,
      timestamp: Date.now(),
      edges: [
        {
          id: 'bad_edge',
          name: 'No Definition',
          status: 'CANDIDATE',
          stats: {},
          confidence: {},
          // Missing definition
        }
      ],
      stats: {}
    };

    const restored = serializer.deserialize(serialized);

    // Should skip edge without definition
    assert.equal(restored.size(), 0);
  });

  it('should preserve edge status and stats', () => {
    const registry = new EdgeRegistry();

    const edge = new Edge({
      id: 'test_edge_4',
      name: 'Stats Test',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    // Modify stats
    edge.stats.trades = 50;
    edge.stats.wins = 30;
    edge.stats.losses = 20;
    edge.stats.avgReturn = 0.002;

    const definition = {
      pattern: {
        id: '4',
        type: 'quantile',
        conditions: [],
        regimes: null,
        direction: 'LONG',
        horizon: 10,
        support: 100,
        forwardReturns: { mean: 0.003, std: 0.005 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.85,
        tests: { sharpeTest: { sharpe: 1.8 } }
      }
    };

    registry.register(edge, definition);

    // Round-trip
    const serialized = serializer.serialize(registry);
    const restored = serializer.deserialize(serialized);

    const restoredEdge = restored.get('discovered_quantile_4');

    assert.equal(restoredEdge.status, 'VALIDATED');
    assert.equal(restoredEdge.stats.trades, 50);
    assert.equal(restoredEdge.stats.wins, 30);
  });
});
