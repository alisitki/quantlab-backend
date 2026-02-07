/**
 * End-to-End Pipeline Integration Test
 *
 * Tests the full pipeline flow with synthetic data (no parquet required):
 * - EdgeSerializer round-trip
 * - EdgeRegistry → EdgeValidation → StrategyFactory → StrategyLifecycle
 * - EdgeHealth integration in DemotionEvaluator
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
import { Edge } from '../core/edge/Edge.js';
import { EdgeSerializer } from '../core/edge/EdgeSerializer.js';
import { StrategyLifecycleManager } from '../core/strategy/lifecycle/StrategyLifecycleManager.js';

const TEST_EDGES_FILE = '/tmp/test-pipeline-edges.json';
const TEST_LIFECYCLE_DIR = '/tmp/test-pipeline-lifecycle';

describe('Pipeline Integration', () => {
  afterEach(async () => {
    try {
      await fs.unlink(TEST_EDGES_FILE);
      await fs.rm(TEST_LIFECYCLE_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore
    }
  });

  it('should serialize and deserialize edges with definitions', async () => {
    const registry = new EdgeRegistry();

    // Create edge with definition
    const edge = new Edge({
      id: 'test_edge_serialize_1',
      name: 'Test Serialization',
      entryCondition: (features) => ({
        active: features.liquidity_pressure > 0.5,
        direction: 'LONG'
      }),
      exitCondition: () => ({ exit: false }),
      regimes: [0, 1],
      status: 'CANDIDATE'
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

    // Serialize to file
    const serializer = new EdgeSerializer();
    await serializer.saveToFile(TEST_EDGES_FILE, registry);

    // Deserialize from file
    const loadedRegistry = await serializer.loadFromFile(TEST_EDGES_FILE);

    assert.equal(loadedRegistry.size(), 1);

    const loadedEdge = loadedRegistry.get('discovered_threshold_1');
    assert.ok(loadedEdge);
    assert.ok(loadedEdge.entryCondition);
    assert.ok(loadedEdge.exitCondition);

    // Test closure works
    const result = loadedEdge.entryCondition({ liquidity_pressure: 0.7 }, 0);
    assert.equal(result.active, true);
    assert.equal(result.direction, 'LONG');
  });

  it('should integrate EdgeRegistry with StrategyLifecycleManager', async () => {
    const registry = new EdgeRegistry();

    // Create edge
    const edge = new Edge({
      id: 'test_edge_lifecycle_1',
      name: 'Lifecycle Test',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    edge.stats.trades = 50;
    edge.stats.wins = 20;
    edge.stats.losses = 30;

    const definition = {
      pattern: {
        id: '2',
        type: 'cluster',
        conditions: [{ feature: 'cluster', operator: '==', value: 2 }],
        regimes: null,
        direction: 'SHORT',
        horizon: 50,
        support: 80,
        forwardReturns: { mean: 0.002, std: 0.004 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.8,
        tests: { sharpeTest: { sharpe: 1.5 } }
      }
    };

    registry.register(edge, definition);

    // Create lifecycle manager
    const lifecycleManager = new StrategyLifecycleManager(TEST_LIFECYCLE_DIR);

    // Connect EdgeRegistry
    lifecycleManager.connectEdgeRegistry(registry);

    // Register strategy
    const deployResult = {
      strategyId: 'strat_test_1',
      edgeId: edge.id,
      templateType: 'momentum',
      backtestSummary: { trades: 50, sharpe: 1.0, maxDrawdownPct: 3 },
      validationScore: 0.75
    };

    lifecycleManager.register(deployResult);

    // Record some runs
    for (let i = 0; i < 5; i++) {
      lifecycleManager.recordRunResult('strat_test_1', {
        runId: `run-${i}`,
        completedAt: new Date().toISOString(),
        trades: 10,
        returnPct: i % 2 === 0 ? 1.0 : -0.5,
        maxDrawdownPct: 1.0,
        winRate: 0.5,
        sharpe: 0.8
      });
    }

    // Evaluate strategy (should use edge health)
    const evaluation = lifecycleManager.evaluateStrategy('strat_test_1');

    assert.ok(evaluation);
    assert.equal(evaluation.strategyId, 'strat_test_1');
    assert.ok(evaluation.demotion); // DemotionEvaluator should have received edge health

    // Edge health should have been calculated
    const edgeHealth = edge.getHealthScore();
    assert.ok(typeof edgeHealth === 'number');
    assert.ok(edgeHealth >= 0 && edgeHealth <= 1);
  });

  it('should preserve edge status through serialization', async () => {
    const registry = new EdgeRegistry();

    // Create VALIDATED edge
    const edge = new Edge({
      id: 'test_edge_status_1',
      name: 'Status Test',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    const definition = {
      pattern: {
        id: '3',
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

    // Serialize
    const serializer = new EdgeSerializer();
    await serializer.saveToFile(TEST_EDGES_FILE, registry);

    // Deserialize
    const loadedRegistry = await serializer.loadFromFile(TEST_EDGES_FILE);

    const loadedEdge = loadedRegistry.get('discovered_quantile_3');
    assert.ok(loadedEdge);
    assert.equal(loadedEdge.status, 'VALIDATED');
  });

  it('should handle multiple edges in pipeline', async () => {
    const registry = new EdgeRegistry();

    // Create multiple edges
    for (let i = 0; i < 3; i++) {
      const edge = new Edge({
        id: `test_edge_multi_${i}`,
        name: `Multi Edge ${i}`,
        entryCondition: () => ({ active: true }),
        exitCondition: () => ({ exit: false }),
        status: i === 0 ? 'VALIDATED' : 'CANDIDATE'
      });

      const definition = {
        pattern: {
          id: `${i + 10}`,
          type: 'threshold',
          conditions: [{ feature: 'test_feature', operator: '>', value: i * 0.1 }],
          regimes: null,
          direction: 'LONG',
          horizon: 10,
          support: 50 + i * 10,
          forwardReturns: { mean: 0.001 * (i + 1), std: 0.002 }
        },
        testResult: {
          recommendation: 'ACCEPT',
          overallScore: 0.7 + i * 0.05,
          tests: { sharpeTest: { sharpe: 1.0 + i * 0.2 } }
        }
      };

      registry.register(edge, definition);
    }

    // Serialize
    const serializer = new EdgeSerializer();
    await serializer.saveToFile(TEST_EDGES_FILE, registry);

    // Deserialize
    const loadedRegistry = await serializer.loadFromFile(TEST_EDGES_FILE);

    assert.equal(loadedRegistry.size(), 3);

    const validatedCount = loadedRegistry.getByStatus('VALIDATED').length;
    const candidateCount = loadedRegistry.getByStatus('CANDIDATE').length;

    assert.equal(validatedCount, 1);
    assert.equal(candidateCount, 2);
  });

  it('should persist and restore lifecycle state', async () => {
    const registry = new EdgeRegistry();

    const edge = new Edge({
      id: 'test_edge_persist_1',
      name: 'Persist Test',
      entryCondition: () => ({ active: true }),
      exitCondition: () => ({ exit: false }),
      status: 'VALIDATED'
    });

    const definition = {
      pattern: {
        id: '20',
        type: 'threshold',
        conditions: [],
        regimes: null,
        direction: 'LONG',
        horizon: 10,
        support: 100,
        forwardReturns: { mean: 0.002, std: 0.003 }
      },
      testResult: {
        recommendation: 'ACCEPT',
        overallScore: 0.8,
        tests: { sharpeTest: { sharpe: 1.5 } }
      }
    };

    registry.register(edge, definition);

    // Create manager, register strategy
    const manager1 = new StrategyLifecycleManager(TEST_LIFECYCLE_DIR);
    manager1.connectEdgeRegistry(registry);

    const deployResult = {
      strategyId: 'strat_persist_1',
      edgeId: edge.id,
      templateType: 'momentum',
      backtestSummary: { trades: 50, sharpe: 1.2, maxDrawdownPct: 3 }
    };

    manager1.register(deployResult);
    await manager1.persist();

    // Create new manager, restore
    const manager2 = new StrategyLifecycleManager(TEST_LIFECYCLE_DIR);
    await manager2.restore();

    const strategy = manager2.getStrategy('strat_persist_1');
    assert.ok(strategy);
    assert.equal(strategy.strategyId, 'strat_persist_1');
    assert.equal(strategy.edgeId, edge.id);
  });
});
