import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Edge } from '../Edge.js';
import { EdgeRegistry } from '../EdgeRegistry.js';
import { MANUAL_EDGES } from '../examples/ManualEdges.js';

describe('Edge - Milestone 4', () => {
  describe('Edge Class', () => {
    let edge;

    beforeEach(() => {
      edge = new Edge({
        id: 'test_edge_1',
        name: 'Test Edge',
        entryCondition: (features, regime) => {
          return features.micro_reversion > 0.7 && features.return_momentum < 0;
        },
        exitCondition: (features, regime, entryTime) => {
          return features.return_momentum > 0;
        },
        regimes: ['low_vol', 0],
        timeHorizon: 10000
      });
    });

    it('creates edge with required fields', () => {
      assert.equal(edge.id, 'test_edge_1');
      assert.equal(edge.name, 'Test Edge');
      assert.equal(edge.status, 'CANDIDATE');
      assert.ok(typeof edge.entryCondition === 'function');
      assert.ok(typeof edge.exitCondition === 'function');
    });

    it('evaluates entry condition correctly', () => {
      const features = {
        micro_reversion: 0.8,
        return_momentum: -0.5
      };

      const result = edge.evaluateEntry(features, 'low_vol');
      assert.ok(result.active, 'Should be active when conditions met');
    });

    it('rejects entry when regime does not match', () => {
      const features = {
        micro_reversion: 0.8,
        return_momentum: -0.5
      };

      const result = edge.evaluateEntry(features, 'high_vol');
      assert.equal(result.active, false, 'Should be inactive in wrong regime');
      assert.equal(result.reason, 'regime_mismatch');
    });

    it('rejects entry when conditions not met', () => {
      const features = {
        micro_reversion: 0.5, // Too low
        return_momentum: -0.5
      };

      const result = edge.evaluateEntry(features, 'low_vol');
      assert.equal(result.active, false, 'Should be inactive when conditions not met');
    });

    it('evaluates exit condition correctly', () => {
      const features = {
        return_momentum: 0.3 // Positive
      };

      const result = edge.evaluateExit(features, 'low_vol', Date.now());
      assert.ok(result.exit, 'Should exit when exit condition met');
    });

    it('exits on time horizon', () => {
      const entryTime = Date.now() - 15000; // 15 seconds ago
      const currentTime = Date.now();

      const features = { return_momentum: -0.2 }; // Exit condition not met

      const result = edge.evaluateExit(features, 'low_vol', entryTime, currentTime);
      assert.ok(result.exit, 'Should exit when time horizon exceeded');
      assert.equal(result.reason, 'time_horizon_exceeded');
    });

    it('updates stats with winning trade', () => {
      const trade = { return: 0.001, returnPct: 0.1 };

      edge.updateStats(trade);

      assert.equal(edge.stats.trades, 1);
      assert.equal(edge.stats.wins, 1);
      assert.equal(edge.stats.losses, 0);
      assert.equal(edge.stats.totalReturn, 0.001);
    });

    it('updates stats with losing trade', () => {
      const trade = { return: -0.001, returnPct: -0.1 };

      edge.updateStats(trade);

      assert.equal(edge.stats.trades, 1);
      assert.equal(edge.stats.wins, 0);
      assert.equal(edge.stats.losses, 1);
      assert.equal(edge.stats.totalReturn, -0.001);
    });

    it('calculates health score', () => {
      // Add some winning trades
      for (let i = 0; i < 10; i++) {
        edge.updateStats({ return: 0.001, returnPct: 0.1 });
      }

      const health = edge.getHealthScore();
      assert.ok(health > 0.5, 'Health score should be positive with wins');
      assert.ok(health <= 1, 'Health score should be <= 1');
    });

    it('should retire after poor performance', () => {
      // Add many losing trades
      for (let i = 0; i < 60; i++) {
        edge.updateStats({ return: -0.002, returnPct: -0.2 });
      }

      assert.ok(edge.shouldRetire(), 'Should retire after consistent losses');
    });

    it('serializes to JSON', () => {
      const json = edge.toJSON();

      assert.equal(json.id, 'test_edge_1');
      assert.equal(json.name, 'Test Edge');
      assert.ok(json.stats);
      assert.ok(json.confidence);
      // Functions should not be serialized
      assert.equal(json.entryCondition, undefined);
      assert.equal(json.exitCondition, undefined);
    });
  });

  describe('EdgeRegistry', () => {
    let registry;
    let edge1, edge2;

    beforeEach(() => {
      registry = new EdgeRegistry();

      edge1 = new Edge({
        id: 'edge_1',
        name: 'Edge 1',
        entryCondition: () => true,
        exitCondition: () => false,
        regimes: ['low_vol'],
        status: 'VALIDATED'
      });

      edge2 = new Edge({
        id: 'edge_2',
        name: 'Edge 2',
        entryCondition: () => true,
        exitCondition: () => false,
        regimes: ['high_vol'],
        status: 'DEPLOYED'
      });

      registry.register(edge1);
      registry.register(edge2);
    });

    it('registers edges', () => {
      assert.equal(registry.size(), 2);
      assert.ok(registry.get('edge_1'));
      assert.ok(registry.get('edge_2'));
    });

    it('gets all edges', () => {
      const all = registry.getAll();
      assert.equal(all.length, 2);
    });

    it('gets edges by status', () => {
      const validated = registry.getByStatus('VALIDATED');
      assert.equal(validated.length, 1);
      assert.equal(validated[0].id, 'edge_1');

      const deployed = registry.getByStatus('DEPLOYED');
      assert.equal(deployed.length, 1);
      assert.equal(deployed[0].id, 'edge_2');
    });

    it('gets edges by regime', () => {
      const lowVolEdges = registry.getByRegime('low_vol');
      assert.equal(lowVolEdges.length, 1);
      assert.equal(lowVolEdges[0].id, 'edge_1');

      const highVolEdges = registry.getByRegime('high_vol');
      assert.equal(highVolEdges.length, 1);
      assert.equal(highVolEdges[0].id, 'edge_2');
    });

    it('gets active edges', () => {
      const features = { micro_reversion: 0.8 };
      const active = registry.getActiveEdges(features, 'low_vol');

      // edge_1 matches regime and entry condition returns true
      assert.ok(active.length >= 1);
      assert.ok(active.some(a => a.edge.id === 'edge_1'));
    });

    it('excludes retired edges from active', () => {
      edge1.status = 'RETIRED';

      const features = { micro_reversion: 0.8 };
      const active = registry.getActiveEdges(features, 'low_vol');

      assert.ok(!active.some(a => a.edge.id === 'edge_1'), 'Retired edge should not be active');
    });

    it('updates edge stats', () => {
      const trade = { return: 0.001, returnPct: 0.1 };

      registry.updateEdgeStats('edge_1', trade);

      const edge = registry.get('edge_1');
      assert.equal(edge.stats.trades, 1);
      assert.equal(edge.stats.wins, 1);
    });

    it('removes edges', () => {
      const removed = registry.remove('edge_1');
      assert.ok(removed);
      assert.equal(registry.size(), 1);
      assert.equal(registry.get('edge_1'), undefined);
    });

    it('gets registry stats', () => {
      const stats = registry.getStats();

      assert.equal(stats.total, 2);
      assert.equal(stats.byStatus.VALIDATED, 1);
      assert.equal(stats.byStatus.DEPLOYED, 1);
      assert.ok(stats.avgHealthScore >= 0);
    });

    it('retires underperforming edges', () => {
      // Create a fresh edge (updateStats auto-retires after 50 trades)
      const edge3 = new Edge({
        id: 'edge_3',
        name: 'Edge 3',
        entryCondition: () => true,
        exitCondition: () => false,
        status: 'DEPLOYED'
      });
      registry.register(edge3);

      // Make edge_3 underperform (35 trades = won't auto-retire in updateStats)
      for (let i = 0; i < 35; i++) {
        edge3.updateStats({ return: -0.002, returnPct: -0.2 });
      }

      // Manually check shouldRetire (it should say yes)
      assert.ok(edge3.shouldRetire(), 'Edge should qualify for retirement');

      // Now call registry retire method
      const retired = registry.retireUnderperformingEdges();

      assert.ok(retired.includes('edge_3'), 'edge_3 should be in retired list');
      assert.equal(registry.get('edge_3').status, 'RETIRED');
    });

    it('clears registry', () => {
      registry.clear();
      assert.equal(registry.size(), 0);
    });
  });

  describe('Manual Edges Integration', () => {
    it('loads all manual edges', () => {
      assert.ok(Array.isArray(MANUAL_EDGES));
      assert.equal(MANUAL_EDGES.length, 3);
    });

    it('manual edges have required properties', () => {
      for (const edge of MANUAL_EDGES) {
        assert.ok(edge.id);
        assert.ok(edge.name);
        assert.ok(typeof edge.entryCondition === 'function');
        assert.ok(typeof edge.exitCondition === 'function');
        assert.equal(edge.status, 'CANDIDATE');
        assert.equal(edge.discoveryMethod, 'manual_theory');
      }
    });

    it('mean reversion edge evaluates correctly', () => {
      const edge = MANUAL_EDGES[0]; // meanReversionLowVolEdge

      const features = {
        regime_volatility: 0,
        volatility_ratio: 0.4,
        micro_reversion: 0.7,
        return_momentum: -0.4,
        behavior_divergence: 0.5
      };

      const result = edge.evaluateEntry(features, 0);
      assert.ok(result.active, 'Should activate in low vol with high reversion');
      assert.equal(result.direction, 'LONG');
    });

    it('momentum continuation edge evaluates correctly', () => {
      const edge = MANUAL_EDGES[1]; // momentumContinuationEdge

      const features = {
        return_momentum: 0.6,
        liquidity_pressure: 0.4,
        micro_reversion: 0.3,
        regime_trend: 1
      };

      const result = edge.evaluateEntry(features, 1);
      assert.ok(result.active, 'Should activate with strong aligned momentum');
      assert.equal(result.direction, 'LONG');
    });

    it('volatility breakout edge evaluates correctly', () => {
      const edge = MANUAL_EDGES[2]; // volatilityBreakoutEdge

      const features = {
        volatility_compression_score: 0.8,
        spread_compression: 0.4,
        quote_intensity: 0.8,
        volatility_ratio: 0.7,
        return_momentum: 0.2
      };

      const result = edge.evaluateEntry(features, null);
      assert.ok(result.active, 'Should activate with high compression');
      assert.equal(result.direction, 'LONG');
    });

    it('can register manual edges in registry', () => {
      const registry = new EdgeRegistry();

      for (const edge of MANUAL_EDGES) {
        registry.register(edge);
      }

      assert.equal(registry.size(), 3);
      assert.equal(registry.getByStatus('CANDIDATE').length, 3);
    });
  });
});
