/**
 * Test: BehaviorRefinementEngine
 *
 * Validates behavior refinement proposal generation.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorRefinementEngine } from '../BehaviorRefinementEngine.js';
import { EdgeRegistry } from '../../edge/EdgeRegistry.js';

test('BehaviorRefinementEngine - WEIGHT_ADJUST proposals for high importance', () => {
  const engine = new BehaviorRefinementEngine({
    highImportanceThreshold: 0.6
  });

  const registry = new EdgeRegistry();

  // Edge with pattern using featureA
  registry.register({
    id: 'edge_high',
    definition: {
      pattern: {
        conditions: [
          { feature: 'featureA', operator: '>', value: 0.5 }
        ]
      }
    }
  });

  // Importance data with high importance for featureA
  const importanceData = {
    'edge_high': {
      'featureA': {
        importance: 0.75,
        correlation: 0.6,
        pValue: 0.01
      },
      'featureB': {
        importance: 0.2,
        correlation: 0.1,
        pValue: 0.5
      }
    }
  };

  const proposals = engine.generateProposals(importanceData, registry);

  const weightAdjustProposals = proposals.filter(p => p.type === 'WEIGHT_ADJUST');

  assert.ok(weightAdjustProposals.length > 0, 'Should generate WEIGHT_ADJUST proposals');

  const featureAProposal = weightAdjustProposals.find(p => p.featureName === 'featureA');
  assert.ok(featureAProposal, 'Should have proposal for high-importance feature');
  assert.ok(featureAProposal.priority === 'MEDIUM', 'Used feature should be MEDIUM priority');

  console.log('✅ WEIGHT_ADJUST proposals generated correctly');
  console.log(`   Feature: ${featureAProposal.featureName}`);
  console.log(`   Reasoning: ${featureAProposal.reasoning}`);
});

test('BehaviorRefinementEngine - PRUNE_CANDIDATE proposals for low importance', () => {
  const engine = new BehaviorRefinementEngine({
    lowImportanceThreshold: 0.15,
    minEdgesForPrune: 2
  });

  const registry = new EdgeRegistry();

  registry.register({ id: 'edge1' });
  registry.register({ id: 'edge2' });
  registry.register({ id: 'edge3' });

  // noiseFeature shows low importance across multiple edges
  const importanceData = {
    'edge1': {
      'noiseFeature': { importance: 0.1, correlation: 0.05, pValue: 0.8 }
    },
    'edge2': {
      'noiseFeature': { importance: 0.08, correlation: 0.02, pValue: 0.9 }
    },
    'edge3': {
      'noiseFeature': { importance: 0.12, correlation: 0.03, pValue: 0.85 }
    }
  };

  const proposals = engine.generateProposals(importanceData, registry);

  const pruneProposals = proposals.filter(p => p.type === 'PRUNE_CANDIDATE');

  assert.ok(pruneProposals.length > 0, 'Should generate PRUNE_CANDIDATE proposals');

  const noiseProposal = pruneProposals.find(p => p.featureName === 'noiseFeature');
  assert.ok(noiseProposal, 'Should identify noiseFeature as prune candidate');
  assert.ok(noiseProposal.data.edgeCount === 3, 'Should track edge count');

  console.log('✅ PRUNE_CANDIDATE proposals generated correctly');
  console.log(`   Feature: ${noiseProposal.featureName}`);
  console.log(`   Reasoning: ${noiseProposal.reasoning}`);
});

test('BehaviorRefinementEngine - NEW_FEATURE_SIGNAL proposals', () => {
  const engine = new BehaviorRefinementEngine({
    newFeatureCorrelation: 0.5
  });

  const registry = new EdgeRegistry();

  // Edge does not use hiddenFeature
  registry.register({
    id: 'edge_hidden',
    definition: {
      pattern: {
        conditions: [
          { feature: 'usedFeature', operator: '>', value: 0 }
        ]
      }
    }
  });

  // hiddenFeature has high correlation but not used
  const importanceData = {
    'edge_hidden': {
      'usedFeature': { importance: 0.4, correlation: 0.3, pValue: 0.1 },
      'hiddenFeature': { importance: 0.7, correlation: 0.75, pValue: 0.01 }  // > 0.7 for HIGH priority
    }
  };

  const proposals = engine.generateProposals(importanceData, registry);

  const newFeatureProposals = proposals.filter(p => p.type === 'NEW_FEATURE_SIGNAL');

  assert.ok(newFeatureProposals.length > 0, 'Should generate NEW_FEATURE_SIGNAL proposals');

  const hiddenProposal = newFeatureProposals.find(p => p.featureName === 'hiddenFeature');
  assert.ok(hiddenProposal, 'Should identify hiddenFeature as new feature signal');
  assert.ok(hiddenProposal.priority === 'HIGH', 'High correlation should be HIGH priority');

  console.log('✅ NEW_FEATURE_SIGNAL proposals generated correctly');
  console.log(`   Feature: ${hiddenProposal.featureName}`);
  console.log(`   Reasoning: ${hiddenProposal.reasoning}`);
});

test('BehaviorRefinementEngine - proposal priority sorting', () => {
  const engine = new BehaviorRefinementEngine({
    highImportanceThreshold: 0.6,
    lowImportanceThreshold: 0.15,
    minEdgesForPrune: 1
  });

  const registry = new EdgeRegistry();
  registry.register({
    id: 'edge_priority',
    definition: {
      pattern: {
        conditions: [{ feature: 'usedFeature', operator: '>', value: 0 }]
      }
    }
  });

  const importanceData = {
    'edge_priority': {
      'highFeature': { importance: 0.8, correlation: 0.7, pValue: 0.01 }, // HIGH priority (unused but important)
      'lowFeature': { importance: 0.1, correlation: 0.05, pValue: 0.9 }, // MEDIUM/LOW priority (prune candidate)
      'usedFeature': { importance: 0.7, correlation: 0.5, pValue: 0.05 }  // MEDIUM priority (used, refine threshold)
    }
  };

  const proposals = engine.generateProposals(importanceData, registry);

  // Check that HIGH priority comes first
  const priorities = proposals.map(p => p.priority);
  const firstHigh = priorities.indexOf('HIGH');
  const firstMedium = priorities.indexOf('MEDIUM');
  const firstLow = priorities.indexOf('LOW');

  if (firstHigh !== -1 && firstMedium !== -1) {
    assert.ok(firstHigh < firstMedium, 'HIGH priority should come before MEDIUM');
  }

  if (firstMedium !== -1 && firstLow !== -1) {
    assert.ok(firstMedium < firstLow, 'MEDIUM priority should come before LOW');
  }

  console.log('✅ Proposal priority sorting works');
  console.log(`   Priorities: ${priorities.join(', ')}`);
});

test('BehaviorRefinementEngine - getSummary', () => {
  const engine = new BehaviorRefinementEngine();

  const registry = new EdgeRegistry();
  registry.register({ id: 'edge_sum' });

  const importanceData = {
    'edge_sum': {
      'feature1': { importance: 0.7, correlation: 0.6, pValue: 0.01 },
      'feature2': { importance: 0.1, correlation: 0.05, pValue: 0.9 }
    }
  };

  engine.generateProposals(importanceData, registry);

  const summary = engine.getSummary();

  assert.ok(summary.totalProposals > 0, 'Should have proposals');
  assert.ok(summary.byType, 'Should have type breakdown');
  assert.ok(summary.byPriority, 'Should have priority breakdown');

  console.log('✅ Summary generation works');
  console.log(`   Total: ${summary.totalProposals}`);
  console.log(`   By Type: ${JSON.stringify(summary.byType)}`);
  console.log(`   By Priority: ${JSON.stringify(summary.byPriority)}`);
});

test('BehaviorRefinementEngine - serialization', () => {
  const engine = new BehaviorRefinementEngine({
    highImportanceThreshold: 0.7
  });

  const registry = new EdgeRegistry();
  registry.register({ id: 'edge_ser' });

  const importanceData = {
    'edge_ser': {
      'feature1': { importance: 0.8, correlation: 0.7, pValue: 0.01 }
    }
  };

  engine.generateProposals(importanceData, registry);

  const json = engine.toJSON();
  const restored = BehaviorRefinementEngine.fromJSON(json);

  assert.ok(restored.getSummary().totalProposals === engine.getSummary().totalProposals,
    'Should preserve proposals');

  console.log('✅ Serialization works');
});

test('BehaviorRefinementEngine - handles empty data', () => {
  const engine = new BehaviorRefinementEngine();
  const registry = new EdgeRegistry();

  const proposals = engine.generateProposals({}, registry);

  assert.ok(proposals.length === 0, 'Should return empty array for empty data');

  console.log('✅ Handles empty data gracefully');
});

console.log('\n✅ All BehaviorRefinementEngine tests passed!');
