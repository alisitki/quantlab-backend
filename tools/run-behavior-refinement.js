#!/usr/bin/env node
/**
 * Behavior Refinement CLI
 *
 * Run feature importance analysis and generate behavior refinement proposals.
 *
 * Usage:
 *   node tools/run-behavior-refinement.js \
 *     --outcomes-dir=data/learning/outcomes \
 *     --edges-file=data/pipeline-output/pipeline-edges-*.json \
 *     --output-dir=data/learning/refinements
 *
 * This is ANALYSIS only - proposals are NOT auto-applied.
 */

import { readdir, readFile, mkdir, writeFile } from 'fs/promises';
import { join, basename } from 'path';
import { glob } from 'glob';
import { FeatureImportanceTracker } from '../core/learning/FeatureImportanceTracker.js';
import { BehaviorRefinementEngine } from '../core/learning/BehaviorRefinementEngine.js';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
import { LEARNING_CONFIG } from '../core/learning/config.js';

// Parse CLI args
const args = process.argv.slice(2).reduce((acc, arg) => {
  const [key, value] = arg.replace(/^--/, '').split('=');
  acc[key] = value;
  return acc;
}, {});

const outcomesDir = args['outcomes-dir'] || 'data/learning/outcomes';
const edgesFilePattern = args['edges-file'] || 'data/pipeline-output/pipeline-edges-*.json';
const outputDir = args['output-dir'] || LEARNING_CONFIG.schedule.refinementOutputDir;
const since = args['since'] ? parseInt(args['since']) : Date.now() - 30 * 24 * 60 * 60 * 1000; // Last 30 days
const verbose = args['verbose'] === 'true';

console.log('[BehaviorRefinement] Starting analysis...');
console.log(`[BehaviorRefinement] Outcomes dir: ${outcomesDir}`);
console.log(`[BehaviorRefinement] Edges pattern: ${edgesFilePattern}`);
console.log(`[BehaviorRefinement] Output dir: ${outputDir}`);
console.log(`[BehaviorRefinement] Since: ${new Date(since).toISOString()}`);

/**
 * Load trade outcomes from JSONL files
 */
async function loadOutcomes(dir, sinceTimestamp) {
  const outcomes = [];

  try {
    const files = await readdir(dir);
    const jsonlFiles = files.filter(f => f.endsWith('.jsonl'));

    console.log(`[BehaviorRefinement] Found ${jsonlFiles.length} outcome files`);

    for (const file of jsonlFiles) {
      const filepath = join(dir, file);
      const content = await readFile(filepath, 'utf8');
      const lines = content.trim().split('\n').filter(l => l.length > 0);

      for (const line of lines) {
        try {
          const outcome = JSON.parse(line);

          // Filter by timestamp
          if (outcome.timestamp && outcome.timestamp >= sinceTimestamp) {
            outcomes.push(outcome);
          }
        } catch (err) {
          console.warn(`[BehaviorRefinement] Skipping invalid line in ${file}`);
        }
      }
    }
  } catch (err) {
    console.error(`[BehaviorRefinement] Error loading outcomes: ${err.message}`);
    return [];
  }

  console.log(`[BehaviorRefinement] Loaded ${outcomes.length} outcomes`);
  return outcomes;
}

/**
 * Load edges from JSON file
 */
async function loadEdges(pattern) {
  const files = await glob(pattern);

  if (files.length === 0) {
    throw new Error(`No edges file found matching: ${pattern}`);
  }

  // Use most recent file
  files.sort();
  const latestFile = files[files.length - 1];

  console.log(`[BehaviorRefinement] Loading edges from: ${latestFile}`);

  const content = await readFile(latestFile, 'utf8');
  const data = JSON.parse(content);

  return data.edges || [];
}

/**
 * Main
 */
async function main() {
  const startTime = Date.now();

  try {
    // 1. Load trade outcomes
    const outcomes = await loadOutcomes(outcomesDir, since);

    if (outcomes.length === 0) {
      console.log('[BehaviorRefinement] No outcomes found. Exiting.');
      return;
    }

    // 2. Load edges
    const edges = await loadEdges(edgesFilePattern);

    if (edges.length === 0) {
      console.log('[BehaviorRefinement] No edges found. Exiting.');
      return;
    }

    console.log(`[BehaviorRefinement] Loaded ${edges.length} edges`);

    // 3. Create edge registry
    const registry = new EdgeRegistry();
    for (const edge of edges) {
      registry.register(edge);
    }

    // 4. Analyze feature importance
    console.log('[BehaviorRefinement] Analyzing feature importance...');

    const tracker = new FeatureImportanceTracker({
      maxHistorySize: LEARNING_CONFIG.importance.maxHistorySize
    });

    const importanceData = tracker.analyze(outcomes);

    const edgesAnalyzed = Object.keys(importanceData).length;
    console.log(`[BehaviorRefinement] Analyzed ${edgesAnalyzed} edges`);

    if (edgesAnalyzed === 0) {
      console.log('[BehaviorRefinement] No edges with sufficient outcomes. Exiting.');
      return;
    }

    // 5. Generate refinement proposals
    console.log('[BehaviorRefinement] Generating refinement proposals...');

    const engine = new BehaviorRefinementEngine({
      highImportanceThreshold: LEARNING_CONFIG.refinement.highImportanceThreshold,
      lowImportanceThreshold: LEARNING_CONFIG.refinement.lowImportanceThreshold,
      minEdgesForPrune: LEARNING_CONFIG.refinement.minEdgesForPrune,
      newFeatureCorrelation: LEARNING_CONFIG.refinement.newFeatureCorrelation
    });

    const proposals = engine.generateProposals(importanceData, registry);

    console.log(`[BehaviorRefinement] Generated ${proposals.length} proposals`);

    // 6. Print summary
    const summary = engine.getSummary();

    console.log('\n=== Refinement Proposal Summary ===');
    console.log(`Total Proposals: ${summary.totalProposals}`);
    console.log('By Type:');
    console.log(`  - WEIGHT_ADJUST: ${summary.byType.WEIGHT_ADJUST || 0}`);
    console.log(`  - PRUNE_CANDIDATE: ${summary.byType.PRUNE_CANDIDATE || 0}`);
    console.log(`  - NEW_FEATURE_SIGNAL: ${summary.byType.NEW_FEATURE_SIGNAL || 0}`);
    console.log('By Priority:');
    console.log(`  - HIGH: ${summary.byPriority.HIGH || 0}`);
    console.log(`  - MEDIUM: ${summary.byPriority.MEDIUM || 0}`);
    console.log(`  - LOW: ${summary.byPriority.LOW || 0}`);

    // 7. Print high-priority proposals
    const highPriority = engine.getHighPriorityProposals();

    if (highPriority.length > 0) {
      console.log('\n=== High Priority Proposals ===');
      for (const proposal of highPriority.slice(0, 5)) {
        console.log(`\n[${proposal.type}] ${proposal.featureName}`);
        console.log(`  Edge: ${proposal.edgeId || 'Multiple'}`);
        console.log(`  Reasoning: ${proposal.reasoning}`);
      }
    }

    // 8. Save proposals to disk
    if (proposals.length > 0) {
      await mkdir(outputDir, { recursive: true });

      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const filename = `refinement-proposals-${timestamp}.json`;
      const filepath = join(outputDir, filename);

      const report = {
        timestamp: new Date().toISOString(),
        analysis: {
          outcomesAnalyzed: outcomes.length,
          edgesAnalyzed,
          dateRange: {
            start: new Date(since).toISOString(),
            end: new Date().toISOString()
          }
        },
        summary,
        proposals
      };

      await writeFile(filepath, JSON.stringify(report, null, 2));

      console.log(`\n✅ Proposals saved to: ${filepath}`);
    } else {
      console.log('\n⚠️ No proposals generated. No file saved.');
    }

    // 9. Optional: verbose output
    if (verbose) {
      console.log('\n=== Feature Importance Details ===');
      for (const [edgeId, features] of Object.entries(importanceData)) {
        console.log(`\nEdge: ${edgeId}`);
        const ranking = tracker.getFeatureRanking(edgeId);
        for (const rank of ranking.slice(0, 5)) {
          console.log(`  ${rank.feature}: importance=${rank.importance.toFixed(3)}, correlation=${rank.correlation.toFixed(3)}, trend=${rank.trend}`);
        }
      }
    }

    const durationMs = Date.now() - startTime;
    console.log(`\n✅ Behavior refinement complete (${durationMs}ms)`);

  } catch (err) {
    console.error(`\n❌ Error: ${err.message}`);
    if (verbose) {
      console.error(err.stack);
    }
    process.exit(1);
  }
}

main();
