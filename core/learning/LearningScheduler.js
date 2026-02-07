/**
 * Learning Scheduler
 *
 * Orchestrates daily, weekly, and monthly closed-loop learning cycles.
 *
 * Daily Loop:
 * 1. Read last 24h trade outcomes
 * 2. Update edge confidence via EMA
 * 3. Detect drift and generate alerts
 * 4. (Optional) Trigger auto-revalidation
 *
 * Weekly Loop:
 * 1. Daily loop
 * 2. Re-validate all VALIDATED edges
 * 3. Report edge status changes
 *
 * Monthly Loop:
 * 1. Weekly loop
 * 2. Feature importance analysis
 * 3. Behavior refinement proposals
 * 4. Save proposals to disk
 *
 * Usage:
 *   const scheduler = new LearningScheduler({ ... });
 *   const dailyResult = await scheduler.runDaily();
 *   const weeklyResult = await scheduler.runWeekly(dataset);
 *   const monthlyResult = await scheduler.runMonthly(dataset);
 */

import { LEARNING_CONFIG } from './config.js';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';

export class LearningScheduler {
  constructor({
    edgeRegistry,
    confidenceUpdater,
    revalidationRunner,
    outcomeCollector,
    importanceTracker = null,
    refinementEngine = null,
    config = {}
  }) {
    this.registry = edgeRegistry;
    this.updater = confidenceUpdater;
    this.runner = revalidationRunner;
    this.collector = outcomeCollector;
    this.importanceTracker = importanceTracker;
    this.refinementEngine = refinementEngine;
    this.config = { ...LEARNING_CONFIG.schedule, ...config };

    this.lastDailyRun = null;
    this.lastWeeklyRun = null;
    this.lastMonthlyRun = null;
    this.runHistory = [];
  }

  /**
   * Daily learning loop
   * @returns {Promise<Object>} - Daily run result
   */
  async runDaily() {
    const startTime = Date.now();

    // 1. Read last 24 hours of outcomes
    const since = Date.now() - 24 * 60 * 60 * 1000;
    const outcomes = await this.collector.readOutcomes({ since });

    if (outcomes.length === 0) {
      const result = {
        type: 'daily',
        timestamp: new Date().toISOString(),
        skipped: true,
        reason: 'no_outcomes',
        outcomesProcessed: 0,
        alertsGenerated: 0,
        revalidationFlags: 0,
        durationMs: Date.now() - startTime
      };

      this.lastDailyRun = Date.now();
      this.runHistory.push(result);

      return result;
    }

    // 2. Update confidence for all edges from outcomes
    const alerts = this.updater.processBatch(outcomes);

    // 3. Flag edges for re-validation (if auto-revalidation enabled)
    let revalidationFlags = [];

    if (this.config.enableAutoRevalidation && alerts.length > 0) {
      // Don't actually revalidate in daily loop (requires dataset)
      // Just flag which edges need it
      revalidationFlags = alerts.map(alertGroup => {
        const edgeAlerts = Array.isArray(alertGroup) ? alertGroup : [alertGroup];
        return edgeAlerts.map(a => ({
          edgeId: a.edgeId,
          alertType: a.type,
          status: 'FLAGGED_FOR_REVALIDATION'
        }));
      }).flat();
    }

    const result = {
      type: 'daily',
      timestamp: new Date().toISOString(),
      outcomesProcessed: outcomes.length,
      alertsGenerated: alerts.filter(a => a !== null).length,
      revalidationFlags: revalidationFlags.length,
      edgesAffected: this.#getUniqueEdges(outcomes).length,
      alerts: alerts.map(alertGroup => {
        const edgeAlerts = Array.isArray(alertGroup) ? alertGroup : [alertGroup];
        return {
          edgeId: edgeAlerts[0]?.edgeId,
          alertTypes: edgeAlerts.map(a => a.type)
        };
      }),
      flaggedEdges: revalidationFlags,
      durationMs: Date.now() - startTime
    };

    this.lastDailyRun = Date.now();
    this.runHistory.push(result);

    return result;
  }

  /**
   * Weekly learning loop
   * @param {Object} dataset - Validation dataset
   * @returns {Promise<Object>} - Weekly run result
   */
  async runWeekly(dataset) {
    const startTime = Date.now();

    // Run daily loop first
    const dailyResult = await this.runDaily();

    // Re-validate all VALIDATED edges
    const revalidationResults = await this.runner.revalidateAll(dataset);

    // Count status changes
    const statusChanges = revalidationResults.filter(r => r.statusChanged).length;
    const validated = revalidationResults.filter(r => r.newStatus === 'VALIDATED').length;
    const rejected = revalidationResults.filter(r => r.newStatus === 'REJECTED').length;
    const errors = revalidationResults.filter(r => r.status === 'ERROR').length;

    const result = {
      type: 'weekly',
      timestamp: new Date().toISOString(),
      daily: dailyResult,
      revalidation: {
        edgesRevalidated: revalidationResults.filter(r => r.status !== 'SKIPPED').length,
        edgesSkipped: revalidationResults.filter(r => r.status === 'SKIPPED').length,
        statusChanges,
        validated,
        rejected,
        errors,
        results: revalidationResults.map(r => ({
          edgeId: r.edgeId,
          previousStatus: r.previousStatus,
          newStatus: r.newStatus,
          statusChanged: r.statusChanged,
          score: r.score
        }))
      },
      durationMs: Date.now() - startTime
    };

    this.lastWeeklyRun = Date.now();
    this.runHistory.push(result);

    return result;
  }

  /**
   * Monthly learning loop
   * @param {Object} dataset - Validation dataset
   * @param {string} outputDir - Directory to save refinement proposals
   * @returns {Promise<Object>} - Monthly run result
   */
  async runMonthly(dataset, outputDir = 'data/learning/refinements') {
    const startTime = Date.now();

    // Verify components
    if (!this.importanceTracker) {
      throw new Error('FeatureImportanceTracker not provided to scheduler');
    }

    if (!this.refinementEngine) {
      throw new Error('BehaviorRefinementEngine not provided to scheduler');
    }

    // Run weekly loop first
    const weeklyResult = await this.runWeekly(dataset);

    // Read last 30 days of outcomes for importance analysis
    const since = Date.now() - 30 * 24 * 60 * 60 * 1000;
    const outcomes = await this.collector.readOutcomes({ since });

    if (outcomes.length === 0) {
      const result = {
        type: 'monthly',
        timestamp: new Date().toISOString(),
        weekly: weeklyResult,
        importance: { skipped: true, reason: 'no_outcomes' },
        refinement: { skipped: true, reason: 'no_outcomes' },
        durationMs: Date.now() - startTime
      };

      this.lastMonthlyRun = Date.now();
      this.runHistory.push(result);

      return result;
    }

    // Analyze feature importance
    const importanceData = this.importanceTracker.analyze(outcomes);

    // Generate refinement proposals
    const proposals = this.refinementEngine.generateProposals(importanceData, this.registry);

    // Save proposals to disk
    let savedPath = null;

    if (proposals.length > 0) {
      try {
        await mkdir(outputDir, { recursive: true });

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const filename = `refinement-proposals-${timestamp}.json`;
        const filepath = join(outputDir, filename);

        await writeFile(filepath, JSON.stringify({
          timestamp: new Date().toISOString(),
          outcomesAnalyzed: outcomes.length,
          edgesAnalyzed: Object.keys(importanceData).length,
          proposalsGenerated: proposals.length,
          proposals
        }, null, 2));

        savedPath = filepath;
      } catch (err) {
        console.error('[LearningScheduler] Failed to save proposals:', err);
      }
    }

    const result = {
      type: 'monthly',
      timestamp: new Date().toISOString(),
      weekly: weeklyResult,
      importance: {
        outcomesAnalyzed: outcomes.length,
        edgesAnalyzed: Object.keys(importanceData).length,
        summary: this.importanceTracker.getSummary()
      },
      refinement: {
        proposalsGenerated: proposals.length,
        byType: proposals.reduce((acc, p) => {
          acc[p.type] = (acc[p.type] || 0) + 1;
          return acc;
        }, {}),
        byPriority: proposals.reduce((acc, p) => {
          acc[p.priority] = (acc[p.priority] || 0) + 1;
          return acc;
        }, {}),
        savedPath
      },
      durationMs: Date.now() - startTime
    };

    this.lastMonthlyRun = Date.now();
    this.runHistory.push(result);

    return result;
  }

  /**
   * Get learning loop summary
   * @returns {Object}
   */
  getSummary() {
    const recentRuns = this.runHistory.slice(-5);

    return {
      lastDailyRun: this.lastDailyRun ? new Date(this.lastDailyRun).toISOString() : null,
      lastWeeklyRun: this.lastWeeklyRun ? new Date(this.lastWeeklyRun).toISOString() : null,
      lastMonthlyRun: this.lastMonthlyRun ? new Date(this.lastMonthlyRun).toISOString() : null,
      totalRuns: this.runHistory.length,
      dailyRuns: this.runHistory.filter(r => r.type === 'daily').length,
      weeklyRuns: this.runHistory.filter(r => r.type === 'weekly').length,
      monthlyRuns: this.runHistory.filter(r => r.type === 'monthly').length,
      recentRuns: recentRuns.map(r => ({
        type: r.type,
        timestamp: r.timestamp,
        outcomesProcessed: r.outcomesProcessed || 0,
        alertsGenerated: r.alertsGenerated || 0,
        durationMs: r.durationMs
      }))
    };
  }

  /**
   * Get run history with filters
   * @param {Object} options - Filter options
   * @returns {Array}
   */
  getHistory(options = {}) {
    let history = [...this.runHistory];

    if (options.type) {
      history = history.filter(r => r.type === options.type);
    }

    if (options.since) {
      const sinceDate = new Date(options.since);
      history = history.filter(r => new Date(r.timestamp) >= sinceDate);
    }

    if (options.limit) {
      history = history.slice(-options.limit);
    }

    return history;
  }

  /**
   * Get unique edges from outcomes
   * @private
   */
  #getUniqueEdges(outcomes) {
    const edgeIds = new Set();
    for (const outcome of outcomes) {
      edgeIds.add(outcome.edgeId);
    }
    return Array.from(edgeIds);
  }
}
