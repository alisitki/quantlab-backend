/**
 * Edge Revalidation Runner
 *
 * Triggers edge re-validation based on drift alerts.
 * Implements cooldown and concurrency limits for controlled re-validation.
 *
 * Features:
 * - Alert-triggered re-validation
 * - Scheduled re-validation for all active edges
 * - 24-hour cooldown per edge
 * - Max 3 concurrent re-validations
 * - Min 500 rows dataset requirement
 *
 * Usage:
 *   const runner = new EdgeRevalidationRunner({ edgeRegistry, validationPipeline });
 *   const results = await runner.processAlerts(alerts, dataset);
 *   const scheduledResults = await runner.revalidateAll(dataset);
 */

import { LEARNING_CONFIG } from './config.js';

export class EdgeRevalidationRunner {
  constructor({ edgeRegistry, validationPipeline, config = {} }) {
    this.registry = edgeRegistry;
    this.validationPipeline = validationPipeline;
    this.config = { ...LEARNING_CONFIG.revalidation, ...config };

    this.lastRevalidation = new Map();  // edgeId → timestamp
    this.running = new Set();           // edgeId set (concurrency tracking)
    this.revalidationHistory = [];     // All re-validation results
  }

  /**
   * Process drift alerts and trigger re-validation
   * @param {Array} alerts - Array of alert groups from EdgeConfidenceUpdater
   * @param {Object} dataset - Validation dataset
   * @returns {Promise<Array>} - Re-validation results
   */
  async processAlerts(alerts, dataset) {
    // Check dataset size
    if (dataset.rows.length < this.config.minDataRows) {
      console.warn(`EdgeRevalidationRunner: Dataset too small (${dataset.rows.length} < ${this.config.minDataRows})`);
      return [];
    }

    const results = [];

    for (const alertGroup of alerts) {
      // Handle both single alert and array of alerts
      const edgeAlerts = Array.isArray(alertGroup) ? alertGroup : [alertGroup];

      for (const alert of edgeAlerts) {
        if (!alert || !alert.edgeId) continue;

        if (!this.#shouldRevalidate(alert.edgeId)) {
          results.push({
            edgeId: alert.edgeId,
            status: 'SKIPPED',
            reason: this.#getSkipReason(alert.edgeId),
            trigger: alert.type
          });
          continue;
        }

        const result = await this.#revalidateEdge(alert.edgeId, dataset, alert);
        results.push(result);
      }
    }

    return results;
  }

  /**
   * Re-validate all VALIDATED edges (scheduled)
   * @param {Object} dataset - Validation dataset
   * @returns {Promise<Array>} - Re-validation results
   */
  async revalidateAll(dataset) {
    // Check dataset size
    if (dataset.rows.length < this.config.minDataRows) {
      console.warn(`EdgeRevalidationRunner: Dataset too small (${dataset.rows.length} < ${this.config.minDataRows})`);
      return [];
    }

    const activeEdges = this.registry.getByStatus('VALIDATED');
    const results = [];

    for (const edge of activeEdges) {
      if (!this.#shouldRevalidate(edge.id)) {
        results.push({
          edgeId: edge.id,
          status: 'SKIPPED',
          reason: this.#getSkipReason(edge.id),
          trigger: 'SCHEDULED'
        });
        continue;
      }

      const result = await this.#revalidateEdge(edge.id, dataset, { type: 'SCHEDULED' });
      results.push(result);
    }

    return results;
  }

  /**
   * Get re-validation history
   * @param {Object} options - Filter options
   * @returns {Array} - Filtered history
   */
  getHistory(options = {}) {
    let history = [...this.revalidationHistory];

    if (options.edgeId) {
      history = history.filter(r => r.edgeId === options.edgeId);
    }

    if (options.since) {
      history = history.filter(r => new Date(r.revalidatedAt) >= new Date(options.since));
    }

    if (options.limit) {
      history = history.slice(-options.limit);
    }

    return history;
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    return {
      totalRevalidations: this.revalidationHistory.length,
      currentlyRunning: this.running.size,
      trackedEdges: this.lastRevalidation.size,
      recentRevalidations: this.getHistory({ limit: 10 })
    };
  }

  /**
   * Clear cooldown for edge (manual override)
   * @param {string} edgeId - Edge ID
   */
  clearCooldown(edgeId) {
    this.lastRevalidation.delete(edgeId);
  }

  /**
   * Check if edge should be re-validated
   * @private
   */
  #shouldRevalidate(edgeId) {
    // Check concurrency limit
    if (this.running.size >= this.config.maxConcurrent) {
      return false;
    }

    // Check if already running
    if (this.running.has(edgeId)) {
      return false;
    }

    // Check cooldown
    const lastTime = this.lastRevalidation.get(edgeId);
    if (lastTime) {
      const hoursSince = (Date.now() - lastTime) / (1000 * 60 * 60);
      if (hoursSince < this.config.cooldownHours) {
        return false;
      }
    }

    return true;
  }

  /**
   * Get reason why revalidation was skipped
   * @private
   */
  #getSkipReason(edgeId) {
    if (this.running.size >= this.config.maxConcurrent) {
      return 'MAX_CONCURRENT_REACHED';
    }

    if (this.running.has(edgeId)) {
      return 'ALREADY_RUNNING';
    }

    const lastTime = this.lastRevalidation.get(edgeId);
    if (lastTime) {
      const hoursSince = (Date.now() - lastTime) / (1000 * 60 * 60);
      if (hoursSince < this.config.cooldownHours) {
        return `COOLDOWN (${hoursSince.toFixed(1)}h / ${this.config.cooldownHours}h)`;
      }
    }

    return 'UNKNOWN';
  }

  /**
   * Re-validate single edge
   * @private
   */
  async #revalidateEdge(edgeId, dataset, alert) {
    this.running.add(edgeId);

    try {
      const edge = this.registry.get(edgeId);
      if (!edge) {
        return {
          edgeId,
          status: 'NOT_FOUND',
          trigger: alert.type
        };
      }

      const previousStatus = edge.status;

      // Call EdgeValidationPipeline.revalidate()
      const validationResult = await this.validationPipeline.revalidate(edge, dataset);

      this.lastRevalidation.set(edgeId, Date.now());

      const result = {
        edgeId,
        previousStatus,
        newStatus: validationResult.newStatus,
        score: validationResult.score,
        trigger: alert.type,
        revalidatedAt: new Date().toISOString(),
        statusChanged: previousStatus !== validationResult.newStatus
      };

      this.revalidationHistory.push(result);

      return result;
    } catch (error) {
      const result = {
        edgeId,
        status: 'ERROR',
        error: error.message,
        trigger: alert.type,
        revalidatedAt: new Date().toISOString()
      };

      this.revalidationHistory.push(result);

      return result;
    } finally {
      this.running.delete(edgeId);
    }
  }
}
