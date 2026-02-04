/**
 * SLO Alerter
 *
 * Periodically checks SLO status and emits alerts when thresholds are breached.
 * Integrates with AlertManager for Slack notifications.
 */

import { SLO_STATUS } from './SLOCalculator.js';

/**
 * Alert types for SLO events
 */
export const SLO_ALERT_TYPES = {
  BREACHED: 'SLO_BREACHED',
  WARNING: 'SLO_WARNING',
  RECOVERED: 'SLO_RECOVERED',
  BUDGET_LOW: 'SLO_BUDGET_LOW'
};

export class SLOAlerter {
  #calculator;
  #alertManager;
  #checkIntervalMs;
  #intervalHandle;
  #lastStatuses; // Track previous statuses for recovery detection
  #budgetAlertThreshold; // Alert when error budget consumed exceeds this %

  /**
   * @param {SLOCalculator} calculator - SLO calculator instance
   * @param {Object} alertManager - AlertManager instance (must have emit() method)
   * @param {Object} options - Configuration options
   * @param {number} options.checkIntervalMs - How often to check SLOs (default: 5 min)
   * @param {number} options.budgetAlertThreshold - Alert when budget consumed > this % (default: 80)
   */
  constructor(calculator, alertManager, options = {}) {
    this.#calculator = calculator;
    this.#alertManager = alertManager;
    this.#checkIntervalMs = options.checkIntervalMs || 5 * 60 * 1000; // 5 minutes
    this.#budgetAlertThreshold = options.budgetAlertThreshold || 80;
    this.#intervalHandle = null;
    this.#lastStatuses = new Map();
  }

  /**
   * Emit an alert through AlertManager
   * @param {string} type - Alert type
   * @param {string} severity - Alert severity (info, warning, error, critical)
   * @param {string} message - Alert message
   * @param {Object} metadata - Additional metadata
   */
  async #emitAlert(type, severity, message, metadata = {}) {
    if (!this.#alertManager?.emit) {
      console.warn(`[SLOAlerter] AlertManager not available, skipping alert: ${message}`);
      return;
    }

    try {
      await this.#alertManager.emit({
        type,
        severity,
        message,
        source: 'SLOAlerter',
        ...metadata
      });
    } catch (err) {
      console.error(`[SLOAlerter] Failed to emit alert: ${err.message}`);
    }
  }

  /**
   * Check a single SLO and emit alerts if needed
   * @param {Object} status - Current SLO status
   */
  async #checkSLO(status) {
    const { slo_id, name, status: currentStatus, tier, current_value, target, error_budget_consumed_pct } = status;
    const previousStatus = this.#lastStatuses.get(slo_id);

    // Determine severity based on tier
    const getSeverity = (tier, isBreached) => {
      if (isBreached) {
        return tier === 1 ? 'critical' : tier === 2 ? 'error' : 'warning';
      }
      return tier === 1 ? 'warning' : 'info';
    };

    // Check for status changes
    if (currentStatus === SLO_STATUS.BREACHED && previousStatus !== SLO_STATUS.BREACHED) {
      // SLO just breached
      await this.#emitAlert(
        SLO_ALERT_TYPES.BREACHED,
        getSeverity(tier, true),
        `SLO breached: ${name} (Tier ${tier})`,
        {
          slo_id,
          tier,
          current_value,
          target,
          previous_status: previousStatus || 'UNKNOWN'
        }
      );
    } else if (currentStatus === SLO_STATUS.WARNING && previousStatus === SLO_STATUS.OK) {
      // SLO degraded to warning
      await this.#emitAlert(
        SLO_ALERT_TYPES.WARNING,
        getSeverity(tier, false),
        `SLO warning: ${name} approaching threshold`,
        {
          slo_id,
          tier,
          current_value,
          target
        }
      );
    } else if (currentStatus === SLO_STATUS.OK &&
               (previousStatus === SLO_STATUS.BREACHED || previousStatus === SLO_STATUS.WARNING)) {
      // SLO recovered
      await this.#emitAlert(
        SLO_ALERT_TYPES.RECOVERED,
        'info',
        `SLO recovered: ${name}`,
        {
          slo_id,
          tier,
          current_value,
          target,
          previous_status: previousStatus
        }
      );
    }

    // Check error budget consumption
    if (error_budget_consumed_pct >= this.#budgetAlertThreshold) {
      const budgetKey = `${slo_id}_budget_alert`;
      const lastBudgetAlert = this.#lastStatuses.get(budgetKey);

      // Only alert once per threshold breach (reset when budget recovers)
      if (!lastBudgetAlert) {
        await this.#emitAlert(
          SLO_ALERT_TYPES.BUDGET_LOW,
          tier === 1 ? 'warning' : 'info',
          `SLO error budget ${error_budget_consumed_pct.toFixed(1)}% consumed: ${name}`,
          {
            slo_id,
            tier,
            error_budget_consumed_pct,
            threshold: this.#budgetAlertThreshold
          }
        );
        this.#lastStatuses.set(budgetKey, true);
      }
    } else {
      // Budget recovered, clear flag
      this.#lastStatuses.delete(`${slo_id}_budget_alert`);
    }

    // Update last status
    this.#lastStatuses.set(slo_id, currentStatus);
  }

  /**
   * Check all SLOs and emit alerts
   */
  async checkAndAlert() {
    try {
      const statuses = this.#calculator.evaluateAll();

      for (const status of statuses) {
        await this.#checkSLO(status);
      }

      // Log summary
      const health = this.#calculator.getOverallHealth();
      if (health.breached > 0) {
        console.log(`[SLOAlerter] Check complete: ${health.breached} breached, ${health.warning} warning, ${health.ok} ok`);
      }

    } catch (err) {
      console.error(`[SLOAlerter] Check failed: ${err.message}`);
    }
  }

  /**
   * Start periodic SLO checking
   */
  start() {
    if (this.#intervalHandle) {
      console.warn('[SLOAlerter] Already running');
      return;
    }

    console.log(`[SLOAlerter] Starting periodic checks every ${this.#checkIntervalMs / 1000}s`);

    // Initial check
    this.checkAndAlert();

    // Schedule periodic checks
    this.#intervalHandle = setInterval(() => {
      this.checkAndAlert();
    }, this.#checkIntervalMs);

    // Don't prevent process exit
    this.#intervalHandle.unref();
  }

  /**
   * Stop periodic SLO checking
   */
  stop() {
    if (this.#intervalHandle) {
      clearInterval(this.#intervalHandle);
      this.#intervalHandle = null;
      console.log('[SLOAlerter] Stopped');
    }
  }

  /**
   * Check if alerter is running
   * @returns {boolean}
   */
  isRunning() {
    return this.#intervalHandle !== null;
  }

  /**
   * Get current check interval
   * @returns {number} Interval in milliseconds
   */
  getCheckInterval() {
    return this.#checkIntervalMs;
  }

  /**
   * Update check interval
   * @param {number} intervalMs - New interval in milliseconds
   */
  setCheckInterval(intervalMs) {
    this.#checkIntervalMs = intervalMs;
    if (this.#intervalHandle) {
      this.stop();
      this.start();
    }
  }
}
