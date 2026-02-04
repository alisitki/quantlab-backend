/**
 * AutoResponder: Automatic incident response based on alert patterns.
 *
 * Features:
 * - Alert -> Incident creation
 * - Severity-based runbook triggering
 * - Cooldown to prevent duplicate incidents
 * - Configurable response rules
 */
import { getIncidentManager, SEVERITY_LEVELS } from './IncidentManager.js';
import { RunbookExecutor } from '../runbook/index.js';
import path from 'path';

/**
 * Default response rules: alert type -> runbook mapping
 */
const DEFAULT_RESPONSE_RULES = {
  // Critical alerts
  'kill_switch_activated': {
    severity: SEVERITY_LEVELS.CRITICAL,
    runbook: null, // No auto-runbook, requires human
    createIncident: true
  },
  'exchange_unhealthy': {
    severity: SEVERITY_LEVELS.CRITICAL,
    runbook: 'emergency-recovery',
    createIncident: true,
    cooldownMs: 10 * 60 * 1000 // 10 min cooldown
  },
  'bridge_error': {
    severity: SEVERITY_LEVELS.ERROR,
    runbook: 'service-restart',
    runbookVariables: {
      service: 'strategyd',
      healthUrl: 'http://localhost:3031/health',
      type: 'systemd',
      unit: 'quantlab-strategyd.service'
    },
    createIncident: true,
    cooldownMs: 5 * 60 * 1000
  },
  'position_mismatch': {
    severity: SEVERITY_LEVELS.ERROR,
    runbook: null,
    createIncident: true
  },
  'slo_breached': {
    severity: SEVERITY_LEVELS.WARNING,
    runbook: null,
    createIncident: true,
    cooldownMs: 30 * 60 * 1000
  },
  'budget_exceeded': {
    severity: SEVERITY_LEVELS.WARNING,
    runbook: null,
    createIncident: true,
    cooldownMs: 60 * 60 * 1000 // 1 hour
  },
  'service_unhealthy': {
    severity: SEVERITY_LEVELS.ERROR,
    runbook: 'service-restart',
    createIncident: true,
    cooldownMs: 5 * 60 * 1000
  }
};

export class AutoResponder {
  #incidentManager;
  #runbookExecutor;
  #rules;
  #cooldowns; // Map<ruleKey, lastTriggeredAt>
  #enabled;
  #dryRun;

  constructor(options = {}) {
    this.#incidentManager = options.incidentManager || getIncidentManager();
    this.#runbookExecutor = options.runbookExecutor || new RunbookExecutor({
      templatesDir: path.resolve(process.cwd(), 'core/runbook/templates'),
      logPath: path.resolve(process.cwd(), 'logs/runbook.jsonl')
    });
    this.#rules = { ...DEFAULT_RESPONSE_RULES, ...options.rules };
    this.#cooldowns = new Map();
    this.#enabled = options.enabled ?? true;
    this.#dryRun = options.dryRun ?? false;
  }

  /**
   * Enable/disable auto-response.
   */
  setEnabled(enabled) {
    this.#enabled = enabled;
    console.log(`[AutoResponder] ${enabled ? 'Enabled' : 'Disabled'}`);
  }

  /**
   * Set dry-run mode.
   */
  setDryRun(dryRun) {
    this.#dryRun = dryRun;
  }

  /**
   * Process an alert and trigger appropriate response.
   * @param {Object} alert - Alert object
   * @returns {Promise<Object>} Response result
   */
  async processAlert(alert) {
    const { type, severity, message, timestamp, metadata } = alert;

    if (!this.#enabled) {
      return { action: 'skipped', reason: 'auto-response disabled' };
    }

    // Find matching rule
    const rule = this.#rules[type];
    if (!rule) {
      return { action: 'skipped', reason: 'no matching rule' };
    }

    // Check cooldown
    const cooldownKey = `${type}:${metadata?.symbol || 'global'}`;
    if (rule.cooldownMs) {
      const lastTriggered = this.#cooldowns.get(cooldownKey);
      if (lastTriggered && (Date.now() - lastTriggered) < rule.cooldownMs) {
        const remainingMs = rule.cooldownMs - (Date.now() - lastTriggered);
        return {
          action: 'skipped',
          reason: 'cooldown active',
          cooldownRemainingMs: remainingMs
        };
      }
    }

    // Update cooldown
    this.#cooldowns.set(cooldownKey, Date.now());

    const result = {
      alert: { type, severity, message },
      incident: null,
      runbook: null
    };

    // Create incident if configured
    if (rule.createIncident) {
      const incident = await this.#incidentManager.createIncident({
        severity: rule.severity || severity,
        title: this.#generateTitle(type, metadata),
        description: message,
        source: 'auto-responder',
        alertId: alert.id || null
      });
      result.incident = {
        id: incident.id,
        state: incident.state
      };

      // Trigger runbook if configured
      if (rule.runbook) {
        try {
          const runbookResult = await this.#triggerRunbook(
            incident.id,
            rule.runbook,
            rule.runbookVariables || metadata || {}
          );
          result.runbook = runbookResult;
        } catch (err) {
          console.error(`[AutoResponder] Runbook trigger failed: ${err.message}`);
          result.runbook = { error: err.message };
        }
      }
    }

    return {
      action: 'responded',
      ...result
    };
  }

  /**
   * Trigger a runbook for an incident.
   */
  async #triggerRunbook(incidentId, runbookName, variables) {
    console.log(`[AutoResponder] Triggering runbook ${runbookName} for incident ${incidentId}`);

    // Load runbook
    const runbook = await this.#runbookExecutor.loadRunbook(runbookName);

    // Record trigger
    const executionId = `exec-${Date.now()}`;
    await this.#incidentManager.recordRunbookTrigger(incidentId, {
      runbook: runbookName,
      executionId,
      auto: true
    });

    // Execute (or dry-run)
    const execResult = await this.#runbookExecutor.execute(runbook, {
      dryRun: this.#dryRun,
      variables
    });

    // Record completion
    await this.#incidentManager.recordRunbookCompletion(incidentId, {
      executionId,
      status: execResult.status,
      stepsCompleted: execResult.stepsCompleted,
      stepsFailed: execResult.stepsFailed
    });

    return {
      executionId,
      runbook: runbookName,
      status: execResult.status,
      dryRun: this.#dryRun,
      stepsCompleted: execResult.stepsCompleted,
      stepsFailed: execResult.stepsFailed
    };
  }

  /**
   * Generate incident title from alert type and metadata.
   */
  #generateTitle(type, metadata) {
    const titles = {
      'kill_switch_activated': 'Kill Switch Activated',
      'exchange_unhealthy': `Exchange ${metadata?.exchange || 'Unknown'} Unhealthy`,
      'bridge_error': 'Bridge Execution Error',
      'position_mismatch': `Position Mismatch: ${metadata?.symbol || 'Unknown'}`,
      'slo_breached': `SLO Breached: ${metadata?.sloId || 'Unknown'}`,
      'budget_exceeded': 'GPU Budget Exceeded',
      'service_unhealthy': `Service Unhealthy: ${metadata?.service || 'Unknown'}`
    };

    return titles[type] || `Alert: ${type}`;
  }

  /**
   * Manually trigger a runbook for an existing incident.
   * @param {string} incidentId - Incident ID
   * @param {string} runbookName - Runbook name
   * @param {Object} variables - Variables for runbook
   * @returns {Promise<Object>} Execution result
   */
  async triggerManualRunbook(incidentId, runbookName, variables = {}) {
    const incident = await this.#incidentManager.get(incidentId);
    if (!incident) {
      throw new Error(`Incident ${incidentId} not found`);
    }

    console.log(`[AutoResponder] Manual runbook ${runbookName} for incident ${incidentId}`);

    // Load runbook
    const runbook = await this.#runbookExecutor.loadRunbook(runbookName);

    // Record trigger
    const executionId = `exec-${Date.now()}`;
    await this.#incidentManager.recordRunbookTrigger(incidentId, {
      runbook: runbookName,
      executionId,
      auto: false
    });

    // Execute
    const execResult = await this.#runbookExecutor.execute(runbook, {
      dryRun: this.#dryRun,
      variables
    });

    // Record completion
    await this.#incidentManager.recordRunbookCompletion(incidentId, {
      executionId,
      status: execResult.status,
      stepsCompleted: execResult.stepsCompleted,
      stepsFailed: execResult.stepsFailed
    });

    return {
      executionId,
      runbook: runbookName,
      status: execResult.status,
      dryRun: this.#dryRun,
      stepsCompleted: execResult.stepsCompleted,
      stepsFailed: execResult.stepsFailed,
      steps: execResult.steps
    };
  }

  /**
   * Get current response rules.
   */
  getRules() {
    return { ...this.#rules };
  }

  /**
   * Update a response rule.
   */
  updateRule(alertType, rule) {
    this.#rules[alertType] = { ...this.#rules[alertType], ...rule };
  }

  /**
   * Clear cooldowns (for testing).
   */
  clearCooldowns() {
    this.#cooldowns.clear();
  }

  /**
   * Get current state.
   */
  getState() {
    return {
      enabled: this.#enabled,
      dryRun: this.#dryRun,
      rulesCount: Object.keys(this.#rules).length,
      activeCooldowns: Array.from(this.#cooldowns.entries()).map(([key, ts]) => ({
        key,
        triggeredAt: new Date(ts).toISOString(),
        remainingMs: Math.max(0, (this.#rules[key.split(':')[0]]?.cooldownMs || 0) - (Date.now() - ts))
      }))
    };
  }
}

/**
 * Singleton instance
 */
let instance = null;
export function getAutoResponder() {
  if (!instance) {
    instance = new AutoResponder();
  }
  return instance;
}
