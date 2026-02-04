/**
 * AlertManager — Centralized alert management with multiple channels.
 *
 * Supports:
 * - Slack webhook notifications
 * - File logging (JSONL)
 * - Console output
 *
 * Usage:
 *   import { alertManager } from './AlertManager.js';
 *   alertManager.send({
 *     type: 'KILL_SWITCH_ACTIVATED',
 *     severity: 'critical',
 *     message: 'Kill switch activated by operator',
 *     metadata: { ip: '192.168.1.1' }
 *   });
 *
 * Environment:
 *   SLACK_WEBHOOK_URL - Slack incoming webhook URL
 *   SLACK_ALERTS_ENABLED - Set to '1' to enable Slack alerts
 *   ALERT_LOG_PATH - Path to alert log file (default: logs/alerts.jsonl)
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

// Alert severity levels
export const AlertSeverity = {
  INFO: 'info',
  WARNING: 'warning',
  ERROR: 'error',
  CRITICAL: 'critical'
};

// Alert types
export const AlertType = {
  // System alerts
  KILL_SWITCH_ACTIVATED: 'KILL_SWITCH_ACTIVATED',
  KILL_SWITCH_DEACTIVATED: 'KILL_SWITCH_DEACTIVATED',

  // Live run alerts
  LIVE_RUN_STARTED: 'LIVE_RUN_STARTED',
  LIVE_RUN_STOPPED: 'LIVE_RUN_STOPPED',
  LIVE_RUN_ERROR: 'LIVE_RUN_ERROR',

  // Guard alerts
  GUARD_TRIGGERED: 'GUARD_TRIGGERED',
  BUDGET_EXCEEDED: 'BUDGET_EXCEEDED',
  RISK_REJECTION: 'RISK_REJECTION',

  // ML alerts
  ML_MODEL_PROMOTED: 'ML_MODEL_PROMOTED',
  ML_TRAINING_FAILED: 'ML_TRAINING_FAILED',

  // Infrastructure alerts
  SERVICE_DOWN: 'SERVICE_DOWN',
  SERVICE_RECOVERED: 'SERVICE_RECOVERED',
  WEBSOCKET_DISCONNECTED: 'WEBSOCKET_DISCONNECTED',

  // Scheduler alerts
  CRON_FAILURE: 'CRON_FAILURE',
  RETENTION_DRIFT: 'RETENTION_DRIFT'
};

// Severity to Slack color mapping
const SEVERITY_COLORS = {
  info: '#36a64f',      // Green
  warning: '#ffcc00',   // Yellow
  error: '#ff6600',     // Orange
  critical: '#ff0000'   // Red
};

// Severity to Slack emoji mapping
const SEVERITY_EMOJI = {
  info: ':information_source:',
  warning: ':warning:',
  error: ':x:',
  critical: ':rotating_light:'
};

/**
 * @typedef {Object} AlertPayload
 * @property {string} type - Alert type (from AlertType)
 * @property {string} severity - Alert severity (from AlertSeverity)
 * @property {string} message - Human-readable message
 * @property {Object} [metadata] - Additional context
 * @property {string} [source] - Source service/component
 */

class AlertManager {
  #slackWebhookUrl = null;
  #slackEnabled = false;
  #logPath = 'logs/alerts.jsonl';
  #consoleEnabled = true;

  constructor() {
    this.#slackWebhookUrl = process.env.SLACK_WEBHOOK_URL || null;
    this.#slackEnabled = process.env.SLACK_ALERTS_ENABLED === '1' && !!this.#slackWebhookUrl;
    this.#logPath = process.env.ALERT_LOG_PATH || 'logs/alerts.jsonl';
    this.#consoleEnabled = process.env.ALERT_CONSOLE_ENABLED !== '0';

    // Ensure log directory exists
    const logDir = path.dirname(this.#logPath);
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
  }

  /**
   * Send an alert to all configured channels.
   *
   * @param {AlertPayload} payload - Alert payload
   * @returns {Promise<Object>} Send results
   */
  async send(payload) {
    const alert = this.#buildAlert(payload);
    const results = {
      alert_id: alert.alert_id,
      channels: {}
    };

    // File logging (always enabled)
    try {
      this.#writeToFile(alert);
      results.channels.file = 'ok';
    } catch (err) {
      results.channels.file = `error: ${err.message}`;
    }

    // Console output
    if (this.#consoleEnabled) {
      this.#writeToConsole(alert);
      results.channels.console = 'ok';
    }

    // Slack webhook
    if (this.#slackEnabled) {
      try {
        await this.#sendToSlack(alert);
        results.channels.slack = 'ok';
      } catch (err) {
        results.channels.slack = `error: ${err.message}`;
      }
    }

    return results;
  }

  /**
   * Build standardized alert object.
   */
  #buildAlert(payload) {
    const timestamp = new Date().toISOString();
    const alertId = crypto.createHash('sha256')
      .update(`${payload.type}:${timestamp}:${payload.message}`)
      .digest('hex')
      .substring(0, 16);

    return {
      alert_id: alertId,
      type: payload.type || 'UNKNOWN',
      severity: payload.severity || AlertSeverity.INFO,
      message: payload.message || 'No message provided',
      timestamp,
      source: payload.source || process.env.SERVICE_NAME || 'quantlab',
      metadata: payload.metadata || {},
      environment: process.env.NODE_ENV || 'development'
    };
  }

  /**
   * Write alert to JSONL file.
   */
  #writeToFile(alert) {
    const line = JSON.stringify(alert) + '\n';
    fs.appendFileSync(this.#logPath, line, 'utf8');
  }

  /**
   * Write alert to console.
   */
  #writeToConsole(alert) {
    const severity = alert.severity.toUpperCase();
    const prefix = `[ALERT:${severity}]`;

    console.error('='.repeat(60));
    console.error(`${prefix} ${alert.type}`);
    console.error(`Alert ID: ${alert.alert_id}`);
    console.error(`Message: ${alert.message}`);
    console.error(`Time: ${alert.timestamp}`);
    console.error(`Source: ${alert.source}`);
    if (Object.keys(alert.metadata).length > 0) {
      console.error(`Metadata: ${JSON.stringify(alert.metadata)}`);
    }
    console.error('='.repeat(60));
  }

  /**
   * Send alert to Slack webhook.
   */
  async #sendToSlack(alert) {
    const color = SEVERITY_COLORS[alert.severity] || SEVERITY_COLORS.info;
    const emoji = SEVERITY_EMOJI[alert.severity] || SEVERITY_EMOJI.info;

    const slackPayload = {
      attachments: [{
        color,
        blocks: [
          {
            type: 'header',
            text: {
              type: 'plain_text',
              text: `${emoji} ${alert.type}`,
              emoji: true
            }
          },
          {
            type: 'section',
            text: {
              type: 'mrkdwn',
              text: alert.message
            }
          },
          {
            type: 'context',
            elements: [
              {
                type: 'mrkdwn',
                text: `*Severity:* ${alert.severity} | *Source:* ${alert.source} | *ID:* ${alert.alert_id}`
              }
            ]
          },
          {
            type: 'context',
            elements: [
              {
                type: 'mrkdwn',
                text: `*Time:* ${alert.timestamp}`
              }
            ]
          }
        ]
      }]
    };

    // Add metadata if present
    if (Object.keys(alert.metadata).length > 0) {
      const metadataText = Object.entries(alert.metadata)
        .map(([k, v]) => `*${k}:* ${typeof v === 'object' ? JSON.stringify(v) : v}`)
        .join(' | ');

      slackPayload.attachments[0].blocks.push({
        type: 'context',
        elements: [
          {
            type: 'mrkdwn',
            text: metadataText
          }
        ]
      });
    }

    const response = await fetch(this.#slackWebhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(slackPayload)
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Slack webhook failed: ${response.status} ${text}`);
    }
  }

  /**
   * Check if Slack alerts are enabled.
   */
  isSlackEnabled() {
    return this.#slackEnabled;
  }

  /**
   * Get alert configuration status.
   */
  getStatus() {
    return {
      slack_enabled: this.#slackEnabled,
      slack_configured: !!this.#slackWebhookUrl,
      log_path: this.#logPath,
      console_enabled: this.#consoleEnabled
    };
  }
}

// Singleton instance
export const alertManager = new AlertManager();

// Convenience function for sending alerts
export function sendAlert(payload) {
  return alertManager.send(payload);
}

export default AlertManager;
