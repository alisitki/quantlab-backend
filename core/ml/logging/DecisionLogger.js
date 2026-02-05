/**
 * DecisionLogger - ML decision logging system
 *
 * Logs ML predictions, confidence scores, and feature values
 * at inference time for analysis and debugging.
 */

import fs from 'fs';
import path from 'path';

/**
 * DecisionLogger singleton class
 */
class DecisionLoggerClass {
  #logPath = null;
  #buffer = [];
  #bufferSize = 100;
  #enabled = false;
  #stream = null;

  /**
   * Initialize the decision logger
   * @param {Object} config
   */
  init(config = {}) {
    const {
      logPath = process.env.DECISION_LOG_PATH || 'logs/decisions.jsonl',
      bufferSize = 100,
      enabled = process.env.DECISION_LOGGING_ENABLED !== '0'
    } = config;

    this.#logPath = logPath;
    this.#bufferSize = bufferSize;
    this.#enabled = enabled;

    if (this.#enabled) {
      // Ensure directory exists
      const dir = path.dirname(this.#logPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      // Open append stream
      this.#stream = fs.createWriteStream(this.#logPath, { flags: 'a' });
      console.log(`DecisionLogger initialized: ${this.#logPath}`);
    }
  }

  /**
   * Log a decision event
   * @param {Object} params
   */
  logDecision(params) {
    if (!this.#enabled) return;

    const {
      timestamp = Date.now(),
      symbol,
      features,
      prediction,
      confidence,
      threshold,
      signal,
      modelId = null,
      metadata = {}
    } = params;

    const entry = {
      ts: new Date(timestamp).toISOString(),
      ts_epoch: timestamp,
      symbol,
      features: this.#compactFeatures(features),
      pred: prediction,
      conf: confidence,
      thresh: threshold,
      signal,
      model_id: modelId,
      ...metadata
    };

    this.#buffer.push(JSON.stringify(entry));

    if (this.#buffer.length >= this.#bufferSize) {
      this.flush();
    }
  }

  /**
   * Compact features for logging (round to 6 decimal places)
   * @param {Object} features
   * @returns {Object}
   */
  #compactFeatures(features) {
    if (!features) return null;

    const compact = {};
    for (const [key, value] of Object.entries(features)) {
      if (typeof value === 'number') {
        compact[key] = Math.round(value * 1000000) / 1000000;
      } else {
        compact[key] = value;
      }
    }
    return compact;
  }

  /**
   * Flush buffer to disk
   */
  flush() {
    if (!this.#enabled || this.#buffer.length === 0) return;

    try {
      const content = this.#buffer.join('\n') + '\n';
      this.#stream.write(content);
      this.#buffer = [];
    } catch (err) {
      console.error('DecisionLogger flush error:', err.message);
    }
  }

  /**
   * Get recent decisions from log
   * @param {number} limit
   * @returns {Object[]}
   */
  getRecentDecisions(limit = 100) {
    if (!fs.existsSync(this.#logPath)) {
      return [];
    }

    try {
      const content = fs.readFileSync(this.#logPath, 'utf8');
      const lines = content.trim().split('\n').filter(l => l);
      const recent = lines.slice(-limit);
      return recent.map(line => JSON.parse(line));
    } catch (err) {
      console.error('DecisionLogger read error:', err.message);
      return [];
    }
  }

  /**
   * Get decision statistics
   * @param {number} limit - Number of recent decisions to analyze
   * @returns {Object}
   */
  getStats(limit = 1000) {
    const decisions = this.getRecentDecisions(limit);

    if (decisions.length === 0) {
      return { count: 0 };
    }

    const stats = {
      count: decisions.length,
      signals: {},
      avgConfidence: 0,
      confidenceDistribution: {
        low: 0,    // < 0.5
        medium: 0, // 0.5 - 0.7
        high: 0    // > 0.7
      }
    };

    let confSum = 0;

    for (const d of decisions) {
      // Signal counts
      stats.signals[d.signal] = (stats.signals[d.signal] || 0) + 1;

      // Confidence
      if (typeof d.conf === 'number') {
        confSum += d.conf;
        if (d.conf < 0.5) stats.confidenceDistribution.low++;
        else if (d.conf < 0.7) stats.confidenceDistribution.medium++;
        else stats.confidenceDistribution.high++;
      }
    }

    stats.avgConfidence = confSum / decisions.length;

    return stats;
  }

  /**
   * Close the logger
   */
  close() {
    this.flush();
    if (this.#stream) {
      this.#stream.end();
      this.#stream = null;
    }
  }

  /**
   * Check if logging is enabled
   * @returns {boolean}
   */
  isEnabled() {
    return this.#enabled;
  }
}

// Singleton instance
const DecisionLogger = new DecisionLoggerClass();

export { DecisionLogger };
export default DecisionLogger;
