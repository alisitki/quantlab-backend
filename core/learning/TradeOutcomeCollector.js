/**
 * Trade Outcome Collector
 *
 * Captures feature vectors and regime state at trade entry/exit for closed-loop learning.
 * Follows DecisionLogger JSONL pattern for append-only logging.
 *
 * Usage:
 *   const collector = new TradeOutcomeCollector();
 *   collector.recordEntry(tradeId, { features, regime, edgeId, direction, price, timestamp });
 *   // ... later ...
 *   const outcome = collector.recordExit(tradeId, { price, timestamp, pnl, exitReason });
 *
 * JSONL Format (one line per outcome):
 * {
 *   "tradeId": "t_123",
 *   "edgeId": "discovered_threshold_5",
 *   "direction": "LONG",
 *   "entryPrice": 0.4523,
 *   "entryTimestamp": 1738800000000,
 *   "entryFeatures": {"liquidity_pressure": 0.72, "volatility_ratio": 1.3, ...},
 *   "entryRegime": {"cluster": 2, "volatility": "high"},
 *   "exitPrice": 0.4531,
 *   "exitTimestamp": 1738800060000,
 *   "pnl": 0.0008,
 *   "exitReason": "signal_exit",
 *   "holdingPeriodMs": 60000
 * }
 */

import fs from 'node:fs';
import path from 'node:path';
import { createWriteStream } from 'node:fs';
import { LEARNING_CONFIG } from './config.js';

export class TradeOutcomeCollector {
  constructor(config = {}) {
    this.logDir = config.logDir || LEARNING_CONFIG.outcome.logDir;
    this.flushIntervalMs = config.flushIntervalMs || LEARNING_CONFIG.outcome.flushIntervalMs;
    this.featureDecimals = config.featureDecimals || LEARNING_CONFIG.outcome.featureDecimals;
    this.maxFileSize = config.maxFileSize || LEARNING_CONFIG.outcome.maxFileSize;

    this.buffer = [];
    this.pendingTrades = new Map();  // tradeId → entrySnapshot
    this.stream = null;
    this.currentFile = null;
    this.flushTimer = null;
    this.bytesFlushed = 0;
  }

  /**
   * Record trade entry with feature snapshot
   * @param {string} tradeId - Unique trade ID
   * @param {Object} data - Entry data
   */
  recordEntry(tradeId, { features, regime, edgeId, direction, price, timestamp }) {
    const entrySnapshot = {
      tradeId,
      edgeId,
      direction,
      entryPrice: price,
      entryTimestamp: timestamp,
      entryFeatures: this.#compactFeatures(features),
      entryRegime: regime
    };

    this.pendingTrades.set(tradeId, entrySnapshot);
  }

  /**
   * Record trade exit and complete outcome
   * @param {string} tradeId - Unique trade ID
   * @param {Object} data - Exit data
   * @returns {Object|null} - Completed outcome or null if entry not found
   */
  recordExit(tradeId, { price, timestamp, pnl, exitReason }) {
    const entry = this.pendingTrades.get(tradeId);
    if (!entry) {
      console.warn(`TradeOutcomeCollector: No entry found for trade ${tradeId}`);
      return null;
    }

    const outcome = {
      ...entry,
      exitPrice: price,
      exitTimestamp: timestamp,
      pnl,
      exitReason,
      holdingPeriodMs: timestamp - entry.entryTimestamp
    };

    this.pendingTrades.delete(tradeId);
    this.#appendToLog(outcome);

    return outcome;
  }

  /**
   * Read outcomes from JSONL file
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Array of outcomes
   */
  async readOutcomes(options = {}) {
    const { since, edgeId, limit } = options;

    await this.flush();  // Ensure buffer is written

    if (!this.currentFile || !fs.existsSync(this.currentFile)) {
      return [];
    }

    const content = await fs.promises.readFile(this.currentFile, 'utf-8');
    const lines = content.trim().split('\n').filter(line => line.length > 0);

    let outcomes = lines.map(line => {
      try {
        return JSON.parse(line);
      } catch (err) {
        console.warn('Failed to parse outcome line:', err.message);
        return null;
      }
    }).filter(Boolean);

    // Filter by timestamp
    if (since) {
      outcomes = outcomes.filter(o => o.exitTimestamp >= since);
    }

    // Filter by edgeId
    if (edgeId) {
      outcomes = outcomes.filter(o => o.edgeId === edgeId);
    }

    // Limit
    if (limit) {
      outcomes = outcomes.slice(-limit);
    }

    return outcomes;
  }

  /**
   * Flush buffered outcomes to disk
   * @returns {Promise<void>}
   */
  async flush() {
    if (this.buffer.length === 0) return;

    await this.#ensureStream();

    // Build content
    const lines = this.buffer.map(outcome => JSON.stringify(outcome) + '\n');
    const content = lines.join('');

    this.bytesFlushed += Buffer.byteLength(content);
    this.buffer = [];

    // Write to stream - use synchronous appendFileSync for reliability
    // (streams can buffer internally and not immediately write to disk)
    await fs.promises.appendFile(this.currentFile, content);
  }

  /**
   * Close stream and flush remaining data
   * @returns {Promise<void>}
   */
  async close() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    await this.flush();

    if (this.stream) {
      return new Promise((resolve) => {
        this.stream.end(() => {
          this.stream = null;
          resolve();
        });
      });
    }
  }

  /**
   * Append outcome to buffer and schedule flush
   * @private
   */
  #appendToLog(outcome) {
    this.buffer.push(outcome);

    // Auto-flush if buffer is large
    if (this.buffer.length >= 100) {
      this.flush().catch(err => {
        console.error('TradeOutcomeCollector flush error:', err);
      });
    }

    // Schedule periodic flush
    if (!this.flushTimer) {
      this.flushTimer = setInterval(() => {
        this.flush().catch(err => {
          console.error('TradeOutcomeCollector periodic flush error:', err);
        });
      }, this.flushIntervalMs);

      // Allow Node.js to exit if this is the only timer
      this.flushTimer.unref();
    }
  }

  /**
   * Ensure stream is open and rotate if needed
   * @private
   */
  async #ensureStream() {
    // Check if rotation needed
    if (this.stream && this.bytesFlushed >= this.maxFileSize) {
      await this.close();
    }

    if (!this.stream) {
      // Create log directory
      await fs.promises.mkdir(this.logDir, { recursive: true });

      // Create new file with timestamp
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      this.currentFile = path.join(this.logDir, `outcomes-${timestamp}.jsonl`);

      this.stream = createWriteStream(this.currentFile, { flags: 'a' });
      this.bytesFlushed = 0;
    }
  }

  /**
   * Compact features to reduce file size
   * @private
   */
  #compactFeatures(features) {
    const compacted = {};

    for (const [key, value] of Object.entries(features)) {
      if (typeof value === 'number') {
        compacted[key] = parseFloat(value.toFixed(this.featureDecimals));
      } else {
        compacted[key] = value;
      }
    }

    return compacted;
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    return {
      pendingTrades: this.pendingTrades.size,
      bufferedOutcomes: this.buffer.length,
      currentFile: this.currentFile,
      bytesFlushed: this.bytesFlushed
    };
  }
}
