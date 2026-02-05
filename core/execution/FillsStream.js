/**
 * FillsStream - Disk-backed fill storage for memory-constrained backtests
 *
 * Pattern: Buffered I/O (like DecisionLogger.js, RegimeLogger.js)
 *
 * Features:
 * - Buffered writes (default 100 fills)
 * - JSONL format (one JSON per line)
 * - BigInt serialization (ts_event → string)
 * - Sequential read-back for metrics
 *
 * Memory: ~1 MB buffer vs. ~190 MB for 1.9M fills in-memory
 */

import fs from 'fs';
import path from 'path';

export class FillsStream {
  #filePath;
  #buffer = [];
  #bufferSize;
  #stream = null;
  #fillCount = 0;
  #closed = false;

  /**
   * @param {string} filePath - Path to JSONL file
   * @param {number} [bufferSize=100] - Number of fills to buffer before flush
   */
  constructor(filePath, bufferSize = 100) {
    this.#filePath = filePath;
    this.#bufferSize = bufferSize;

    // Ensure temp directory exists
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    // Open write stream
    this.#stream = fs.createWriteStream(filePath, { flags: 'w' });
  }

  /**
   * Write a fill to stream (buffered)
   * @param {import('./fill.js').FillResult} fill - Fill to write
   */
  writeFill(fill) {
    if (this.#closed) {
      throw new Error('Cannot write to closed FillsStream');
    }

    // Serialize fill (BigInt → string for JSON)
    const serialized = {
      id: fill.id,
      orderId: fill.orderId,
      symbol: fill.symbol,
      side: fill.side,
      qty: fill.qty,
      fillPrice: fill.fillPrice,
      fillValue: fill.fillValue,
      fee: fill.fee,
      ts_event: fill.ts_event.toString()  // BigInt → string
    };

    this.#buffer.push(serialized);
    this.#fillCount++;

    if (this.#buffer.length >= this.#bufferSize) {
      this.flush();
    }
  }

  /**
   * Flush buffer to disk
   */
  flush() {
    if (this.#buffer.length === 0) return;

    const content = this.#buffer.map(f => JSON.stringify(f)).join('\n') + '\n';
    this.#stream.write(content);
    this.#buffer = [];
  }

  /**
   * Close stream (must call before reading)
   * @returns {Promise<void>}
   */
  close() {
    if (this.#closed) return Promise.resolve();

    this.flush();

    return new Promise((resolve, reject) => {
      this.#stream.end((err) => {
        if (err) {
          reject(err);
        } else {
          this.#closed = true;
          resolve();
        }
      });
    });
  }

  /**
   * Read all fills back from disk (for metrics computation)
   * @param {string} filePath - Path to JSONL file
   * @returns {import('./fill.js').FillResult[]} - Array of fills
   */
  static readFills(filePath) {
    if (!fs.existsSync(filePath)) {
      return [];
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.trim().split('\n').filter(l => l);

    return lines.map(line => {
      const fill = JSON.parse(line);
      // Convert ts_event back to BigInt
      fill.ts_event = BigInt(fill.ts_event);
      return fill;
    });
  }

  /**
   * Get total fill count
   * @returns {number}
   */
  getFillCount() {
    return this.#fillCount;
  }

  /**
   * Get file path
   * @returns {string}
   */
  getFilePath() {
    return this.#filePath;
  }
}
