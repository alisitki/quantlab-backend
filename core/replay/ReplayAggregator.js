/**
 * QuantLab Replay Aggregator
 * 
 * Aggregates tick-level events into snapshots or filtered streams.
 * Crucial for speeding up evaluation runs (10-100x).
 * 
 * Logic:
 *   - '1s': Emits the latest event in each 1000ms window.
 *   - 'trade-only': (Planned if stream contains trades)
 */

export class ReplayAggregator {
  /** @type {string|null} */
  #mode;
  /** @type {Map<string, Object>} last record per symbol */
  #buffer = new Map();
  /** @type {bigint|null} current bucket starting ts */
  #currentIntervalStart = null;
  /** @type {bigint} 1s in ms */
  #intervalMs = 1000n;

  /**
   * @param {string} mode - '1s', 'trade-only', or null
   */
  constructor(mode) {
    this.#mode = mode;
  }

  /**
   * Process a row and optionally return an aggregated record
   * @param {Object} row
   * @yields {Object}
   */
  async *process(row) {
    if (!this.#mode) {
      yield row;
      return;
    }

    if (this.#mode === '1s') {
      const ts = BigInt(row.ts_event);
      const symbol = row.symbol || 'default';

      // First call
      if (this.#currentIntervalStart === null) {
        this.#currentIntervalStart = ts - (ts % this.#intervalMs);
      }

      // Check if we crossed the interval boundary
      if (ts >= this.#currentIntervalStart + this.#intervalMs) {
        // Yield buffered records for the previous interval(s)
        for (const buffered of this.#buffer.values()) {
          yield buffered;
        }
        this.#buffer.clear();
        
        // Move interval forward
        this.#currentIntervalStart = ts - (ts % this.#intervalMs);
      }

      // Buffer the latest for this second
      this.#buffer.set(symbol, row);
    } else if (this.#mode === 'trade-only') {
      // Logic: Only emit if it's a trade event.
      // In BBO dataset, there are no trades, so this might be for specialized datasets.
      // v1: bypass if not a known trade field
      if (row.trade_price !== undefined || row.side !== undefined) {
         yield row;
      }
    } else {
      yield row;
    }
  }

  /**
   * Flush any remaining buffered records
   */
  async *flush() {
    if (this.#mode === '1s' && this.#buffer.size > 0) {
      for (const buffered of this.#buffer.values()) {
        yield buffered;
      }
      this.#buffer.clear();
    }
  }
}
