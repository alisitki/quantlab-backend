/**
 * QuantLab Execution Engine v1 — Main Engine
 * Deterministic paper trading execution for backtesting.
 * 
 * EXECUTION v1 FINAL — DO NOT MODIFY CORE LOGIC
 * 
 * Fill Semantics (v1):
 * - Same-tick execution: order fills on the same event it was placed
 * - Zero-latency: no delay between order and fill
 * - Zero-slippage: fill at exact BBO price (BUY@ask, SELL@bid)
 * - Deterministic IDs: counter-based ord_N, fill_N
 * - No wall-clock dependency: only ts_event is used
 * 
 * Stream Requirement:
 * - Requires BBO stream with bid_price/ask_price fields
 * - Other stream types (trade, OHLC) will throw error
 */

import { createOrder, resetOrderCounter } from './order.js';
import { createFill, resetFillCounter, DEFAULT_FEE_RATE } from './fill.js';
import { ExecutionState } from './state.js';

/**
 * @typedef {Object} ExecutionEngineOptions
 * @property {number} [initialCapital=10000] - Starting capital
 * @property {number} [feeRate=0.0004] - Fee rate per trade
 * @property {boolean} [recordEquityCurve=true] - Whether to record equity on each event
 * @property {boolean} [requiresBbo=true] - If true, only BBO events are accepted (throws on other streams)
 * @property {boolean} [streamingMaxDD=false] - Enable streaming maxDD calculation (O(1) memory)
 * @property {boolean} [streamFills=false] - Enable fills streaming to disk
 * @property {string} [fillsStreamPath] - Custom path for fills stream
 */

/**
 * Deterministic paper trading execution engine
 */
export class ExecutionEngine {
  /** @type {ExecutionState} */
  #state;
  
  /** @type {number} */
  #feeRate;
  
  /** @type {boolean} */
  #recordEquityCurve;
  
  /** @type {number|null} Current bid price */
  #currentBid = null;
  
  /** @type {number|null} Current ask price */
  #currentAsk = null;
  
  /** @type {string|null} Current symbol from events */
  #currentSymbol = null;
  
  /** @type {bigint|null} Current event timestamp */
  #currentTs = null;
  
  /** @type {boolean} Require BBO stream (strict mode) */
  #requiresBbo;
  
  /** @type {number} Count of events processed (for order validation) */
  #eventCount = 0;

  // TODO: Add runId parameter for multi-run isolation (v2)
  
  /**
   * @param {ExecutionEngineOptions} [options={}]
   */
  constructor(options = {}) {
    const {
      initialCapital = 10000,
      feeRate = DEFAULT_FEE_RATE,
      recordEquityCurve = true,
      requiresBbo = true  // v1 default: strict BBO only
    } = options;

    // Feature flags (from options or environment variables)
    const enableStreamingMaxDD = options.streamingMaxDD ??
      (process.env.EXECUTION_STREAMING_MAXDD === '1');

    const enableStreamFills = options.streamFills ??
      (process.env.EXECUTION_STREAM_FILLS === '1');

    const fillsStreamPath = options.fillsStreamPath ??
      process.env.EXECUTION_FILLS_STREAM_PATH;

    // Initialize state with optimizations
    this.#state = new ExecutionState(initialCapital, {
      streamingMaxDD: enableStreamingMaxDD,
      streamFills: enableStreamFills,
      fillsStreamPath
    });

    this.#feeRate = feeRate;
    this.#recordEquityCurve = recordEquityCurve;
    this.#requiresBbo = requiresBbo;

    // Reset counters for determinism
    resetOrderCounter();
    resetFillCounter();
  }

  /**
   * Process a market event (updates current prices and unrealized PnL)
   * 
   * v1 STRICT MODE: Only accepts BBO stream events
   * - Requires bid_price and ask_price fields
   * - Throws error if fields are missing (prevents silent failures)
   * 
   * @param {Object} event - Replay event (must be BBO stream)
   * @throws {Error} If requiresBbo=true and event lacks bid_price/ask_price
   */
  onEvent(event) {
    const symbol = event.symbol;
    const ts_event = event.ts_event;

    // STRICT BBO VALIDATION (v1 safety guard)
    const hasBbo = event.bid_price !== undefined && event.ask_price !== undefined;
    
    if (this.#requiresBbo && !hasBbo) {
      throw new Error(
        'EXECUTION_ERROR: BBO stream required - event missing bid_price/ask_price. ' +
        'ExecutionEngine v1 only supports BBO stream for deterministic fills.'
      );
    }
    
    if (!hasBbo) {
      // Non-strict mode fallback (not recommended for v1)
      return;
    }
    
    // Extract BBO prices
    this.#currentBid = Number(event.bid_price);
    this.#currentAsk = Number(event.ask_price);
    
    // Validate prices are positive
    if (this.#currentBid <= 0 || this.#currentAsk <= 0) {
      throw new Error(
        `EXECUTION_ERROR: Invalid BBO prices - bid=${this.#currentBid}, ask=${this.#currentAsk}`
      );
    }

    this.#currentSymbol = symbol;
    this.#currentTs = ts_event;
    this.#eventCount++;

    // Update MTM for all positions with matching symbol (use mid price)
    const midPrice = (this.#currentBid + this.#currentAsk) / 2;
    if (symbol && this.#state.positions.has(symbol)) {
      this.#state.getPosition(symbol).updateMtm(midPrice);
    }

    // Record equity point if enabled
    if (this.#recordEquityCurve && ts_event !== undefined) {
      this.#state.recordEquity(ts_event);
    }
  }

  /**
   * Execute a market order (SAME-TICK, ZERO-LATENCY, ZERO-SLIPPAGE)
   * 
   * Fill Semantics:
   * - BUY: fills at current ask_price (crossing the spread)
   * - SELL: fills at current bid_price (crossing the spread)
   * - Execution is instant (same tick as order placement)
   * - No partial fills (v1)
   * 
   * @param {Object} orderIntent - Order intent from strategy
   * @param {string} orderIntent.symbol
   * @param {string} orderIntent.side - 'BUY' or 'SELL'
   * @param {number} orderIntent.qty
   * @returns {import('./fill.js').FillResult}
   * @throws {Error} If called before any event has been processed
   */
  onOrder(orderIntent) {
    // SAFETY GUARD: Ensure at least one event has been processed
    if (this.#eventCount === 0) {
      throw new Error(
        'EXECUTION_ERROR: Cannot place order before any event. ' +
        'onOrder() must be called after onEvent() has processed at least one BBO event.'
      );
    }
    
    // Validate we have valid BBO prices
    if (this.#currentBid === null || this.#currentAsk === null) {
      throw new Error('EXECUTION_ERROR: No valid BBO prices available');
    }

    const ts_event = orderIntent.ts_event ?? this.#currentTs;
    if (ts_event === undefined) {
      throw new Error('EXECUTION_ERROR: ts_event required for order');
    }

    // Create order with deterministic ID
    const order = createOrder({
      symbol: orderIntent.symbol,
      side: orderIntent.side,
      qty: orderIntent.qty,
      ts_event
    });

    // Determine fill price based on order side
    // BUY: pay the ask (crossing the spread)
    // SELL: receive the bid (crossing the spread)
    const fillPrice = order.side === 'BUY' ? this.#currentAsk : this.#currentBid;

    // Create fill at determined price
    const fill = createFill(order, fillPrice, this.#feeRate);

    // Update position
    const position = this.#state.getPosition(order.symbol);
    position.applyFill(fill);

    // Record fill
    this.#state.recordFill(fill);

    return fill;
  }

  /**
   * Get current state snapshot
   * @returns {import('./state.js').ExecutionStateSnapshot}
   */
  snapshot() {
    return this.#state.snapshot();
  }

  /**
   * Get current equity
   * @returns {number}
   */
  getEquity() {
    return this.#state.getEquity();
  }

  /**
   * Get total fills count
   * @returns {number}
   */
  getFillCount() {
    return this.#state.fills.length;
  }

  /**
   * Get the underlying state (for advanced access)
   * @returns {ExecutionState}
   */
  getState() {
    return this.#state;
  }
}
