/**
 * QuantLab Evaluation Engine — Deterministic Metrics
 * 
 * Computes performance metrics from ExecutionEngine snapshots.
 * Ensures numerical stability (1e-8 rounding) and ts_event-based time calculations.
 */

/**
 * @typedef {Object} EvaluationResults
 * @property {number} equity_start
 * @property {number} equity_end
 * @property {number} pnl_abs
 * @property {number} pnl_pct
 * @property {number} max_drawdown_pct
 * @property {number} win_rate
 * @property {number} trades_count
 * @property {number} avg_trade_pnl
 * @property {number} avg_holding_time_sec
 * @property {number} fees_total
 * @property {number} exposure_time_pct
 * @property {number} sharpe_like
 */

export class EvalEngine {
  static ROUND_FACTOR = 1e8;

  /**
   * Round to 8 decimal places for determinism
   */
  static round(val) {
    if (typeof val !== 'number' || !isFinite(val)) return 0;
    return Math.round(val * EvalEngine.ROUND_FACTOR) / EvalEngine.ROUND_FACTOR;
  }

  /**
   * Compute full metrics suite from execution snapshot
   * @param {import('../execution/state.js').ExecutionStateSnapshot} snapshot
   * @param {number} initialCapital
   * @returns {EvaluationResults}
   */
  static computeResults(snapshot, initialCapital) {
    const fills = snapshot.fills;
    const equityCurve = snapshot.equityCurve;
    
    // 1. Returns
    const equity_start = initialCapital;
    const equity_end = snapshot.equity;
    const pnl_abs = equity_end - equity_start;
    const pnl_pct = pnl_abs / equity_start;

    // 2. Max Drawdown (Event-Level)
    let peak = equity_start;
    let maxDd = 0;
    for (const point of equityCurve) {
      if (point.equity > peak) peak = point.equity;
      const dd = (point.equity - peak) / peak;
      if (dd < maxDd) maxDd = dd;
    }

    // 3. Trade Metrics (Round-Trips)
    const roundTrips = this.computeRoundTrips(fills);
    const trades_count = roundTrips.length;
    const wins = roundTrips.filter(rt => rt.pnl > 0).length;
    const win_rate = trades_count > 0 ? wins / trades_count : 0;
    const total_trade_pnl = roundTrips.reduce((sum, rt) => sum + rt.pnl, 0);
    const avg_trade_pnl = trades_count > 0 ? total_trade_pnl / trades_count : 0;

    // 4. Time-based Metrics (ts_event basis)
    let total_holding_time_ms = 0n;
    for (const rt of roundTrips) {
      total_holding_time_ms += rt.exit_ts - rt.entry_ts;
    }
    const avg_holding_time_sec = trades_count > 0 ? Number(total_holding_time_ms / BigInt(trades_count)) / 1000 : 0;

    // Exposure Time
    let total_exposure_ms = 0n;
    if (equityCurve.length > 1) {
      const start_ts = BigInt(equityCurve[0].ts_event);
      const end_ts = BigInt(equityCurve[equityCurve.length - 1].ts_event);
      const total_time_ms = end_ts - start_ts;
      
      // Simplified exposure: time between entry and exit of each round trip
      // (This assumes single asset for v1)
      total_exposure_ms = total_holding_time_ms;
      var exposure_time_pct = total_time_ms > 0n ? Number(total_exposure_ms) / Number(total_time_ms) : 0;
    } else {
      var exposure_time_pct = 0;
    }

    // 5. Risk-Adjusted (Sharpe-like)
    // Sharpe = avg_return / std_return. Here we use a simpler Return / MaxDD or similar.
    // User asked for "sharpe_like (basit)". Let's use Return / Abs(MaxDD).
    const sharpe_like = maxDd !== 0 ? pnl_pct / Math.abs(maxDd) : 0;

    const fees_total = fills.reduce((sum, f) => sum + f.fee, 0);

    return {
      equity_start: this.round(equity_start),
      equity_end: this.round(equity_end),
      pnl_abs: this.round(pnl_abs),
      pnl_pct: this.round(pnl_pct),
      max_drawdown_pct: this.round(maxDd),
      win_rate: this.round(win_rate),
      trades_count,
      avg_trade_pnl: this.round(avg_trade_pnl),
      avg_holding_time_sec: this.round(avg_holding_time_sec),
      fees_total: this.round(fees_total),
      exposure_time_pct: this.round(exposure_time_pct),
      sharpe_like: this.round(sharpe_like)
    };
  }

  /**
   * Helper to pair fills into round-trips
   */
  static computeRoundTrips(fills) {
    const roundTrips = [];
    const openPositions = new Map(); // symbol -> { qty, entry_ts, costBasis, fees }

    for (const fill of fills) {
      const symbol = fill.symbol;
      const side = fill.side;
      const qty = fill.qty;
      const ts = BigInt(fill.ts_event);

      if (!openPositions.has(symbol)) {
        openPositions.set(symbol, { qty: 0, entries: [] });
      }

      const pos = openPositions.get(symbol);

      if (side === 'BUY') {
        if (pos.qty < 0) {
          // Closing some short
          const closingQty = Math.min(Math.abs(pos.qty), qty);
          // For evaluation v1: we assume FIFO for holding time
          const entry = pos.entries.shift();
          roundTrips.push({
            symbol,
            entry_ts: entry.ts,
            exit_ts: ts,
            pnl: (entry.price - fill.fillPrice) * closingQty - (entry.fee * (closingQty/entry.qty)) - fill.fee // Simplistic
          });
          pos.qty += closingQty;
        } else {
          pos.entries.push({ ts, qty, price: fill.fillPrice, fee: fill.fee });
          pos.qty += qty;
        }
      } else {
        // SELL
        if (pos.qty > 0) {
          const closingQty = Math.min(pos.qty, qty);
          const entry = pos.entries.shift();
          roundTrips.push({
            symbol,
            entry_ts: entry.ts,
            exit_ts: ts,
            pnl: (fill.fillPrice - entry.price) * closingQty - (entry.fee * (closingQty/entry.qty)) - fill.fee
          });
          pos.qty -= closingQty;
        } else {
          pos.entries.push({ ts, qty, price: fill.fillPrice, fee: fill.fee });
          pos.qty -= qty;
        }
      }
    }
    return roundTrips;
  }
}
