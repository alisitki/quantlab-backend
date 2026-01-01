import crypto from 'node:crypto';

/**
 * FeatureBuilderV1: Deterministic BBO feature and label generator.
 * Follows Contract v1 specification.
 */
export class FeatureBuilderV1 {
  static VERSION = 'v1';

  /**
   * Process raw BBO rows and return featured rows.
   * @param {Array<Object>} rows - Raw BBO rows (bid_price, ask_price, bid_qty, ask_qty, ts_event, seq)
   * @returns {Array<Object>} - Processed rows
   */
  process(rows) {
    if (!rows || rows.length === 0) return [];

    // 1. Deterministic Sort: ts_event ASC, seq ASC
    // Fallback: Stable sort (Node.js Array.sort is stable)
    const sorted = [...rows].sort((a, b) => {
      const tsA = BigInt(a.ts_event);
      const tsB = BigInt(b.ts_event);
      if (tsA !== tsB) return tsA < tsB ? -1 : 1;
      
      if (a.seq !== undefined && b.seq !== undefined) {
        const seqA = BigInt(a.seq);
        const seqB = BigInt(b.seq);
        if (seqA !== seqB) return seqA < seqB ? -1 : 1;
      }
      return 0;
    });

    const n = sorted.length;
    const mids = sorted.map(r => (Number(r.bid_price) + Number(r.ask_price)) / 2);
    const ts = sorted.map(r => Number(r.ts_event));

    // Pre-calculate ret_1s for volatility calculation
    const ret1s = new Array(n);
    for (let i = 0; i < n; i++) {
      ret1s[i] = this.#calculateReturn(ts, mids, i, ts[i] - 1000);
    }

    const result = [];
    
    // Sliding window indices for volatility
    let volWindowStart = 0;
    let volSum = 0;
    let volSumSq = 0;
    let volCount = 0;

    const firstTs = ts[0];

    for (let i = 0; i < n; i++) {
      const currentTs = ts[i];
      const mid = mids[i];
      const row = sorted[i];

      // Volatility: std_dev(ret_1s, window=10s)
      // Update sliding window even for cold-start rows to ensure window is warm when we start
      while (volWindowStart < i && ts[volWindowStart] < currentTs - 10000) {
        const val = ret1s[volWindowStart];
        if (!isNaN(val)) {
          volSum -= val;
          volSumSq -= val * val;
          volCount--;
        }
        volWindowStart++;
      }
      const currentRet1s = ret1s[i];
      if (!isNaN(currentRet1s)) {
        volSum += currentRet1s;
        volSumSq += currentRet1s * currentRet1s;
        volCount++;
      }

      // Cold Start Drop: Must have at least 30s of history
      if (currentTs < firstTs + 30000) continue;

      // Basic Features
      const spread = Number(row.ask_price) - Number(row.bid_price);
      const spread_bps = mid > 0 ? (spread / mid) * 10000 : 0;
      const bQty = Number(row.bid_qty);
      const aQty = Number(row.ask_qty);
      const totalQty = bQty + aQty;
      const imbalance = totalQty > 0 ? (bQty - aQty) / totalQty : 0;
      const microprice = totalQty > 0 ? (Number(row.bid_price) * aQty + Number(row.ask_price) * bQty) / totalQty : mid;

      // Returns
      const f_ret_1s = ret1s[i];
      const f_ret_5s = this.#calculateReturn(ts, mids, i, currentTs - 5000);
      const f_ret_10s = this.#calculateReturn(ts, mids, i, currentTs - 10000);
      const f_ret_30s = this.#calculateReturn(ts, mids, i, currentTs - 30000);

      let f_vol_10s = NaN;
      if (volCount >= 2) {
        const mean = volSum / volCount;
        const variance = (volSumSq / volCount) - (mean * mean);
        f_vol_10s = Math.sqrt(Math.max(0, variance));
      }

      // Labeling: 10s Horizon (Forward Lookup)
      const targetIdx = this.#findFirstIndexAfterOrAt(ts, currentTs + 10000);
      let label_dir_10s = NaN;
      if (targetIdx !== -1) {
        label_dir_10s = mids[targetIdx] > mid ? 1 : 0;
      }

      if (
        isNaN(f_ret_1s) || isNaN(f_ret_5s) || isNaN(f_ret_10s) || isNaN(f_ret_30s) ||
        isNaN(f_vol_10s) || isNaN(label_dir_10s)
      ) {
        continue;
      }

      result.push({
        ts_event: BigInt(row.ts_event),
        f_mid: mid,
        f_spread: spread,
        f_spread_bps: spread_bps,
        f_imbalance: imbalance,
        f_microprice: microprice,
        f_ret_1s,
        f_ret_5s,
        f_ret_10s,
        f_ret_30s,
        f_vol_10s,
        label_dir_10s
      });
    }

    return result;
  }

  /**
   * Calculate log return: log(mid[t] / mid[as-of t-offset])
   */
  #calculateReturn(ts, mids, currentIndex, targetTs) {
    const idx = this.#findLastIndexBeforeOrAt(ts, targetTs);
    if (idx === -1) return NaN;
    const startMid = mids[idx];
    const endMid = mids[currentIndex];
    if (startMid <= 0 || endMid <= 0) return 0;
    return Math.log(endMid / startMid);
  }

  #findLastIndexBeforeOrAt(ts, target) {
    let low = 0;
    let high = ts.length - 1;
    let ans = -1;
    while (low <= high) {
      let mid = Math.floor((low + high) / 2);
      if (ts[mid] <= target) {
        ans = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return ans;
  }

  #findFirstIndexAfterOrAt(ts, target) {
    let low = 0;
    let high = ts.length - 1;
    let ans = -1;
    while (low <= high) {
      let mid = Math.floor((low + high) / 2);
      if (ts[mid] >= target) {
        ans = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return ans;
  }

  /**
   * Generates a config hash for meta.json
   */
  getConfigHash() {
    return crypto.createHash('sha256').update(FeatureBuilderV1.VERSION).digest('hex');
  }
}
