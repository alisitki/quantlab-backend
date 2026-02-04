/**
 * PromotionGuard v1: Hard-rule gatekeeper for model promotion.
 * 
 * Input:
 *   - BacktestSummary (from backtest/summary.js)
 *   - DummyBaselineResult (baseline return_pct)
 * 
 * Rules (ALL must pass):
 *   - return_pct > 0
 *   - max_drawdown_pct < 25%
 *   - return_pct > baseline.return_pct (beats dummy)
 * 
 * Output:
 *   PromotionDecision { safety_pass, reasons, metrics_snapshot }
 * 
 * Behavior:
 *   - Any uncertainty → REJECT
 *   - Missing/null metrics → REJECT
 *   - No retries, no fallbacks
 */

/**
 * @typedef {Object} BacktestSummary
 * @property {number} return_pct - Return percentage
 * @property {number} max_drawdown_pct - Max drawdown percentage
 * @property {number} trades - Number of trades
 */

/**
 * @typedef {Object} DummyBaselineResult
 * @property {number} return_pct - Baseline return percentage (buy-hold)
 */

/**
 * @typedef {Object} PromotionDecision
 * @property {boolean} safety_pass - Whether all rules passed
 * @property {string[]} reasons - Failure reasons (empty if pass)
 * @property {Object} metrics_snapshot - Snapshot of evaluated metrics
 */

// Hard rule thresholds
const RULES = {
  MIN_RETURN_PCT: 0,           // return_pct must be > 0
  MAX_DRAWDOWN_PCT: 25,        // max_drawdown_pct must be < 25%
};

/**
 * Evaluate whether a model should be promoted based on hard rules.
 * 
 * @param {BacktestSummary} backtestSummary - Backtest summary from backtest/summary.js
 * @param {DummyBaselineResult} dummyBaseline - Baseline result for comparison
 * @returns {PromotionDecision}
 */
export function evaluate(backtestSummary, dummyBaseline) {
  const reasons = [];
  
  // Validate inputs - missing metrics = REJECT
  if (!backtestSummary || typeof backtestSummary !== 'object') {
    return {
      safety_pass: false,
      reasons: ['Missing or invalid backtest summary'],
      metrics_snapshot: null
    };
  }
  
  if (!dummyBaseline || typeof dummyBaseline !== 'object') {
    return {
      safety_pass: false,
      reasons: ['Missing or invalid baseline result'],
      metrics_snapshot: null
    };
  }
  
  const returnPct = backtestSummary.return_pct;
  const maxDrawdownPct = backtestSummary.max_drawdown_pct;
  const baselineReturnPct = dummyBaseline.return_pct;
  const trades = backtestSummary.trades;
  
  // Check for null/undefined values - any uncertainty = REJECT
  if (typeof returnPct !== 'number' || isNaN(returnPct)) {
    reasons.push('Missing or invalid return_pct');
  }
  
  if (typeof maxDrawdownPct !== 'number' || isNaN(maxDrawdownPct)) {
    reasons.push('Missing or invalid max_drawdown_pct');
  }
  
  if (typeof baselineReturnPct !== 'number' || isNaN(baselineReturnPct)) {
    reasons.push('Missing or invalid baseline return_pct');
  }
  
  // If any metrics are missing, reject early
  if (reasons.length > 0) {
    return {
      safety_pass: false,
      reasons,
      metrics_snapshot: {
        return_pct: returnPct,
        max_drawdown_pct: maxDrawdownPct,
        baseline_return_pct: baselineReturnPct,
        trades: trades
      }
    };
  }
  
  // Rule 1: Positive Return
  if (returnPct <= RULES.MIN_RETURN_PCT) {
    reasons.push(`Negative return: ${returnPct.toFixed(2)}% <= 0%`);
  }
  
  // Rule 2: Max Drawdown < 25%
  if (maxDrawdownPct >= RULES.MAX_DRAWDOWN_PCT) {
    reasons.push(`Drawdown exceeds limit: ${maxDrawdownPct.toFixed(2)}% >= ${RULES.MAX_DRAWDOWN_PCT}%`);
  }
  
  // Rule 3: Beats Baseline
  if (returnPct <= baselineReturnPct) {
    reasons.push(`Underperforms baseline: ${returnPct.toFixed(2)}% <= ${baselineReturnPct.toFixed(2)}%`);
  }
  
  const safetyPass = reasons.length === 0;
  
  return {
    safety_pass: safetyPass,
    reasons,
    metrics_snapshot: {
      return_pct: returnPct,
      max_drawdown_pct: maxDrawdownPct,
      baseline_return_pct: baselineReturnPct,
      trades: trades
    }
  };
}

