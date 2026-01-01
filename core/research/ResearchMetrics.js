/**
 * ResearchMetrics computes cheap, indicative metrics for strategy screening.
 */
export class ResearchMetrics {
  /**
   * Compute indicative metrics from execution results.
   * @param {Object} executionSnapshot - Snapshot from ResearchExecution
   * @returns {Object} Indicative metrics
   */
  static compute(executionSnapshot) {
    const { trades, totalPnl, tradeCount } = executionSnapshot;
    
    let maxDrawdown = 0;
    let peakPnl = 0;
    let currentPnl = 0;
    let wins = 0;

    for (const trade of trades) {
      currentPnl += trade.pnl;
      if (currentPnl > peakPnl) {
        peakPnl = currentPnl;
      }
      const dd = peakPnl - currentPnl;
      if (dd > maxDrawdown) {
        maxDrawdown = dd;
      }
      if (trade.pnl > 0) {
        wins++;
      }
    }

    const winRate = tradeCount > 0 ? wins / tradeCount : 0;

    return {
      totalReturn: totalPnl,
      maxDrawdown: maxDrawdown,
      tradeCount: tradeCount,
      winRate: winRate,
      wins: wins,
      losses: tradeCount - wins
    };
  }
}
