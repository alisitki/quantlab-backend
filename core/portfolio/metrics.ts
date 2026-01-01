/**
 * Performance metrics for Paper Portfolio v1
 */
export interface PerformanceMetrics {
    total_trades: number;
    win_trades: number;
    loss_trades: number;
    win_rate: number;
    total_pnl: number;
    max_drawdown: number;
}

/**
 * Pure function to calculate metrics from equity history and realized PnLs.
 */
export function calculateMetrics(
    realizedPnls: number[],
    equityHistory: { equity: number }[]
): PerformanceMetrics {
    const total = realizedPnls.length;
    const wins = realizedPnls.filter(p => p > 0).length;
    const losses = realizedPnls.filter(p => p < 0).length;
    const totalPnl = realizedPnls.reduce((a, b) => a + b, 0);

    // Max Drawdown calculation
    let maxEquity = 0;
    let maxDD = 0;
    for (const point of equityHistory) {
        maxEquity = Math.max(maxEquity, point.equity);
        const dd = maxEquity > 0 ? (maxEquity - point.equity) / maxEquity : 0;
        maxDD = Math.max(maxDD, dd);
    }

    return {
        total_trades: total,
        win_trades: wins,
        loss_trades: losses,
        win_rate: total > 0 ? wins / total : 0,
        total_pnl: totalPnl,
        max_drawdown: maxDD
    };
}
