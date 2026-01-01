import { PaperExecutionResult } from "../paper/paper_execution_result";
import { PortfolioState, PortfolioSnapshot } from "./portfolio_state";
import { updatePosition, calculateUnrealized } from "./pnl";
import { calculateMetrics, PerformanceMetrics } from "./metrics";

/**
 * Portfolio Engine v1
 * Orchestrates the application of executions to the portfolio state.
 */
export class PortfolioEngine {
    private state: PortfolioState;

    constructor(initialCapital: number) {
        this.state = {
            cash: initialCapital,
            positions: {},
            equity_history: [{ timestamp: 0, equity: initialCapital }]
        };
    }

    /**
     * Process a single execution result.
     */
    public applyExecution(exec: PaperExecutionResult): void {
        if (exec.status !== "FILLED") return;

        const symbol = exec.symbol;
        const currentPos = this.state.positions[symbol];

        // 1. Update cash (for the trade cost)
        // BUY: cash decreases, SELL: cash increases
        const cost = exec.filled_quantity * exec.fill_price;
        if (exec.side === "BUY") {
            this.state.cash -= cost;
        } else {
            this.state.cash += cost;
        }

        // 2. Update position state
        const nextPos = updatePosition(currentPos, exec);
        this.state.positions[symbol] = nextPos;

        // 3. Update equity tracking (simplified: use fill price as current price for this symbol)
        this.updateEquity(exec.executed_at);
    }

    /**
     * Update equity based on mark-to-market prices.
     * In v1, we assume the latest execution price or a placeholder for unrealized PnL.
     */
    public updateEquity(timestamp: number): void {
        let unrealized = 0;
        for (const symbol in this.state.positions) {
            const pos = this.state.positions[symbol];
            // Note: In a real system, we'd inject current market price here.
            // For v1 DRY/Paper, we use avg_entry_price for unrealized if not provided, 
            // but pnl.ts allows injecting a price.
            // Here we assume equity = cash + realized_pnl + unrealized_pnl
            // Simplified v1: equity = cash + market_value_of_all_positions
            unrealized += calculateUnrealized(pos, pos.avg_entry_price); // Defaults to 0 if market price = entry

            // Market value of position:
            // For long: quantity * price
            // For short: cash received from sell is already in 'cash', 
            // but we need to subtract the cost to close at 'price'.
            // Equity = Cash + Sum(qty * price)
        }

        let positionsValue = 0;
        for (const symbol in this.state.positions) {
            const pos = this.state.positions[symbol];
            positionsValue += pos.quantity * pos.avg_entry_price;
        }

        const currentEquity = this.state.cash + positionsValue;
        this.state.equity_history.push({ timestamp, equity: currentEquity });
    }

    public snapshot(timestamp: number): PortfolioSnapshot {
        let positionsValue = 0;
        for (const symbol in this.state.positions) {
            const pos = this.state.positions[symbol];
            positionsValue += pos.quantity * pos.avg_entry_price;
        }

        return {
            timestamp,
            equity: this.state.cash + positionsValue,
            cash: this.state.cash,
            positions: { ...this.state.positions }
        };
    }

    public getMetrics(): PerformanceMetrics {
        const realizedPnls: number[] = [];
        for (const symbol in this.state.positions) {
            if (this.state.positions[symbol].realized_pnl !== 0) {
                realizedPnls.push(this.state.positions[symbol].realized_pnl);
            }
        }

        return calculateMetrics(realizedPnls, this.state.equity_history);
    }
}
