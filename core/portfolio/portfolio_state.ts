import { Position } from "./position";

/**
 * PortfolioState for Paper Portfolio Engine v1
 */
export interface PortfolioState {
    cash: number;
    positions: Record<string, Position>;
    equity_history: { timestamp: number; equity: number }[];
}

/**
 * PortfolioSnapshot for reporting
 */
export interface PortfolioSnapshot {
    timestamp: number;
    equity: number;
    cash: number;
    positions: Record<string, Position>;
}
