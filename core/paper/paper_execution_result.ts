/**
 * PaperExecutionResult interface for Paper Execution Adapter v1
 */
export interface PaperExecutionResult {
    execution_id: string;            // Deterministic hash
    intent_id: string;               // Links back to RiskAdjustedOrder
    symbol: string;
    side: "BUY" | "SELL";
    requested_quantity: number;
    filled_quantity: number;
    fill_price: number;
    slippage_bps: number;
    latency_ms: number;
    status: "FILLED" | "REJECTED";
    mode: "PAPER";
    executed_at: number;             // now + latency
}
