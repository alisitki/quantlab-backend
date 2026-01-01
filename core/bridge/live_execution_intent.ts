/**
 * LiveExecutionIntent interface for Paper -> Live Execution Bridge v1
 * 
 * This is NOT a real order, but a gated intent that has passed all safety checks.
 */
export interface LiveExecutionIntent {
    bridge_id: string;              // Deterministic hash of intent
    source_execution_id: string;    // Reference to PaperExecutionResult
    symbol: string;
    side: "BUY" | "SELL";
    quantity: number;
    price: number;
    mode: "LIVE";
    gated_at: number;               // Timestamp when it passed the gate
}
