/**
 * RiskAdjustedOrder interface for Position Sizing Engine v1
 */
export interface RiskAdjustedOrder {
    intent_id: string;
    symbol: string;
    side: "BUY" | "SELL";
    quantity: number;
    notional_usd: number;
    risk_pct: number;
    max_loss_usd: number;
    assumed_stop_pct: number;
    mode: "DRY_RUN";
    evaluated_at: number;

    // Optional: reason if quantity was reduced/capped
    adjustment_reason?: string;
}
