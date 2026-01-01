export interface OrderIntent {
    intent_id: string;            // deterministic hash
    decision_id: string;
    symbol: string;
    side: "BUY" | "SELL";
    order_type: "MARKET" | "LIMIT";
    quantity: number;
    price?: number;               // only for LIMIT
    time_in_force: "IOC" | "GTC";
    mode: "DRY_RUN";
    generated_at: number;
}
