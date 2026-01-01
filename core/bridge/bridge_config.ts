/**
 * BridgeConfig interface for Paper -> Live Execution Bridge v1
 */
export interface BridgeConfig {
    live_enabled: boolean;              // Global kill-switch (must be false by default)
    allowed_symbols: string[];          // Canary symbol allow-list
    max_orders_per_day: number;         // Hard limit on daily order count
    max_notional_per_day: number;       // Hard limit on daily USD notional
    mode: "PAPER_ONLY" | "CANARY" | "LIVE";
}

/**
 * State of current daily limits (injected into the pure gate function)
 */
export interface BridgeLimitsState {
    current_order_count: number;
    current_notional_usd: number;
}
