/**
 * FuturesOrderIntent — Immutable contract for futures order placement.
 * Phase-2.4: NO DEFAULTS. NO IMPLICIT VALUES.
 */

export type FuturesOrderSide = "BUY" | "SELL";
export type FuturesOrderPositionSide = "LONG" | "SHORT";
export type FuturesOrderType = "MARKET" | "LIMIT";
export type FuturesTimeInForce = "GTC" | "IOC" | "FOK" | "GTX";
export type FuturesOrderMode = "SHADOW" | "CANARY" | "LIVE";

export interface FuturesOrderIntent {
    /** Trading symbol (e.g., BTCUSDT) */
    readonly symbol: string;

    /** Order direction (BUY/SELL) */
    readonly side: FuturesOrderSide;

    /** Position direction (LONG/SHORT) - Explicit in hedge mode, required for adapter */
    readonly position_side: FuturesOrderPositionSide;

    /** Order quantity in base asset units */
    readonly qty: number;

    /** Limit price (required if order_type is LIMIT) */
    readonly price?: number;

    /** Order type (MARKET/LIMIT) */
    readonly order_type: FuturesOrderType;

    /** Time in force policy */
    readonly time_in_force: FuturesTimeInForce;

    /** Whether this is a reduce-only order - MUST be true for canary */
    readonly reduce_only: boolean;

    /** Whether to close the entire position */
    readonly close_position: boolean;

    /** Leverage used for the symbol */
    readonly leverage: number;

    /** Margin mode (ISOLATED/CROSS) - MUST be ISOLATED for canary */
    readonly margin_mode: "ISOLATED" | "CROSS";

    /** Unique client order ID */
    readonly client_order_id: string;

    /** Execution mode - LIVE is structurally unreachable */
    readonly mode: FuturesOrderMode;

    /** Hash of the policy snapshot for audit */
    readonly policy_snapshot_hash: string;
}
