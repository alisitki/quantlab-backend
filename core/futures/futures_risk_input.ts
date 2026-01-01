/**
 * FuturesRiskInput — Immutable input contract for futures risk sizing.
 * Phase-2.2: NO DEFAULTS. All fields required.
 */

export type FuturesRiskSide = "LONG" | "SHORT";

export interface FuturesRiskInput {
    /** Trading symbol */
    readonly symbol: string;

    /** Trade direction */
    readonly side: FuturesRiskSide;

    /** Total account equity in USD */
    readonly equity_usd: number;

    /** Maximum risk per trade (e.g., 0.002 = 0.2%) */
    readonly max_risk_pct: number;

    /** Maximum leverage allowed by policy */
    readonly leverage_cap: number;

    /** Expected entry price */
    readonly entry_price: number;

    /** Stop-loss price (worst-case exit) */
    readonly stop_price: number;

    /** Current funding rate snapshot */
    readonly funding_rate_snapshot: number;

    /** Maintenance margin rate (e.g., 0.004 = 0.4%) */
    readonly maintenance_margin_rate: number;

    /** Policy snapshot hash for audit */
    readonly policy_snapshot_hash: string;

    /** Input creation timestamp */
    readonly created_at: number;
}

export interface FuturesSizeResult {
    /** Notional value in USD */
    readonly notional_usd: number;

    /** Position quantity (base asset units) */
    readonly qty: number;

    /** Actual leverage used */
    readonly effective_leverage: number;

    /** Estimated liquidation price */
    readonly estimated_liquidation_price: number;

    /** Maximum loss if stop is hit */
    readonly worst_case_loss_usd: number;

    /** Distance from entry to stop (%) */
    readonly stop_distance_pct: number;

    /** Distance from entry to liquidation (%) */
    readonly liquidation_distance_pct: number;
}
