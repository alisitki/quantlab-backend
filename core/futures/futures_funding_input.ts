/**
 * FuturesFundingInput — Immutable input contract for funding cost analysis.
 * Phase-2.3: NO DEFAULTS. All fields required.
 */

export type FundingSide = "LONG" | "SHORT";

export interface FuturesFundingInput {
    /** Trading symbol */
    readonly symbol: string;

    /** Trade direction */
    readonly side: FundingSide;

    /** Position notional in USD */
    readonly notional_usd: number;

    /** Funding rate per 8h period (e.g., 0.0001 = 0.01%) */
    readonly funding_rate_snapshot: number;

    /** Expected hold duration in hours */
    readonly expected_hold_hours: number;

    /** Maximum funding cost as % of equity (e.g., 0.005 = 0.5%) */
    readonly funding_budget_pct: number;

    /** Total account equity in USD */
    readonly equity_usd: number;

    /** Policy snapshot hash for audit */
    readonly policy_snapshot_hash: string;

    /** Input creation timestamp */
    readonly created_at: number;
}

export interface FundingCostResult {
    /** Number of 8h funding periods */
    readonly funding_periods: number;

    /** Total funding cost in USD */
    readonly funding_cost_usd: number;

    /** Funding cost as % of equity */
    readonly funding_cost_pct_equity: number;

    /** Whether funding is paid (negative) or received (positive) */
    readonly funding_direction: "PAY" | "RECEIVE";

    /** Annualized funding rate (for reference) */
    readonly annualized_rate_pct: number;
}
