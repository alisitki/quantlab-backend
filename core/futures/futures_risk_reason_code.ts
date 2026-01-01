/**
 * FuturesRiskReasonCode — Strict enum for risk gate rejection reasons.
 * Phase-2.2: Every rejection MUST have an explicit reason code.
 */

export enum FuturesRiskReasonCode {
    /** All checks passed */
    PASSED = "PASSED",

    /** Worst-case loss exceeds allowed risk */
    LOSS_EXCEEDS_LIMIT = "LOSS_EXCEEDS_LIMIT",

    /** Liquidation price crosses stop price */
    LIQUIDATION_BEFORE_STOP = "LIQUIDATION_BEFORE_STOP",

    /** Effective leverage exceeds cap */
    LEVERAGE_EXCEEDED = "LEVERAGE_EXCEEDED",

    /** Invalid stop direction (stop on wrong side of entry) */
    INVALID_STOP_DIRECTION = "INVALID_STOP_DIRECTION",

    /** Position size too small (below minimum) */
    SIZE_TOO_SMALL = "SIZE_TOO_SMALL",
}

export type FuturesRiskOutcome = "PASSED" | "REJECTED";

export interface FuturesRiskGateResult {
    readonly symbol: string;
    readonly outcome: FuturesRiskOutcome;
    readonly reason_code: FuturesRiskReasonCode;
    readonly evaluated_at: number;
    readonly policy_snapshot_hash: string;

    /** Key risk metrics for audit (redacted) */
    readonly risk_metrics: {
        readonly effective_leverage: number;
        readonly worst_case_loss_usd: number;
        readonly stop_distance_pct: number;
        readonly liquidation_distance_pct: number;
    };
}
