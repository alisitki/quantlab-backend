/**
 * FuturesReasonCode — Strict enum for futures canary gate rejection reasons.
 * Phase-2.1: Every rejection MUST have an explicit reason code.
 */

export enum FuturesReasonCode {
    /** All checks passed */
    PASSED = "PASSED",

    /** Leverage exceeds CANARY_MAX_LEVERAGE */
    LEVERAGE_EXCEEDED = "LEVERAGE_EXCEEDED",

    /** Margin mode is not ISOLATED */
    NOT_ISOLATED = "NOT_ISOLATED",

    /** reduce_only is false */
    NOT_REDUCE_ONLY = "NOT_REDUCE_ONLY",

    /** position_side is not ONE_WAY */
    NOT_ONE_WAY = "NOT_ONE_WAY",

    /** Liquidation price within worst-case move window */
    LIQUIDATION_TOO_CLOSE = "LIQUIDATION_TOO_CLOSE",

    /** Mode is LIVE (structurally unreachable) */
    LIVE_MODE_BLOCKED = "LIVE_MODE_BLOCKED",

    /** Global kill-switch is active */
    GLOBAL_KILL_ACTIVE = "GLOBAL_KILL_ACTIVE",

    /** Symbol-specific kill-switch is active */
    SYMBOL_KILL_ACTIVE = "SYMBOL_KILL_ACTIVE",
}

export type FuturesCanaryOutcome = "PASSED" | "REJECTED";

export interface FuturesCanaryResult {
    readonly intent_id: string;
    readonly symbol: string;
    readonly outcome: FuturesCanaryOutcome;
    readonly reason_code: FuturesReasonCode;
    readonly evaluated_at: number;
    readonly policy_snapshot_hash: string;
    readonly mode: "SHADOW" | "CANARY";
}
