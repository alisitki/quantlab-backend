/**
 * FuturesAdapterReasonCode — Rejection reasons for the exchange adapter.
 * Phase-2.4: Every rejection MUST have an explicit code.
 */
export enum FuturesAdapterReasonCode {
    /** Mapping successful */
    PASSED = "PASSED",

    /** Order is not reduceOnly (Violation for canary) */
    NOT_REDUCE_ONLY = "NOT_REDUCE_ONLY",

    /** Margin mode is not ISOLATED */
    NOT_ISOLATED = "NOT_ISOLATED",

    /** Structural safety block for LIVE mode */
    LIVE_MODE_BLOCKED = "LIVE_MODE_BLOCKED",

    /** Client order ID missing or invalid */
    INVALID_CLIENT_ID = "INVALID_CLIENT_ID",
}

export type FuturesAdapterOutcome = "MAPPED" | "REJECTED";

export interface FuturesAdapterGateResult {
    readonly outcome: FuturesAdapterOutcome;
    readonly reason_code: FuturesAdapterReasonCode;
    readonly client_order_id: string;
    readonly policy_snapshot_hash: string;
    readonly evaluated_at: number;
}
