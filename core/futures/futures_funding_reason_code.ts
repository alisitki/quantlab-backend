/**
 * FuturesFundingReasonCode — Rejection reasons for funding gate.
 * Phase-2.3: Every rejection MUST have an explicit reason code.
 */

export enum FuturesFundingReasonCode {
    /** All checks passed */
    PASSED = "PASSED",

    /** Funding cost exceeds allocated budget */
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED",

    /** Funding rate is too high/toxic (wrong sign or extreme) */
    TOXIC_FUNDING_RATE = "TOXIC_FUNDING_RATE",
}

export type FuturesFundingOutcome = "PASSED" | "REJECTED";

export interface FuturesFundingGateResult {
    readonly symbol: string;
    readonly outcome: FuturesFundingOutcome;
    readonly reason_code: FuturesFundingReasonCode;
    readonly evaluated_at: number;
    readonly policy_snapshot_hash: string;

    /** Funding metrics for audit */
    readonly funding_metrics: {
        readonly funding_cost_usd: number;
        readonly funding_cost_pct_equity: number;
        readonly funding_direction: "PAY" | "RECEIVE";
    };
}
