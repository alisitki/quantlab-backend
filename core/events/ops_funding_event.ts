/**
 * OpsFundingEvent — Observability event for futures funding evaluations.
 * Phase-2.3: Deterministic, pure, hash-stable.
 */

export interface OpsFundingEvent {
    /** Event type identifier */
    readonly event_type: "FUTURES_FUNDING_EVALUATED";

    /** Deterministic event ID */
    readonly event_id: string;

    /** Trading symbol */
    readonly symbol: string;

    /** Evaluation outcome */
    readonly outcome: "PASSED" | "REJECTED";

    /** Reason code */
    readonly reason_code: string;

    /** Timestamp of evaluation */
    readonly evaluated_at: number;

    /** Hash of the policy snapshot used */
    readonly policy_snapshot_hash: string;

    /** Funding estimates (redacted) */
    readonly funding_summary: {
        readonly funding_cost_usd: number;
        readonly funding_cost_pct_equity: number;
        readonly funding_direction: "PAY" | "RECEIVE";
    };
}
