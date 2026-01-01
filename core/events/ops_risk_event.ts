/**
 * OpsRiskEvent — Observability event for futures risk evaluations.
 * Phase-2.2: Deterministic, pure, hash-stable.
 */

export interface OpsRiskEvent {
    /** Event type identifier */
    readonly event_type: "FUTURES_RISK_EVALUATED";

    /** Deterministic event ID (SHA-256, 16 chars) */
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

    /** Redacted risk metrics */
    readonly risk_metrics: {
        readonly effective_leverage: number;
        readonly worst_case_loss_usd: number;
        readonly stop_distance_pct: number;
    };
}
