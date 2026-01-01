/**
 * OpsFuturesEvent — Observability event for futures canary evaluations.
 * Phase-2.1: Deterministic, pure, hash-stable.
 */

export interface OpsFuturesEvent {
    /** Event type identifier */
    readonly event_type: "FUTURES_CANARY_EVALUATED";

    /** Deterministic event ID (SHA-256, 16 chars) */
    readonly event_id: string;

    /** Original intent ID */
    readonly intent_id: string;

    /** Trading symbol */
    readonly symbol: string;

    /** Evaluation outcome */
    readonly outcome: "PASSED" | "REJECTED";

    /** Reason code (from FuturesReasonCode enum) */
    readonly reason_code: string;

    /** Timestamp of evaluation */
    readonly evaluated_at: number;

    /** Hash of the policy snapshot used */
    readonly policy_snapshot_hash: string;

    /** Execution mode (redacted: never exposes LIVE) */
    readonly mode: "SHADOW" | "CANARY";

    /** Redacted futures context fields (subset for audit) */
    readonly futures_fields: {
        readonly side: "LONG" | "SHORT";
        readonly leverage: number;
        readonly margin_mode: "ISOLATED" | "CROSS";
        readonly reduce_only: boolean;
    };
}
