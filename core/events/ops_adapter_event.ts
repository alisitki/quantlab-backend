/**
 * OpsAdapterEvent — Observability event for futures order mapping.
 * Phase-2.4: Deterministic, pure, hash-stable.
 */
export interface OpsAdapterEvent {
    /** Event type identifier */
    readonly event_type: "FUTURES_ORDER_INTENT_MAPPED";

    /** Deterministic event ID (SHA-256, 16 chars) */
    readonly event_id: string;

    /** Client order ID from intent */
    readonly client_order_id: string;

    /** Outcome: MAPPED or REJECTED */
    readonly outcome: "MAPPED" | "REJECTED";

    /** Why the mapping failed or PASSED */
    readonly reason_code: string;

    /** Policy snapshot hash for audit */
    readonly policy_snapshot_hash: string;

    /** Evaluation timestamp */
    readonly evaluated_at: number;
}
