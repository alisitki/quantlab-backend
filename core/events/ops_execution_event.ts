export interface OpsExecutionEvent {
    event_type: "EXECUTION_EVALUATED";
    event_id: string;              // deterministic hash
    decision_id: string;
    symbol: string;
    outcome: string;
    reason_code: string;
    evaluated_at: number;
    policy_version: string;
    policy_snapshot_hash: string;
    mode: "DRY_RUN" | "PROD";
}
