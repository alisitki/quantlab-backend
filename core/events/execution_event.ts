export type ExecutionOutcome = "WOULD_EXECUTE" | "REJECTED" | "SKIPPED";

export enum ReasonCode {
    PASSED = "PASSED",
    LOW_CONFIDENCE = "LOW_CONFIDENCE",
    EXPIRED_DECISION = "EXPIRED_DECISION",
    COOLDOWN_ACTIVE = "COOLDOWN_ACTIVE",
    POLICY_REJECTED = "POLICY_REJECTED",
    OPS_BLACKLISTED = "OPS_BLACKLISTED",
    NO_ACTIVE_DECISION_ALLOWED = "NO_ACTIVE_DECISION_ALLOWED",
    INVALID_MODE = "INVALID_MODE"
}

export interface PolicySnapshot {
    min_confidence: number;
    allowed_policy_versions: string[];
    ops_blacklist_symbols: string[];
    cooldown_ms: number;
    mode: "DRY_RUN" | "PROD";
}

export interface ExecutionResult {
    decision_id: string;
    symbol: string;
    outcome: ExecutionOutcome;
    reason_code: ReasonCode;
    evaluated_at: number;
    policy_snapshot: PolicySnapshot;
    policy_version: string;
}
