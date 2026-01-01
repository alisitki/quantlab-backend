export interface Decision {
    decision_id: string;        // deterministic hash
    symbol: string;
    side: "LONG" | "SHORT" | "FLAT";
    confidence: number;         // 0..1
    horizon_ms: number;
    valid_until_ts: number;
    model_hash: string;
    features_hash: string;
    policy_version: string;
}
