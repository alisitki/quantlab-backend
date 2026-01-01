import { ExecutionResult, ReasonCode } from './events/execution_event.ts';
import { buildOrderIntent, AdapterConfig } from './execution_adapter/adapter.ts';

const mockResult: ExecutionResult = {
    decision_id: "DEC_123",
    symbol: "BTCUSDT",
    outcome: "WOULD_EXECUTE",
    reason_code: ReasonCode.PASSED,
    evaluated_at: 1000000000,
    policy_snapshot: {
        min_confidence: 0.6,
        allowed_policy_versions: ["v1"],
        ops_blacklist_symbols: [],
        cooldown_ms: 1000,
        mode: "DRY_RUN"
    },
    policy_version: "v1"
};

const config: AdapterConfig = {
    default_quantity: 0.001
};

const now = 1000000005;

console.log("--- Testing LONG -> BUY Transformation ---");
const intent1 = buildOrderIntent(mockResult, config, now, "LONG");
console.log(JSON.stringify(intent1, null, 2));

console.log("\n--- Testing SHORT -> SELL Transformation ---");
const intent2 = buildOrderIntent(mockResult, config, now, "SHORT");
console.log(JSON.stringify(intent2, null, 2));

console.log("\n--- Testing Determinism ---");
const intent3 = buildOrderIntent(mockResult, config, now, "LONG");
if (intent1?.intent_id === intent3?.intent_id) {
    console.log("✅ Deterministic intent_id passed.");
}

console.log("\n--- Testing REJECTED Filtering ---");
const rejectedResult = { ...mockResult, outcome: "REJECTED" as any };
const intentRejected = buildOrderIntent(rejectedResult, config, now, "LONG");
if (intentRejected === null) {
    console.log("✅ Rejected outcome correctly produced null intent.");
}
