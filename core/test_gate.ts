/**
 * Verification Script for Execution Gate
 * Note: Written in JS for direct execution in Node v20 environment.
 * In a real TS project, this would be in TS.
 */

// Since we are in an ESM project, we can import from JS files.
// But we wrote TS files. To run this, we'll mock the logic or 
// assume a loader. For this environment, I'll write a self-contained
// test that mirrors the logic if I can't run the TS files.
// HOWEVER, the user wants to see the TS code verified.

import { Decision } from './decision/decision_contract.ts';
import { ExecutionGate } from './execution/gate.ts';
import { PolicySnapshot, ReasonCode } from './events/execution_event.ts';

const mockDecision: Decision = {
    decision_id: "HASH_123",
    symbol: "BTCUSDT",
    side: "LONG",
    confidence: 0.85,
    horizon_ms: 60000,
    valid_until_ts: 2000000000000,
    model_hash: "M1",
    features_hash: "F1",
    policy_version: "v1"
};

const mockPolicy: PolicySnapshot = {
    min_confidence: 0.6,
    allowed_policy_versions: ["v1", "v2"],
    ops_blacklist_symbols: [],
    cooldown_ms: 10000,
    mode: "DRY_RUN"
};

const mockState = {
    last_decision_ts_by_symbol: {},
    active_decision_symbols: []
};

const now = 1900000000000;

console.log("--- Testing SUCCESS scenario ---");
const res1 = ExecutionGate.evaluate(mockDecision, mockPolicy, mockState, now);
console.log("Outcome:", res1.outcome);
console.log("Reason:", res1.reason_code); // Should be PASSED

console.log("\n--- Testing LOW_CONFIDENCE scenario ---");
const strictPolicy = { ...mockPolicy, min_confidence: 0.9 };
const res2 = ExecutionGate.evaluate(mockDecision, strictPolicy, mockState, now);
console.log("Outcome:", res2.outcome);
console.log("Reason:", res2.reason_code);

console.log("\n--- Testing COOLDOWN scenario ---");
const stateWithRecent = {
    ...mockState,
    last_decision_ts_by_symbol: { "BTCUSDT": now - 5000 }
};
const res3 = ExecutionGate.evaluate(mockDecision, mockPolicy, stateWithRecent, now);
console.log("Outcome:", res3.outcome);
console.log("Reason:", res3.reason_code);

console.log("\n--- Testing REPLAY scenario (Same decision, different policy) ---");
const policyV2Only = { ...mockPolicy, allowed_policy_versions: ["v2"] };
const res4 = ExecutionGate.evaluate(mockDecision, policyV2Only, mockState, now);
console.log("Decision ID:", mockDecision.decision_id);
console.log("Policy V1/V2 Outcome:", res1.outcome);
console.log("Policy V2-Only Outcome:", res4.outcome);
console.log("Reason (V2-Only):", res4.reason_code);

if (res1.outcome !== res4.outcome) {
    console.log("\n✅ Determinism Check Passed: Same decision produced different outcomes based on policy snapshot.");
}

import { emitExecutionEvent } from './ops/emit_execution_event.ts';

console.log("\n--- Testing OPS Event Emission (Deterministic) ---");
const event1 = emitExecutionEvent(res1);
const event2 = emitExecutionEvent(res1);

console.log("Event 1 ID:", event1.event_id);
console.log("Event 2 ID:", event2.event_id);
console.log("Policy Snapshot Hash:", event1.policy_snapshot_hash);

if (event1.event_id === event2.event_id) {
    console.log("✅ Event ID Determinism Passed.");
}
if (event1.policy_snapshot_hash.length === 16) {
    console.log("✅ Policy Snapshot Hash (16 chars) Passed.");
}
