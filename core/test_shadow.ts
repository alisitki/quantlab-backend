import { Decision } from './decision/decision_contract.ts';
import { PolicySnapshot } from './events/execution_event.ts';
import { runShadowReplay } from './shadow/replay_driver.ts';
import { GateState } from './execution/rules/cooldown.ts';

const decisions: Decision[] = [
    {
        decision_id: "D1",
        symbol: "BTCUSDT",
        side: "LONG",
        confidence: 0.8,
        horizon_ms: 1000,
        valid_until_ts: 1000001000,
        model_hash: "M1",
        features_hash: "F1",
        policy_version: "v1"
    },
    {
        decision_id: "D2",
        symbol: "BTCUSDT", // Same symbol, should hit cooldown
        side: "LONG",
        confidence: 0.9,
        horizon_ms: 1000,
        valid_until_ts: 1000002000,
        model_hash: "M1",
        features_hash: "F2",
        policy_version: "v1"
    },
    {
        decision_id: "D3",
        symbol: "ETHUSDT",
        side: "SHORT",
        confidence: 0.1, // Low confidence, should be rejected
        horizon_ms: 1000,
        valid_until_ts: 1000003000,
        model_hash: "M1",
        features_hash: "F3",
        policy_version: "v1"
    }
];

const policy: PolicySnapshot = {
    min_confidence: 0.6,
    allowed_policy_versions: ["v1"],
    ops_blacklist_symbols: [],
    cooldown_ms: 5000,
    mode: "DRY_RUN"
};

const initialState: GateState = {
    last_decision_ts_by_symbol: {},
    active_decision_symbols: []
};

// Replay
const { metrics, finalState } = runShadowReplay(decisions, { policy, initialState });

console.log("\n--- SHADOW REPLAY METRICS ---");
console.log(JSON.stringify(metrics, null, 2));

console.log("\n--- VERIFICATION ---");
if (metrics.total_decisions === 3) console.log("✅ Total Decisions Passed");
if (metrics.would_execute_count === 1) console.log("✅ Would Execute Count Passed (D1)");
if (metrics.skipped_count === 1) console.log("✅ Skipped Count Passed (D2 - Cooldown)");
if (metrics.rejected_count === 1) console.log("✅ Rejected Count Passed (D3 - Low Confidence)");

console.log("\n--- FINAL STATE ---");
console.log(JSON.stringify(finalState, null, 2));
