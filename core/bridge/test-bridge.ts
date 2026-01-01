/**
 * Live Bridge Gate Unit Tests (CRITICAL SAFETY TESTS)
 * Validates: Kill-switch, mode checks, canary allowlist, limits, determinism
 */

import { gateToLive } from "./gate";
import { BridgeConfig, BridgeLimitsState } from "./bridge_config";
import { PaperExecutionResult } from "../paper/paper_execution_result";

const basePaperExec: PaperExecutionResult = {
    execution_id: "PAPER_001",
    intent_id: "INTENT_001",
    symbol: "BTCUSDT",
    side: "BUY",
    requested_quantity: 0.1,
    filled_quantity: 0.1,
    fill_price: 50000,
    slippage_bps: 10,
    latency_ms: 50,
    status: "FILLED",
    mode: "PAPER",
    executed_at: 1000000000
};

const baseLimits: BridgeLimitsState = {
    current_order_count: 0,
    current_notional_usd: 0
};

const now = 1000000005;

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string) {
    if (condition) {
        console.log(`✅ ${message}`);
        passed++;
    } else {
        console.log(`❌ FAIL: ${message}`);
        failed++;
    }
}

console.log("\\n========================================");
console.log("  LIVE BRIDGE GATE TESTS (CRITICAL)");
console.log("========================================\\n");

// ============================================
// CRITICAL TEST 1: Kill-Switch (live_enabled=false)
// ============================================
console.log("--- CRITICAL TEST 1: Kill-Switch (live_enabled=false) ---\\n");

// TEST 1.1: live_enabled=false + mode=LIVE → null
console.log("TEST 1.1: live_enabled=false + mode=LIVE → null");
const killSwitchOffLiveMode: BridgeConfig = {
    live_enabled: false,
    allowed_symbols: ["BTCUSDT"],
    max_orders_per_day: 100,
    max_notional_per_day: 100000,
    mode: "LIVE"
};
const result1 = gateToLive(basePaperExec, killSwitchOffLiveMode, baseLimits, now);
assert(result1 === null, "live_enabled=false + mode=LIVE → null (PARANOID TEST)");

// TEST 1.2: live_enabled=false + mode=CANARY → null
console.log("\\nTEST 1.2: live_enabled=false + mode=CANARY → null");
const killSwitchOffCanaryMode: BridgeConfig = {
    ...killSwitchOffLiveMode,
    mode: "CANARY"
};
const result2 = gateToLive(basePaperExec, killSwitchOffCanaryMode, baseLimits, now);
assert(result2 === null, "live_enabled=false + mode=CANARY → null");

// TEST 1.3: live_enabled=false + mode=PAPER_ONLY → null
console.log("\\nTEST 1.3: live_enabled=false + mode=PAPER_ONLY → null");
const killSwitchOffPaperMode: BridgeConfig = {
    ...killSwitchOffLiveMode,
    mode: "PAPER_ONLY"
};
const result3 = gateToLive(basePaperExec, killSwitchOffPaperMode, baseLimits, now);
assert(result3 === null, "live_enabled=false + mode=PAPER_ONLY → null");

// ============================================
// TEST GROUP 2: Mode Checks (with live_enabled=true)
// ============================================
console.log("\\n--- TEST GROUP 2: Mode Checks ---\\n");

// TEST 2.1: mode=PAPER_ONLY → null
console.log("TEST 2.1: mode=PAPER_ONLY → null");
const paperOnlyConfig: BridgeConfig = {
    live_enabled: true,
    allowed_symbols: ["BTCUSDT"],
    max_orders_per_day: 100,
    max_notional_per_day: 100000,
    mode: "PAPER_ONLY"
};
const paperOnlyResult = gateToLive(basePaperExec, paperOnlyConfig, baseLimits, now);
assert(paperOnlyResult === null, "mode=PAPER_ONLY → null");

// TEST 2.2: mode=CANARY + symbol in allowlist → Live intent
console.log("\\nTEST 2.2: mode=CANARY + symbol in allowlist → intent");
const canaryConfig: BridgeConfig = {
    live_enabled: true,
    allowed_symbols: ["BTCUSDT", "ETHUSDT"],
    max_orders_per_day: 100,
    max_notional_per_day: 100000,
    mode: "CANARY"
};
const canaryResult = gateToLive(basePaperExec, canaryConfig, baseLimits, now);
assert(canaryResult !== null, "CANARY + allowed symbol → intent");
if (canaryResult) {
    assert(canaryResult.symbol === "BTCUSDT", "Intent symbol = BTCUSDT");
    assert(canaryResult.mode === "LIVE", "Intent mode = LIVE");
}

// TEST 2.3: mode=CANARY + symbol NOT in allowlist → null
console.log("\\nTEST 2.3: mode=CANARY + symbol NOT in allowlist → null");
const canaryNotAllowed: BridgeConfig = {
    ...canaryConfig,
    allowed_symbols: ["ETHUSDT"]  // BTCUSDT not allowed
};
const canaryBlockResult = gateToLive(basePaperExec, canaryNotAllowed, baseLimits, now);
assert(canaryBlockResult === null, "CANARY + non-allowed symbol → null");

// TEST 2.4: mode=LIVE + all checks pass → intent
console.log("\\nTEST 2.4: mode=LIVE + all checks pass → intent");
const liveConfig: BridgeConfig = {
    live_enabled: true,
    allowed_symbols: [],
    max_orders_per_day: 100,
    max_notional_per_day: 100000,
    mode: "LIVE"
};
const liveResult = gateToLive(basePaperExec, liveConfig, baseLimits, now);
assert(liveResult !== null, "LIVE mode all checks pass → intent");

// ============================================
// TEST GROUP 3: Limit Checks
// ============================================
console.log("\\n--- TEST GROUP 3: Limit Checks ---\\n");

// TEST 3.1: Order count limit exceeded → null
console.log("TEST 3.1: Order count limit exceeded → null");
const limitedConfig: BridgeConfig = {
    live_enabled: true,
    allowed_symbols: [],
    max_orders_per_day: 5,
    max_notional_per_day: 100000,
    mode: "LIVE"
};
const atLimitState: BridgeLimitsState = {
    current_order_count: 5,   // Already at limit
    current_notional_usd: 0
};
const orderLimitResult = gateToLive(basePaperExec, limitedConfig, atLimitState, now);
assert(orderLimitResult === null, "Order count at limit → null");

// TEST 3.2: Notional limit exceeded → null
console.log("\\nTEST 3.2: Notional limit exceeded → null");
const notionalLimitedConfig: BridgeConfig = {
    live_enabled: true,
    allowed_symbols: [],
    max_orders_per_day: 100,
    max_notional_per_day: 1000,  // Very low limit
    mode: "LIVE"
};
// Paper exec notional = 0.1 * 50000 = 5000, which exceeds 1000
const notionalLimitResult = gateToLive(basePaperExec, notionalLimitedConfig, baseLimits, now);
assert(notionalLimitResult === null, "Notional exceeds limit → null");

// ============================================
// TEST GROUP 4: Non-FILLED Status Check
// ============================================
console.log("\\n--- TEST GROUP 4: Status Check ---\\n");

// TEST 4.1: Status REJECTED → null
console.log("TEST 4.1: Status REJECTED → null");
const rejectedExec: PaperExecutionResult = {
    ...basePaperExec,
    status: "REJECTED",
    filled_quantity: 0
};
const rejectedResult = gateToLive(rejectedExec, liveConfig, baseLimits, now);
assert(rejectedResult === null, "Status REJECTED → null");

// ============================================
// TEST GROUP 5: Determinism
// ============================================
console.log("\\n--- TEST GROUP 5: Determinism ---\\n");

// TEST 5.1: Same input → same bridge_id
console.log("TEST 5.1: Same input → same bridge_id");
const det1 = gateToLive(basePaperExec, liveConfig, baseLimits, now);
const det2 = gateToLive(basePaperExec, liveConfig, baseLimits, now);
assert(det1?.bridge_id === det2?.bridge_id, "Same input → same bridge_id");

// TEST 5.2: Different timestamp → different bridge_id
console.log("\\nTEST 5.2: Different timestamp → different bridge_id");
const det3 = gateToLive(basePaperExec, liveConfig, baseLimits, now + 1);
assert(det1?.bridge_id !== det3?.bridge_id, "Different timestamp → different bridge_id");

// ============================================
// TEST GROUP 6: Default Config Safety
// ============================================
console.log("\\n--- TEST GROUP 6: Default Config Safety ---\\n");

// TEST 6.1: Safe defaults (PAPER_ONLY + live_enabled=false)
console.log("TEST 6.1: Verify safe default config");
const safeDefaultConfig: BridgeConfig = {
    live_enabled: false,           // Kill-switch OFF by default
    allowed_symbols: [],
    max_orders_per_day: 10,
    max_notional_per_day: 1000,
    mode: "PAPER_ONLY"             // Paper only by default
};
const safeResult = gateToLive(basePaperExec, safeDefaultConfig, baseLimits, now);
assert(safeResult === null, "Safe defaults → no live intent produced");

// ============================================
// PARANOID TEST: Fail-Open Prevention
// ============================================
console.log("\\n--- PARANOID TEST: Fail-Open Prevention ---\\n");

// Even if ALL other conditions are perfect, live_enabled=false MUST block
console.log("Extreme test: Perfect conditions + live_enabled=false → null");
const perfectButOff: BridgeConfig = {
    live_enabled: false,           // THE ONLY BLOCK
    allowed_symbols: ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    max_orders_per_day: 10000,
    max_notional_per_day: 10000000,
    mode: "LIVE"
};
const perfectButOffResult = gateToLive(basePaperExec, perfectButOff, baseLimits, now);
assert(perfectButOffResult === null, "PARANOID: Perfect conditions but live_enabled=false → MUST be null");

console.log("\\n========================================");
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log("========================================");

if (failed > 0) {
    console.log("\\n❌ SOME TESTS FAILED - BRIDGE IS NOT SAFE");
    process.exit(1);
} else {
    console.log("\\n✅ ALL TESTS PASSED - BRIDGE IS SAFE");
}
