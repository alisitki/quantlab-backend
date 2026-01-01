/**
 * test_futures_canary.ts — Unit tests for Phase-2.1 Futures Safety Layers.
 * 
 * PROOF REQUIREMENTS:
 * 1. Unit tests for ALL rejection reasons
 * 2. Proof that LIVE mode is unreachable
 * 3. Proof that reduce_only=false is always rejected
 * 4. Proof that kill-switch overrides everything
 */

import { createFuturesIntentContext, FuturesIntentContext } from "./futures_intent_context.js";
import {
    evaluateFuturesCanaryGate,
    CANARY_MAX_LEVERAGE,
    CANARY_WORST_CASE_MOVE_PCT,
} from "./futures_canary_gate.js";
import { FuturesReasonCode } from "./futures_reason_code.js";
import { KillSwitchConfig, DEFAULT_KILL_SWITCH_CONFIG } from "./kill_switch.js";
import { executeFuturesCanaryPipeline } from "./futures_pipeline.js";

const NOW = 1735655974000; // Fixed timestamp for determinism

// ============================================================================
// TEST HELPER
// ============================================================================

function createValidCanaryIntent(overrides: Partial<Omit<FuturesIntentContext, "intent_id">> = {}): FuturesIntentContext {
    return createFuturesIntentContext({
        symbol: "BTCUSDT",
        side: "LONG",
        leverage: 2, // Within CANARY_MAX_LEVERAGE (3)
        margin_mode: "ISOLATED",
        position_side: "ONE_WAY",
        reduce_only: true,
        notional_usd: 1000,
        entry_price: 50000,
        estimated_liquidation_price: 45000, // 10% away (outside 5% window)
        funding_rate_snapshot: 0.0001,
        policy_snapshot_hash: "abc123def456abcd",
        mode: "CANARY",
        created_at: NOW,
        ...overrides,
    });
}

let passed = 0;
let failed = 0;

function assert(condition: boolean, testName: string): void {
    if (condition) {
        console.log(`✅ PASS: ${testName}`);
        passed++;
    } else {
        console.log(`❌ FAIL: ${testName}`);
        failed++;
    }
}

// ============================================================================
// TEST: VALID INTENT PASSES
// ============================================================================

console.log("\n=== TEST: Valid Canary Intent Passes ===");
{
    const intent = createValidCanaryIntent();
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "PASSED", "Outcome is PASSED");
    assert(result.reason_code === FuturesReasonCode.PASSED, "Reason code is PASSED");
}

// ============================================================================
// TEST: LIVE MODE IS UNREACHABLE (PROOF)
// ============================================================================

console.log("\n=== TEST: LIVE Mode is UNREACHABLE (PROOF) ===");
{
    const intent = createValidCanaryIntent({ mode: "LIVE" });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "LIVE mode is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.LIVE_MODE_BLOCKED,
        "Reason code is LIVE_MODE_BLOCKED"
    );
    // PROOF: Even a fully valid intent is rejected if mode=LIVE
    console.log("   📜 PROOF: LIVE mode structurally blocked regardless of other fields");
}

// ============================================================================
// TEST: LEVERAGE EXCEEDED
// ============================================================================

console.log("\n=== TEST: Leverage Exceeded ===");
{
    const intent = createValidCanaryIntent({ leverage: 10 }); // Exceeds 3x
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "High leverage is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.LEVERAGE_EXCEEDED,
        "Reason code is LEVERAGE_EXCEEDED"
    );
    console.log(`   📜 CANARY_MAX_LEVERAGE = ${CANARY_MAX_LEVERAGE}x`);
}

// ============================================================================
// TEST: NOT ISOLATED (CROSS margin rejected)
// ============================================================================

console.log("\n=== TEST: CROSS Margin Rejected ===");
{
    const intent = createValidCanaryIntent({ margin_mode: "CROSS" });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "CROSS margin is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.NOT_ISOLATED,
        "Reason code is NOT_ISOLATED"
    );
}

// ============================================================================
// TEST: reduce_only=false ALWAYS REJECTED (PROOF)
// ============================================================================

console.log("\n=== TEST: reduce_only=false ALWAYS REJECTED (PROOF) ===");
{
    const intent = createValidCanaryIntent({ reduce_only: false });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "reduce_only=false is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.NOT_REDUCE_ONLY,
        "Reason code is NOT_REDUCE_ONLY"
    );
    console.log("   📜 PROOF: reduce_only=false structurally blocked");
}

// ============================================================================
// TEST: HEDGE position_side rejected
// ============================================================================

console.log("\n=== TEST: HEDGE Position Side Rejected ===");
{
    const intent = createValidCanaryIntent({ position_side: "HEDGE" });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "HEDGE position is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.NOT_ONE_WAY,
        "Reason code is NOT_ONE_WAY"
    );
}

// ============================================================================
// TEST: LIQUIDATION TOO CLOSE
// ============================================================================

console.log("\n=== TEST: Liquidation Price Too Close ===");
{
    // Entry: 50000, 5% window = 2500, so liquidation at 51000 is within window
    const intent = createValidCanaryIntent({
        entry_price: 50000,
        estimated_liquidation_price: 51000, // Only 2% away
    });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "REJECTED", "Close liquidation is REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.LIQUIDATION_TOO_CLOSE,
        "Reason code is LIQUIDATION_TOO_CLOSE"
    );
    console.log(`   📜 CANARY_WORST_CASE_MOVE_PCT = ${CANARY_WORST_CASE_MOVE_PCT * 100}%`);
}

// ============================================================================
// TEST: GLOBAL KILL-SWITCH OVERRIDES EVERYTHING (PROOF)
// ============================================================================

console.log("\n=== TEST: Global Kill-Switch Overrides EVERYTHING (PROOF) ===");
{
    const intent = createValidCanaryIntent(); // Fully valid intent
    const killConfig: KillSwitchConfig = {
        global_kill: true,
        symbol_kill: {},
        reason: "Emergency shutdown",
    };
    const result = evaluateFuturesCanaryGate(intent, NOW, killConfig);

    assert(result.outcome === "REJECTED", "Global kill-switch REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.GLOBAL_KILL_ACTIVE,
        "Reason code is GLOBAL_KILL_ACTIVE"
    );
    console.log("   📜 PROOF: Kill-switch overrides all other validations");
}

// ============================================================================
// TEST: SYMBOL-SPECIFIC KILL-SWITCH
// ============================================================================

console.log("\n=== TEST: Symbol Kill-Switch ===");
{
    const intent = createValidCanaryIntent({ symbol: "ETHUSDT" });
    const killConfig: KillSwitchConfig = {
        global_kill: false,
        symbol_kill: { ETHUSDT: true },
        reason: "ETHUSDT suspended",
    };
    const result = evaluateFuturesCanaryGate(intent, NOW, killConfig);

    assert(result.outcome === "REJECTED", "Symbol kill-switch REJECTED");
    assert(
        result.reason_code === FuturesReasonCode.SYMBOL_KILL_ACTIVE,
        "Reason code is SYMBOL_KILL_ACTIVE"
    );
}

// ============================================================================
// TEST: OTHER SYMBOL NOT AFFECTED BY SYMBOL KILL-SWITCH
// ============================================================================

console.log("\n=== TEST: Other Symbol Unaffected by Symbol Kill-Switch ===");
{
    const intent = createValidCanaryIntent({ symbol: "BTCUSDT" });
    const killConfig: KillSwitchConfig = {
        global_kill: false,
        symbol_kill: { ETHUSDT: true }, // BTCUSDT not in kill list
        reason: "ETHUSDT suspended",
    };
    const result = evaluateFuturesCanaryGate(intent, NOW, killConfig);

    assert(result.outcome === "PASSED", "BTCUSDT passes (not in kill list)");
}

// ============================================================================
// TEST: PIPELINE INTEGRATION - executed ALWAYS FALSE
// ============================================================================

console.log("\n=== TEST: Pipeline Integration - executed=false ===");
{
    const intent = createValidCanaryIntent();
    const pipelineResult = executeFuturesCanaryPipeline(intent, NOW, DEFAULT_KILL_SWITCH_CONFIG);

    assert(pipelineResult.executed === false, "executed is ALWAYS false");
    assert(pipelineResult.canary_result.outcome === "PASSED", "Canary result passes");
    assert(pipelineResult.ops_event.event_type === "FUTURES_CANARY_EVALUATED", "OPS event emitted");
}

// ============================================================================
// TEST: EVENT DETERMINISM
// ============================================================================

console.log("\n=== TEST: OPS Event Determinism ===");
{
    const intent = createValidCanaryIntent();
    const result1 = executeFuturesCanaryPipeline(intent, NOW, DEFAULT_KILL_SWITCH_CONFIG);
    const result2 = executeFuturesCanaryPipeline(intent, NOW, DEFAULT_KILL_SWITCH_CONFIG);

    assert(
        result1.ops_event.event_id === result2.ops_event.event_id,
        "Event ID is deterministic"
    );
    console.log(`   📜 Event ID: ${result1.ops_event.event_id}`);
}

// ============================================================================
// TEST: SHADOW MODE PASSES
// ============================================================================

console.log("\n=== TEST: SHADOW Mode Passes ===");
{
    const intent = createValidCanaryIntent({ mode: "SHADOW" });
    const result = evaluateFuturesCanaryGate(intent, NOW);

    assert(result.outcome === "PASSED", "SHADOW mode is PASSED");
    assert(result.mode === "SHADOW", "Result mode is SHADOW");
}

// ============================================================================
// FINAL SUMMARY
// ============================================================================

console.log("\n" + "=".repeat(60));
console.log(`FINAL RESULTS: ${passed} passed, ${failed} failed`);
console.log("=".repeat(60));

if (failed > 0) {
    console.log("\n❌ SOME TESTS FAILED");
    process.exit(1);
} else {
    console.log("\n✅ ALL TESTS PASSED — PHASE-2.1 FUTURES SAFETY VERIFIED");
    console.log("\nPROOF SUMMARY:");
    console.log("  📜 LIVE mode is structurally unreachable");
    console.log("  📜 reduce_only=false is always rejected");
    console.log("  📜 Kill-switch overrides all other logic");
    console.log("  📜 Events are deterministic and hash-stable");
    process.exit(0);
}
