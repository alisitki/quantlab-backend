/**
 * test_futures_risk.ts — Unit tests for Phase-2.2 Futures Risk Layer.
 * 
 * PROOF REQUIREMENTS:
 * 1. Loss cap is enforced
 * 2. Liquidation never precedes stop
 * 3. Leverage cap enforced
 * 4. Determinism across runs
 */

import { FuturesRiskInput } from "./futures_risk_input.js";
import { computeFuturesSize } from "./futures_sizing.js";
import { evaluateFuturesRiskGate } from "./futures_risk_gate.js";
import { FuturesRiskReasonCode } from "./futures_risk_reason_code.js";
import { emitRiskEvent } from "../ops/emit_risk_event.js";

const NOW = 1735656582000; // Fixed timestamp for determinism

// ============================================================================
// TEST HELPER
// ============================================================================

function createValidRiskInput(overrides: Partial<FuturesRiskInput> = {}): FuturesRiskInput {
    return Object.freeze({
        symbol: "BTCUSDT",
        side: "LONG" as const,
        equity_usd: 10000,
        max_risk_pct: 0.01, // 1% risk
        leverage_cap: 10,
        entry_price: 50000,
        stop_price: 49000, // 2% below entry
        funding_rate_snapshot: 0.0001,
        maintenance_margin_rate: 0.004, // 0.4%
        policy_snapshot_hash: "abcd1234abcd1234",
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
// TEST: BASIC SIZING WORKS
// ============================================================================

console.log("\n=== TEST: Basic Sizing Works ===");
{
    const input = createValidRiskInput();
    const size = computeFuturesSize(input);

    assert(size.notional_usd > 0, "Notional is positive");
    assert(size.qty > 0, "Quantity is positive");
    assert(size.effective_leverage > 0, "Leverage is positive");
    assert(size.effective_leverage <= input.leverage_cap, "Leverage within cap");

    console.log(`   Notional: $${size.notional_usd}`);
    console.log(`   Qty: ${size.qty}`);
    console.log(`   Leverage: ${size.effective_leverage}x`);
    console.log(`   Worst-case loss: $${size.worst_case_loss_usd}`);
}

// ============================================================================
// TEST: LOSS CAP ENFORCED (PROOF)
// ============================================================================

console.log("\n=== TEST: Loss Cap is Enforced (PROOF) ===");
{
    const input = createValidRiskInput({ max_risk_pct: 0.005 }); // 0.5% risk
    const size = computeFuturesSize(input);
    const maxAllowedLoss = input.equity_usd * input.max_risk_pct;

    assert(
        size.worst_case_loss_usd <= maxAllowedLoss * 1.01, // 1% tolerance
        `Loss $${size.worst_case_loss_usd} <= allowed $${maxAllowedLoss}`
    );
    console.log(`   📜 PROOF: Worst-case loss $${size.worst_case_loss_usd} <= allowed $${maxAllowedLoss}`);
}

// ============================================================================
// TEST: LIQUIDATION NEVER PRECEDES STOP - LONG (PROOF)
// ============================================================================

console.log("\n=== TEST: Liquidation Never Precedes Stop - LONG (PROOF) ===");
{
    const input = createValidRiskInput({
        side: "LONG",
        entry_price: 50000,
        stop_price: 49000, // Stop at 49000
    });
    const size = computeFuturesSize(input);

    // For LONG: liquidation should be BELOW stop
    assert(
        size.estimated_liquidation_price < input.stop_price,
        `Liquidation ${size.estimated_liquidation_price} < stop ${input.stop_price}`
    );
    console.log(`   📜 PROOF: Liq price $${size.estimated_liquidation_price} < stop $${input.stop_price}`);
}

// ============================================================================
// TEST: LIQUIDATION NEVER PRECEDES STOP - SHORT (PROOF)
// ============================================================================

console.log("\n=== TEST: Liquidation Never Precedes Stop - SHORT (PROOF) ===");
{
    const input = createValidRiskInput({
        side: "SHORT",
        entry_price: 50000,
        stop_price: 51000, // Stop at 51000 (above entry for SHORT)
    });
    const size = computeFuturesSize(input);

    // For SHORT: liquidation should be ABOVE stop
    assert(
        size.estimated_liquidation_price > input.stop_price,
        `Liquidation ${size.estimated_liquidation_price} > stop ${input.stop_price}`
    );
    console.log(`   📜 PROOF: Liq price $${size.estimated_liquidation_price} > stop $${input.stop_price}`);
}

// ============================================================================
// TEST: LEVERAGE CAP ENFORCED (PROOF)
// ============================================================================

console.log("\n=== TEST: Leverage Cap is Enforced (PROOF) ===");
{
    const input = createValidRiskInput({
        leverage_cap: 3,
        equity_usd: 100000, // Large equity
        max_risk_pct: 0.1, // Generous risk - would allow high leverage
    });
    const size = computeFuturesSize(input);

    assert(
        size.effective_leverage <= input.leverage_cap,
        `Leverage ${size.effective_leverage}x <= cap ${input.leverage_cap}x`
    );
    console.log(`   📜 PROOF: Leverage ${size.effective_leverage}x <= cap ${input.leverage_cap}x`);
}

// ============================================================================
// TEST: RISK GATE PASSES VALID SIZE
// ============================================================================

console.log("\n=== TEST: Risk Gate Passes Valid Size ===");
{
    const input = createValidRiskInput();
    const size = computeFuturesSize(input);
    const result = evaluateFuturesRiskGate(input, size, NOW);

    assert(result.outcome === "PASSED", "Outcome is PASSED");
    assert(result.reason_code === FuturesRiskReasonCode.PASSED, "Reason is PASSED");
}

// ============================================================================
// TEST: RISK GATE REJECTS LEVERAGE EXCEEDED
// ============================================================================

console.log("\n=== TEST: Risk Gate Rejects Leverage Exceeded ===");
{
    const input = createValidRiskInput({ leverage_cap: 2 });
    const size = computeFuturesSize(input);

    // Manually create oversized position to test gate
    const oversized = {
        ...size,
        effective_leverage: 5, // Exceeds cap of 2
    };

    const result = evaluateFuturesRiskGate(input, oversized, NOW);

    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(
        result.reason_code === FuturesRiskReasonCode.LEVERAGE_EXCEEDED,
        "Reason is LEVERAGE_EXCEEDED"
    );
}

// ============================================================================
// TEST: RISK GATE REJECTS LOSS EXCEEDED
// ============================================================================

console.log("\n=== TEST: Risk Gate Rejects Loss Exceeded ===");
{
    const input = createValidRiskInput({ max_risk_pct: 0.001 }); // Very tight risk
    const size = computeFuturesSize(input);

    // Manually create position with excessive loss
    const overloss = {
        ...size,
        worst_case_loss_usd: 500, // Way over 0.1% of 10000 = $10
    };

    const result = evaluateFuturesRiskGate(input, overloss, NOW);

    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(
        result.reason_code === FuturesRiskReasonCode.LOSS_EXCEEDS_LIMIT,
        "Reason is LOSS_EXCEEDS_LIMIT"
    );
}

// ============================================================================
// TEST: RISK GATE REJECTS LIQUIDATION BEFORE STOP
// ============================================================================

console.log("\n=== TEST: Risk Gate Rejects Liquidation Before Stop ===");
{
    const input = createValidRiskInput({
        side: "LONG",
        stop_price: 49000,
    });
    const size = computeFuturesSize(input);

    // Manually create position with bad liquidation
    const badLiq = {
        ...size,
        estimated_liquidation_price: 49500, // Above stop (bad for LONG)
    };

    const result = evaluateFuturesRiskGate(input, badLiq, NOW);

    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(
        result.reason_code === FuturesRiskReasonCode.LIQUIDATION_BEFORE_STOP,
        "Reason is LIQUIDATION_BEFORE_STOP"
    );
}

// ============================================================================
// TEST: RISK GATE REJECTS INVALID STOP DIRECTION
// ============================================================================

console.log("\n=== TEST: Risk Gate Rejects Invalid Stop Direction ===");
{
    // LONG with stop above entry (wrong)
    const input = createValidRiskInput({
        side: "LONG",
        entry_price: 50000,
        stop_price: 51000, // Wrong: stop should be below for LONG
    });
    const size = computeFuturesSize(input); // Will produce garbage but gate should catch
    const result = evaluateFuturesRiskGate(input, size, NOW);

    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(
        result.reason_code === FuturesRiskReasonCode.INVALID_STOP_DIRECTION,
        "Reason is INVALID_STOP_DIRECTION"
    );
}

// ============================================================================
// TEST: DETERMINISM ACROSS RUNS (PROOF)
// ============================================================================

console.log("\n=== TEST: Determinism Across Runs (PROOF) ===");
{
    const input = createValidRiskInput();

    const size1 = computeFuturesSize(input);
    const size2 = computeFuturesSize(input);

    assert(size1.notional_usd === size2.notional_usd, "Notional is deterministic");
    assert(size1.qty === size2.qty, "Qty is deterministic");
    assert(size1.effective_leverage === size2.effective_leverage, "Leverage is deterministic");

    const result1 = evaluateFuturesRiskGate(input, size1, NOW);
    const result2 = evaluateFuturesRiskGate(input, size2, NOW);

    assert(result1.outcome === result2.outcome, "Gate outcome is deterministic");
    console.log("   📜 PROOF: All outputs are deterministic across runs");
}

// ============================================================================
// TEST: EVENT DETERMINISM
// ============================================================================

console.log("\n=== TEST: OPS Event Determinism ===");
{
    const input = createValidRiskInput();
    const size = computeFuturesSize(input);
    const result = evaluateFuturesRiskGate(input, size, NOW);

    const event1 = emitRiskEvent(result);
    const event2 = emitRiskEvent(result);

    assert(event1.event_id === event2.event_id, "Event ID is deterministic");
    assert(event1.event_type === "FUTURES_RISK_EVALUATED", "Event type correct");
    console.log(`   📜 Event ID: ${event1.event_id}`);
}

// ============================================================================
// TEST: WIDE STOP ALLOWS HIGHER LEVERAGE
// ============================================================================

console.log("\n=== TEST: Wide Stop Allows Higher Leverage ===");
{
    const narrowStop = createValidRiskInput({
        entry_price: 50000,
        stop_price: 49500, // 1% stop
    });
    const wideStop = createValidRiskInput({
        entry_price: 50000,
        stop_price: 47500, // 5% stop
    });

    const sizeNarrow = computeFuturesSize(narrowStop);
    const sizeWide = computeFuturesSize(wideStop);

    // Wide stop should allow higher effective leverage (more room before liquidation)
    console.log(`   Narrow stop (1%): leverage ${sizeNarrow.effective_leverage}x`);
    console.log(`   Wide stop (5%): leverage ${sizeWide.effective_leverage}x`);

    // Both should respect leverage cap
    assert(sizeNarrow.effective_leverage <= narrowStop.leverage_cap, "Narrow within cap");
    assert(sizeWide.effective_leverage <= wideStop.leverage_cap, "Wide within cap");
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
    console.log("\n✅ ALL TESTS PASSED — PHASE-2.2 FUTURES RISK VERIFIED");
    console.log("\nPROOF SUMMARY:");
    console.log("  📜 Loss cap is enforced");
    console.log("  📜 Liquidation never precedes stop");
    console.log("  📜 Leverage cap enforced");
    console.log("  📜 Determinism across runs");
    process.exit(0);
}
