/**
 * test_futures_funding.ts — Unit tests for Phase-2.3 Futures Funding Layer.
 * 
 * PROOF REQUIREMENTS:
 * 1. Correct funding period calculation
 * 2. Budget enforcement
 * 3. Determinism across runs
 * 4. Zero funding when hold < 8h (if hold_hours=0)
 */

import { FuturesFundingInput } from "./futures_funding_input.js";
import { estimateFundingCost } from "./futures_funding.js";
import { evaluateFuturesFundingGate } from "./futures_funding_gate.js";
import { FuturesFundingReasonCode } from "./futures_funding_reason_code.js";
import { emitFundingEvent } from "../ops/emit_funding_event.js";

const NOW = 1735657000000;

function createValidFundingInput(overrides: Partial<FuturesFundingInput> = {}): FuturesFundingInput {
    return Object.freeze({
        symbol: "BTCUSDT",
        side: "LONG" as const,
        notional_usd: 10000,
        funding_rate_snapshot: 0.0001, // 0.01% per 8h
        expected_hold_hours: 24, // 3 periods
        funding_budget_pct: 0.001, // 0.1% budget
        equity_usd: 100000,
        policy_snapshot_hash: "funding_policy_123",
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
// TEST: CORRECT PERIOD CALCULATION (PROOF)
// ============================================================================

console.log("\n=== TEST: Correct Funding Period Calculation (PROOF) ===");
{
    const input8h = createValidFundingInput({ expected_hold_hours: 8 });
    const cost8h = estimateFundingCost(input8h);
    assert(cost8h.funding_periods === 1, "8h = 1 period");

    const input24h = createValidFundingInput({ expected_hold_hours: 24 });
    const cost24h = estimateFundingCost(input24h);
    assert(cost24h.funding_periods === 3, "24h = 3 periods");

    const input1h = createValidFundingInput({ expected_hold_hours: 1 });
    const cost1h = estimateFundingCost(input1h);
    assert(cost1h.funding_periods === 1, "1h = 1 period (conservative)");
}

// ============================================================================
// TEST: ZERO FUNDING WHEN HOLD IS ZERO (PROOF)
// ============================================================================

console.log("\n=== TEST: Zero Funding When Hold < 8h (Hold=0) (PROOF) ===");
{
    const input0h = createValidFundingInput({ expected_hold_hours: 0 });
    const cost0h = estimateFundingCost(input0h);
    assert(cost0h.funding_periods === 0, "0h = 0 periods");
    assert(cost0h.funding_cost_usd === 0, "0h = $0 cost");
}

// ============================================================================
// TEST: BUDGET ENFORCEMENT (PROOF)
// ============================================================================

console.log("\n=== TEST: Budget Enforcement (PROOF) ===");
{
    // Total cost: 10000 * 0.0001 * 3 = $3
    // Cost pct: 3 / 100000 = 0.00003 (0.003%)
    // Budget: 0.00001 (0.001%)
    const input = createValidFundingInput({
        funding_budget_pct: 0.00001,
        notional_usd: 100000, // Cost = $30
        equity_usd: 100000,    // Cost % = 0.03%
    });
    const cost = estimateFundingCost(input);
    const result = evaluateFuturesFundingGate(input, cost, NOW);

    assert(result.outcome === "REJECTED", "Rejects when budget exceeded");
    assert(result.reason_code === FuturesFundingReasonCode.BUDGET_EXCEEDED, "Reason: BUDGET_EXCEEDED");
    console.log(`   📜 PROOF: Rejects cost ${cost.funding_cost_pct_equity * 100}% > budget ${input.funding_budget_pct * 100}%`);
}

// ============================================================================
// TEST: TOXIC FUNDING RATE REJECTION
// ============================================================================

console.log("\n=== TEST: Toxic Funding Rate Rejection ===");
{
    const input = createValidFundingInput({
        funding_rate_snapshot: 0.002, // 0.2% per 8h (very high)
    });
    const cost = estimateFundingCost(input);
    const result = evaluateFuturesFundingGate(input, cost, NOW);

    assert(result.outcome === "REJECTED", "Rejects toxic funding");
    assert(result.reason_code === FuturesFundingReasonCode.TOXIC_FUNDING_RATE, "Reason: TOXIC_FUNDING_RATE");
}

// ============================================================================
// TEST: DETERMINISM ACROSS RUNS (PROOF)
// ============================================================================

console.log("\n=== TEST: Determinism Across Runs (PROOF) ===");
{
    const input = createValidFundingInput();
    const cost1 = estimateFundingCost(input);
    const cost2 = estimateFundingCost(input);

    assert(cost1.funding_cost_usd === cost2.funding_cost_usd, "Cost is deterministic");

    const result1 = evaluateFuturesFundingGate(input, cost1, NOW);
    const result2 = evaluateFuturesFundingGate(input, cost2, NOW);

    assert(result1.reason_code === result2.reason_code, "Outcome is deterministic");
}

// ============================================================================
// TEST: EVENT DETERMINISM (PROOF)
// ============================================================================

console.log("\n=== TEST: OPS Event Determinism (PROOF) ===");
{
    const input = createValidFundingInput();
    const cost = estimateFundingCost(input);
    const result = evaluateFuturesFundingGate(input, cost, NOW);

    const event1 = emitFundingEvent(result);
    const event2 = emitFundingEvent(result);

    assert(event1.event_id === event2.event_id, "Event ID is deterministic");
    console.log(`   📜 Event ID: ${event1.event_id}`);
}

// ============================================================================
// TEST: RECEIVING FUNDING PASSES BUDGET
// ============================================================================

console.log("\n=== TEST: Receiving Funding Passes Budget Regardless of Value ===");
{
    const input = createValidFundingInput({
        side: "SHORT",
        funding_rate_snapshot: 0.001, // Funding is positive, SHORT receives
    });
    const cost = estimateFundingCost(input);
    const result = evaluateFuturesFundingGate(input, cost, NOW);

    assert(cost.funding_direction === "RECEIVE", "Funding direction is RECEIVE");
    assert(result.outcome === "PASSED", "Passes when receiving funding");
}

// ============================================================================
// FINAL SUMMARY
// ============================================================================

console.log("\n" + "=".repeat(60));
console.log(`FINAL RESULTS: ${passed} passed, ${failed} failed`);
console.log("=".repeat(60));

if (failed > 0) {
    process.exit(1);
} else {
    console.log("\n✅ ALL TESTS PASSED — PHASE-2.3 FUTURES FUNDING VERIFIED");
    process.exit(0);
}
