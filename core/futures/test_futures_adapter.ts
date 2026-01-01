/**
 * test_futures_adapter.ts — Unit tests for Phase-2.4 Futures Adapter.
 * 
 * PROOF REQUIREMENTS:
 * 1. Correct mapping of all fields (MARKET & LIMIT)
 * 2. reduceOnly=false rejected
 * 3. marginMode !== ISOLATED rejected
 * 4. LIVE mode structurally unreachable
 * 5. Determinism of payload + event
 */

import { FuturesOrderIntent } from "./futures_order_intent.js";
import { mapToExchangePayload } from "./futures_adapter_payload.js";
import { evaluateFuturesAdapterGate } from "./futures_adapter_gate.js";
import { FuturesAdapterReasonCode } from "./futures_adapter_reason_code.js";
import { emitAdapterEvent } from "../ops/emit_adapter_event.js";

const NOW = 1735657500000;

function createValidCanaryIntent(overrides: Partial<FuturesOrderIntent> = {}): FuturesOrderIntent {
    return Object.freeze({
        symbol: "BTCUSDT",
        side: "BUY" as const,
        position_side: "LONG" as const,
        qty: 0.1,
        order_type: "MARKET" as const,
        time_in_force: "GTC" as const,
        reduce_only: true,
        close_position: false,
        leverage: 3,
        margin_mode: "ISOLATED" as const,
        client_order_id: "test-order-123",
        mode: "CANARY" as const,
        policy_snapshot_hash: "abcd1234abcd1234",
        ...overrides,
    });
}

function assert(condition: boolean, testName: string): void {
    if (condition) {
        console.log(`✅ PASS: ${testName}`);
    } else {
        console.log(`❌ FAIL: ${testName}`);
        process.exit(1);
    }
}

// ----------------------------------------------------------------------------
// TEST 1: CORRECT MAPPING
// ----------------------------------------------------------------------------
console.log("\n--- TEST 1: Correct Mapping ---");
{
    const intent = createValidCanaryIntent({
        order_type: "LIMIT",
        price: 50000,
        qty: 0.5,
    });
    const payload = mapToExchangePayload(intent);

    assert(payload.symbol === "BTCUSDT", "Symbol mapped");
    assert(payload.side === "BUY", "Side mapped");
    assert(payload.positionSide === "LONG", "positionSide mapped");
    assert(payload.quantity === "0.5", "Quantity mapped");
    assert(payload.price === "50000", "Price mapped");
    assert(payload.type === "LIMIT", "Type mapped");
    assert(payload.timeInForce === "GTC", "timeInForce mapped");
    assert(payload.reduceOnly === "true", "reduceOnly mapped");
    assert((payload as any).client_order_id === undefined, "Binance uses newClientOrderId");
    assert(payload.newClientOrderId === intent.client_order_id, "newClientOrderId mapped");
}

// ----------------------------------------------------------------------------
// TEST 2: REDUCE_ONLY=FALSE REJECTED
// ----------------------------------------------------------------------------
console.log("\n--- TEST 2: reduce_only=false Rejected ---");
{
    const intent = createValidCanaryIntent({ reduce_only: false });
    const result = evaluateFuturesAdapterGate(intent, NOW);
    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(result.reason_code === FuturesAdapterReasonCode.NOT_REDUCE_ONLY, "Reason: NOT_REDUCE_ONLY");
    console.log("   📜 PROOF: reduce_only=false is blocked by gate.");
}

// ----------------------------------------------------------------------------
// TEST 3: MARGIN_MODE !== ISOLATED REJECTED
// ----------------------------------------------------------------------------
console.log("\n--- TEST 3: margin_mode !== ISOLATED Rejected ---");
{
    const intent = createValidCanaryIntent({ margin_mode: "CROSS" });
    const result = evaluateFuturesAdapterGate(intent, NOW);
    assert(result.outcome === "REJECTED", "Outcome is REJECTED");
    assert(result.reason_code === FuturesAdapterReasonCode.NOT_ISOLATED, "Reason: NOT_ISOLATED");
    console.log("   📜 PROOF: CROSS margin is blocked by gate.");
}

// ----------------------------------------------------------------------------
// TEST 4: LIVE MODE STRUCTURALLY UNREACHABLE
// ----------------------------------------------------------------------------
console.log("\n--- TEST 4: LIVE mode structurally unreachable ---");
{
    const intent = createValidCanaryIntent({ mode: "LIVE" });

    // Check gate
    const gateResult = evaluateFuturesAdapterGate(intent, NOW);
    assert(gateResult.outcome === "REJECTED", "Gate rejects LIVE mode");
    assert(gateResult.reason_code === FuturesAdapterReasonCode.LIVE_MODE_BLOCKED, "Reason: LIVE_MODE_BLOCKED");

    // Check payload throw
    try {
        mapToExchangePayload(intent);
        assert(false, "mapToExchangePayload should have thrown for LIVE mode");
    } catch (e: any) {
        assert(e.message.includes("SAFETY VIOLATION"), "mapToExchangePayload throws for LIVE mode");
    }
    console.log("   📜 PROOF: LIVE mode is blocked via gate AND mapping throw.");
}

// ----------------------------------------------------------------------------
// TEST 5: DETERMINISM
// ----------------------------------------------------------------------------
console.log("\n--- TEST 5: Determinism of Payload + Event ---");
{
    const intent = createValidCanaryIntent();

    // Payload determinism
    const payload1 = mapToExchangePayload(intent);
    const payload2 = mapToExchangePayload(intent);
    assert(JSON.stringify(payload1) === JSON.stringify(payload2), "Payload is deterministic");

    // Event determinism
    const gateResult = evaluateFuturesAdapterGate(intent, NOW);
    const event1 = emitAdapterEvent(gateResult);
    const event2 = emitAdapterEvent(gateResult);
    assert(event1.event_id === event2.event_id, "Event ID is deterministic");
    assert(event1.event_type === "FUTURES_ORDER_INTENT_MAPPED", "Event type correct");
    console.log(`   📜 Event ID: ${event1.event_id}`);
}

console.log("\n✅ ALL TESTS PASSED — PHASE-2.4 FUTURES ADAPTER VERIFIED");
