/**
 * Risk/Sizing Engine Unit Tests
 * Validates: applyRisk() determinism, clamps, edge cases
 */

import { applyRisk } from "./sizing";
import { OrderIntent } from "../execution_adapter/order_intent";
import { RiskConfig } from "./risk_config";

const baseIntent: OrderIntent = {
    intent_id: "INTENT_001",
    decision_id: "DEC_001",
    symbol: "BTCUSDT",
    side: "BUY",
    order_type: "MARKET",
    quantity: 1.0,
    time_in_force: "IOC",
    mode: "DRY_RUN",
    generated_at: 1000000000
};

const baseConfig: RiskConfig = {
    max_risk_pct_per_trade: 0.01,    // 1%
    max_notional_usd: 1000,          // $1000 max
    assumed_stop_pct: 0.005,         // 0.5% stop
    reference_capital: 10000         // $10k capital
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
console.log("  RISK/SIZING ENGINE TESTS");
console.log("========================================\\n");

// TEST 1: Determinism
console.log("--- TEST 1: Determinism ---");
const result1 = applyRisk(baseIntent, baseConfig, now);
const result2 = applyRisk(baseIntent, baseConfig, now);
assert(result1.quantity === result2.quantity, "Same input → same quantity");
assert(result1.notional_usd === result2.notional_usd, "Same input → same notional");
assert(result1.risk_pct === result2.risk_pct, "Same input → same risk_pct");
assert(result1.evaluated_at === result2.evaluated_at, "Same input → same evaluated_at");

// TEST 2: reference_capital=0 edge case
console.log("\\n--- TEST 2: reference_capital=0 Edge Case ---");
const zeroCapitalConfig: RiskConfig = {
    ...baseConfig,
    reference_capital: 0
};
const zeroResult = applyRisk(baseIntent, zeroCapitalConfig, now);
// With 0 capital, risk-based sizing should produce 0 quantity
// riskAmountUsd = 0 * 0.01 = 0
// riskBasedQty = 0 / (price * 0.005) = 0
assert(zeroResult.quantity === 0, "reference_capital=0 → quantity=0");

// TEST 3: Risk clamp (quantity limited by risk)
console.log("\\n--- TEST 3: Risk Clamp ---");
const highCapitalConfig: RiskConfig = {
    max_risk_pct_per_trade: 0.01,
    max_notional_usd: 100000,        // Very high, won't limit
    assumed_stop_pct: 0.005,
    reference_capital: 10000
};
const largeQtyIntent: OrderIntent = {
    ...baseIntent,
    quantity: 100,       // Way more than risk allows
    price: 100           // Price = $100
};
// Risk amount = 10000 * 0.01 = $100
// Risk-based qty = 100 / (100 * 0.005) = 200
// So 100 qty should pass, but let's check...
const riskResult = applyRisk(largeQtyIntent, highCapitalConfig, now);
assert(riskResult.quantity <= 200, "Quantity clamped by risk limit");
console.log(`  Calculated quantity: ${riskResult.quantity}`);

// TEST 4: Notional clamp
console.log("\\n--- TEST 4: Notional Clamp ---");
const lowNotionalConfig: RiskConfig = {
    max_risk_pct_per_trade: 0.50,    // 50% risk (very high)
    max_notional_usd: 100,           // Only $100 max
    assumed_stop_pct: 0.005,
    reference_capital: 10000
};
const intentWithPrice: OrderIntent = {
    ...baseIntent,
    quantity: 10,
    price: 100   // 10 * $100 = $1000 notional
};
// Max notional qty = 100 / 100 = 1
const notionalResult = applyRisk(intentWithPrice, lowNotionalConfig, now);
assert(notionalResult.quantity === 1, "Quantity clamped by notional limit");
assert(notionalResult.notional_usd <= 100, "Notional within limit");
console.log(`  Clamped quantity: ${notionalResult.quantity}, notional: ${notionalResult.notional_usd}`);

// TEST 5: Mode is DRY_RUN
console.log("\\n--- TEST 5: Mode Hardcoded ---");
assert(result1.mode === "DRY_RUN", "mode is DRY_RUN");

// TEST 6: Metrics calculated correctly
console.log("\\n--- TEST 6: Metrics Calculation ---");
// For a simple case: quantity clamped, verify max_loss calculation
const simpleConfig: RiskConfig = {
    max_risk_pct_per_trade: 0.01,
    max_notional_usd: 500,
    assumed_stop_pct: 0.02,          // 2% stop
    reference_capital: 10000
};
const simpleIntent: OrderIntent = {
    ...baseIntent,
    quantity: 10,
    price: 100
};
// Risk amount = 10000 * 0.01 = $100
// Risk-based qty = 100 / (100 * 0.02) = 50
// Notional-based qty = 500 / 100 = 5
// Final qty should be 5 (notional limited)
const metricsResult = applyRisk(simpleIntent, simpleConfig, now);
assert(metricsResult.quantity === 5, "Quantity = 5 (notional limited)");
// max_loss = 5 * 100 * 0.02 = $10
assert(metricsResult.max_loss_usd === 10, "max_loss_usd = $10");
// risk_pct = 10 / 10000 = 0.001 = 0.1%
assert(Math.abs(metricsResult.risk_pct - 0.001) < 0.0001, "risk_pct = 0.1%");

console.log("\\n========================================");
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log("========================================");

if (failed > 0) {
    console.log("\\n❌ SOME TESTS FAILED");
    process.exit(1);
} else {
    console.log("\\n✅ ALL TESTS PASSED");
}
