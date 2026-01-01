/**
 * Paper Execution Adapter Unit Tests
 * Validates: executePaper() determinism, slippage direction, fill logic
 */

import { executePaper } from "./executor";
import { RiskAdjustedOrder } from "../risk/risk_adjusted_order";
import { PaperConfig } from "./paper_config";

const baseOrder: RiskAdjustedOrder = {
    intent_id: "INTENT_001",
    symbol: "BTCUSDT",
    side: "BUY",
    quantity: 0.1,
    notional_usd: 10000,     // 0.1 * $100000 = $10k
    risk_pct: 0.01,
    max_loss_usd: 100,
    assumed_stop_pct: 0.01,
    mode: "DRY_RUN",
    evaluated_at: 1000000000
};

const baseConfig: PaperConfig = {
    fill_probability: 1.0,
    avg_latency_ms: 50,
    slippage_bps: 10,         // 0.1% slippage
    price_placeholder: 100000
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
console.log("  PAPER EXECUTOR TESTS");
console.log("========================================\\n");

// TEST 1: Determinism
console.log("--- TEST 1: Determinism ---");
const result1 = executePaper(baseOrder, baseConfig, now);
const result2 = executePaper(baseOrder, baseConfig, now);
assert(result1.execution_id === result2.execution_id, "Same input → same execution_id");
assert(result1.filled_quantity === result2.filled_quantity, "Same input → same filled_quantity");
assert(result1.fill_price === result2.fill_price, "Same input → same fill_price");
assert(result1.status === result2.status, "Same input → same status");

// TEST 2: BUY slippage direction (price should INCREASE)
console.log("\\n--- TEST 2: BUY Slippage Direction ---");
const buyOrder = { ...baseOrder, side: "BUY" as const };
const buyResult = executePaper(buyOrder, baseConfig, now);
const basePrice = baseOrder.notional_usd / baseOrder.quantity;  // 100000
// BUY: price * (1 + slippage) = 100000 * 1.001 = 100100
const expectedBuyPrice = basePrice * 1.001;
assert(Math.abs(buyResult.fill_price - expectedBuyPrice) < 0.01,
    `BUY slippage increases price: ${buyResult.fill_price.toFixed(2)} ≈ ${expectedBuyPrice.toFixed(2)}`);

// TEST 3: SELL slippage direction (price should DECREASE)
console.log("\\n--- TEST 3: SELL Slippage Direction ---");
const sellOrder = { ...baseOrder, side: "SELL" as const };
const sellResult = executePaper(sellOrder, baseConfig, now);
// SELL: price * (1 - slippage) = 100000 * 0.999 = 99900
const expectedSellPrice = basePrice * 0.999;
assert(Math.abs(sellResult.fill_price - expectedSellPrice) < 0.01,
    `SELL slippage decreases price: ${sellResult.fill_price.toFixed(2)} ≈ ${expectedSellPrice.toFixed(2)}`);

// TEST 4: fill_probability deterministic (based on intent_id)
console.log("\\n--- TEST 4: Fill Probability Deterministic ---");
const partialFillConfig: PaperConfig = {
    ...baseConfig,
    fill_probability: 0.5   // 50% fill chance
};
// Same intent_id should produce same fill decision
const fill1 = executePaper(baseOrder, partialFillConfig, now);
const fill2 = executePaper(baseOrder, partialFillConfig, now);
assert(fill1.status === fill2.status, `Same intent_id → same fill decision: ${fill1.status}`);

// TEST 5: Different intent_ids, one fills one doesn't (probabilistic but deterministic)
console.log("\\n--- TEST 5: Different Intent IDs, Different Outcomes ---");
const order2 = { ...baseOrder, intent_id: "INTENT_002" };
const order3 = { ...baseOrder, intent_id: "INTENT_003" };
const result_id2 = executePaper(order2, partialFillConfig, now);
const result_id3 = executePaper(order3, partialFillConfig, now);
// These may or may not be the same, but they should be consistent across runs
const result_id2_again = executePaper(order2, partialFillConfig, now);
assert(result_id2.status === result_id2_again.status, "Same intent_id always same outcome");

// TEST 6: execution_id is unique per intent+now
console.log("\\n--- TEST 6: Unique Execution IDs ---");
const r1 = executePaper(baseOrder, baseConfig, now);
const r2 = executePaper({ ...baseOrder, intent_id: "DIFFERENT" }, baseConfig, now);
assert(r1.execution_id !== r2.execution_id, "Different intent_id → different execution_id");

// TEST 7: No randomness (run 10 times, all same)
console.log("\\n--- TEST 7: No Randomness ---");
let allSame = true;
for (let i = 0; i < 10; i++) {
    const r = executePaper(baseOrder, baseConfig, now);
    if (r.execution_id !== result1.execution_id || r.fill_price !== result1.fill_price) {
        allSame = false;
        break;
    }
}
assert(allSame, "10 identical runs → identical results");

// TEST 8: Latency applied correctly
console.log("\\n--- TEST 8: Latency Applied ---");
assert(result1.executed_at === now + baseConfig.avg_latency_ms,
    `executed_at = now + latency: ${result1.executed_at} = ${now} + ${baseConfig.avg_latency_ms}`);

// TEST 9: Mode is PAPER
console.log("\\n--- TEST 9: Mode Check ---");
assert(result1.mode === "PAPER", "mode is PAPER");

console.log("\\n========================================");
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log("========================================");

if (failed > 0) {
    console.log("\\n❌ SOME TESTS FAILED");
    process.exit(1);
} else {
    console.log("\\n✅ ALL TESTS PASSED");
}
