/**
 * Portfolio & PnL Engine Unit Tests
 * Validates: updatePosition(), PortfolioEngine, metrics
 */

import { PortfolioEngine } from "./engine";
import { updatePosition, calculateUnrealized } from "./pnl";
import { calculateMetrics } from "./metrics";
import { PaperExecutionResult } from "../paper/paper_execution_result";
import { Position } from "./position";

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

function approxEqual(a: number, b: number, tolerance: number = 0.0001): boolean {
    return Math.abs(a - b) < tolerance;
}

console.log("\\n========================================");
console.log("  PORTFOLIO & PNL ENGINE TESTS");
console.log("========================================\\n");

// === TEST GROUP 1: updatePosition ===
console.log("--- TEST GROUP 1: updatePosition ---\\n");

// TEST 1.1: Open long position
console.log("TEST 1.1: Open Long Position");
const buyExec: PaperExecutionResult = {
    execution_id: "EX001",
    intent_id: "INT001",
    symbol: "BTCUSDT",
    side: "BUY",
    requested_quantity: 1,
    filled_quantity: 1,
    fill_price: 100,
    slippage_bps: 10,
    latency_ms: 50,
    status: "FILLED",
    mode: "PAPER",
    executed_at: 1000000000
};
const pos1 = updatePosition(undefined, buyExec);
assert(pos1.quantity === 1, "Long: quantity = 1");
assert(pos1.avg_entry_price === 100, "Long: avg_entry_price = 100");
assert(pos1.realized_pnl === 0, "Long: realized_pnl = 0 (position open)");

// TEST 1.2: Add to long position
console.log("\\nTEST 1.2: Add to Long Position");
const buyExec2: PaperExecutionResult = {
    ...buyExec,
    execution_id: "EX002",
    fill_price: 110
};
const pos2 = updatePosition(pos1, buyExec2);
assert(pos2.quantity === 2, "Added: quantity = 2");
// avg = (1*100 + 1*110) / 2 = 105
assert(pos2.avg_entry_price === 105, "Avg entry price = 105");

// TEST 1.3: Partial close (realize profit)
console.log("\\nTEST 1.3: Partial Close (Profit)");
const sellExec: PaperExecutionResult = {
    ...buyExec,
    execution_id: "EX003",
    side: "SELL",
    fill_price: 120  // Selling at profit
};
const pos3 = updatePosition(pos2, sellExec);
assert(pos3.quantity === 1, "Partial close: quantity = 1");
// Realized PnL = 1 * (120 - 105) = 15
assert(pos3.realized_pnl === 15, `Partial close: realized_pnl = 15 (got ${pos3.realized_pnl})`);

// TEST 1.4: Full close
console.log("\\nTEST 1.4: Full Close");
const sellExec2: PaperExecutionResult = {
    ...buyExec,
    execution_id: "EX004",
    side: "SELL",
    fill_price: 130
};
const pos4 = updatePosition(pos3, sellExec2);
assert(pos4.quantity === 0, "Full close: quantity = 0");
// Additional realized = 1 * (130 - 105) = 25
// Total realized = 15 + 25 = 40
assert(pos4.realized_pnl === 40, `Full close: total realized_pnl = 40 (got ${pos4.realized_pnl})`);

// TEST 1.5: Short position
console.log("\\nTEST 1.5: Short Position");
const shortExec: PaperExecutionResult = {
    ...buyExec,
    execution_id: "EX005",
    side: "SELL",
    fill_price: 100
};
const shortPos = updatePosition(undefined, shortExec);
assert(shortPos.quantity === -1, "Short: quantity = -1");

// === TEST GROUP 2: calculateUnrealized ===
console.log("\\n--- TEST GROUP 2: calculateUnrealized ---\\n");

// TEST 2.1: Long position unrealized profit
console.log("TEST 2.1: Long Unrealized Profit");
const longPos: Position = {
    symbol: "BTCUSDT",
    quantity: 2,
    avg_entry_price: 100,
    realized_pnl: 0,
    unrealized_pnl: 0
};
const unrealizedProfit = calculateUnrealized(longPos, 120);  // Current price 120
// Unrealized = 2 * (120 - 100) = 40
assert(unrealizedProfit === 40, `Long unrealized profit = 40 (got ${unrealizedProfit})`);

// TEST 2.2: Short position unrealized profit
console.log("\\nTEST 2.2: Short Unrealized Profit");
const shortPosTest: Position = {
    symbol: "BTCUSDT",
    quantity: -2,
    avg_entry_price: 100,
    realized_pnl: 0,
    unrealized_pnl: 0
};
const shortUnrealized = calculateUnrealized(shortPosTest, 80);  // Price dropped to 80
// Short profit = 2 * (100 - 80) = 40
assert(shortUnrealized === 40, `Short unrealized profit = 40 (got ${shortUnrealized})`);

// TEST 2.3: No position
console.log("\\nTEST 2.3: No Position");
const noPos: Position = { symbol: "X", quantity: 0, avg_entry_price: 0, realized_pnl: 0, unrealized_pnl: 0 };
assert(calculateUnrealized(noPos, 100) === 0, "No position → unrealized = 0");

// === TEST GROUP 3: PortfolioEngine ===
console.log("\\n--- TEST GROUP 3: PortfolioEngine ---\\n");

// TEST 3.1: Equity = cash + positions
console.log("TEST 3.1: Equity Calculation");
const engine = new PortfolioEngine(10000);
engine.applyExecution(buyExec);  // Buy 1 @ 100
const snapshot = engine.snapshot(1000000001);
// Cash = 10000 - 100 = 9900
// Position value = 1 * 100 = 100
// Equity = 9900 + 100 = 10000
assert(snapshot.cash === 9900, `Cash = 9900 (got ${snapshot.cash})`);
assert(snapshot.equity === 10000, `Equity = 10000 (got ${snapshot.equity})`);

// TEST 3.2: Multiple trades
console.log("\\nTEST 3.2: Multiple Trades");
engine.applyExecution({ ...buyExec, execution_id: "EX010", fill_price: 110 });  // Buy 1 more @ 110
const s2 = engine.snapshot(1000000002);
// Cash = 9900 - 110 = 9790
// Position value = 2 * 105 = 210 (avg entry)
// Equity = 9790 + 210 = 10000
assert(s2.cash === 9790, `Cash = 9790 (got ${s2.cash})`);

// === TEST GROUP 4: Metrics ===
console.log("\\n--- TEST GROUP 4: Metrics ---\\n");

// TEST 4.1: Max Drawdown Calculation
console.log("TEST 4.1: Max Drawdown");
const equityHistory = [
    { equity: 10000 },
    { equity: 11000 },  // Peak
    { equity: 9900 },   // Drawdown from 11000: (11000-9900)/11000 = 0.1 = 10%
    { equity: 10500 }
];
const metrics = calculateMetrics([100, -50, 150], equityHistory);
// Max DD = (11000 - 9900) / 11000 = 0.1
assert(approxEqual(metrics.max_drawdown, 0.1, 0.001),
    `Max drawdown = 10% (got ${(metrics.max_drawdown * 100).toFixed(2)}%)`);

// TEST 4.2: Win rate
console.log("\\nTEST 4.2: Win Rate");
assert(metrics.total_trades === 3, "Total trades = 3");
assert(metrics.win_trades === 2, "Win trades = 2");  // 100, 150 positive
assert(metrics.loss_trades === 1, "Loss trades = 1");  // -50 negative
assert(approxEqual(metrics.win_rate, 2 / 3, 0.01), `Win rate = 66.67% (got ${(metrics.win_rate * 100).toFixed(2)}%)`);

// TEST 4.3: Total PnL
console.log("\\nTEST 4.3: Total PnL");
// Total = 100 - 50 + 150 = 200
assert(metrics.total_pnl === 200, `Total PnL = 200 (got ${metrics.total_pnl})`);

// === TEST GROUP 5: Determinism ===
console.log("\\n--- TEST GROUP 5: Determinism ---\\n");

console.log("TEST 5.1: Same Executions → Same Metrics");
const eng1 = new PortfolioEngine(10000);
const eng2 = new PortfolioEngine(10000);

const execs = [
    { ...buyExec, execution_id: "A1", fill_price: 100 },
    { ...buyExec, execution_id: "A2", side: "SELL" as const, fill_price: 110 }
];

for (const e of execs) {
    eng1.applyExecution(e);
    eng2.applyExecution(e);
}

const m1 = eng1.getMetrics();
const m2 = eng2.getMetrics();

assert(m1.total_pnl === m2.total_pnl, "Deterministic: same total_pnl");
assert(m1.max_drawdown === m2.max_drawdown, "Deterministic: same max_drawdown");

console.log("\\n========================================");
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log("========================================");

if (failed > 0) {
    console.log("\\n❌ SOME TESTS FAILED");
    process.exit(1);
} else {
    console.log("\\n✅ ALL TESTS PASSED");
}
