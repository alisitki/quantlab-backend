/**
 * End-to-End Paranoid Test: DRY → PAPER → PORTFOLIO
 * 
 * Validates the complete pipeline from OrderIntent through Paper Execution
 * to Portfolio with deterministic equity curve output.
 */

import { buildOrderIntent, AdapterConfig } from "./execution_adapter/adapter";
import { applyRisk } from "./risk/sizing";
import { executePaper } from "./paper/executor";
import { PortfolioEngine } from "./portfolio/engine";
import { ExecutionResult, ReasonCode } from "./events/execution_event";
import { RiskConfig } from "./risk/risk_config";
import { PaperConfig } from "./paper/paper_config";
import crypto from "crypto";

// === Configuration ===
const adapterConfig: AdapterConfig = {
    default_quantity: 0.1
};

const riskConfig: RiskConfig = {
    max_risk_pct_per_trade: 0.02,
    max_notional_usd: 10000,
    assumed_stop_pct: 0.01,
    reference_capital: 100000
};

const paperConfig: PaperConfig = {
    fill_probability: 1.0,
    avg_latency_ms: 10,
    slippage_bps: 5,
    price_placeholder: 50000
};

const INITIAL_CAPITAL = 100000;

// === Mock Execution Results (3 decisions) ===
const mockResults: ExecutionResult[] = [
    {
        decision_id: "DEC_001",
        symbol: "BTCUSDT",
        outcome: "WOULD_EXECUTE",
        reason_code: ReasonCode.PASSED,
        evaluated_at: 1000000000,
        policy_snapshot: {
            min_confidence: 0.6,
            allowed_policy_versions: ["v1"],
            ops_blacklist_symbols: [],
            cooldown_ms: 1000,
            mode: "DRY_RUN"
        },
        policy_version: "v1"
    },
    {
        decision_id: "DEC_002",
        symbol: "BTCUSDT",
        outcome: "WOULD_EXECUTE",
        reason_code: ReasonCode.PASSED,
        evaluated_at: 1000001000,
        policy_snapshot: {
            min_confidence: 0.6,
            allowed_policy_versions: ["v1"],
            ops_blacklist_symbols: [],
            cooldown_ms: 1000,
            mode: "DRY_RUN"
        },
        policy_version: "v1"
    },
    {
        decision_id: "DEC_003",
        symbol: "BTCUSDT",
        outcome: "WOULD_EXECUTE",
        reason_code: ReasonCode.PASSED,
        evaluated_at: 1000002000,
        policy_snapshot: {
            min_confidence: 0.6,
            allowed_policy_versions: ["v1"],
            ops_blacklist_symbols: [],
            cooldown_ms: 1000,
            mode: "DRY_RUN"
        },
        policy_version: "v1"
    }
];

const sides: ("LONG" | "SHORT")[] = ["LONG", "SHORT", "LONG"];

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

function hashEquityCurve(engine: { snapshot: (ts: number) => { equity: number } }, ts: number): string {
    const s = engine.snapshot(ts);
    return crypto.createHash("md5").update(s.equity.toFixed(8)).digest("hex").substring(0, 8);
}

function runPipeline(): { finalEquity: number; hash: string } {
    const engine = new PortfolioEngine(INITIAL_CAPITAL);

    for (let i = 0; i < mockResults.length; i++) {
        const result = mockResults[i];
        const side = sides[i];
        const now = result.evaluated_at + 1;

        // Step 1: Build OrderIntent
        const intent = buildOrderIntent(result, adapterConfig, now, side);
        if (!intent) continue;

        // Step 2: Apply Risk
        const risked = applyRisk({ ...intent, price: paperConfig.price_placeholder }, riskConfig, now);

        // Step 3: Execute Paper
        const exec = executePaper(risked, paperConfig, now);

        // Step 4: Apply to Portfolio
        engine.applyExecution(exec);
    }

    const finalSnapshot = engine.snapshot(1000003000);
    const hash = crypto.createHash("sha256")
        .update(JSON.stringify({
            equity: finalSnapshot.equity.toFixed(8),
            cash: finalSnapshot.cash.toFixed(8),
            positions: finalSnapshot.positions
        }))
        .digest("hex")
        .substring(0, 16);

    return { finalEquity: finalSnapshot.equity, hash };
}

console.log("\\n========================================");
console.log("  E2E PIPELINE TEST: DRY → PAPER → PORTFOLIO");
console.log("========================================\\n");

// === RUN 1 ===
console.log("--- RUN 1 ---");
const run1 = runPipeline();
console.log(`Final Equity: ${run1.finalEquity.toFixed(2)}`);
console.log(`Hash: ${run1.hash}`);

// === RUN 2 ===
console.log("\\n--- RUN 2 ---");
const run2 = runPipeline();
console.log(`Final Equity: ${run2.finalEquity.toFixed(2)}`);
console.log(`Hash: ${run2.hash}`);

// === VERIFICATION ===
console.log("\\n--- VERIFICATION ---");

assert(run1.finalEquity === run2.finalEquity, `Deterministic equity: ${run1.finalEquity} = ${run2.finalEquity}`);
assert(run1.hash === run2.hash, `Deterministic hash: ${run1.hash} = ${run2.hash}`);
assert(run1.finalEquity !== INITIAL_CAPITAL, "Equity changed (trades occurred)");

// === DETAILED TRACE ===
console.log("\\n--- DETAILED TRACE ---");
const traceEngine = new PortfolioEngine(INITIAL_CAPITAL);
console.log(`Initial Capital: $${INITIAL_CAPITAL}`);

for (let i = 0; i < mockResults.length; i++) {
    const result = mockResults[i];
    const side = sides[i];
    const now = result.evaluated_at + 1;

    const intent = buildOrderIntent(result, adapterConfig, now, side);
    if (!intent) {
        console.log(`[${i + 1}] No intent (filtered)`);
        continue;
    }

    const risked = applyRisk({ ...intent, price: paperConfig.price_placeholder }, riskConfig, now);
    const exec = executePaper(risked, paperConfig, now);
    traceEngine.applyExecution(exec);

    const s = traceEngine.snapshot(now);
    console.log(`[${i + 1}] ${side} ${exec.filled_quantity.toFixed(4)} @ ${exec.fill_price.toFixed(2)} | ` +
        `Cash: $${s.cash.toFixed(2)} | Equity: $${s.equity.toFixed(2)}`);
}

const metrics = traceEngine.getMetrics();
console.log(`\\nMetrics: trades=${metrics.total_trades}, pnl=${metrics.total_pnl.toFixed(4)}, ` +
    `dd=${(metrics.max_drawdown * 100).toFixed(2)}%`);

console.log("\\n========================================");
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log("========================================");

if (failed > 0) {
    console.log("\\n❌ E2E PIPELINE NOT DETERMINISTIC");
    process.exit(1);
} else {
    console.log("\\n✅ E2E PIPELINE IS DETERMINISTIC");
}
