import { PaperExecutionResult } from "../paper/paper_execution_result";
import { BridgeConfig, BridgeLimitsState } from "./bridge_config";
import { LiveExecutionIntent } from "./live_execution_intent";
import crypto from "node:crypto";

/**
 * gateToLive - The paranoid security gate.
 * 
 * Returns a LiveExecutionIntent if all checks pass, otherwise returns null.
 * Pure function: all state (limits) is injected.
 */
export function gateToLive(
    exec: PaperExecutionResult,
    config: BridgeConfig,
    limits: BridgeLimitsState,
    now: number
): LiveExecutionIntent | null {
    // RULE 1: Global Kill-Switch (Fail-fast)
    if (!config.live_enabled) return null;

    // RULE 2: Status check (Only filled paper orders can go live)
    if (exec.status !== "FILLED") return null;

    // RULE 3: Mode Check
    if (config.mode === "PAPER_ONLY") return null;

    // RULE 4: Canary Symbol Check (Only for CANARY mode)
    if (config.mode === "CANARY" && !config.allowed_symbols.includes(exec.symbol)) {
        return null;
    }

    // RULE 5: Daily Limit Guards
    const projectedCount = limits.current_order_count + 1;
    const projectedNotional = limits.current_notional_usd + (exec.filled_quantity * exec.fill_price);

    if (projectedCount > config.max_orders_per_day) return null;
    if (projectedNotional > config.max_notional_per_day) return null;

    // All checks passed -> Produce deterministic Live Intent
    const bridge_id = crypto
        .createHash("sha256")
        .update(`${exec.execution_id}:${now}`)
        .digest("hex")
        .substring(0, 16);

    return {
        bridge_id,
        source_execution_id: exec.execution_id,
        symbol: exec.symbol,
        side: exec.side,
        quantity: exec.filled_quantity,
        price: exec.fill_price,
        mode: "LIVE",
        gated_at: now
    };
}
