import { createHash } from "crypto";
import { ExecutionResult } from "../events/execution_event";
import { OrderIntent } from "./order_intent";

export interface AdapterConfig {
    default_quantity: number;
    fixed_limit_price?: number; // For testing/mocking
}

/**
 * Builds a deterministic OrderIntent from an ExecutionResult.
 * Pure function: No external dependencies, no I/O.
 */
export function buildOrderIntent(
    result: ExecutionResult,
    config: AdapterConfig,
    now: number,
    sideOverride?: "LONG" | "SHORT" | "FLAT" // Typically comes from Decision, here we can infer or pass
): OrderIntent | null {
    if (result.outcome !== "WOULD_EXECUTE") {
        return null;
    }

    // In real systems, Decision would be passed here too.
    // For this scope, we infer side from common patterns or assumed LONG/SHORT metadata
    // If Decision isn't available, we'd need it to know if LONG means BUY or SHORT means SELL.
    // In the prompt's context, LONG -> BUY, SHORT -> SELL.

    // Deterministic ID based on decision and evaluation time
    const intentId = createHash("sha256")
        .update(`${result.decision_id}:${now}`)
        .digest("hex")
        .substring(0, 16);

    const side: "BUY" | "SELL" = sideOverride === "SHORT" ? "SELL" : "BUY";

    return {
        intent_id: intentId,
        decision_id: result.decision_id,
        symbol: result.symbol,
        side: side,
        order_type: "MARKET", // Default for now
        quantity: config.default_quantity,
        time_in_force: "IOC",
        mode: "DRY_RUN",
        generated_at: now
    };
}
