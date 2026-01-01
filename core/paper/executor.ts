import { RiskAdjustedOrder } from "../risk/risk_adjusted_order";
import { PaperConfig } from "./paper_config";
import { PaperExecutionResult } from "./paper_execution_result";
import crypto from "node:crypto";

/**
 * Pure function to simulate execution in a paper/sandbox environment.
 * 
 * Determinism:
 * - execution_id is a hash of intent_id and now.
 * - Fill check uses a deterministic threshold derived from intent_id.
 * - Slippage and Latency are applied based on config.
 */
export function executePaper(
    order: RiskAdjustedOrder,
    config: PaperConfig,
    now: number
): PaperExecutionResult {
    const execution_id = crypto
        .createHash("md5")
        .update(`${order.intent_id}:${now}`)
        .digest("hex")
        .substring(0, 12);

    // 1. Determine execution price with slippage
    // We don't have Market Data here, so we use notional_usd / quantity
    // as the "decision price" if quantity > 0, otherwise placeholder.
    let basePrice = order.quantity > 0 ? order.notional_usd / order.quantity : config.price_placeholder;

    if (basePrice <= 0) basePrice = config.price_placeholder;

    const slippageMultiplier = 1 + (config.slippage_bps / 10000) * (order.side === "BUY" ? 1 : -1);
    const fillPrice = basePrice * slippageMultiplier;

    // 2. Deterministic Fill Check (derived from intent_id)
    // Simple MD5 hash of intent_id converted to a float between 0 and 1.
    const hash = crypto.createHash("md5").update(order.intent_id).digest("hex");
    const threshold = parseInt(hash.substring(0, 8), 16) / 0xffffffff;

    const isFilled = config.fill_probability >= 1.0 || threshold <= config.fill_probability;

    // 3. Execution metrics
    const filledQty = isFilled ? order.quantity : 0;
    const status = isFilled ? "FILLED" : "REJECTED";
    const executedAt = now + config.avg_latency_ms;

    return {
        execution_id,
        intent_id: order.intent_id,
        symbol: order.symbol,
        side: order.side,
        requested_quantity: order.quantity,
        filled_quantity: filledQty,
        fill_price: fillPrice,
        slippage_bps: config.slippage_bps,
        latency_ms: config.avg_latency_ms,
        status,
        mode: "PAPER",
        executed_at: executedAt
    };
}
