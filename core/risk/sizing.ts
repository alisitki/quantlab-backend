import { OrderIntent } from "../execution_adapter/order_intent";
import { RiskConfig } from "./risk_config";
import { RiskAdjustedOrder } from "./risk_adjusted_order";

/**
 * Pure function to apply risk rules and calculate position size.
 * 
 * Logic:
 * 1. Determine execution price (use intent price or placeholder).
 * 2. Calculate risk-based quantity based on assumed_stop_pct and max_risk_pct_per_trade.
 * 3. Calculate notional-based quantity based on max_notional_usd.
 * 4. Clamp the original intent quantity by both risk and notional limits.
 */
export function applyRisk(
    intent: OrderIntent,
    config: RiskConfig,
    now: number
): RiskAdjustedOrder {
    const price = intent.price ?? 1.0; // Placeholder price if not provided
    const capital = config.reference_capital ?? 10000;

    // 1. Risk-based sizing
    // Risk Amount = Capital * max_risk_pct_per_trade
    // Required Qty = Risk Amount / (Price * assumed_stop_pct)
    const riskAmountUsd = capital * config.max_risk_pct_per_trade;
    const riskBasedQty = riskAmountUsd / (price * config.assumed_stop_pct);

    // 2. Notional-based sizing
    // Max Qty = max_notional_usd / Price
    const notionalBasedQty = config.max_notional_usd / price;

    // 3. Final Quantity: Clamped by intent quantity and both limits
    let finalQty = intent.quantity;
    let adjustmentReason = "";

    if (finalQty > riskBasedQty) {
        finalQty = riskBasedQty;
        adjustmentReason = "Risk limit";
    }

    if (finalQty > notionalBasedQty) {
        const prevQty = finalQty;
        finalQty = notionalBasedQty;
        adjustmentReason = adjustmentReason ? `${adjustmentReason} & Notional limit` : "Notional limit";
    }

    // Calculate final metrics
    const notionalUsd = finalQty * price;
    const maxLossUsd = notionalUsd * config.assumed_stop_pct;
    const riskPct = maxLossUsd / capital;

    return {
        intent_id: intent.intent_id,
        symbol: intent.symbol,
        side: intent.side,
        quantity: finalQty,
        notional_usd: notionalUsd,
        risk_pct: riskPct,
        max_loss_usd: maxLossUsd,
        assumed_stop_pct: config.assumed_stop_pct,
        mode: "DRY_RUN",
        evaluated_at: now,
        adjustment_reason: adjustmentReason || undefined
    };
}
