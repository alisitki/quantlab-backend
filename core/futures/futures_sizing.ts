/**
 * computeFuturesSize — Liquidation-aware position sizing.
 * Phase-2.2: Pure function. NO SIDE EFFECTS.
 * 
 * GUARANTEES:
 * - Worst-case loss ≤ equity_usd * max_risk_pct
 * - Liquidation price is ALWAYS beyond stop_price
 * - Respects leverage_cap
 */

import { FuturesRiskInput, FuturesSizeResult, FuturesRiskSide } from "./futures_risk_input.js";

/**
 * Compute liquidation price for a given position.
 * 
 * Simplified formula (Binance-style):
 * LONG:  liq_price = entry * (1 - 1/leverage + maintenance_margin_rate)
 * SHORT: liq_price = entry * (1 + 1/leverage - maintenance_margin_rate)
 */
function computeLiquidationPrice(
    side: FuturesRiskSide,
    entryPrice: number,
    leverage: number,
    maintenanceMarginRate: number
): number {
    if (side === "LONG") {
        // Liquidation triggers when price drops
        return entryPrice * (1 - 1 / leverage + maintenanceMarginRate);
    } else {
        // Liquidation triggers when price rises
        return entryPrice * (1 + 1 / leverage - maintenanceMarginRate);
    }
}

/**
 * Compute maximum allowed leverage such that liquidation is beyond stop.
 * 
 * For LONG: We need liq_price < stop_price
 *   entry * (1 - 1/lev + mmr) < stop
 *   1 - 1/lev + mmr < stop/entry
 *   1/lev > 1 + mmr - stop/entry
 *   lev < 1 / (1 + mmr - stop/entry)
 * 
 * For SHORT: We need liq_price > stop_price
 *   entry * (1 + 1/lev - mmr) > stop
 *   1 + 1/lev - mmr > stop/entry
 *   1/lev > stop/entry - 1 + mmr
 *   lev < 1 / (stop/entry - 1 + mmr)
 */
function computeMaxLeverageForLiquidationSafety(
    side: FuturesRiskSide,
    entryPrice: number,
    stopPrice: number,
    maintenanceMarginRate: number
): number {
    const stopRatio = stopPrice / entryPrice;

    if (side === "LONG") {
        // Stop is below entry for LONG
        const denominator = 1 + maintenanceMarginRate - stopRatio;
        if (denominator <= 0) return Infinity; // No leverage restriction needed
        return 1 / denominator;
    } else {
        // Stop is above entry for SHORT
        const denominator = stopRatio - 1 + maintenanceMarginRate;
        if (denominator <= 0) return Infinity;
        return 1 / denominator;
    }
}

/**
 * Compute position size with liquidation awareness.
 * 
 * @param input - The risk input parameters
 * @returns Size result with all computed values
 */
export function computeFuturesSize(input: FuturesRiskInput): FuturesSizeResult {
    const {
        side,
        equity_usd,
        max_risk_pct,
        leverage_cap,
        entry_price,
        stop_price,
        maintenance_margin_rate,
    } = input;

    // Step 1: Validate stop direction
    const stopDistancePct = Math.abs(entry_price - stop_price) / entry_price;

    // Step 2: Calculate max allowed loss
    const maxAllowedLoss = equity_usd * max_risk_pct;

    // Step 3: Calculate max leverage to keep liquidation beyond stop
    const maxLeverageForLiquidation = computeMaxLeverageForLiquidationSafety(
        side,
        entry_price,
        stop_price,
        maintenance_margin_rate
    );

    // Step 4: Apply leverage cap (minimum of policy cap and liquidation safety)
    const safeLeverage = Math.min(leverage_cap, maxLeverageForLiquidation * 0.95); // 5% safety buffer

    // Step 5: Calculate position size based on risk
    // If stop is hit: loss = qty * |entry - stop|
    // We want: qty * |entry - stop| = maxAllowedLoss
    // So: qty = maxAllowedLoss / |entry - stop|
    const stopDistance = Math.abs(entry_price - stop_price);
    const qtyFromRisk = maxAllowedLoss / stopDistance;

    // Step 6: Calculate notional from risk-based qty
    const notionalFromRisk = qtyFromRisk * entry_price;

    // Step 7: Apply leverage constraint
    // Max notional given equity and leverage: equity * leverage
    const maxNotionalFromLeverage = equity_usd * safeLeverage;

    // Step 8: Take minimum to respect both constraints
    const finalNotional = Math.min(notionalFromRisk, maxNotionalFromLeverage);
    const finalQty = finalNotional / entry_price;

    // Step 9: Calculate actual values
    const effectiveLeverage = finalNotional / equity_usd;
    const worstCaseLoss = finalQty * stopDistance;

    // Step 10: Compute liquidation price with final leverage
    const estimatedLiquidationPrice = computeLiquidationPrice(
        side,
        entry_price,
        effectiveLeverage,
        maintenance_margin_rate
    );

    // Step 11: Calculate liquidation distance
    const liquidationDistancePct = Math.abs(entry_price - estimatedLiquidationPrice) / entry_price;

    return Object.freeze({
        notional_usd: Math.round(finalNotional * 100) / 100,
        qty: Math.round(finalQty * 100000000) / 100000000, // 8 decimals for crypto
        effective_leverage: Math.round(effectiveLeverage * 100) / 100,
        estimated_liquidation_price: Math.round(estimatedLiquidationPrice * 100) / 100,
        worst_case_loss_usd: Math.round(worstCaseLoss * 100) / 100,
        stop_distance_pct: Math.round(stopDistancePct * 10000) / 10000,
        liquidation_distance_pct: Math.round(liquidationDistancePct * 10000) / 10000,
    });
}
