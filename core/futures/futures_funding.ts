/**
 * estimateFundingCost — Pure funding cost estimation.
 * Phase-2.3: NO SIDE EFFECTS. Deterministic.
 * 
 * Funding mechanics:
 * - Funding is exchanged every 8 hours
 * - LONG pays funding when rate > 0 (bullish market)
 * - SHORT pays funding when rate < 0 (bearish market)
 */

import { FuturesFundingInput, FundingCostResult } from "./futures_funding_input.js";

/**
 * Estimate total funding cost for a position.
 * 
 * @param input - The funding input parameters
 * @returns Funding cost breakdown
 */
export function estimateFundingCost(input: FuturesFundingInput): FundingCostResult {
    const {
        side,
        notional_usd,
        funding_rate_snapshot,
        expected_hold_hours,
        equity_usd,
    } = input;

    // Step 1: Calculate number of funding periods
    // Funding occurs at 00:00, 08:00, 16:00 UTC
    // If hold < 8h, we might miss funding entirely, but conservatively assume 1 period minimum
    // if hold > 0, otherwise 0
    const fundingPeriods = expected_hold_hours > 0
        ? Math.ceil(expected_hold_hours / 8)
        : 0;

    // Step 2: Determine funding direction based on rate and side
    // - Positive rate: LONG pays, SHORT receives
    // - Negative rate: SHORT pays, LONG receives
    let effectiveRate = funding_rate_snapshot;
    let fundingDirection: "PAY" | "RECEIVE";

    if (side === "LONG") {
        if (funding_rate_snapshot > 0) {
            // LONG pays when rate is positive
            fundingDirection = "PAY";
            effectiveRate = funding_rate_snapshot;
        } else {
            // LONG receives when rate is negative
            fundingDirection = "RECEIVE";
            effectiveRate = Math.abs(funding_rate_snapshot);
        }
    } else {
        // SHORT
        if (funding_rate_snapshot < 0) {
            // SHORT pays when rate is negative
            fundingDirection = "PAY";
            effectiveRate = Math.abs(funding_rate_snapshot);
        } else {
            // SHORT receives when rate is positive
            fundingDirection = "RECEIVE";
            effectiveRate = funding_rate_snapshot;
        }
    }

    // Step 3: Calculate total funding cost
    // cost = notional × rate × periods
    const fundingCostUsd = notional_usd * effectiveRate * fundingPeriods;

    // Step 4: Calculate as percentage of equity
    const fundingCostPctEquity = equity_usd > 0
        ? fundingCostUsd / equity_usd
        : 0;

    // Step 5: Calculate annualized rate (3 periods per day × 365 days)
    const annualizedRatePct = effectiveRate * 3 * 365 * 100;

    return Object.freeze({
        funding_periods: fundingPeriods,
        funding_cost_usd: Math.round(fundingCostUsd * 100) / 100,
        funding_cost_pct_equity: Math.round(fundingCostPctEquity * 10000) / 10000,
        funding_direction: fundingDirection,
        annualized_rate_pct: Math.round(annualizedRatePct * 100) / 100,
    });
}
