/**
 * FuturesFundingGate — Pre-execution funding risk validation.
 * Phase-2.3: Pure function. NO SIDE EFFECTS.
 * 
 * Rules:
 * - Reject if funding_cost_pct_equity > funding_budget_pct
 * - Reject if funding_rate_snapshot is too toxic (extreme pay scenario)
 */

import { FuturesFundingInput, FundingCostResult } from "./futures_funding_input.js";
import {
    FuturesFundingReasonCode,
    FuturesFundingGateResult,
    FuturesFundingOutcome,
} from "./futures_funding_reason_code.js";

// Extreme toxic funding threshold (e.g., 0.1% per 8h = ~110% APR)
const TOXIC_FUNDING_THRESHOLD = 0.001;

/**
 * Evaluate funding risk for a position.
 * 
 * @param input - The risk input parameters
 * @param cost - The estimated funding cost from estimateFundingCost
 * @param now - Current timestamp (injected for determinism)
 */
export function evaluateFuturesFundingGate(
    input: FuturesFundingInput,
    cost: FundingCostResult,
    now: number
): FuturesFundingGateResult {
    const {
        symbol,
        funding_budget_pct,
        policy_snapshot_hash,
    } = input;

    const {
        funding_cost_usd,
        funding_cost_pct_equity,
        funding_direction,
    } = cost;

    const fundingMetrics = Object.freeze({
        funding_cost_usd,
        funding_cost_pct_equity,
        funding_direction,
    });

    // ========================================================================
    // RULE 1: Budget Enforcement
    // ========================================================================
    if (funding_direction === "PAY" && funding_cost_pct_equity > funding_budget_pct) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesFundingReasonCode.BUDGET_EXCEEDED,
            now,
            policy_snapshot_hash,
            fundingMetrics
        );
    }

    // ========================================================================
    // RULE 2: Toxic Funding Check
    // Reject if we are paying more than the extreme threshold per period
    // ========================================================================
    const perPeriodRate = funding_cost_usd / (input.notional_usd * cost.funding_periods || 1);
    if (funding_direction === "PAY" && perPeriodRate > TOXIC_FUNDING_THRESHOLD) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesFundingReasonCode.TOXIC_FUNDING_RATE,
            now,
            policy_snapshot_hash,
            fundingMetrics
        );
    }

    // ========================================================================
    // ALL CHECKS PASSED
    // ========================================================================
    return createResult(
        symbol,
        "PASSED",
        FuturesFundingReasonCode.PASSED,
        now,
        policy_snapshot_hash,
        fundingMetrics
    );
}

function createResult(
    symbol: string,
    outcome: FuturesFundingOutcome,
    reason_code: FuturesFundingReasonCode,
    now: number,
    policy_snapshot_hash: string,
    funding_metrics: FuturesFundingGateResult["funding_metrics"]
): FuturesFundingGateResult {
    return Object.freeze({
        symbol,
        outcome,
        reason_code,
        evaluated_at: now,
        policy_snapshot_hash,
        funding_metrics,
    });
}
