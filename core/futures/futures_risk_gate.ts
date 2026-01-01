/**
 * FuturesRiskGate — Pre-execution risk validation gate.
 * Phase-2.2: Pure function. NO SIDE EFFECTS. NO EXCHANGE CALLS.
 * 
 * GUARANTEES:
 * - Reject if worst_case_loss > allowed
 * - Reject if liquidation crosses stop
 * - Reject if effective_leverage > cap
 */

import { FuturesRiskInput, FuturesSizeResult } from "./futures_risk_input.js";
import {
    FuturesRiskReasonCode,
    FuturesRiskOutcome,
    FuturesRiskGateResult,
} from "./futures_risk_reason_code.js";

// Minimum position size in USD
const MIN_NOTIONAL_USD = 10;

/**
 * Evaluate position size against risk rules.
 * 
 * @param input - The risk input parameters
 * @param size - The computed size result from computeFuturesSize
 * @param now - Current timestamp (injected for determinism)
 */
export function evaluateFuturesRiskGate(
    input: FuturesRiskInput,
    size: FuturesSizeResult,
    now: number
): FuturesRiskGateResult {
    const {
        symbol,
        side,
        equity_usd,
        max_risk_pct,
        leverage_cap,
        entry_price,
        stop_price,
        policy_snapshot_hash,
    } = input;

    const {
        effective_leverage,
        worst_case_loss_usd,
        estimated_liquidation_price,
        stop_distance_pct,
        liquidation_distance_pct,
        notional_usd,
    } = size;

    const riskMetrics = Object.freeze({
        effective_leverage,
        worst_case_loss_usd,
        stop_distance_pct,
        liquidation_distance_pct,
    });

    // ========================================================================
    // RULE 1: Validate stop direction
    // ========================================================================
    if (side === "LONG" && stop_price >= entry_price) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.INVALID_STOP_DIRECTION,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }
    if (side === "SHORT" && stop_price <= entry_price) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.INVALID_STOP_DIRECTION,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }

    // ========================================================================
    // RULE 2: Position size too small
    // ========================================================================
    if (notional_usd < MIN_NOTIONAL_USD) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.SIZE_TOO_SMALL,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }

    // ========================================================================
    // RULE 3: Leverage cap exceeded
    // ========================================================================
    if (effective_leverage > leverage_cap) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.LEVERAGE_EXCEEDED,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }

    // ========================================================================
    // RULE 4: Worst-case loss exceeds allowed risk
    // ========================================================================
    const maxAllowedLoss = equity_usd * max_risk_pct;
    // Allow 1% tolerance for rounding
    if (worst_case_loss_usd > maxAllowedLoss * 1.01) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.LOSS_EXCEEDS_LIMIT,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }

    // ========================================================================
    // RULE 5: Liquidation must be beyond stop
    // For LONG: liq_price < stop_price (liquidation is lower)
    // For SHORT: liq_price > stop_price (liquidation is higher)
    // ========================================================================
    if (side === "LONG" && estimated_liquidation_price >= stop_price) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.LIQUIDATION_BEFORE_STOP,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }
    if (side === "SHORT" && estimated_liquidation_price <= stop_price) {
        return createResult(
            symbol,
            "REJECTED",
            FuturesRiskReasonCode.LIQUIDATION_BEFORE_STOP,
            now,
            policy_snapshot_hash,
            riskMetrics
        );
    }

    // ========================================================================
    // ALL CHECKS PASSED
    // ========================================================================
    return createResult(
        symbol,
        "PASSED",
        FuturesRiskReasonCode.PASSED,
        now,
        policy_snapshot_hash,
        riskMetrics
    );
}

function createResult(
    symbol: string,
    outcome: FuturesRiskOutcome,
    reason_code: FuturesRiskReasonCode,
    now: number,
    policy_snapshot_hash: string,
    risk_metrics: FuturesRiskGateResult["risk_metrics"]
): FuturesRiskGateResult {
    return Object.freeze({
        symbol,
        outcome,
        reason_code,
        evaluated_at: now,
        policy_snapshot_hash,
        risk_metrics,
    });
}
