/**
 * FuturesCanaryGate — Pre-order safety evaluation for futures intents.
 * Phase-2.1: Enforces ALL canary rules. NO SIDE EFFECTS. NO EXCHANGE CALLS.
 * 
 * CRITICAL: LIVE mode is STRUCTURALLY UNREACHABLE.
 */

import { FuturesIntentContext } from "./futures_intent_context.js";
import {
    FuturesReasonCode,
    FuturesCanaryResult,
    FuturesCanaryOutcome,
} from "./futures_reason_code.js";
import {
    KillSwitchConfig,
    evaluateKillSwitch,
    DEFAULT_KILL_SWITCH_CONFIG,
} from "./kill_switch.js";

// ============================================================================
// HARD CONSTANTS — CANARY LIMITS (Phase-2.1)
// ============================================================================

/** Maximum leverage allowed in CANARY mode */
export const CANARY_MAX_LEVERAGE = 3;

/** Worst-case move window for liquidation price validation (5%) */
export const CANARY_WORST_CASE_MOVE_PCT = 0.05;

// ============================================================================
// GATE EVALUATION
// ============================================================================

/**
 * Evaluate a FuturesIntentContext against all canary rules.
 * Pure function: deterministic, no side effects, no I/O.
 * 
 * @param intent - The futures intent to evaluate
 * @param now - Current timestamp (injected for determinism)
 * @param killSwitchConfig - Optional kill-switch config (injectable)
 */
export function evaluateFuturesCanaryGate(
    intent: FuturesIntentContext,
    now: number,
    killSwitchConfig: KillSwitchConfig = DEFAULT_KILL_SWITCH_CONFIG
): FuturesCanaryResult {
    // ========================================================================
    // RULE 0: Kill-switch (HIGHEST PRIORITY — overrides ALL other rules)
    // ========================================================================
    const killResult = evaluateKillSwitch(intent, killSwitchConfig);
    if (killResult.killed) {
        return createResult(
            intent,
            "REJECTED",
            killResult.reason_code!,
            now
        );
    }

    // ========================================================================
    // RULE 1: Mode MUST be SHADOW or CANARY — NEVER LIVE
    // ========================================================================
    if (intent.mode === "LIVE") {
        // This is a STRUCTURAL SAFETY: LIVE mode should never reach here.
        // If it does, the system is misconfigured. Hard reject.
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.LIVE_MODE_BLOCKED,
            now
        );
    }

    // ========================================================================
    // RULE 2: Leverage <= CANARY_MAX_LEVERAGE
    // ========================================================================
    if (intent.leverage > CANARY_MAX_LEVERAGE) {
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.LEVERAGE_EXCEEDED,
            now
        );
    }

    // ========================================================================
    // RULE 3: Margin mode MUST be ISOLATED
    // ========================================================================
    if (intent.margin_mode !== "ISOLATED") {
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.NOT_ISOLATED,
            now
        );
    }

    // ========================================================================
    // RULE 4: reduce_only MUST be true
    // ========================================================================
    if (intent.reduce_only !== true) {
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.NOT_REDUCE_ONLY,
            now
        );
    }

    // ========================================================================
    // RULE 5: position_side MUST be ONE_WAY
    // ========================================================================
    if (intent.position_side !== "ONE_WAY") {
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.NOT_ONE_WAY,
            now
        );
    }

    // ========================================================================
    // RULE 6: Liquidation price MUST be outside worst-case move window
    // ========================================================================
    const worstCaseMove = intent.entry_price * CANARY_WORST_CASE_MOVE_PCT;
    const distanceToLiquidation = Math.abs(
        intent.estimated_liquidation_price - intent.entry_price
    );

    if (distanceToLiquidation <= worstCaseMove) {
        return createResult(
            intent,
            "REJECTED",
            FuturesReasonCode.LIQUIDATION_TOO_CLOSE,
            now
        );
    }

    // ========================================================================
    // ALL CHECKS PASSED
    // ========================================================================
    return createResult(intent, "PASSED", FuturesReasonCode.PASSED, now);
}

// ============================================================================
// HELPER
// ============================================================================

function createResult(
    intent: FuturesIntentContext,
    outcome: FuturesCanaryOutcome,
    reason_code: FuturesReasonCode,
    now: number
): FuturesCanaryResult {
    // Validate mode is not LIVE before creating result (defensive)
    const safeMode: "SHADOW" | "CANARY" =
        intent.mode === "LIVE" ? "SHADOW" : intent.mode;

    return Object.freeze({
        intent_id: intent.intent_id,
        symbol: intent.symbol,
        outcome,
        reason_code,
        evaluated_at: now,
        policy_snapshot_hash: intent.policy_snapshot_hash,
        mode: safeMode,
    });
}
