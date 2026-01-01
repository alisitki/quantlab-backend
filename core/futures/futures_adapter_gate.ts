import { FuturesOrderIntent } from "./futures_order_intent.js";
import {
    FuturesAdapterReasonCode,
    FuturesAdapterGateResult,
    FuturesAdapterOutcome,
} from "./futures_adapter_reason_code.js";

/**
 * evaluateFuturesAdapterGate — Pre-mapping safety gate.
 * Phase-2.4: Pure function. NO SIDE EFFECTS.
 * 
 * Rules:
 * - Reject if reduce_only === false
 * - Reject if margin_mode !== ISOLATED
 * - Reject if mode === LIVE
 */
export function evaluateFuturesAdapterGate(
    intent: FuturesOrderIntent,
    now: number
): FuturesAdapterGateResult {
    // Rule 1: Structural block for LIVE
    if (intent.mode === "LIVE") {
        return createResult(
            "REJECTED",
            FuturesAdapterReasonCode.LIVE_MODE_BLOCKED,
            intent,
            now
        );
    }

    // Rule 2: Margin Mode Check
    if (intent.margin_mode !== "ISOLATED") {
        return createResult(
            "REJECTED",
            FuturesAdapterReasonCode.NOT_ISOLATED,
            intent,
            now
        );
    }

    // Rule 3: Reduce Only Check
    if (intent.reduce_only !== true) {
        return createResult(
            "REJECTED",
            FuturesAdapterReasonCode.NOT_REDUCE_ONLY,
            intent,
            now
        );
    }

    // Success
    return createResult(
        "MAPPED",
        FuturesAdapterReasonCode.PASSED,
        intent,
        now
    );
}

function createResult(
    outcome: FuturesAdapterOutcome,
    reason_code: FuturesAdapterReasonCode,
    intent: FuturesOrderIntent,
    now: number
): FuturesAdapterGateResult {
    return Object.freeze({
        outcome,
        reason_code,
        client_order_id: intent.client_order_id,
        policy_snapshot_hash: intent.policy_snapshot_hash,
        evaluated_at: now,
    });
}
