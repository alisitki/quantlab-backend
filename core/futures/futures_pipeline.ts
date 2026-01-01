/**
 * FuturesPipeline — Integration point for futures canary evaluation.
 * Phase-2.1: Wires KillSwitch → FuturesCanaryGate → OPS Event.
 * 
 * INACTIVE: No live orders, no exchange calls.
 * Runs ONLY when mode = CANARY or SHADOW.
 * Does NOT modify Phase-1/1.5 logic.
 */

import {
    FuturesIntentContext,
    createFuturesIntentContext,
} from "./futures_intent_context.js";
import {
    evaluateFuturesCanaryGate,
    CANARY_MAX_LEVERAGE,
    CANARY_WORST_CASE_MOVE_PCT,
} from "./futures_canary_gate.js";
import {
    FuturesCanaryResult,
    FuturesReasonCode,
} from "./futures_reason_code.js";
import {
    KillSwitchConfig,
    loadKillSwitchFromEnv,
    DEFAULT_KILL_SWITCH_CONFIG,
} from "./kill_switch.js";
import { emitFuturesEvent } from "../ops/emit_futures_event.js";
import { OpsFuturesEvent } from "../events/ops_futures_event.js";

// ============================================================================
// PIPELINE OUTPUT
// ============================================================================

export interface FuturesPipelineResult {
    readonly canary_result: FuturesCanaryResult;
    readonly ops_event: OpsFuturesEvent;
    readonly executed: false; // Always false — Phase-2.1 is INACTIVE
}

// ============================================================================
// MAIN PIPELINE
// ============================================================================

/**
 * Execute the futures canary pipeline.
 * 
 * This is the INTEGRATION POINT that sits AFTER existing Decision/Gate
 * but BEFORE any execution adapter.
 * 
 * SAFETY GUARANTEES:
 * - LIVE mode is structurally blocked
 * - No exchange calls
 * - No order execution
 * - Pure evaluation only
 * 
 * @param intent - The futures intent context
 * @param now - Current timestamp (injected for determinism)
 * @param killSwitchConfig - Optional kill-switch config (defaults to env-loaded or DEFAULT)
 */
export function executeFuturesCanaryPipeline(
    intent: FuturesIntentContext,
    now: number,
    killSwitchConfig?: KillSwitchConfig
): FuturesPipelineResult {
    // Load kill-switch from env if not provided
    const effectiveKillSwitch =
        killSwitchConfig ?? loadKillSwitchFromEnv();

    // Step 1: Evaluate through canary gate (includes kill-switch)
    const canaryResult = evaluateFuturesCanaryGate(
        intent,
        now,
        effectiveKillSwitch
    );

    // Step 2: Emit OPS event (deterministic)
    const opsEvent = emitFuturesEvent(canaryResult, intent);

    // Step 3: Return result (NEVER executes — Phase-2.1 is INACTIVE)
    return Object.freeze({
        canary_result: canaryResult,
        ops_event: opsEvent,
        executed: false,
    });
}

// ============================================================================
// EXPORTS (for external use)
// ============================================================================

export {
    FuturesIntentContext,
    createFuturesIntentContext,
    FuturesCanaryResult,
    FuturesReasonCode,
    KillSwitchConfig,
    DEFAULT_KILL_SWITCH_CONFIG,
    evaluateFuturesCanaryGate,
    CANARY_MAX_LEVERAGE,
    CANARY_WORST_CASE_MOVE_PCT,
};
