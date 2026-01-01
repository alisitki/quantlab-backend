import { Decision } from "../decision/decision_contract";
import { PolicySnapshot } from "../events/execution_event";
import { ExecutionGate } from "../execution/gate";
import { GateState } from "../execution/rules/cooldown";
import { emitExecutionEvent } from "../ops/emit_execution_event";
import { aggregateMetrics, createInitialMetrics, ShadowMetrics } from "./metrics";

export interface ReplayContext {
    policy: PolicySnapshot;
    initialState: GateState;
}

/**
 * Drives a batch of decisions through the ExecutionGate in Shadow Mode.
 * Deterministic: Injected state and results aggregated.
 */
export function runShadowReplay(
    decisions: Decision[],
    context: ReplayContext,
    nowOverride?: number
): { metrics: ShadowMetrics; finalState: GateState } {
    let currentState = { ...context.initialState };
    let metrics = createInitialMetrics();

    console.log(`[SHADOW_REPLAY] Starting replay for ${decisions.length} decisions...`);

    for (const decision of decisions) {
        // Injected time (either provided or decision's own horizon/validity base)
        const now = nowOverride || decision.valid_until_ts - decision.horizon_ms;

        // 1. Evaluate through Gate
        const result = ExecutionGate.evaluate(decision, context.policy, currentState, now);

        // 2. Emit OPS Event (for downstream logic/logs)
        emitExecutionEvent(result);

        // 3. Aggregate Metrics
        metrics = aggregateMetrics(metrics, result);

        // 4. Update state (Gate is pure, but driver manages state progression)
        // IMPORTANT: Only WOULD_EXECUTE decisions update the last_decision_ts for cooldown
        if (result.outcome === "WOULD_EXECUTE") {
            currentState.last_decision_ts_by_symbol[decision.symbol] = now;
            // Note: In real live gate, active_decision_symbols would be set and then cleared by execution layer.
            // For shadow replay, we might need more logic or just assume stateless evaluation per decision
            // depending on the policy. Here we keep it simple as requested.
        }
    }

    console.log(`[SHADOW_REPLAY] Replay complete.`);
    return { metrics, finalState: currentState };
}
