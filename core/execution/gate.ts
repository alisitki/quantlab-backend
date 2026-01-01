import { Decision } from "../decision/decision_contract";
import { ExecutionResult, PolicySnapshot } from "../events/execution_event";
import { GateState } from "./rules/cooldown";
import { evaluateDecision } from "./evaluate";

/**
 * ExecutionGate
 * Pure implementation of the Decision -> Execution logic.
 * No side effects, no external dependencies.
 */
export class ExecutionGate {
    /**
     * Evaluate a single decision against a policy and current state.
     * Deterministic: context (now) and state (gateState) are injected.
     */
    public static evaluate(
        decision: Decision,
        policy: PolicySnapshot,
        state: GateState,
        now: number
    ): ExecutionResult {
        return evaluateDecision(decision, policy, state, now);
    }
}
