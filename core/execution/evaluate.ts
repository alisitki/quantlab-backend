import { Decision } from "../decision/decision_contract";
import { ExecutionResult, PolicySnapshot, ReasonCode } from "../events/execution_event";
import { checkConfidence } from "./rules/confidence";
import { checkValidity } from "./rules/validity";
import { checkPolicy } from "./rules/policy";
import { checkCooldown, GateState } from "./rules/cooldown";

export function evaluateDecision(
    decision: Decision,
    policy: PolicySnapshot,
    state: GateState,
    now: number
): ExecutionResult {
    // Fail-fast checks in order

    // 1. Validity (Time)
    let reason = checkValidity(decision, now);
    if (reason) return createResult(decision, policy, "REJECTED", reason, now);

    // 2. Confidence
    reason = checkConfidence(decision, policy);
    if (reason) return createResult(decision, policy, "REJECTED", reason, now);

    // 3. Policy & Blacklist
    reason = checkPolicy(decision, policy);
    if (reason) return createResult(decision, policy, "REJECTED", reason, now);

    // 4. Cooldown & Active Decision (State dependent)
    reason = checkCooldown(decision, policy, state, now);
    if (reason) return createResult(decision, policy, "SKIPPED", reason, now);

    // All checks passed
    return createResult(decision, policy, "WOULD_EXECUTE", ReasonCode.PASSED, now);
}

function createResult(
    decision: Decision,
    policy: PolicySnapshot,
    outcome: ExecutionResult["outcome"],
    reason_code: ReasonCode,
    now: number
): ExecutionResult {
    return {
        decision_id: decision.decision_id,
        symbol: decision.symbol,
        outcome,
        reason_code,
        evaluated_at: now,
        policy_snapshot: { ...policy }, // Clone for audit safety
        policy_version: decision.policy_version
    };
}
