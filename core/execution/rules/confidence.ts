import { Decision } from "../../decision/decision_contract";
import { PolicySnapshot, ReasonCode } from "../../events/execution_event";

export function checkConfidence(decision: Decision, policy: PolicySnapshot): ReasonCode | null {
    if (decision.confidence < policy.min_confidence) {
        return ReasonCode.LOW_CONFIDENCE;
    }
    return null;
}
