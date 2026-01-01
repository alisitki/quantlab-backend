import { Decision } from "../../decision/decision_contract";
import { ReasonCode } from "../../events/execution_event";

export function checkValidity(decision: Decision, now: number): ReasonCode | null {
    if (decision.valid_until_ts <= now) {
        return ReasonCode.EXPIRED_DECISION;
    }
    return null;
}
