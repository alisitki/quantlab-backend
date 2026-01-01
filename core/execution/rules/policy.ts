import { Decision } from "../../decision/decision_contract";
import { PolicySnapshot, ReasonCode } from "../../events/execution_event";

export function checkPolicy(decision: Decision, policy: PolicySnapshot): ReasonCode | null {
    if (!policy.allowed_policy_versions.includes(decision.policy_version)) {
        return ReasonCode.POLICY_REJECTED;
    }

    if (policy.ops_blacklist_symbols.includes(decision.symbol)) {
        return ReasonCode.OPS_BLACKLISTED;
    }

    return null;
}
