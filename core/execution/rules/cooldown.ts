import { Decision } from "../../decision/decision_contract";
import { PolicySnapshot, ReasonCode } from "../../events/execution_event";

export interface GateState {
    last_decision_ts_by_symbol: Record<string, number>;
    active_decision_symbols: string[]; // Symbols that currently have an "active" decision
}

export function checkCooldown(
    decision: Decision,
    policy: PolicySnapshot,
    state: GateState,
    now: number
): ReasonCode | null {
    // 1. Check if same symbol has an active decision
    if (state.active_decision_symbols.includes(decision.symbol)) {
        return ReasonCode.NO_ACTIVE_DECISION_ALLOWED;
    }

    // 2. Check cooldown
    const lastTs = state.last_decision_ts_by_symbol[decision.symbol] || 0;
    if (now - lastTs < policy.cooldown_ms) {
        return ReasonCode.COOLDOWN_ACTIVE;
    }

    return null;
}
