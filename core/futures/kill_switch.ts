/**
 * KillSwitchEvaluator — Global and per-symbol kill-switch.
 * Phase-2.1: Overrides ALL other logic when active.
 * 
 * Injectable at runtime via environment or config.
 * DEFAULT = OFF (all kill-switches disabled).
 */

import { FuturesIntentContext } from "./futures_intent_context.js";
import { FuturesReasonCode, FuturesCanaryResult } from "./futures_reason_code.js";

export interface KillSwitchConfig {
    /** Global kill-switch — if true, ALL intents are rejected */
    readonly global_kill: boolean;

    /** Per-symbol kill-switches */
    readonly symbol_kill: Readonly<Record<string, boolean>>;

    /** Reason for kill-switch activation (for audit) */
    readonly reason: string;
}

/**
 * Default kill-switch config — ALL OFF.
 */
export const DEFAULT_KILL_SWITCH_CONFIG: KillSwitchConfig = Object.freeze({
    global_kill: false,
    symbol_kill: Object.freeze({}),
    reason: "",
});

/**
 * Load kill-switch config from environment (if available).
 * Falls back to DEFAULT if env vars not set.
 */
export function loadKillSwitchFromEnv(): KillSwitchConfig {
    const globalKill = process.env.FUTURES_GLOBAL_KILL === "true";
    const symbolKillRaw = process.env.FUTURES_SYMBOL_KILL || "";
    const reason = process.env.FUTURES_KILL_REASON || "";

    // Parse symbol kill list (comma-separated: "BTCUSDT,ETHUSDT")
    const symbolKill: Record<string, boolean> = {};
    if (symbolKillRaw) {
        symbolKillRaw.split(",").forEach((sym) => {
            const trimmed = sym.trim().toUpperCase();
            if (trimmed) symbolKill[trimmed] = true;
        });
    }

    return Object.freeze({
        global_kill: globalKill,
        symbol_kill: Object.freeze(symbolKill),
        reason,
    });
}

export interface KillSwitchResult {
    readonly killed: boolean;
    readonly reason_code: FuturesReasonCode | null;
    readonly reason: string;
}

/**
 * Evaluate kill-switch state against an intent.
 * Pure function: no side effects, no I/O.
 */
export function evaluateKillSwitch(
    intent: FuturesIntentContext,
    config: KillSwitchConfig
): KillSwitchResult {
    // RULE 1: Global kill-switch overrides everything
    if (config.global_kill) {
        return {
            killed: true,
            reason_code: FuturesReasonCode.GLOBAL_KILL_ACTIVE,
            reason: config.reason || "Global kill-switch active",
        };
    }

    // RULE 2: Per-symbol kill-switch
    if (config.symbol_kill[intent.symbol]) {
        return {
            killed: true,
            reason_code: FuturesReasonCode.SYMBOL_KILL_ACTIVE,
            reason: config.reason || `Symbol ${intent.symbol} kill-switch active`,
        };
    }

    // No kill-switch active
    return {
        killed: false,
        reason_code: null,
        reason: "",
    };
}
