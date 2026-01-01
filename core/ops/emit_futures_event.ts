/**
 * emitFuturesEvent — Deterministically emits a FuturesCanaryResult as an OpsFuturesEvent.
 * Phase-2.1: Pure function, no side effects except console.log for observability.
 */

import { createHash } from "crypto";
import fs from "fs";
import { FuturesIntentContext } from "../futures/futures_intent_context.js";
import { FuturesCanaryResult } from "../futures/futures_reason_code.js";
import { OpsFuturesEvent } from "../events/ops_futures_event.js";

const EVENTS_LOG = "logs/events.jsonl";

/**
 * Emit an OpsFuturesEvent from a FuturesCanaryResult.
 * Deterministic: same inputs always produce the same event_id.
 * 
 * @param result - The canary gate evaluation result
 * @param intent - The original futures intent (for redacted fields)
 */
export function emitFuturesEvent(
    result: FuturesCanaryResult,
    intent: FuturesIntentContext
): OpsFuturesEvent {
    // Generate deterministic event_id
    const eventId = createHash("sha256")
        .update(
            `${result.intent_id}:${result.evaluated_at}:${result.outcome}:${result.reason_code}`
        )
        .digest("hex")
        .substring(0, 16);

    const event: OpsFuturesEvent = Object.freeze({
        event_type: "FUTURES_CANARY_EVALUATED",
        event_id: eventId,
        intent_id: result.intent_id,
        symbol: result.symbol,
        outcome: result.outcome,
        reason_code: result.reason_code,
        evaluated_at: result.evaluated_at,
        policy_snapshot_hash: result.policy_snapshot_hash,
        mode: result.mode,
        futures_fields: Object.freeze({
            side: intent.side,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            reduce_only: intent.reduce_only,
        }),
    });

    // Persistent storage for dashboard trace
    try {
        fs.appendFileSync(EVENTS_LOG, JSON.stringify(event) + "\n");
    } catch (e) {
        // Silent fail
    }

    // Log for observability (standard console output)
    console.log(`[OPS_EVENT] ${JSON.stringify(event)}`);

    return event;
}
