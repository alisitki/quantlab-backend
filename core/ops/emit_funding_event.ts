/**
 * emitFundingEvent — Deterministically emits a FuturesFundingGateResult as an OpsFundingEvent.
 * Phase-2.3: Pure function.
 */

import { createHash } from "crypto";
import fs from "fs";
import { FuturesFundingGateResult } from "../futures/futures_funding_reason_code.js";
import { OpsFundingEvent } from "../events/ops_funding_event.js";

const EVENTS_LOG = "logs/events.jsonl";

/**
 * Emit an OpsFundingEvent from a FuturesFundingGateResult.
 * 
 * @param result - The funding gate result
 */
export function emitFundingEvent(result: FuturesFundingGateResult): OpsFundingEvent {
    // Generate deterministic event_id
    const eventId = createHash("sha256")
        .update(
            `${result.symbol}:${result.evaluated_at}:${result.outcome}:${result.reason_code}:${result.funding_metrics.funding_cost_usd}`
        )
        .digest("hex")
        .substring(0, 16);

    const event: OpsFundingEvent = Object.freeze({
        event_type: "FUTURES_FUNDING_EVALUATED",
        event_id: eventId,
        symbol: result.symbol,
        outcome: result.outcome,
        reason_code: result.reason_code,
        evaluated_at: result.evaluated_at,
        policy_snapshot_hash: result.policy_snapshot_hash,
        funding_summary: Object.freeze({
            funding_cost_usd: result.funding_metrics.funding_cost_usd,
            funding_cost_pct_equity: result.funding_metrics.funding_cost_pct_equity,
            funding_direction: result.funding_metrics.funding_direction,
        }),
    });

    // Persistent storage for dashboard trace
    try {
        fs.appendFileSync(EVENTS_LOG, JSON.stringify(event) + "\n");
    } catch (e) {
        // Silent fail
    }

    console.log(`[OPS_EVENT] ${JSON.stringify(event)}`);

    return event;
}
