/**
 * emitRiskEvent — Deterministically emits a FuturesRiskGateResult as an OpsRiskEvent.
 * Phase-2.2: Pure function, no side effects except console.log for observability.
 */

import { createHash } from "crypto";
import fs from "fs";
import { FuturesRiskGateResult } from "../futures/futures_risk_reason_code.js";
import { OpsRiskEvent } from "../events/ops_risk_event.js";

const EVENTS_LOG = "logs/events.jsonl";

/**
 * Emit an OpsRiskEvent from a FuturesRiskGateResult.
 * Deterministic: same inputs always produce the same event_id.
 * 
 * @param result - The risk gate evaluation result
 */
export function emitRiskEvent(result: FuturesRiskGateResult): OpsRiskEvent {
    // Generate deterministic event_id
    const eventId = createHash("sha256")
        .update(
            `${result.symbol}:${result.evaluated_at}:${result.outcome}:${result.reason_code}:${result.risk_metrics.effective_leverage}`
        )
        .digest("hex")
        .substring(0, 16);

    const event: OpsRiskEvent = Object.freeze({
        event_type: "FUTURES_RISK_EVALUATED",
        event_id: eventId,
        symbol: result.symbol,
        outcome: result.outcome,
        reason_code: result.reason_code,
        evaluated_at: result.evaluated_at,
        policy_snapshot_hash: result.policy_snapshot_hash,
        risk_metrics: Object.freeze({
            effective_leverage: result.risk_metrics.effective_leverage,
            worst_case_loss_usd: result.risk_metrics.worst_case_loss_usd,
            stop_distance_pct: result.risk_metrics.stop_distance_pct,
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
