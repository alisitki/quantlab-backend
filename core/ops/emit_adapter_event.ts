import { createHash } from "crypto";
import fs from "fs";
import { FuturesAdapterGateResult } from "../futures/futures_adapter_reason_code.js";
import { OpsAdapterEvent } from "../events/ops_adapter_event.js";

const EVENTS_LOG = "logs/events.jsonl";

/**
 * emitAdapterEvent — Deterministically transforms a gate result into an OPS event.
 * Phase-2.4: Pure function.
 */
export function emitAdapterEvent(result: FuturesAdapterGateResult): OpsAdapterEvent {
    // Generate deterministic event_id
    const eventId = createHash("sha256")
        .update(
            `${result.client_order_id}:${result.evaluated_at}:${result.outcome}:${result.reason_code}`
        )
        .digest("hex")
        .substring(0, 16);

    const event: OpsAdapterEvent = Object.freeze({
        event_type: "FUTURES_ORDER_INTENT_MAPPED",
        event_id: eventId,
        client_order_id: result.client_order_id,
        outcome: result.outcome,
        reason_code: result.reason_code,
        policy_snapshot_hash: result.policy_snapshot_hash,
        evaluated_at: result.evaluated_at,
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
