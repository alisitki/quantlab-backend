import { FuturesOrderIntent } from "./futures_order_intent.js";

/**
 * ExchangePayload — Binance-style JSON payload for futures orders.
 * Generic structure for inactive analysis.
 */
export interface ExchangePayload {
    symbol: string;
    side: string;
    positionSide: string;
    type: string;
    quantity: string;
    price?: string;
    timeInForce?: string;
    reduceOnly: string;
    closePosition: string;
    newClientOrderId: string;
}

/**
 * mapToExchangePayload — Pure mapping from internal intent to exchange-specific format.
 * Phase-2.4: DETERMINISTIC. NO EXTERNAL CALLS.
 * 
 * Rules:
 * - positionSide mapping: LONG -> LONG, SHORT -> SHORT
 * - side mapping: BUY -> BUY, SELL -> SELL
 * - If mode === LIVE -> THROW (Structural safety block)
 */
export function mapToExchangePayload(intent: FuturesOrderIntent): ExchangePayload {
    // STRUCTURAL SAFETY BLOCK
    if (intent.mode === "LIVE") {
        throw new Error("CRITICAL SAFETY VIOLATION: LIVE mode structurally unreachable in Phase-2.4 Adapter.");
    }

    const payload: ExchangePayload = {
        symbol: intent.symbol,
        side: intent.side,
        positionSide: intent.position_side,
        type: intent.order_type,
        quantity: intent.qty.toString(),
        reduceOnly: intent.reduce_only.toString(),
        closePosition: intent.close_position.toString(),
        newClientOrderId: intent.client_order_id,
    };

    if (intent.order_type === "LIMIT") {
        if (intent.price === undefined) {
            throw new Error(`Invalid LIMIT order: price is missing for ${intent.client_order_id}`);
        }
        payload.price = intent.price.toString();
        payload.timeInForce = intent.time_in_force;
    }

    return Object.freeze(payload);
}
