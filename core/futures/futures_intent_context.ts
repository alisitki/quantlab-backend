/**
 * FuturesIntentContext — Immutable data structure for futures order intent.
 * Phase-2.1: Safety layer contract. NO DEFAULTS. NO IMPLICIT VALUES.
 */

import { createHash } from "crypto";

export type FuturesSide = "LONG" | "SHORT";
export type FuturesMarginMode = "ISOLATED" | "CROSS";
export type FuturesPositionSide = "ONE_WAY" | "HEDGE";
export type FuturesMode = "SHADOW" | "CANARY" | "LIVE";

export interface FuturesIntentContext {
    /** Trading symbol (e.g., BTCUSDT) */
    readonly symbol: string;

    /** Trade direction */
    readonly side: FuturesSide;

    /** Leverage multiplier (e.g., 3 for 3x) */
    readonly leverage: number;

    /** Margin mode - MUST be ISOLATED for canary */
    readonly margin_mode: FuturesMarginMode;

    /** Position side mode - MUST be ONE_WAY for canary */
    readonly position_side: FuturesPositionSide;

    /** Whether this is a reduce-only order - MUST be true for canary */
    readonly reduce_only: boolean;

    /** Notional value in USD */
    readonly notional_usd: number;

    /** Expected entry price */
    readonly entry_price: number;

    /** Estimated liquidation price */
    readonly estimated_liquidation_price: number;

    /** Current funding rate snapshot */
    readonly funding_rate_snapshot: number;

    /** SHA-256 hash (16 chars) of the policy snapshot used */
    readonly policy_snapshot_hash: string;

    /** Execution mode - LIVE is structurally unreachable */
    readonly mode: FuturesMode;

    /** Intent creation timestamp */
    readonly created_at: number;

    /** Unique intent ID (deterministic hash) */
    readonly intent_id: string;
}

/**
 * Factory function to create FuturesIntentContext.
 * Enforces all fields are explicitly provided — NO DEFAULTS.
 */
export function createFuturesIntentContext(
    params: Omit<FuturesIntentContext, "intent_id"> & { intent_id?: never }
): FuturesIntentContext {
    // Generate deterministic intent_id
    const intentId = createHash("sha256")
        .update(
            `${params.symbol}:${params.side}:${params.leverage}:${params.entry_price}:${params.created_at}`
        )
        .digest("hex")
        .substring(0, 16);

    return Object.freeze({
        symbol: params.symbol,
        side: params.side,
        leverage: params.leverage,
        margin_mode: params.margin_mode,
        position_side: params.position_side,
        reduce_only: params.reduce_only,
        notional_usd: params.notional_usd,
        entry_price: params.entry_price,
        estimated_liquidation_price: params.estimated_liquidation_price,
        funding_rate_snapshot: params.funding_rate_snapshot,
        policy_snapshot_hash: params.policy_snapshot_hash,
        mode: params.mode,
        created_at: params.created_at,
        intent_id: intentId,
    });
}
