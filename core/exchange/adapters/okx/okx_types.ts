/**
 * OKX API Type Definitions
 *
 * Based on OKX V5 API documentation.
 * Production: https://www.okx.com
 * AWS: https://aws.okx.com
 * Demo: https://www.okx.com (with x-simulated-trading header)
 */

// ============================================================================
// API Response Wrapper
// ============================================================================

export interface OkxApiResponse<T> {
    readonly code: string;
    readonly msg: string;
    readonly data: T;
}

// ============================================================================
// Order Types
// ============================================================================

export type OkxOrderSide = "buy" | "sell";
export type OkxOrderType = "market" | "limit" | "post_only" | "fok" | "ioc";
export type OkxPositionSide = "long" | "short" | "net";
export type OkxTradeMode = "cross" | "isolated" | "cash";
export type OkxInstType = "SWAP" | "FUTURES" | "SPOT" | "MARGIN";

export type OkxOrderState =
    | "live"
    | "partially_filled"
    | "filled"
    | "canceled"
    | "mmp_canceled";

export interface OkxOrderRequest {
    readonly instId: string;
    readonly tdMode: OkxTradeMode;
    readonly side: OkxOrderSide;
    readonly ordType: OkxOrderType;
    readonly sz: string;
    readonly px?: string;
    readonly clOrdId?: string;
    readonly reduceOnly?: boolean;
    readonly posSide?: OkxPositionSide;
}

export interface OkxOrderResponse {
    readonly ordId: string;
    readonly clOrdId: string;
    readonly sCode: string;
    readonly sMsg: string;
}

export interface OkxOrderDetail {
    readonly ordId: string;
    readonly clOrdId: string;
    readonly instId: string;
    readonly side: OkxOrderSide;
    readonly ordType: OkxOrderType;
    readonly px: string;
    readonly sz: string;
    readonly fillSz: string;
    readonly avgPx: string;
    readonly state: OkxOrderState;
    readonly fee: string;
    readonly feeCcy: string;
    readonly cTime: string;
    readonly uTime: string;
}

// ============================================================================
// Position Types
// ============================================================================

export interface OkxPosition {
    readonly instId: string;
    readonly posSide: OkxPositionSide;
    readonly pos: string;
    readonly avgPx: string;
    readonly upl: string;
    readonly lever: string;
    readonly liqPx: string;
    readonly margin: string;
    readonly mgnMode: OkxTradeMode;
    readonly uTime: string;
}

// ============================================================================
// Account Types
// ============================================================================

export interface OkxAccountBalance {
    readonly ccy: string;
    readonly cashBal: string;
    readonly availBal: string;
    readonly frozenBal: string;
    readonly upl: string;
}

export interface OkxAccountInfo {
    readonly uTime: string;
    readonly totalEq: string;
    readonly details: OkxAccountBalance[];
}

// ============================================================================
// Exchange Info Types
// ============================================================================

export interface OkxInstrument {
    readonly instId: string;
    readonly instType: OkxInstType;
    readonly uly: string;
    readonly baseCcy: string;
    readonly quoteCcy: string;
    readonly settleCcy: string;
    readonly tickSz: string;
    readonly lotSz: string;
    readonly minSz: string;
    readonly maxLmtSz: string;
    readonly maxMktSz: string;
    readonly state: "live" | "suspend" | "preopen";
}

// ============================================================================
// Error Codes
// ============================================================================

export const OKX_ERROR_CODES: Record<string, string> = {
    "0": "OK",
    "1": "OPERATION_FAILED",
    "50000": "BODY_NOT_EMPTY",
    "50001": "SERVICE_TEMPORARILY_UNAVAILABLE",
    "50004": "API_ENDPOINT_TIMEOUT",
    "50005": "API_UNAVAILABLE",
    "50011": "TOO_MANY_REQUESTS",
    "50013": "INVALID_SIGN",
    "50014": "INVALID_API_KEY",
    "50015": "INVALID_PASSPHRASE",
    "51000": "PARAMS_ERROR",
    "51001": "INSTRUMENT_ID_NOT_EXIST",
    "51008": "INSUFFICIENT_BALANCE",
    "51009": "ORDER_PLACEMENT_FAILED",
    "51010": "ACCOUNT_LEVEL_TOO_LOW",
    "51020": "ORDER_ID_NOT_EXIST",
    "51119": "REDUCE_ONLY_ORDER_FAILED",
    "51127": "MAX_POSITION_EXCEEDED"
};

// ============================================================================
// URL Constants
// ============================================================================

export const OKX_API_BASE = "https://www.okx.com";
export const OKX_API_AWS = "https://aws.okx.com";
