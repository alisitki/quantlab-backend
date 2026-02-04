/**
 * Binance Futures API Type Definitions
 *
 * Based on Binance Futures API documentation.
 * Testnet: https://testnet.binancefuture.com
 * Production: https://fapi.binance.com
 */

// ============================================================================
// API Response Wrappers
// ============================================================================

export interface BinanceApiError {
    readonly code: number;
    readonly msg: string;
}

// ============================================================================
// Order Types
// ============================================================================

export type BinanceOrderSide = "BUY" | "SELL";
export type BinanceOrderType = "LIMIT" | "MARKET" | "STOP" | "STOP_MARKET" | "TAKE_PROFIT" | "TAKE_PROFIT_MARKET";
export type BinanceTimeInForce = "GTC" | "IOC" | "FOK" | "GTX";
export type BinancePositionSide = "BOTH" | "LONG" | "SHORT";
export type BinanceWorkingType = "MARK_PRICE" | "CONTRACT_PRICE";

export type BinanceOrderStatus =
    | "NEW"
    | "PARTIALLY_FILLED"
    | "FILLED"
    | "CANCELED"
    | "REJECTED"
    | "EXPIRED";

export interface BinanceOrderRequest {
    readonly symbol: string;
    readonly side: BinanceOrderSide;
    readonly type: BinanceOrderType;
    readonly quantity?: string;
    readonly price?: string;
    readonly newClientOrderId?: string;
    readonly timeInForce?: BinanceTimeInForce;
    readonly reduceOnly?: string;         // "true" or "false"
    readonly positionSide?: BinancePositionSide;
    readonly recvWindow?: number;
    readonly timestamp: number;
}

export interface BinanceOrderResponse {
    readonly orderId: number;
    readonly symbol: string;
    readonly status: BinanceOrderStatus;
    readonly clientOrderId: string;
    readonly price: string;
    readonly avgPrice: string;
    readonly origQty: string;
    readonly executedQty: string;
    readonly cumQuote: string;
    readonly timeInForce: BinanceTimeInForce;
    readonly type: BinanceOrderType;
    readonly reduceOnly: boolean;
    readonly side: BinanceOrderSide;
    readonly positionSide: BinancePositionSide;
    readonly updateTime: number;
}

// ============================================================================
// Position Types
// ============================================================================

export interface BinancePositionRisk {
    readonly symbol: string;
    readonly positionAmt: string;          // Positive = long, Negative = short
    readonly entryPrice: string;
    readonly markPrice: string;
    readonly unRealizedProfit: string;
    readonly liquidationPrice: string;
    readonly leverage: string;
    readonly marginType: "cross" | "isolated";
    readonly isolatedMargin: string;
    readonly positionSide: BinancePositionSide;
    readonly updateTime: number;
}

// ============================================================================
// Account Types
// ============================================================================

export interface BinanceAccountAsset {
    readonly asset: string;
    readonly walletBalance: string;
    readonly unrealizedProfit: string;
    readonly marginBalance: string;
    readonly maintMargin: string;
    readonly initialMargin: string;
    readonly positionInitialMargin: string;
    readonly openOrderInitialMargin: string;
    readonly maxWithdrawAmount: string;
    readonly crossWalletBalance: string;
    readonly crossUnPnl: string;
    readonly availableBalance: string;
}

export interface BinanceAccountInfo {
    readonly totalWalletBalance: string;
    readonly totalUnrealizedProfit: string;
    readonly totalMarginBalance: string;
    readonly availableBalance: string;
    readonly assets: BinanceAccountAsset[];
}

// ============================================================================
// Exchange Info Types
// ============================================================================

export interface BinanceSymbolFilter {
    readonly filterType: string;
    readonly minPrice?: string;
    readonly maxPrice?: string;
    readonly tickSize?: string;
    readonly minQty?: string;
    readonly maxQty?: string;
    readonly stepSize?: string;
    readonly notional?: string;
}

export interface BinanceSymbolInfo {
    readonly symbol: string;
    readonly pair: string;
    readonly contractType: string;
    readonly baseAsset: string;
    readonly quoteAsset: string;
    readonly marginAsset: string;
    readonly pricePrecision: number;
    readonly quantityPrecision: number;
    readonly status: "TRADING" | "BREAK" | "HALT";
    readonly filters: BinanceSymbolFilter[];
}

export interface BinanceExchangeInfo {
    readonly timezone: string;
    readonly serverTime: number;
    readonly symbols: BinanceSymbolInfo[];
}

// ============================================================================
// Error Code Mapping
// ============================================================================

/**
 * Common Binance error codes.
 * Full list: https://binance-docs.github.io/apidocs/futures/en/#error-codes
 */
export const BINANCE_ERROR_CODES: Record<number, string> = {
    // General errors
    "-1000": "UNKNOWN",
    "-1001": "DISCONNECTED",
    "-1002": "UNAUTHORIZED",
    "-1003": "TOO_MANY_REQUESTS",
    "-1015": "TOO_MANY_ORDERS",
    "-1021": "TIMESTAMP_OUTSIDE_RECV_WINDOW",
    "-1022": "INVALID_SIGNATURE",

    // Order errors
    "-2010": "NEW_ORDER_REJECTED",
    "-2011": "CANCEL_REJECTED",
    "-2013": "NO_SUCH_ORDER",
    "-2014": "BAD_API_KEY_FMT",
    "-2015": "REJECTED_MBX_KEY",
    "-2018": "BALANCE_NOT_SUFFICIENT",
    "-2019": "MARGIN_NOT_SUFFICIENT",
    "-2020": "UNABLE_TO_FILL",
    "-2021": "ORDER_WOULD_IMMEDIATELY_TRIGGER",
    "-2022": "REDUCE_ONLY_REJECT",
    "-2023": "USER_IN_LIQUIDATION",
    "-2024": "POSITION_NOT_SUFFICIENT",
    "-2025": "MAX_OPEN_ORDER_EXCEEDED",
    "-2026": "REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED",
    "-4000": "INVALID_ORDER_STATUS",
    "-4001": "PRICE_LESS_THAN_ZERO",
    "-4002": "PRICE_GREATER_THAN_MAX_PRICE",
    "-4003": "QTY_LESS_THAN_ZERO",
    "-4014": "PRICE_EXCEED_TICK_SIZE",
    "-4015": "QTY_LESS_THAN_MIN_QTY",
    "-4028": "STOP_PRICE_LESS_THAN_ZERO"
};

// ============================================================================
// URL Constants
// ============================================================================

export const BINANCE_FUTURES_BASE = "https://fapi.binance.com";
export const BINANCE_FUTURES_TESTNET = "https://testnet.binancefuture.com";
