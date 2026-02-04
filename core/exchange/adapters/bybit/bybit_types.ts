/**
 * Bybit V5 API Type Definitions
 *
 * Based on Bybit V5 Unified Trading API documentation.
 * Testnet: https://api-testnet.bybit.com
 * Production: https://api.bybit.com
 */

// ============================================================================
// API Response Wrapper
// ============================================================================

export interface BybitApiResponse<T> {
    readonly retCode: number;
    readonly retMsg: string;
    readonly result: T;
    readonly time: number;
}

// ============================================================================
// Order Types
// ============================================================================

export type BybitOrderSide = "Buy" | "Sell";
export type BybitOrderType = "Market" | "Limit";
export type BybitTimeInForce = "GTC" | "IOC" | "FOK" | "PostOnly";
export type BybitCategory = "linear" | "inverse" | "spot";

export type BybitOrderStatus =
    | "New"
    | "PartiallyFilled"
    | "Filled"
    | "Cancelled"
    | "Rejected"
    | "Deactivated";

export interface BybitOrderRequest {
    readonly category: BybitCategory;
    readonly symbol: string;
    readonly side: BybitOrderSide;
    readonly orderType: BybitOrderType;
    readonly qty: string;
    readonly price?: string;
    readonly orderLinkId?: string;
    readonly timeInForce?: BybitTimeInForce;
    readonly reduceOnly?: boolean;
    readonly closeOnTrigger?: boolean;
}

export interface BybitOrderResponse {
    readonly orderId: string;
    readonly orderLinkId: string;
}

export interface BybitOrderDetail {
    readonly orderId: string;
    readonly orderLinkId: string;
    readonly symbol: string;
    readonly side: BybitOrderSide;
    readonly orderType: BybitOrderType;
    readonly price: string;
    readonly qty: string;
    readonly cumExecQty: string;
    readonly cumExecValue: string;
    readonly avgPrice: string;
    readonly orderStatus: BybitOrderStatus;
    readonly timeInForce: BybitTimeInForce;
    readonly reduceOnly: boolean;
    readonly createdTime: string;
    readonly updatedTime: string;
}

// ============================================================================
// Position Types
// ============================================================================

export interface BybitPosition {
    readonly symbol: string;
    readonly side: "Buy" | "Sell" | "None";
    readonly size: string;
    readonly avgPrice: string;
    readonly positionValue: string;
    readonly unrealisedPnl: string;
    readonly leverage: string;
    readonly liqPrice: string;
    readonly positionIM: string;
    readonly positionMM: string;
    readonly updatedTime: string;
}

// ============================================================================
// Account Types
// ============================================================================

export interface BybitWalletBalance {
    readonly coin: string;
    readonly equity: string;
    readonly walletBalance: string;
    readonly availableToWithdraw: string;
    readonly unrealisedPnl: string;
}

export interface BybitAccountInfo {
    readonly accountType: string;
    readonly coin: BybitWalletBalance[];
}

// ============================================================================
// Exchange Info Types
// ============================================================================

export interface BybitInstrumentInfo {
    readonly symbol: string;
    readonly baseCoin: string;
    readonly quoteCoin: string;
    readonly status: "Trading" | "Settling" | "Closed";
    readonly priceScale: string;
    readonly leverageFilter: {
        readonly minLeverage: string;
        readonly maxLeverage: string;
    };
    readonly lotSizeFilter: {
        readonly minOrderQty: string;
        readonly maxOrderQty: string;
        readonly qtyStep: string;
    };
    readonly priceFilter: {
        readonly minPrice: string;
        readonly maxPrice: string;
        readonly tickSize: string;
    };
}

// ============================================================================
// Error Codes
// ============================================================================

export const BYBIT_ERROR_CODES: Record<number, string> = {
    0: "OK",
    10001: "PARAMS_ERROR",
    10002: "API_KEY_INVALID",
    10003: "SIGN_ERROR",
    10004: "TIMESTAMP_ERROR",
    10005: "PERMISSION_DENIED",
    10006: "TOO_MANY_REQUESTS",
    10010: "UNRECOGNIZED_IP",
    110001: "ORDER_NOT_EXIST",
    110003: "INSUFFICIENT_BALANCE",
    110004: "INSUFFICIENT_MARGIN",
    110007: "ORDER_REJECTED",
    110012: "CROSS_STATUS_ERROR",
    110013: "POSITION_NOT_EXIST",
    110017: "REDUCE_ONLY_RULE",
    110025: "POSITION_INDEX_NOT_MATCH",
    140003: "ORDER_PRICE_OUT_OF_RANGE"
};

// ============================================================================
// URL Constants
// ============================================================================

export const BYBIT_API_BASE = "https://api.bybit.com";
export const BYBIT_API_TESTNET = "https://api-testnet.bybit.com";
