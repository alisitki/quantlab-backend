/**
 * Exchange Adapter - Abstract Base Class
 *
 * Defines the interface for all exchange adapters.
 * Concrete implementations (Binance, Bybit, OKX) must implement all abstract methods.
 */

import { ExchangeCredentials } from "./ExchangeCredentials.js";

// ============================================================================
// Type Definitions
// ============================================================================

export type OrderStatus =
    | "PENDING"         // Submitted, awaiting confirmation
    | "OPEN"            // Accepted by exchange, waiting for fill
    | "PARTIALLY_FILLED"// Some quantity filled
    | "FILLED"          // Completely filled
    | "CANCELLED"       // Cancelled by user or system
    | "REJECTED"        // Rejected by exchange
    | "EXPIRED";        // Time-in-force expired

export type OrderSide = "BUY" | "SELL";
export type OrderType = "MARKET" | "LIMIT";
export type TimeInForce = "IOC" | "GTC" | "FOK";
export type PositionSide = "LONG" | "SHORT" | "FLAT";

// ============================================================================
// Interfaces
// ============================================================================

export interface SubmitOrderParams {
    readonly clientOrderId: string;
    readonly symbol: string;
    readonly side: OrderSide;
    readonly orderType: OrderType;
    readonly quantity: number;
    readonly price?: number;          // Required for LIMIT orders
    readonly timeInForce: TimeInForce;
    readonly reduceOnly: boolean;
}

export interface ExchangeOrder {
    readonly exchangeOrderId: string;
    readonly clientOrderId: string;
    readonly symbol: string;
    readonly side: OrderSide;
    readonly orderType: OrderType;
    readonly requestedQty: number;
    readonly filledQty: number;
    readonly avgFillPrice: number;
    readonly status: OrderStatus;
    readonly submittedAt: number;     // Unix ms
    readonly updatedAt: number;       // Unix ms
    readonly fees: OrderFee[];
}

export interface OrderFee {
    readonly asset: string;
    readonly amount: number;
}

export interface ExchangePosition {
    readonly symbol: string;
    readonly side: PositionSide;
    readonly quantity: number;        // Always positive
    readonly entryPrice: number;
    readonly markPrice: number;
    readonly unrealizedPnl: number;
    readonly margin: number;
    readonly leverage: number;
    readonly liquidationPrice: number;
    readonly updatedAt: number;
}

export interface AccountBalance {
    readonly asset: string;
    readonly free: number;            // Available balance
    readonly locked: number;          // In orders/positions
    readonly total: number;           // free + locked
}

export interface ExchangeInfo {
    readonly exchange: string;
    readonly testnet: boolean;
    readonly serverTime: number;
    readonly symbols: SymbolInfo[];
}

export interface SymbolInfo {
    readonly symbol: string;
    readonly baseAsset: string;
    readonly quoteAsset: string;
    readonly pricePrecision: number;
    readonly quantityPrecision: number;
    readonly minNotional: number;
    readonly maxNotional: number;
    readonly minQuantity: number;
    readonly maxQuantity: number;
    readonly tickSize: number;
    readonly stepSize: number;
    readonly status: "TRADING" | "BREAK" | "HALT";
}

// ============================================================================
// Abstract Base Class
// ============================================================================

export abstract class ExchangeAdapter {
    protected readonly credentials: ExchangeCredentials;

    abstract readonly exchange: string;
    abstract readonly testnet: boolean;

    constructor(credentials: ExchangeCredentials) {
        this.credentials = credentials;
    }

    // -------------------------------------------------------------------------
    // Order Operations
    // -------------------------------------------------------------------------

    /**
     * Submit a new order to the exchange.
     */
    abstract submitOrder(params: SubmitOrderParams): Promise<ExchangeOrder>;

    /**
     * Cancel an existing order.
     * Returns true if cancellation was successful.
     */
    abstract cancelOrder(symbol: string, orderId: string): Promise<boolean>;

    /**
     * Get current status of an order.
     * Returns null if order not found.
     */
    abstract getOrder(symbol: string, orderId: string): Promise<ExchangeOrder | null>;

    /**
     * Get all open orders for a symbol (or all symbols if not specified).
     */
    abstract getOpenOrders(symbol?: string): Promise<ExchangeOrder[]>;

    // -------------------------------------------------------------------------
    // Position Operations
    // -------------------------------------------------------------------------

    /**
     * Get current position for a symbol.
     * Returns position with quantity=0 if no position exists.
     */
    abstract getPosition(symbol: string): Promise<ExchangePosition>;

    /**
     * Get all positions with non-zero quantity.
     */
    abstract getAllPositions(): Promise<ExchangePosition[]>;

    // -------------------------------------------------------------------------
    // Account Operations
    // -------------------------------------------------------------------------

    /**
     * Get account balances for all assets.
     */
    abstract getBalances(): Promise<AccountBalance[]>;

    /**
     * Get balance for a specific asset.
     */
    abstract getBalance(asset: string): Promise<AccountBalance | null>;

    // -------------------------------------------------------------------------
    // Exchange Info
    // -------------------------------------------------------------------------

    /**
     * Get exchange information including supported symbols.
     */
    abstract getExchangeInfo(): Promise<ExchangeInfo>;

    /**
     * Get information for a specific symbol.
     */
    abstract getSymbolInfo(symbol: string): Promise<SymbolInfo | null>;

    // -------------------------------------------------------------------------
    // Health Check
    // -------------------------------------------------------------------------

    /**
     * Ping the exchange to verify connectivity.
     */
    abstract ping(): Promise<boolean>;

    /**
     * Get exchange server time.
     */
    abstract getServerTime(): Promise<number>;

    // -------------------------------------------------------------------------
    // Utility Methods (Concrete)
    // -------------------------------------------------------------------------

    /**
     * Check if the adapter is in testnet mode.
     */
    isTestnet(): boolean {
        return this.testnet;
    }

    /**
     * Get the exchange name.
     */
    getExchange(): string {
        return this.exchange;
    }

    /**
     * Calculate time drift between local time and server time.
     */
    async getTimeDrift(): Promise<number> {
        const localTime = Date.now();
        const serverTime = await this.getServerTime();
        return Math.abs(localTime - serverTime);
    }
}
