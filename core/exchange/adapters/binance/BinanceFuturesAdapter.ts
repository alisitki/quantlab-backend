/**
 * Binance Futures Adapter
 *
 * REST API client for Binance USDT-Margined Futures.
 * Supports both production and testnet environments.
 *
 * Endpoints:
 * - Production: https://fapi.binance.com
 * - Testnet: https://testnet.binancefuture.com
 */

import {
    ExchangeAdapter,
    SubmitOrderParams,
    ExchangeOrder,
    ExchangePosition,
    AccountBalance,
    ExchangeInfo,
    SymbolInfo,
    OrderStatus,
    OrderFee,
    PositionSide
} from "../base/ExchangeAdapter.js";
import { ExchangeCredentials } from "../base/ExchangeCredentials.js";
import {
    ExchangeError,
    ExchangeErrorCode,
    createExchangeError
} from "../base/ExchangeError.js";
import { BinanceSigner } from "./BinanceSigner.js";
import {
    BINANCE_FUTURES_BASE,
    BINANCE_FUTURES_TESTNET,
    BinanceOrderResponse,
    BinancePositionRisk,
    BinanceAccountInfo,
    BinanceExchangeInfo,
    BinanceSymbolInfo,
    BinanceOrderStatus,
    BINANCE_ERROR_CODES
} from "./binance_types.js";

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_RECV_WINDOW = 5000;
const REQUEST_TIMEOUT = 10000;

// ============================================================================
// Binance Futures Adapter
// ============================================================================

export class BinanceFuturesAdapter extends ExchangeAdapter {
    readonly exchange = "binance";
    readonly testnet: boolean;

    readonly #signer: BinanceSigner;
    readonly #baseUrl: string;
    readonly #recvWindow: number;

    constructor(credentials: ExchangeCredentials, recvWindow: number = DEFAULT_RECV_WINDOW) {
        super(credentials);
        this.testnet = credentials.testnet;
        this.#baseUrl = credentials.testnet ? BINANCE_FUTURES_TESTNET : BINANCE_FUTURES_BASE;
        this.#signer = new BinanceSigner(credentials.apiKey, credentials.secretKey);
        this.#recvWindow = recvWindow;
    }

    // -------------------------------------------------------------------------
    // Order Operations
    // -------------------------------------------------------------------------

    async submitOrder(params: SubmitOrderParams): Promise<ExchangeOrder> {
        const requestParams: Record<string, string | number | boolean> = {
            symbol: params.symbol,
            side: params.side,
            type: params.orderType,
            quantity: params.quantity.toString(),
            newClientOrderId: params.clientOrderId,
            reduceOnly: params.reduceOnly ? "true" : "false"
        };

        // Add price for LIMIT orders
        if (params.orderType === "LIMIT" && params.price !== undefined) {
            requestParams.price = params.price.toString();
            requestParams.timeInForce = this.mapTimeInForce(params.timeInForce);
        }

        // Market orders with IOC/FOK
        if (params.orderType === "MARKET" && params.timeInForce !== "GTC") {
            // Binance market orders don't support IOC/FOK, but we handle it
            // by checking fill status after
        }

        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v1/order",
            requestParams,
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "POST",
            headers: this.#signer.getHeaders()
        });

        const data = await this.handleResponse<BinanceOrderResponse>(response);
        return this.mapOrderResponse(data);
    }

    async cancelOrder(symbol: string, orderId: string): Promise<boolean> {
        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v1/order",
            { symbol, origClientOrderId: orderId },
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "DELETE",
            headers: this.#signer.getHeaders()
        });

        try {
            await this.handleResponse(response);
            return true;
        } catch (error) {
            if (error instanceof ExchangeError) {
                // Order already cancelled or filled
                if (error.code === ExchangeErrorCode.ORDER_NOT_FOUND ||
                    error.code === ExchangeErrorCode.ORDER_ALREADY_CANCELLED ||
                    error.code === ExchangeErrorCode.ORDER_ALREADY_FILLED) {
                    return false;
                }
            }
            throw error;
        }
    }

    async getOrder(symbol: string, orderId: string): Promise<ExchangeOrder | null> {
        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v1/order",
            { symbol, origClientOrderId: orderId },
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "GET",
            headers: this.#signer.getHeaders()
        });

        try {
            const data = await this.handleResponse<BinanceOrderResponse>(response);
            return this.mapOrderResponse(data);
        } catch (error) {
            if (error instanceof ExchangeError && error.code === ExchangeErrorCode.ORDER_NOT_FOUND) {
                return null;
            }
            throw error;
        }
    }

    async getOpenOrders(symbol?: string): Promise<ExchangeOrder[]> {
        const params: Record<string, string> = {};
        if (symbol) {
            params.symbol = symbol;
        }

        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v1/openOrders",
            params,
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "GET",
            headers: this.#signer.getHeaders()
        });

        const data = await this.handleResponse<BinanceOrderResponse[]>(response);
        return data.map(order => this.mapOrderResponse(order));
    }

    // -------------------------------------------------------------------------
    // Position Operations
    // -------------------------------------------------------------------------

    async getPosition(symbol: string): Promise<ExchangePosition> {
        const positions = await this.getAllPositions();
        const position = positions.find(p => p.symbol === symbol);

        if (position) {
            return position;
        }

        // Return empty position if none exists
        return {
            symbol,
            side: "FLAT",
            quantity: 0,
            entryPrice: 0,
            markPrice: 0,
            unrealizedPnl: 0,
            margin: 0,
            leverage: 1,
            liquidationPrice: 0,
            updatedAt: Date.now()
        };
    }

    async getAllPositions(): Promise<ExchangePosition[]> {
        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v2/positionRisk",
            {},
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "GET",
            headers: this.#signer.getHeaders()
        });

        const data = await this.handleResponse<BinancePositionRisk[]>(response);

        // Filter to only positions with non-zero amount
        return data
            .filter(p => parseFloat(p.positionAmt) !== 0)
            .map(p => this.mapPosition(p));
    }

    // -------------------------------------------------------------------------
    // Account Operations
    // -------------------------------------------------------------------------

    async getBalances(): Promise<AccountBalance[]> {
        const url = this.#signer.buildSignedUrl(
            this.#baseUrl,
            "/fapi/v2/balance",
            {},
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, {
            method: "GET",
            headers: this.#signer.getHeaders()
        });

        const data = await this.handleResponse<Array<{
            asset: string;
            balance: string;
            crossWalletBalance: string;
            availableBalance: string;
        }>>(response);

        return data.map(b => ({
            asset: b.asset,
            free: parseFloat(b.availableBalance),
            locked: parseFloat(b.balance) - parseFloat(b.availableBalance),
            total: parseFloat(b.balance)
        }));
    }

    async getBalance(asset: string): Promise<AccountBalance | null> {
        const balances = await this.getBalances();
        return balances.find(b => b.asset === asset) ?? null;
    }

    // -------------------------------------------------------------------------
    // Exchange Info
    // -------------------------------------------------------------------------

    async getExchangeInfo(): Promise<ExchangeInfo> {
        const url = `${this.#baseUrl}/fapi/v1/exchangeInfo`;

        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<BinanceExchangeInfo>(response);

        return {
            exchange: this.exchange,
            testnet: this.testnet,
            serverTime: data.serverTime,
            symbols: data.symbols.map(s => this.mapSymbolInfo(s))
        };
    }

    async getSymbolInfo(symbol: string): Promise<SymbolInfo | null> {
        const info = await this.getExchangeInfo();
        return info.symbols.find(s => s.symbol === symbol) ?? null;
    }

    // -------------------------------------------------------------------------
    // Health Check
    // -------------------------------------------------------------------------

    async ping(): Promise<boolean> {
        try {
            const url = `${this.#baseUrl}/fapi/v1/ping`;
            const response = await this.fetchWithTimeout(url, { method: "GET" });
            return response.ok;
        } catch {
            return false;
        }
    }

    async getServerTime(): Promise<number> {
        const url = `${this.#baseUrl}/fapi/v1/time`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<{ serverTime: number }>(response);
        return data.serverTime;
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private async fetchWithTimeout(url: string, options: RequestInit): Promise<Response> {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

        try {
            return await fetch(url, {
                ...options,
                signal: controller.signal
            });
        } catch (error) {
            if (error instanceof Error && error.name === "AbortError") {
                throw new ExchangeError({
                    code: ExchangeErrorCode.TIMEOUT,
                    message: "Request timeout",
                    exchange: this.exchange,
                    timestamp: Date.now(),
                    retryable: true
                });
            }
            throw createExchangeError(this.exchange, error);
        } finally {
            clearTimeout(timeout);
        }
    }

    private async handleResponse<T>(response: Response): Promise<T> {
        const data = await response.json();

        if (!response.ok || data.code) {
            const errorCode = this.mapBinanceError(data.code);
            throw new ExchangeError({
                code: errorCode,
                message: data.msg || `HTTP ${response.status}`,
                exchange: this.exchange,
                originalCode: data.code,
                originalMessage: data.msg,
                timestamp: Date.now(),
                retryable: this.isRetryableStatusCode(response.status)
            });
        }

        return data as T;
    }

    private mapBinanceError(code: number): ExchangeErrorCode {
        const codeStr = String(code);

        // Common mappings
        switch (code) {
            case -1002:
            case -2014:
            case -2015:
                return ExchangeErrorCode.AUTH_FAILED;
            case -1003:
            case -1015:
                return ExchangeErrorCode.RATE_LIMIT_EXCEEDED;
            case -1021:
                return ExchangeErrorCode.TIMESTAMP_OUTSIDE_RECV_WINDOW;
            case -1022:
                return ExchangeErrorCode.SIGNATURE_INVALID;
            case -2010:
            case -2020:
                return ExchangeErrorCode.ORDER_REJECTED;
            case -2011:
            case -2013:
                return ExchangeErrorCode.ORDER_NOT_FOUND;
            case -2018:
                return ExchangeErrorCode.INSUFFICIENT_BALANCE;
            case -2019:
                return ExchangeErrorCode.INSUFFICIENT_MARGIN;
            case -2022:
                return ExchangeErrorCode.ORDER_REJECTED;  // Reduce only reject
            case -2025:
                return ExchangeErrorCode.POSITION_LIMIT_EXCEEDED;
            case -4015:
                return ExchangeErrorCode.INVALID_QUANTITY;
            default:
                return ExchangeErrorCode.UNKNOWN;
        }
    }

    private isRetryableStatusCode(status: number): boolean {
        return status === 429 || status === 503 || status >= 500;
    }

    private mapTimeInForce(tif: "IOC" | "GTC" | "FOK"): string {
        switch (tif) {
            case "IOC": return "IOC";
            case "GTC": return "GTC";
            case "FOK": return "FOK";
            default: return "GTC";
        }
    }

    private mapOrderStatus(status: BinanceOrderStatus): OrderStatus {
        switch (status) {
            case "NEW": return "OPEN";
            case "PARTIALLY_FILLED": return "PARTIALLY_FILLED";
            case "FILLED": return "FILLED";
            case "CANCELED": return "CANCELLED";
            case "REJECTED": return "REJECTED";
            case "EXPIRED": return "EXPIRED";
            default: return "PENDING";
        }
    }

    private mapOrderResponse(order: BinanceOrderResponse): ExchangeOrder {
        // Binance doesn't return individual fees in order response
        // Fee information would need to be fetched from /fapi/v1/userTrades
        const fees: OrderFee[] = [];

        return {
            exchangeOrderId: String(order.orderId),
            clientOrderId: order.clientOrderId,
            symbol: order.symbol,
            side: order.side,
            orderType: order.type === "MARKET" ? "MARKET" : "LIMIT",
            requestedQty: parseFloat(order.origQty),
            filledQty: parseFloat(order.executedQty),
            avgFillPrice: parseFloat(order.avgPrice) || 0,
            status: this.mapOrderStatus(order.status),
            submittedAt: order.updateTime,  // Binance doesn't have separate submit time
            updatedAt: order.updateTime,
            fees
        };
    }

    private mapPosition(pos: BinancePositionRisk): ExchangePosition {
        const amount = parseFloat(pos.positionAmt);
        const side: PositionSide = amount > 0 ? "LONG" : amount < 0 ? "SHORT" : "FLAT";

        return {
            symbol: pos.symbol,
            side,
            quantity: Math.abs(amount),
            entryPrice: parseFloat(pos.entryPrice),
            markPrice: parseFloat(pos.markPrice),
            unrealizedPnl: parseFloat(pos.unRealizedProfit),
            margin: parseFloat(pos.isolatedMargin) || 0,
            leverage: parseInt(pos.leverage),
            liquidationPrice: parseFloat(pos.liquidationPrice),
            updatedAt: pos.updateTime
        };
    }

    private mapSymbolInfo(info: BinanceSymbolInfo): SymbolInfo {
        // Extract filter values
        let minQty = 0, maxQty = 0, stepSize = 0;
        let minNotional = 0, maxNotional = 0;
        let tickSize = 0;

        for (const filter of info.filters) {
            switch (filter.filterType) {
                case "LOT_SIZE":
                    minQty = parseFloat(filter.minQty || "0");
                    maxQty = parseFloat(filter.maxQty || "0");
                    stepSize = parseFloat(filter.stepSize || "0");
                    break;
                case "MIN_NOTIONAL":
                    minNotional = parseFloat(filter.notional || "0");
                    break;
                case "PRICE_FILTER":
                    tickSize = parseFloat(filter.tickSize || "0");
                    break;
            }
        }

        return {
            symbol: info.symbol,
            baseAsset: info.baseAsset,
            quoteAsset: info.quoteAsset,
            pricePrecision: info.pricePrecision,
            quantityPrecision: info.quantityPrecision,
            minNotional,
            maxNotional: maxNotional || Number.MAX_SAFE_INTEGER,
            minQuantity: minQty,
            maxQuantity: maxQty,
            tickSize,
            stepSize,
            status: info.status
        };
    }
}
