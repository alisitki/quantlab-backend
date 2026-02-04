/**
 * Bybit Futures Adapter
 *
 * REST API client for Bybit USDT Perpetual (Linear) contracts.
 * Uses V5 Unified Trading API.
 *
 * Endpoints:
 * - Production: https://api.bybit.com
 * - Testnet: https://api-testnet.bybit.com
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
import { BybitSigner } from "./BybitSigner.js";
import {
    BYBIT_API_BASE,
    BYBIT_API_TESTNET,
    BybitApiResponse,
    BybitOrderResponse,
    BybitOrderDetail,
    BybitPosition,
    BybitAccountInfo,
    BybitInstrumentInfo,
    BybitOrderStatus
} from "./bybit_types.js";

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_RECV_WINDOW = 5000;
const REQUEST_TIMEOUT = 10000;
const CATEGORY = "linear";  // USDT Perpetual

// ============================================================================
// Bybit Futures Adapter
// ============================================================================

export class BybitFuturesAdapter extends ExchangeAdapter {
    readonly exchange = "bybit";
    readonly testnet: boolean;

    readonly #signer: BybitSigner;
    readonly #baseUrl: string;
    readonly #recvWindow: number;

    constructor(credentials: ExchangeCredentials, recvWindow: number = DEFAULT_RECV_WINDOW) {
        super(credentials);
        this.testnet = credentials.testnet;
        this.#baseUrl = credentials.testnet ? BYBIT_API_TESTNET : BYBIT_API_BASE;
        this.#signer = new BybitSigner(credentials.apiKey, credentials.secretKey);
        this.#recvWindow = recvWindow;
    }

    // -------------------------------------------------------------------------
    // Order Operations
    // -------------------------------------------------------------------------

    async submitOrder(params: SubmitOrderParams): Promise<ExchangeOrder> {
        const body = {
            category: CATEGORY,
            symbol: params.symbol,
            side: params.side === "BUY" ? "Buy" : "Sell",
            orderType: params.orderType === "MARKET" ? "Market" : "Limit",
            qty: params.quantity.toString(),
            orderLinkId: params.clientOrderId,
            reduceOnly: params.reduceOnly,
            timeInForce: this.mapTimeInForce(params.timeInForce)
        };

        if (params.orderType === "LIMIT" && params.price !== undefined) {
            (body as any).price = params.price.toString();
        }

        const { body: signedBody, headers } = this.#signer.signPostRequest(body, this.#recvWindow);

        const response = await this.fetchWithTimeout(`${this.#baseUrl}/v5/order/create`, {
            method: "POST",
            headers,
            body: signedBody
        });

        const data = await this.handleResponse<BybitApiResponse<BybitOrderResponse>>(response);

        // Fetch order details to get fill info
        const orderDetail = await this.getOrder(params.symbol, params.clientOrderId);
        if (orderDetail) {
            return orderDetail;
        }

        // Return basic info if detail fetch fails
        return {
            exchangeOrderId: data.result.orderId,
            clientOrderId: data.result.orderLinkId,
            symbol: params.symbol,
            side: params.side,
            orderType: params.orderType,
            requestedQty: params.quantity,
            filledQty: 0,
            avgFillPrice: 0,
            status: "PENDING",
            submittedAt: Date.now(),
            updatedAt: Date.now(),
            fees: []
        };
    }

    async cancelOrder(symbol: string, orderId: string): Promise<boolean> {
        const body = {
            category: CATEGORY,
            symbol,
            orderLinkId: orderId
        };

        const { body: signedBody, headers } = this.#signer.signPostRequest(body, this.#recvWindow);

        try {
            const response = await this.fetchWithTimeout(`${this.#baseUrl}/v5/order/cancel`, {
                method: "POST",
                headers,
                body: signedBody
            });

            await this.handleResponse(response);
            return true;
        } catch (error) {
            if (error instanceof ExchangeError) {
                if (error.code === ExchangeErrorCode.ORDER_NOT_FOUND) {
                    return false;
                }
            }
            throw error;
        }
    }

    async getOrder(symbol: string, orderId: string): Promise<ExchangeOrder | null> {
        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/v5/order/realtime",
            { category: CATEGORY, symbol, orderLinkId: orderId },
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });

        try {
            const data = await this.handleResponse<BybitApiResponse<{ list: BybitOrderDetail[] }>>(response);

            if (data.result.list.length === 0) {
                return null;
            }

            return this.mapOrderDetail(data.result.list[0]);
        } catch (error) {
            if (error instanceof ExchangeError && error.code === ExchangeErrorCode.ORDER_NOT_FOUND) {
                return null;
            }
            throw error;
        }
    }

    async getOpenOrders(symbol?: string): Promise<ExchangeOrder[]> {
        const params: Record<string, string> = { category: CATEGORY };
        if (symbol) {
            params.symbol = symbol;
        }

        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/v5/order/realtime",
            params,
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<BybitApiResponse<{ list: BybitOrderDetail[] }>>(response);

        return data.result.list.map(order => this.mapOrderDetail(order));
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
        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/v5/position/list",
            { category: CATEGORY, settleCoin: "USDT" },
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<BybitApiResponse<{ list: BybitPosition[] }>>(response);

        return data.result.list
            .filter(p => parseFloat(p.size) !== 0)
            .map(p => this.mapPosition(p));
    }

    // -------------------------------------------------------------------------
    // Account Operations
    // -------------------------------------------------------------------------

    async getBalances(): Promise<AccountBalance[]> {
        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/v5/account/wallet-balance",
            { accountType: "UNIFIED" },
            this.#recvWindow
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<BybitApiResponse<{ list: BybitAccountInfo[] }>>(response);

        const balances: AccountBalance[] = [];
        for (const account of data.result.list) {
            for (const coin of account.coin) {
                balances.push({
                    asset: coin.coin,
                    free: parseFloat(coin.availableToWithdraw),
                    locked: parseFloat(coin.walletBalance) - parseFloat(coin.availableToWithdraw),
                    total: parseFloat(coin.walletBalance)
                });
            }
        }

        return balances;
    }

    async getBalance(asset: string): Promise<AccountBalance | null> {
        const balances = await this.getBalances();
        return balances.find(b => b.asset === asset) ?? null;
    }

    // -------------------------------------------------------------------------
    // Exchange Info
    // -------------------------------------------------------------------------

    async getExchangeInfo(): Promise<ExchangeInfo> {
        const url = `${this.#baseUrl}/v5/market/instruments-info?category=${CATEGORY}`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<BybitApiResponse<{ list: BybitInstrumentInfo[] }>>(response);

        return {
            exchange: this.exchange,
            testnet: this.testnet,
            serverTime: data.time,
            symbols: data.result.list.map(s => this.mapSymbolInfo(s))
        };
    }

    async getSymbolInfo(symbol: string): Promise<SymbolInfo | null> {
        const url = `${this.#baseUrl}/v5/market/instruments-info?category=${CATEGORY}&symbol=${symbol}`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<BybitApiResponse<{ list: BybitInstrumentInfo[] }>>(response);

        if (data.result.list.length === 0) {
            return null;
        }

        return this.mapSymbolInfo(data.result.list[0]);
    }

    // -------------------------------------------------------------------------
    // Health Check
    // -------------------------------------------------------------------------

    async ping(): Promise<boolean> {
        try {
            const url = `${this.#baseUrl}/v5/market/time`;
            const response = await this.fetchWithTimeout(url, { method: "GET" });
            return response.ok;
        } catch {
            return false;
        }
    }

    async getServerTime(): Promise<number> {
        const url = `${this.#baseUrl}/v5/market/time`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<BybitApiResponse<{ timeSecond: string; timeNano: string }>>(response);
        return parseInt(data.result.timeSecond) * 1000;
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
        const data = await response.json() as BybitApiResponse<unknown>;

        if (data.retCode !== 0) {
            const errorCode = this.mapBybitError(data.retCode);
            throw new ExchangeError({
                code: errorCode,
                message: data.retMsg || `Bybit error ${data.retCode}`,
                exchange: this.exchange,
                originalCode: data.retCode,
                originalMessage: data.retMsg,
                timestamp: Date.now(),
                retryable: data.retCode === 10006  // Rate limit
            });
        }

        return data as T;
    }

    private mapBybitError(code: number): ExchangeErrorCode {
        switch (code) {
            case 10001: return ExchangeErrorCode.ORDER_REJECTED;
            case 10002: return ExchangeErrorCode.API_KEY_INVALID;
            case 10003: return ExchangeErrorCode.SIGNATURE_INVALID;
            case 10004: return ExchangeErrorCode.TIMESTAMP_OUTSIDE_RECV_WINDOW;
            case 10005: return ExchangeErrorCode.AUTH_FAILED;
            case 10006: return ExchangeErrorCode.RATE_LIMIT_EXCEEDED;
            case 10010: return ExchangeErrorCode.IP_NOT_WHITELISTED;
            case 110001: return ExchangeErrorCode.ORDER_NOT_FOUND;
            case 110003: return ExchangeErrorCode.INSUFFICIENT_BALANCE;
            case 110004: return ExchangeErrorCode.INSUFFICIENT_MARGIN;
            case 110007: return ExchangeErrorCode.ORDER_REJECTED;
            case 110017: return ExchangeErrorCode.ORDER_REJECTED;
            default: return ExchangeErrorCode.UNKNOWN;
        }
    }

    private mapTimeInForce(tif: "IOC" | "GTC" | "FOK"): string {
        switch (tif) {
            case "IOC": return "IOC";
            case "GTC": return "GTC";
            case "FOK": return "FOK";
            default: return "GTC";
        }
    }

    private mapOrderStatus(status: BybitOrderStatus): OrderStatus {
        switch (status) {
            case "New": return "OPEN";
            case "PartiallyFilled": return "PARTIALLY_FILLED";
            case "Filled": return "FILLED";
            case "Cancelled": return "CANCELLED";
            case "Rejected": return "REJECTED";
            case "Deactivated": return "CANCELLED";
            default: return "PENDING";
        }
    }

    private mapOrderDetail(order: BybitOrderDetail): ExchangeOrder {
        return {
            exchangeOrderId: order.orderId,
            clientOrderId: order.orderLinkId,
            symbol: order.symbol,
            side: order.side === "Buy" ? "BUY" : "SELL",
            orderType: order.orderType === "Market" ? "MARKET" : "LIMIT",
            requestedQty: parseFloat(order.qty),
            filledQty: parseFloat(order.cumExecQty),
            avgFillPrice: parseFloat(order.avgPrice) || 0,
            status: this.mapOrderStatus(order.orderStatus),
            submittedAt: parseInt(order.createdTime),
            updatedAt: parseInt(order.updatedTime),
            fees: []
        };
    }

    private mapPosition(pos: BybitPosition): ExchangePosition {
        const size = parseFloat(pos.size);
        const side: PositionSide = pos.side === "Buy" ? "LONG" : pos.side === "Sell" ? "SHORT" : "FLAT";

        return {
            symbol: pos.symbol,
            side,
            quantity: Math.abs(size),
            entryPrice: parseFloat(pos.avgPrice),
            markPrice: 0,  // Would need separate API call
            unrealizedPnl: parseFloat(pos.unrealisedPnl),
            margin: parseFloat(pos.positionIM),
            leverage: parseFloat(pos.leverage),
            liquidationPrice: parseFloat(pos.liqPrice),
            updatedAt: parseInt(pos.updatedTime)
        };
    }

    private mapSymbolInfo(info: BybitInstrumentInfo): SymbolInfo {
        return {
            symbol: info.symbol,
            baseAsset: info.baseCoin,
            quoteAsset: info.quoteCoin,
            pricePrecision: parseInt(info.priceScale),
            quantityPrecision: info.lotSizeFilter.qtyStep.split(".")[1]?.length || 0,
            minNotional: 0,
            maxNotional: Number.MAX_SAFE_INTEGER,
            minQuantity: parseFloat(info.lotSizeFilter.minOrderQty),
            maxQuantity: parseFloat(info.lotSizeFilter.maxOrderQty),
            tickSize: parseFloat(info.priceFilter.tickSize),
            stepSize: parseFloat(info.lotSizeFilter.qtyStep),
            status: info.status === "Trading" ? "TRADING" : "HALT"
        };
    }
}
