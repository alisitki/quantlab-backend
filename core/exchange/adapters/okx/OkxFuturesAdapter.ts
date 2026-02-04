/**
 * OKX Futures Adapter
 *
 * REST API client for OKX USDT Perpetual Swaps.
 * Uses V5 API.
 *
 * Endpoints:
 * - Production: https://www.okx.com
 * - AWS: https://aws.okx.com
 * - Demo: https://www.okx.com (with x-simulated-trading header)
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
    PositionSide
} from "../base/ExchangeAdapter.js";
import { ExchangeCredentials } from "../base/ExchangeCredentials.js";
import {
    ExchangeError,
    ExchangeErrorCode,
    createExchangeError
} from "../base/ExchangeError.js";
import { OkxSigner } from "./OkxSigner.js";
import {
    OKX_API_BASE,
    OkxApiResponse,
    OkxOrderResponse,
    OkxOrderDetail,
    OkxPosition,
    OkxAccountInfo,
    OkxInstrument,
    OkxOrderState
} from "./okx_types.js";

// ============================================================================
// Constants
// ============================================================================

const REQUEST_TIMEOUT = 10000;
const INST_TYPE = "SWAP";

// ============================================================================
// OKX Futures Adapter
// ============================================================================

export class OkxFuturesAdapter extends ExchangeAdapter {
    readonly exchange = "okx";
    readonly testnet: boolean;

    readonly #signer: OkxSigner;
    readonly #baseUrl: string;
    readonly #simulated: boolean;

    constructor(credentials: ExchangeCredentials) {
        super(credentials);
        this.testnet = credentials.testnet;
        this.#baseUrl = OKX_API_BASE;
        this.#simulated = credentials.testnet;  // Use simulated trading for testnet
        this.#signer = new OkxSigner(
            credentials.apiKey,
            credentials.secretKey,
            credentials.subaccount || ""  // OKX uses passphrase
        );
    }

    // -------------------------------------------------------------------------
    // Order Operations
    // -------------------------------------------------------------------------

    async submitOrder(params: SubmitOrderParams): Promise<ExchangeOrder> {
        const instId = this.toOkxSymbol(params.symbol);

        const body: Record<string, unknown> = {
            instId,
            tdMode: "cross",
            side: params.side.toLowerCase(),
            ordType: params.orderType.toLowerCase(),
            sz: params.quantity.toString(),
            clOrdId: params.clientOrderId,
            reduceOnly: params.reduceOnly
        };

        if (params.orderType === "LIMIT" && params.price !== undefined) {
            body.px = params.price.toString();
        }

        const { url, body: signedBody, headers } = this.#signer.signPostRequest(
            this.#baseUrl,
            "/api/v5/trade/order",
            body,
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, {
            method: "POST",
            headers,
            body: signedBody
        });

        const data = await this.handleResponse<OkxApiResponse<OkxOrderResponse[]>>(response);

        if (data.data.length === 0 || data.data[0].sCode !== "0") {
            throw new ExchangeError({
                code: ExchangeErrorCode.ORDER_REJECTED,
                message: data.data[0]?.sMsg || "Order rejected",
                exchange: this.exchange,
                originalCode: data.data[0]?.sCode,
                originalMessage: data.data[0]?.sMsg,
                timestamp: Date.now(),
                retryable: false
            });
        }

        // Fetch order details
        const orderDetail = await this.getOrder(params.symbol, params.clientOrderId);
        if (orderDetail) {
            return orderDetail;
        }

        return {
            exchangeOrderId: data.data[0].ordId,
            clientOrderId: data.data[0].clOrdId,
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
        const instId = this.toOkxSymbol(symbol);

        const body = {
            instId,
            clOrdId: orderId
        };

        const { url, body: signedBody, headers } = this.#signer.signPostRequest(
            this.#baseUrl,
            "/api/v5/trade/cancel-order",
            body,
            this.#simulated
        );

        try {
            const response = await this.fetchWithTimeout(url, {
                method: "POST",
                headers,
                body: signedBody
            });

            await this.handleResponse(response);
            return true;
        } catch (error) {
            if (error instanceof ExchangeError && error.code === ExchangeErrorCode.ORDER_NOT_FOUND) {
                return false;
            }
            throw error;
        }
    }

    async getOrder(symbol: string, orderId: string): Promise<ExchangeOrder | null> {
        const instId = this.toOkxSymbol(symbol);

        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/api/v5/trade/order",
            { instId, clOrdId: orderId },
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });

        try {
            const data = await this.handleResponse<OkxApiResponse<OkxOrderDetail[]>>(response);

            if (data.data.length === 0) {
                return null;
            }

            return this.mapOrderDetail(data.data[0], symbol);
        } catch (error) {
            if (error instanceof ExchangeError && error.code === ExchangeErrorCode.ORDER_NOT_FOUND) {
                return null;
            }
            throw error;
        }
    }

    async getOpenOrders(symbol?: string): Promise<ExchangeOrder[]> {
        const params: Record<string, string> = { instType: INST_TYPE };
        if (symbol) {
            params.instId = this.toOkxSymbol(symbol);
        }

        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/api/v5/trade/orders-pending",
            params,
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<OkxApiResponse<OkxOrderDetail[]>>(response);

        return data.data.map(order => this.mapOrderDetail(order, this.fromOkxSymbol(order.instId)));
    }

    // -------------------------------------------------------------------------
    // Position Operations
    // -------------------------------------------------------------------------

    async getPosition(symbol: string): Promise<ExchangePosition> {
        const instId = this.toOkxSymbol(symbol);

        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/api/v5/account/positions",
            { instType: INST_TYPE, instId },
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<OkxApiResponse<OkxPosition[]>>(response);

        if (data.data.length === 0 || parseFloat(data.data[0].pos) === 0) {
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

        return this.mapPosition(data.data[0], symbol);
    }

    async getAllPositions(): Promise<ExchangePosition[]> {
        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/api/v5/account/positions",
            { instType: INST_TYPE },
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<OkxApiResponse<OkxPosition[]>>(response);

        return data.data
            .filter(p => parseFloat(p.pos) !== 0)
            .map(p => this.mapPosition(p, this.fromOkxSymbol(p.instId)));
    }

    // -------------------------------------------------------------------------
    // Account Operations
    // -------------------------------------------------------------------------

    async getBalances(): Promise<AccountBalance[]> {
        const { url, headers } = this.#signer.signGetRequest(
            this.#baseUrl,
            "/api/v5/account/balance",
            {},
            this.#simulated
        );

        const response = await this.fetchWithTimeout(url, { method: "GET", headers });
        const data = await this.handleResponse<OkxApiResponse<OkxAccountInfo[]>>(response);

        if (data.data.length === 0) {
            return [];
        }

        return data.data[0].details.map(b => ({
            asset: b.ccy,
            free: parseFloat(b.availBal),
            locked: parseFloat(b.frozenBal),
            total: parseFloat(b.cashBal)
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
        const url = `${this.#baseUrl}/api/v5/public/instruments?instType=${INST_TYPE}`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<OkxApiResponse<OkxInstrument[]>>(response);

        return {
            exchange: this.exchange,
            testnet: this.testnet,
            serverTime: Date.now(),
            symbols: data.data
                .filter(s => s.settleCcy === "USDT")
                .map(s => this.mapSymbolInfo(s))
        };
    }

    async getSymbolInfo(symbol: string): Promise<SymbolInfo | null> {
        const instId = this.toOkxSymbol(symbol);
        const url = `${this.#baseUrl}/api/v5/public/instruments?instType=${INST_TYPE}&instId=${instId}`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<OkxApiResponse<OkxInstrument[]>>(response);

        if (data.data.length === 0) {
            return null;
        }

        return this.mapSymbolInfo(data.data[0]);
    }

    // -------------------------------------------------------------------------
    // Health Check
    // -------------------------------------------------------------------------

    async ping(): Promise<boolean> {
        try {
            const url = `${this.#baseUrl}/api/v5/public/time`;
            const response = await this.fetchWithTimeout(url, { method: "GET" });
            return response.ok;
        } catch {
            return false;
        }
    }

    async getServerTime(): Promise<number> {
        const url = `${this.#baseUrl}/api/v5/public/time`;
        const response = await this.fetchWithTimeout(url, { method: "GET" });
        const data = await this.handleResponse<OkxApiResponse<{ ts: string }[]>>(response);
        return parseInt(data.data[0].ts);
    }

    // -------------------------------------------------------------------------
    // Symbol Conversion
    // -------------------------------------------------------------------------

    /**
     * Convert BTCUSDT to BTC-USDT-SWAP format.
     */
    private toOkxSymbol(symbol: string): string {
        if (symbol.includes("-")) {
            return symbol;  // Already in OKX format
        }

        // Extract base from BTCUSDT format
        const base = symbol.replace("USDT", "");
        return `${base}-USDT-SWAP`;
    }

    /**
     * Convert BTC-USDT-SWAP to BTCUSDT format.
     */
    private fromOkxSymbol(instId: string): string {
        const parts = instId.split("-");
        if (parts.length >= 2) {
            return `${parts[0]}${parts[1]}`;
        }
        return instId;
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
        const data = await response.json() as OkxApiResponse<unknown>;

        if (data.code !== "0") {
            const errorCode = this.mapOkxError(data.code);
            throw new ExchangeError({
                code: errorCode,
                message: data.msg || `OKX error ${data.code}`,
                exchange: this.exchange,
                originalCode: data.code,
                originalMessage: data.msg,
                timestamp: Date.now(),
                retryable: data.code === "50011"  // Rate limit
            });
        }

        return data as T;
    }

    private mapOkxError(code: string): ExchangeErrorCode {
        switch (code) {
            case "50011": return ExchangeErrorCode.RATE_LIMIT_EXCEEDED;
            case "50013": return ExchangeErrorCode.SIGNATURE_INVALID;
            case "50014": return ExchangeErrorCode.API_KEY_INVALID;
            case "50015": return ExchangeErrorCode.AUTH_FAILED;
            case "51001": return ExchangeErrorCode.SYMBOL_NOT_FOUND;
            case "51008": return ExchangeErrorCode.INSUFFICIENT_BALANCE;
            case "51009": return ExchangeErrorCode.ORDER_REJECTED;
            case "51020": return ExchangeErrorCode.ORDER_NOT_FOUND;
            case "51119": return ExchangeErrorCode.ORDER_REJECTED;
            case "51127": return ExchangeErrorCode.POSITION_LIMIT_EXCEEDED;
            default: return ExchangeErrorCode.UNKNOWN;
        }
    }

    private mapOrderStatus(state: OkxOrderState): OrderStatus {
        switch (state) {
            case "live": return "OPEN";
            case "partially_filled": return "PARTIALLY_FILLED";
            case "filled": return "FILLED";
            case "canceled": return "CANCELLED";
            case "mmp_canceled": return "CANCELLED";
            default: return "PENDING";
        }
    }

    private mapOrderDetail(order: OkxOrderDetail, symbol: string): ExchangeOrder {
        return {
            exchangeOrderId: order.ordId,
            clientOrderId: order.clOrdId,
            symbol,
            side: order.side === "buy" ? "BUY" : "SELL",
            orderType: order.ordType === "market" ? "MARKET" : "LIMIT",
            requestedQty: parseFloat(order.sz),
            filledQty: parseFloat(order.fillSz),
            avgFillPrice: parseFloat(order.avgPx) || 0,
            status: this.mapOrderStatus(order.state),
            submittedAt: parseInt(order.cTime),
            updatedAt: parseInt(order.uTime),
            fees: order.fee ? [{
                asset: order.feeCcy,
                amount: Math.abs(parseFloat(order.fee))
            }] : []
        };
    }

    private mapPosition(pos: OkxPosition, symbol: string): ExchangePosition {
        const quantity = parseFloat(pos.pos);
        const side: PositionSide = pos.posSide === "long" ? "LONG" :
            pos.posSide === "short" ? "SHORT" :
                quantity > 0 ? "LONG" : quantity < 0 ? "SHORT" : "FLAT";

        return {
            symbol,
            side,
            quantity: Math.abs(quantity),
            entryPrice: parseFloat(pos.avgPx),
            markPrice: 0,
            unrealizedPnl: parseFloat(pos.upl),
            margin: parseFloat(pos.margin),
            leverage: parseFloat(pos.lever),
            liquidationPrice: parseFloat(pos.liqPx) || 0,
            updatedAt: parseInt(pos.uTime)
        };
    }

    private mapSymbolInfo(info: OkxInstrument): SymbolInfo {
        return {
            symbol: this.fromOkxSymbol(info.instId),
            baseAsset: info.baseCcy,
            quoteAsset: info.quoteCcy,
            pricePrecision: info.tickSz.split(".")[1]?.length || 0,
            quantityPrecision: info.lotSz.split(".")[1]?.length || 0,
            minNotional: 0,
            maxNotional: Number.MAX_SAFE_INTEGER,
            minQuantity: parseFloat(info.minSz),
            maxQuantity: parseFloat(info.maxLmtSz),
            tickSize: parseFloat(info.tickSz),
            stepSize: parseFloat(info.lotSz),
            status: info.state === "live" ? "TRADING" : "HALT"
        };
    }
}
