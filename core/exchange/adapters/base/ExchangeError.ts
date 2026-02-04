/**
 * Exchange Error Taxonomy
 *
 * Standardized error codes for exchange adapter operations.
 * All adapters should map exchange-specific errors to these codes.
 */

export enum ExchangeErrorCode {
    // Authentication errors
    AUTH_FAILED = "AUTH_FAILED",
    API_KEY_INVALID = "API_KEY_INVALID",
    SIGNATURE_INVALID = "SIGNATURE_INVALID",
    IP_NOT_WHITELISTED = "IP_NOT_WHITELISTED",

    // Rate limiting
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED",

    // Order errors
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE",
    INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN",
    ORDER_NOT_FOUND = "ORDER_NOT_FOUND",
    ORDER_REJECTED = "ORDER_REJECTED",
    ORDER_ALREADY_FILLED = "ORDER_ALREADY_FILLED",
    ORDER_ALREADY_CANCELLED = "ORDER_ALREADY_CANCELLED",
    INVALID_QUANTITY = "INVALID_QUANTITY",
    INVALID_PRICE = "INVALID_PRICE",
    MIN_NOTIONAL_NOT_MET = "MIN_NOTIONAL_NOT_MET",
    MAX_NOTIONAL_EXCEEDED = "MAX_NOTIONAL_EXCEEDED",
    POSITION_LIMIT_EXCEEDED = "POSITION_LIMIT_EXCEEDED",

    // Symbol errors
    SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND",
    SYMBOL_NOT_TRADING = "SYMBOL_NOT_TRADING",

    // Network errors
    NETWORK_ERROR = "NETWORK_ERROR",
    TIMEOUT = "TIMEOUT",
    CONNECTION_REFUSED = "CONNECTION_REFUSED",

    // Server errors
    SERVER_ERROR = "SERVER_ERROR",
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE",

    // Time sync
    TIMESTAMP_OUTSIDE_RECV_WINDOW = "TIMESTAMP_OUTSIDE_RECV_WINDOW",

    // Unknown
    UNKNOWN = "UNKNOWN"
}

export interface ExchangeErrorDetail {
    readonly code: ExchangeErrorCode;
    readonly message: string;
    readonly exchange: string;
    readonly originalCode?: string | number;
    readonly originalMessage?: string;
    readonly timestamp: number;
    readonly retryable: boolean;
    readonly metadata?: Record<string, unknown>;
}

export class ExchangeError extends Error {
    readonly code: ExchangeErrorCode;
    readonly exchange: string;
    readonly originalCode?: string | number;
    readonly originalMessage?: string;
    readonly timestamp: number;
    readonly retryable: boolean;
    readonly metadata?: Record<string, unknown>;

    constructor(detail: ExchangeErrorDetail) {
        super(detail.message);
        this.name = "ExchangeError";
        this.code = detail.code;
        this.exchange = detail.exchange;
        this.originalCode = detail.originalCode;
        this.originalMessage = detail.originalMessage;
        this.timestamp = detail.timestamp;
        this.retryable = detail.retryable;
        this.metadata = detail.metadata;

        // Maintains proper stack trace for where error was thrown
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, ExchangeError);
        }
    }

    toJSON(): ExchangeErrorDetail {
        return {
            code: this.code,
            message: this.message,
            exchange: this.exchange,
            originalCode: this.originalCode,
            originalMessage: this.originalMessage,
            timestamp: this.timestamp,
            retryable: this.retryable,
            metadata: this.metadata
        };
    }
}

/**
 * Determines if an error is retryable based on error code.
 */
export function isRetryableError(code: ExchangeErrorCode): boolean {
    const retryableCodes: ExchangeErrorCode[] = [
        ExchangeErrorCode.RATE_LIMIT_EXCEEDED,
        ExchangeErrorCode.NETWORK_ERROR,
        ExchangeErrorCode.TIMEOUT,
        ExchangeErrorCode.SERVER_ERROR,
        ExchangeErrorCode.SERVICE_UNAVAILABLE,
        ExchangeErrorCode.TIMESTAMP_OUTSIDE_RECV_WINDOW
    ];
    return retryableCodes.includes(code);
}

/**
 * Create a standardized ExchangeError from any error.
 */
export function createExchangeError(
    exchange: string,
    error: unknown,
    defaultCode: ExchangeErrorCode = ExchangeErrorCode.UNKNOWN
): ExchangeError {
    const now = Date.now();

    if (error instanceof ExchangeError) {
        return error;
    }

    if (error instanceof Error) {
        // Check for network errors
        if (error.message.includes("ECONNREFUSED")) {
            return new ExchangeError({
                code: ExchangeErrorCode.CONNECTION_REFUSED,
                message: `Connection refused to ${exchange}`,
                exchange,
                originalMessage: error.message,
                timestamp: now,
                retryable: true
            });
        }

        if (error.message.includes("ETIMEDOUT") || error.message.includes("timeout")) {
            return new ExchangeError({
                code: ExchangeErrorCode.TIMEOUT,
                message: `Request timeout to ${exchange}`,
                exchange,
                originalMessage: error.message,
                timestamp: now,
                retryable: true
            });
        }

        return new ExchangeError({
            code: defaultCode,
            message: error.message,
            exchange,
            originalMessage: error.message,
            timestamp: now,
            retryable: isRetryableError(defaultCode)
        });
    }

    return new ExchangeError({
        code: defaultCode,
        message: String(error),
        exchange,
        timestamp: now,
        retryable: isRetryableError(defaultCode)
    });
}
