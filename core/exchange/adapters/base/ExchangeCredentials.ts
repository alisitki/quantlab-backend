/**
 * Exchange Credentials Management
 *
 * Handles secure loading and validation of exchange API credentials.
 * Credentials are loaded from environment variables only - never from files.
 */

export interface ExchangeCredentials {
    readonly exchange: string;
    readonly apiKey: string;
    readonly secretKey: string;
    readonly testnet: boolean;
    readonly subaccount?: string;
}

export interface CredentialValidationResult {
    readonly valid: boolean;
    readonly errors: string[];
}

/**
 * Load Binance credentials from environment.
 */
export function loadBinanceCredentials(): ExchangeCredentials | null {
    const apiKey = process.env.BINANCE_API_KEY;
    const secretKey = process.env.BINANCE_SECRET_KEY;
    const testnet = process.env.BINANCE_TESTNET !== "0";  // Default: testnet

    if (!apiKey || !secretKey) {
        return null;
    }

    return {
        exchange: "binance",
        apiKey,
        secretKey,
        testnet,
        subaccount: process.env.BINANCE_SUBACCOUNT
    };
}

/**
 * Load Bybit credentials from environment.
 */
export function loadBybitCredentials(): ExchangeCredentials | null {
    const apiKey = process.env.BYBIT_API_KEY;
    const secretKey = process.env.BYBIT_SECRET_KEY;
    const testnet = process.env.BYBIT_TESTNET !== "0";

    if (!apiKey || !secretKey) {
        return null;
    }

    return {
        exchange: "bybit",
        apiKey,
        secretKey,
        testnet
    };
}

/**
 * Load OKX credentials from environment.
 */
export function loadOkxCredentials(): ExchangeCredentials | null {
    const apiKey = process.env.OKX_API_KEY;
    const secretKey = process.env.OKX_SECRET_KEY;
    const passphrase = process.env.OKX_PASSPHRASE;
    const testnet = process.env.OKX_TESTNET !== "0";

    if (!apiKey || !secretKey || !passphrase) {
        return null;
    }

    return {
        exchange: "okx",
        apiKey,
        secretKey,
        testnet,
        subaccount: passphrase  // OKX uses passphrase
    };
}

/**
 * Validate credentials format (not connectivity).
 */
export function validateCredentials(creds: ExchangeCredentials): CredentialValidationResult {
    const errors: string[] = [];

    // API key validation
    if (!creds.apiKey || creds.apiKey.length < 10) {
        errors.push("API key is too short or missing");
    }

    // Secret key validation
    if (!creds.secretKey || creds.secretKey.length < 10) {
        errors.push("Secret key is too short or missing");
    }

    // Check for placeholder values
    const placeholders = ["YOUR_API_KEY", "YOUR_SECRET", "xxx", "placeholder"];
    for (const ph of placeholders) {
        if (creds.apiKey.toLowerCase().includes(ph) ||
            creds.secretKey.toLowerCase().includes(ph)) {
            errors.push("Credentials contain placeholder values");
            break;
        }
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

/**
 * Mask credentials for logging (shows only first/last 4 chars).
 */
export function maskCredentials(creds: ExchangeCredentials): {
    exchange: string;
    apiKey: string;
    testnet: boolean
} {
    const masked = creds.apiKey.length > 8
        ? `${creds.apiKey.slice(0, 4)}...${creds.apiKey.slice(-4)}`
        : "****";

    return {
        exchange: creds.exchange,
        apiKey: masked,
        testnet: creds.testnet
    };
}

/**
 * Load credentials for a specific exchange.
 */
export function loadCredentialsForExchange(exchange: string): ExchangeCredentials | null {
    switch (exchange.toLowerCase()) {
        case "binance":
            return loadBinanceCredentials();
        case "bybit":
            return loadBybitCredentials();
        case "okx":
            return loadOkxCredentials();
        default:
            return null;
    }
}
