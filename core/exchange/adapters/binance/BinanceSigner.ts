/**
 * Binance API Request Signer
 *
 * Implements HMAC-SHA256 signature generation for Binance authenticated endpoints.
 * All signed requests require:
 * - timestamp: Server time in milliseconds
 * - signature: HMAC-SHA256 of query string
 * - X-MBX-APIKEY header
 */

import crypto from "node:crypto";

export class BinanceSigner {
    readonly #apiKey: string;
    readonly #secretKey: string;

    constructor(apiKey: string, secretKey: string) {
        this.#apiKey = apiKey;
        this.#secretKey = secretKey;
    }

    /**
     * Get API key for header.
     */
    get apiKey(): string {
        return this.#apiKey;
    }

    /**
     * Sign request parameters.
     * Returns the signature to append to query string.
     */
    sign(params: Record<string, string | number | boolean>): string {
        const queryString = this.buildQueryString(params);
        return this.signString(queryString);
    }

    /**
     * Build query string from parameters.
     * Excludes undefined/null values.
     */
    buildQueryString(params: Record<string, string | number | boolean | undefined | null>): string {
        const entries: string[] = [];

        for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null) {
                entries.push(`${key}=${encodeURIComponent(String(value))}`);
            }
        }

        return entries.join("&");
    }

    /**
     * Generate HMAC-SHA256 signature for a string.
     */
    signString(data: string): string {
        return crypto
            .createHmac("sha256", this.#secretKey)
            .update(data)
            .digest("hex");
    }

    /**
     * Build signed URL for a request.
     * Automatically adds timestamp and signature.
     */
    buildSignedUrl(
        baseUrl: string,
        endpoint: string,
        params: Record<string, string | number | boolean | undefined | null>,
        recvWindow: number = 5000
    ): string {
        const timestamp = Date.now();

        const allParams: Record<string, string | number | boolean> = {
            ...this.cleanParams(params),
            timestamp,
            recvWindow
        };

        const queryString = this.buildQueryString(allParams);
        const signature = this.signString(queryString);

        return `${baseUrl}${endpoint}?${queryString}&signature=${signature}`;
    }

    /**
     * Get headers for authenticated request.
     */
    getHeaders(): Record<string, string> {
        return {
            "X-MBX-APIKEY": this.#apiKey,
            "Content-Type": "application/x-www-form-urlencoded"
        };
    }

    /**
     * Remove undefined/null values from params.
     */
    private cleanParams(
        params: Record<string, string | number | boolean | undefined | null>
    ): Record<string, string | number | boolean> {
        const cleaned: Record<string, string | number | boolean> = {};

        for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null) {
                cleaned[key] = value;
            }
        }

        return cleaned;
    }
}
