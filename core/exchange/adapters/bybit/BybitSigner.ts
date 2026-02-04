/**
 * Bybit API Request Signer
 *
 * Implements HMAC-SHA256 signature generation for Bybit V5 authenticated endpoints.
 * Bybit requires:
 * - X-BAPI-API-KEY header
 * - X-BAPI-TIMESTAMP header
 * - X-BAPI-SIGN header (HMAC-SHA256)
 * - X-BAPI-RECV-WINDOW header
 */

import crypto from "node:crypto";

export class BybitSigner {
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
     * Generate signature for request.
     * Bybit signature format: timestamp + apiKey + recvWindow + queryString (or body)
     */
    sign(timestamp: number, recvWindow: number, params: string): string {
        const preSign = `${timestamp}${this.#apiKey}${recvWindow}${params}`;
        return crypto
            .createHmac("sha256", this.#secretKey)
            .update(preSign)
            .digest("hex");
    }

    /**
     * Build query string from parameters.
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
     * Get headers for authenticated request.
     */
    getHeaders(timestamp: number, recvWindow: number, signature: string): Record<string, string> {
        return {
            "X-BAPI-API-KEY": this.#apiKey,
            "X-BAPI-TIMESTAMP": String(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": String(recvWindow),
            "Content-Type": "application/json"
        };
    }

    /**
     * Sign a GET request and return full URL with headers.
     */
    signGetRequest(
        baseUrl: string,
        endpoint: string,
        params: Record<string, string | number | boolean | undefined | null>,
        recvWindow: number = 5000
    ): { url: string; headers: Record<string, string> } {
        const timestamp = Date.now();
        const queryString = this.buildQueryString(params);
        const signature = this.sign(timestamp, recvWindow, queryString);

        return {
            url: `${baseUrl}${endpoint}${queryString ? `?${queryString}` : ""}`,
            headers: this.getHeaders(timestamp, recvWindow, signature)
        };
    }

    /**
     * Sign a POST request and return headers with body.
     */
    signPostRequest(
        body: Record<string, unknown>,
        recvWindow: number = 5000
    ): { body: string; headers: Record<string, string> } {
        const timestamp = Date.now();
        const bodyString = JSON.stringify(body);
        const signature = this.sign(timestamp, recvWindow, bodyString);

        return {
            body: bodyString,
            headers: this.getHeaders(timestamp, recvWindow, signature)
        };
    }
}
