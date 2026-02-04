/**
 * OKX API Request Signer
 *
 * Implements signature generation for OKX V5 authenticated endpoints.
 * OKX requires:
 * - OK-ACCESS-KEY header
 * - OK-ACCESS-SIGN header (Base64 HMAC-SHA256)
 * - OK-ACCESS-TIMESTAMP header (ISO format)
 * - OK-ACCESS-PASSPHRASE header
 */

import crypto from "node:crypto";

export class OkxSigner {
    readonly #apiKey: string;
    readonly #secretKey: string;
    readonly #passphrase: string;

    constructor(apiKey: string, secretKey: string, passphrase: string) {
        this.#apiKey = apiKey;
        this.#secretKey = secretKey;
        this.#passphrase = passphrase;
    }

    /**
     * Get API key for header.
     */
    get apiKey(): string {
        return this.#apiKey;
    }

    /**
     * Generate signature for request.
     * OKX signature format: timestamp + method + requestPath + body
     * Result is Base64 encoded HMAC-SHA256
     */
    sign(timestamp: string, method: string, requestPath: string, body: string = ""): string {
        const preSign = timestamp + method.toUpperCase() + requestPath + body;
        return crypto
            .createHmac("sha256", this.#secretKey)
            .update(preSign)
            .digest("base64");
    }

    /**
     * Get current timestamp in ISO format.
     */
    getTimestamp(): string {
        return new Date().toISOString();
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
    getHeaders(timestamp: string, signature: string, simulated: boolean = false): Record<string, string> {
        const headers: Record<string, string> = {
            "OK-ACCESS-KEY": this.#apiKey,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": this.#passphrase,
            "Content-Type": "application/json"
        };

        // Demo trading mode
        if (simulated) {
            headers["x-simulated-trading"] = "1";
        }

        return headers;
    }

    /**
     * Sign a GET request and return URL with headers.
     */
    signGetRequest(
        baseUrl: string,
        endpoint: string,
        params: Record<string, string | number | boolean | undefined | null>,
        simulated: boolean = false
    ): { url: string; headers: Record<string, string> } {
        const timestamp = this.getTimestamp();
        const queryString = this.buildQueryString(params);
        const requestPath = queryString ? `${endpoint}?${queryString}` : endpoint;
        const signature = this.sign(timestamp, "GET", requestPath);

        return {
            url: `${baseUrl}${requestPath}`,
            headers: this.getHeaders(timestamp, signature, simulated)
        };
    }

    /**
     * Sign a POST request and return headers with body.
     */
    signPostRequest(
        baseUrl: string,
        endpoint: string,
        body: Record<string, unknown>,
        simulated: boolean = false
    ): { url: string; body: string; headers: Record<string, string> } {
        const timestamp = this.getTimestamp();
        const bodyString = JSON.stringify(body);
        const signature = this.sign(timestamp, "POST", endpoint, bodyString);

        return {
            url: `${baseUrl}${endpoint}`,
            body: bodyString,
            headers: this.getHeaders(timestamp, signature, simulated)
        };
    }
}
