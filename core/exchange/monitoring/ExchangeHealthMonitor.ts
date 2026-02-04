/**
 * Exchange Health Monitor
 *
 * Monitors exchange connectivity, latency, and server time drift.
 * Sends alerts on connection failures or excessive drift.
 */

import { EventEmitter } from "node:events";
import { ExchangeAdapter } from "../adapters/base/ExchangeAdapter.js";

// ============================================================================
// Types
// ============================================================================

export interface HealthStatus {
    readonly exchange: string;
    readonly testnet: boolean;
    readonly healthy: boolean;
    readonly pingMs?: number;
    readonly serverTimeDriftMs?: number;
    readonly checkedAt: number;
    readonly error?: string;
    readonly consecutiveFailures: number;
}

export interface HealthMonitorConfig {
    /** Ping interval in milliseconds */
    readonly pingIntervalMs: number;

    /** Maximum acceptable time drift in milliseconds */
    readonly maxDriftMs: number;

    /** Number of consecutive failures before alerting */
    readonly alertAfterFailures: number;

    /** Enable alerting */
    readonly enableAlerts: boolean;
}

const DEFAULT_CONFIG: HealthMonitorConfig = {
    pingIntervalMs: 30000,      // 30 seconds
    maxDriftMs: 5000,           // 5 seconds
    alertAfterFailures: 3,
    enableAlerts: true
};

// ============================================================================
// Exchange Health Monitor
// ============================================================================

export class ExchangeHealthMonitor extends EventEmitter {
    readonly #adapter: ExchangeAdapter;
    readonly #config: HealthMonitorConfig;

    #timer?: NodeJS.Timeout;
    #lastStatus?: HealthStatus;
    #consecutiveFailures: number = 0;
    #alertSent: boolean = false;

    constructor(adapter: ExchangeAdapter, config: Partial<HealthMonitorConfig> = {}) {
        super();
        this.#adapter = adapter;
        this.#config = { ...DEFAULT_CONFIG, ...config };
    }

    /**
     * Start monitoring.
     */
    start(): void {
        if (this.#timer) {
            return;
        }

        // Run immediately
        this.checkHealth();

        // Schedule periodic checks
        this.#timer = setInterval(() => {
            this.checkHealth();
        }, this.#config.pingIntervalMs);

        console.log(
            `[HealthMonitor] Started for ${this.#adapter.exchange} ` +
            `(interval: ${this.#config.pingIntervalMs}ms)`
        );
    }

    /**
     * Stop monitoring.
     */
    stop(): void {
        if (this.#timer) {
            clearInterval(this.#timer);
            this.#timer = undefined;
            console.log(`[HealthMonitor] Stopped for ${this.#adapter.exchange}`);
        }
    }

    /**
     * Get the last health status.
     */
    getLastStatus(): HealthStatus | undefined {
        return this.#lastStatus;
    }

    /**
     * Check health now (manual trigger).
     */
    async checkHealth(): Promise<HealthStatus> {
        const checkedAt = Date.now();

        try {
            // Ping test
            const pingStart = Date.now();
            const pingOk = await this.#adapter.ping();
            const pingMs = Date.now() - pingStart;

            if (!pingOk) {
                throw new Error("Ping failed");
            }

            // Time drift check
            const serverTime = await this.#adapter.getServerTime();
            const driftMs = Math.abs(Date.now() - serverTime);

            const healthy = driftMs <= this.#config.maxDriftMs;

            // Reset failure counter on success
            this.#consecutiveFailures = 0;
            this.#alertSent = false;

            this.#lastStatus = {
                exchange: this.#adapter.exchange,
                testnet: this.#adapter.testnet,
                healthy,
                pingMs,
                serverTimeDriftMs: driftMs,
                checkedAt,
                consecutiveFailures: 0
            };

            // Warn on drift (but not critical)
            if (!healthy) {
                console.warn(
                    `[HealthMonitor] Time drift detected for ${this.#adapter.exchange}: ${driftMs}ms`
                );

                if (this.#config.enableAlerts) {
                    await this.sendDriftWarning(driftMs);
                }
            }

            this.emit("health", this.#lastStatus);

        } catch (error) {
            this.#consecutiveFailures++;

            this.#lastStatus = {
                exchange: this.#adapter.exchange,
                testnet: this.#adapter.testnet,
                healthy: false,
                checkedAt,
                error: error instanceof Error ? error.message : String(error),
                consecutiveFailures: this.#consecutiveFailures
            };

            console.error(
                `[HealthMonitor] Check failed for ${this.#adapter.exchange}: ${this.#lastStatus.error}`
            );

            // Alert after consecutive failures
            if (
                this.#config.enableAlerts &&
                this.#consecutiveFailures >= this.#config.alertAfterFailures &&
                !this.#alertSent
            ) {
                await this.sendConnectionAlert();
                this.#alertSent = true;
            }

            this.emit("health", this.#lastStatus);
        }

        return this.#lastStatus;
    }

    /**
     * Check if currently healthy.
     */
    isHealthy(): boolean {
        return this.#lastStatus?.healthy ?? false;
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private async sendConnectionAlert(): Promise<void> {
        try {
            const { sendAlert, AlertType, AlertSeverity } = await import("../../alerts/index.js");

            await sendAlert({
                type: AlertType.SERVICE_DOWN,
                severity: AlertSeverity.CRITICAL,
                message: `Exchange connection failed ${this.#consecutiveFailures} times: ${this.#adapter.exchange}`,
                metadata: {
                    exchange: this.#adapter.exchange,
                    testnet: this.#adapter.testnet,
                    consecutive_failures: this.#consecutiveFailures,
                    last_error: this.#lastStatus?.error
                }
            });
        } catch (err) {
            console.error("[HealthMonitor] Failed to send alert:", err);
        }
    }

    private async sendDriftWarning(driftMs: number): Promise<void> {
        try {
            const { sendAlert, AlertType, AlertSeverity } = await import("../../alerts/index.js");

            await sendAlert({
                type: AlertType.WEBSOCKET_DISCONNECTED,
                severity: AlertSeverity.WARNING,
                message: `Exchange time drift detected: ${driftMs}ms (max: ${this.#config.maxDriftMs}ms)`,
                metadata: {
                    exchange: this.#adapter.exchange,
                    testnet: this.#adapter.testnet,
                    drift_ms: driftMs,
                    max_drift_ms: this.#config.maxDriftMs
                }
            });
        } catch (err) {
            console.error("[HealthMonitor] Failed to send drift warning:", err);
        }
    }
}
