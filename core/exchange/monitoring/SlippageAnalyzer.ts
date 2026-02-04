/**
 * Slippage Analyzer
 *
 * Tracks and analyzes slippage between expected and actual fill prices.
 * Useful for understanding execution quality.
 */

// ============================================================================
// Types
// ============================================================================

export interface SlippageRecord {
    readonly bridgeId: string;
    readonly symbol: string;
    readonly side: "BUY" | "SELL";
    readonly expectedPrice: number;
    readonly actualPrice: number;
    readonly slippageBps: number;      // Positive = worse than expected
    readonly quantity: number;
    readonly notional: number;
    readonly timestamp: number;
}

export interface SlippageStats {
    readonly symbol: string;
    readonly count: number;
    readonly avgSlippageBps: number;
    readonly maxSlippageBps: number;
    readonly minSlippageBps: number;
    readonly totalNotional: number;
    readonly costBps: number;          // Weighted average by notional
    readonly periodStart: number;
    readonly periodEnd: number;
}

export interface SlippageAnalyzerConfig {
    /** Maximum records to keep in memory */
    readonly maxRecords: number;

    /** Alert threshold in basis points */
    readonly alertThresholdBps: number;

    /** Enable alerting */
    readonly enableAlerts: boolean;
}

const DEFAULT_CONFIG: SlippageAnalyzerConfig = {
    maxRecords: 1000,
    alertThresholdBps: 50,  // Alert if slippage > 0.5%
    enableAlerts: true
};

// ============================================================================
// Slippage Analyzer
// ============================================================================

export class SlippageAnalyzer {
    readonly #config: SlippageAnalyzerConfig;
    readonly #records: SlippageRecord[] = [];
    readonly #statsBySymbol: Map<string, SlippageStats> = new Map();

    constructor(config: Partial<SlippageAnalyzerConfig> = {}) {
        this.#config = { ...DEFAULT_CONFIG, ...config };
    }

    /**
     * Record a new slippage observation.
     */
    async record(params: {
        bridgeId: string;
        symbol: string;
        side: "BUY" | "SELL";
        expectedPrice: number;
        actualPrice: number;
        quantity: number;
    }): Promise<SlippageRecord> {
        const { bridgeId, symbol, side, expectedPrice, actualPrice, quantity } = params;

        // Calculate slippage (positive = worse)
        // For BUY: actual > expected is bad (paid more)
        // For SELL: actual < expected is bad (received less)
        const priceDiff = actualPrice - expectedPrice;
        const slippageBps = expectedPrice > 0
            ? Math.round((priceDiff / expectedPrice) * 10000) * (side === "BUY" ? 1 : -1)
            : 0;

        const record: SlippageRecord = {
            bridgeId,
            symbol,
            side,
            expectedPrice,
            actualPrice,
            slippageBps,
            quantity,
            notional: quantity * actualPrice,
            timestamp: Date.now()
        };

        // Store record
        this.#records.push(record);

        // Trim if over limit
        while (this.#records.length > this.#config.maxRecords) {
            this.#records.shift();
        }

        // Update stats
        this.updateStats(record);

        // Alert on high slippage
        if (this.#config.enableAlerts && Math.abs(slippageBps) > this.#config.alertThresholdBps) {
            await this.sendSlippageAlert(record);
        }

        return record;
    }

    /**
     * Get stats for a specific symbol.
     */
    getStats(symbol: string): SlippageStats | undefined {
        return this.#statsBySymbol.get(symbol);
    }

    /**
     * Get stats for all symbols.
     */
    getAllStats(): SlippageStats[] {
        return Array.from(this.#statsBySymbol.values());
    }

    /**
     * Get recent records.
     */
    getRecentRecords(limit: number = 100): SlippageRecord[] {
        return this.#records.slice(-limit);
    }

    /**
     * Get records for a specific symbol.
     */
    getRecordsBySymbol(symbol: string): SlippageRecord[] {
        return this.#records.filter(r => r.symbol === symbol);
    }

    /**
     * Get aggregate stats across all symbols.
     */
    getAggregateStats(): {
        totalRecords: number;
        avgSlippageBps: number;
        totalNotional: number;
        weightedSlippageBps: number;
    } {
        if (this.#records.length === 0) {
            return {
                totalRecords: 0,
                avgSlippageBps: 0,
                totalNotional: 0,
                weightedSlippageBps: 0
            };
        }

        const totalNotional = this.#records.reduce((sum, r) => sum + r.notional, 0);
        const weightedSum = this.#records.reduce(
            (sum, r) => sum + r.slippageBps * r.notional,
            0
        );
        const simpleSum = this.#records.reduce((sum, r) => sum + r.slippageBps, 0);

        return {
            totalRecords: this.#records.length,
            avgSlippageBps: simpleSum / this.#records.length,
            totalNotional,
            weightedSlippageBps: totalNotional > 0 ? weightedSum / totalNotional : 0
        };
    }

    /**
     * Clear all records and stats.
     */
    clear(): void {
        this.#records.length = 0;
        this.#statsBySymbol.clear();
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private updateStats(record: SlippageRecord): void {
        const existing = this.#statsBySymbol.get(record.symbol);

        if (!existing) {
            this.#statsBySymbol.set(record.symbol, {
                symbol: record.symbol,
                count: 1,
                avgSlippageBps: record.slippageBps,
                maxSlippageBps: record.slippageBps,
                minSlippageBps: record.slippageBps,
                totalNotional: record.notional,
                costBps: record.slippageBps,
                periodStart: record.timestamp,
                periodEnd: record.timestamp
            });
            return;
        }

        const newCount = existing.count + 1;
        const newTotalNotional = existing.totalNotional + record.notional;
        const newWeightedSum =
            existing.costBps * existing.totalNotional +
            record.slippageBps * record.notional;

        this.#statsBySymbol.set(record.symbol, {
            symbol: record.symbol,
            count: newCount,
            avgSlippageBps: (existing.avgSlippageBps * existing.count + record.slippageBps) / newCount,
            maxSlippageBps: Math.max(existing.maxSlippageBps, record.slippageBps),
            minSlippageBps: Math.min(existing.minSlippageBps, record.slippageBps),
            totalNotional: newTotalNotional,
            costBps: newTotalNotional > 0 ? newWeightedSum / newTotalNotional : 0,
            periodStart: existing.periodStart,
            periodEnd: record.timestamp
        });
    }

    private async sendSlippageAlert(record: SlippageRecord): Promise<void> {
        try {
            const { sendAlert, AlertType, AlertSeverity } = await import("../../alerts/index.js");

            const severity = Math.abs(record.slippageBps) > 100
                ? AlertSeverity.ERROR   // >1% slippage
                : AlertSeverity.WARNING;

            await sendAlert({
                type: AlertType.RISK_REJECTION,
                severity,
                message: `High slippage detected: ${record.slippageBps}bps on ${record.symbol} ${record.side}`,
                metadata: {
                    bridge_id: record.bridgeId,
                    symbol: record.symbol,
                    side: record.side,
                    expected_price: record.expectedPrice,
                    actual_price: record.actualPrice,
                    slippage_bps: record.slippageBps,
                    notional: record.notional
                }
            });
        } catch (err) {
            console.error("[SlippageAnalyzer] Failed to send alert:", err);
        }
    }
}
