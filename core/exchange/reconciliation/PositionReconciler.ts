/**
 * Position Reconciler
 *
 * Compares paper trading positions against actual exchange positions.
 * Detects mismatches and triggers alerts for discrepancies.
 */

import { ExchangeAdapter, ExchangePosition } from "../adapters/base/ExchangeAdapter.js";
import {
    PaperPosition,
    ExchangePositionSnapshot,
    PositionMatch,
    PositionMismatch,
    MismatchSeverity,
    ReconciliationReport,
    createReconciliationReport,
    formatReportSummary
} from "./ReconciliationReport.js";

// ============================================================================
// Configuration
// ============================================================================

export interface ReconcilerConfig {
    /** Tolerance percentage for quantity mismatch (0.01 = 1%) */
    readonly tolerancePct: number;

    /** Tolerance percentage for PnL mismatch (0.05 = 5%) */
    readonly pnlTolerancePct: number;

    /** Enable alerting on mismatches */
    readonly enableAlerts: boolean;

    /** Symbols to reconcile (empty = all) */
    readonly symbols?: readonly string[];
}

const DEFAULT_CONFIG: ReconcilerConfig = {
    tolerancePct: 0.01,      // 1% quantity tolerance
    pnlTolerancePct: 0.05,   // 5% PnL tolerance
    enableAlerts: true
};

// ============================================================================
// Position Reconciler
// ============================================================================

export class PositionReconciler {
    readonly #adapter: ExchangeAdapter;
    readonly #config: ReconcilerConfig;

    constructor(adapter: ExchangeAdapter, config: Partial<ReconcilerConfig> = {}) {
        this.#adapter = adapter;
        this.#config = { ...DEFAULT_CONFIG, ...config };
    }

    /**
     * Reconcile paper positions against exchange positions.
     */
    async reconcile(paperPositions: PaperPosition[]): Promise<ReconciliationReport> {
        // Fetch exchange positions
        const exchangePositions = await this.#adapter.getAllPositions();

        // Filter by configured symbols if specified
        const filteredExchange = this.#config.symbols
            ? exchangePositions.filter(p => this.#config.symbols!.includes(p.symbol))
            : exchangePositions;

        const filteredPaper = this.#config.symbols
            ? paperPositions.filter(p => this.#config.symbols!.includes(p.symbol))
            : paperPositions;

        // Build lookup maps
        const exchangeBySymbol = new Map<string, ExchangePosition>();
        for (const pos of filteredExchange) {
            if (pos.quantity !== 0) {
                exchangeBySymbol.set(pos.symbol, pos);
            }
        }

        const paperBySymbol = new Map<string, PaperPosition>();
        for (const pos of filteredPaper) {
            if (pos.quantity !== 0) {
                paperBySymbol.set(pos.symbol, pos);
            }
        }

        // Compare positions
        const matches: PositionMatch[] = [];
        const mismatches: PositionMismatch[] = [];
        const orphanedPaper: PaperPosition[] = [];

        for (const [symbol, paper] of paperBySymbol) {
            const exchange = exchangeBySymbol.get(symbol);

            if (!exchange) {
                // Paper position has no corresponding exchange position
                orphanedPaper.push(paper);
                continue;
            }

            // Convert exchange position to signed quantity
            const exchangeSignedQty = exchange.side === "SHORT"
                ? -exchange.quantity
                : exchange.quantity;

            const qtyDiff = Math.abs(paper.quantity - exchangeSignedQty);
            const qtyDiffPct = paper.quantity !== 0
                ? qtyDiff / Math.abs(paper.quantity)
                : exchangeSignedQty !== 0 ? 1 : 0;

            const pnlDiff = Math.abs(paper.unrealizedPnl - exchange.unrealizedPnl);

            const match: PositionMatch = {
                symbol,
                paperQty: paper.quantity,
                exchangeQty: exchangeSignedQty,
                qtyDiff,
                qtyDiffPct,
                pnlDiff
            };

            if (qtyDiffPct > this.#config.tolerancePct) {
                // Mismatch detected
                const severity = this.determineSeverity(qtyDiffPct);
                mismatches.push({
                    ...match,
                    severity,
                    reason: `Quantity mismatch: ${(qtyDiffPct * 100).toFixed(2)}% difference`
                });
            } else {
                matches.push(match);
            }

            // Remove from exchange map (remaining are orphaned)
            exchangeBySymbol.delete(symbol);
        }

        // Remaining exchange positions are orphaned (on exchange but not in paper)
        const orphanedExchange: ExchangePositionSnapshot[] = [];
        for (const pos of exchangeBySymbol.values()) {
            orphanedExchange.push({
                symbol: pos.symbol,
                side: pos.side,
                quantity: pos.quantity,
                entryPrice: pos.entryPrice,
                markPrice: pos.markPrice,
                unrealizedPnl: pos.unrealizedPnl,
                leverage: pos.leverage,
                margin: pos.margin
            });
        }

        // Create report
        const report = createReconciliationReport({
            exchange: this.#adapter.exchange,
            testnet: this.#adapter.testnet,
            matches,
            mismatches,
            orphanedExchange,
            orphanedPaper
        });

        // Send alerts if enabled and unhealthy
        if (this.#config.enableAlerts && !report.isHealthy) {
            await this.sendAlert(report);
        }

        return report;
    }

    /**
     * Get a formatted summary of the last reconciliation.
     */
    formatReport(report: ReconciliationReport): string {
        return formatReportSummary(report);
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private determineSeverity(qtyDiffPct: number): MismatchSeverity {
        if (qtyDiffPct > 0.1) {   // >10% difference
            return "CRITICAL";
        }
        if (qtyDiffPct > 0.05) {  // >5% difference
            return "WARNING";
        }
        return "INFO";
    }

    private async sendAlert(report: ReconciliationReport): Promise<void> {
        try {
            const { sendAlert, AlertType, AlertSeverity } = await import("../../alerts/index.js");

            const hasCritical = report.mismatches.some(m => m.severity === "CRITICAL");
            const hasOrphaned = report.orphanedExchange.length > 0 || report.orphanedPaper.length > 0;

            const severity = hasCritical
                ? AlertSeverity.CRITICAL
                : hasOrphaned
                    ? AlertSeverity.ERROR
                    : AlertSeverity.WARNING;

            const summary = [
                `Mismatches: ${report.mismatches.length}`,
                `Orphaned exchange: ${report.orphanedExchange.length}`,
                `Orphaned paper: ${report.orphanedPaper.length}`
            ].join(", ");

            await sendAlert({
                type: AlertType.RISK_REJECTION,
                severity,
                message: `Position reconciliation mismatch detected: ${summary}`,
                metadata: {
                    exchange: report.exchange,
                    testnet: report.testnet,
                    total_mismatches: report.totalMismatches,
                    worst_mismatch_pct: report.worstMismatchPct,
                    orphaned_exchange: report.orphanedExchange.length,
                    orphaned_paper: report.orphanedPaper.length,
                    mismatch_symbols: report.mismatches.map(m => m.symbol)
                }
            });
        } catch (error) {
            console.error("[Reconciler] Failed to send alert:", error);
        }
    }
}

// ============================================================================
// Reconciliation Scheduler
// ============================================================================

export class ReconciliationScheduler {
    readonly #reconciler: PositionReconciler;
    readonly #getPaperPositions: () => PaperPosition[] | Promise<PaperPosition[]>;
    readonly #intervalMs: number;
    #timer?: NodeJS.Timeout;
    #lastReport?: ReconciliationReport;

    constructor(
        reconciler: PositionReconciler,
        getPaperPositions: () => PaperPosition[] | Promise<PaperPosition[]>,
        intervalMs: number = 60000
    ) {
        this.#reconciler = reconciler;
        this.#getPaperPositions = getPaperPositions;
        this.#intervalMs = intervalMs;
    }

    /**
     * Start periodic reconciliation.
     */
    start(): void {
        if (this.#timer) {
            return;
        }

        // Run immediately
        this.runReconciliation();

        // Schedule periodic runs
        this.#timer = setInterval(() => {
            this.runReconciliation();
        }, this.#intervalMs);
    }

    /**
     * Stop periodic reconciliation.
     */
    stop(): void {
        if (this.#timer) {
            clearInterval(this.#timer);
            this.#timer = undefined;
        }
    }

    /**
     * Get the last reconciliation report.
     */
    getLastReport(): ReconciliationReport | undefined {
        return this.#lastReport;
    }

    /**
     * Run reconciliation now (manual trigger).
     */
    async runReconciliation(): Promise<ReconciliationReport> {
        try {
            const paperPositions = await this.#getPaperPositions();
            this.#lastReport = await this.#reconciler.reconcile(paperPositions);

            if (!this.#lastReport.isHealthy) {
                console.warn(
                    "[Reconciler] Mismatch detected:",
                    this.#reconciler.formatReport(this.#lastReport)
                );
            }

            return this.#lastReport;
        } catch (error) {
            console.error("[Reconciler] Reconciliation failed:", error);
            throw error;
        }
    }
}
