/**
 * Reconciliation Report Types
 *
 * Defines the structure of position reconciliation results.
 */

// ============================================================================
// Position Types
// ============================================================================

export interface PaperPosition {
    readonly symbol: string;
    readonly quantity: number;      // Positive = LONG, Negative = SHORT
    readonly avgEntryPrice: number;
    readonly unrealizedPnl: number;
}

export interface ExchangePositionSnapshot {
    readonly symbol: string;
    readonly side: "LONG" | "SHORT" | "FLAT";
    readonly quantity: number;      // Always positive
    readonly entryPrice: number;
    readonly markPrice: number;
    readonly unrealizedPnl: number;
    readonly leverage: number;
    readonly margin: number;
}

// ============================================================================
// Match Types
// ============================================================================

export interface PositionMatch {
    readonly symbol: string;
    readonly paperQty: number;
    readonly exchangeQty: number;
    readonly qtyDiff: number;
    readonly qtyDiffPct: number;
    readonly pnlDiff: number;
}

export type MismatchSeverity = "INFO" | "WARNING" | "CRITICAL";

export interface PositionMismatch extends PositionMatch {
    readonly severity: MismatchSeverity;
    readonly reason: string;
}

// ============================================================================
// Report Types
// ============================================================================

export interface ReconciliationReport {
    readonly timestamp: number;
    readonly exchange: string;
    readonly testnet: boolean;

    // Matching positions (within tolerance)
    readonly matches: readonly PositionMatch[];

    // Mismatching positions (outside tolerance)
    readonly mismatches: readonly PositionMismatch[];

    // Positions on exchange but not in paper (unexpected)
    readonly orphanedExchange: readonly ExchangePositionSnapshot[];

    // Positions in paper but not on exchange (missing)
    readonly orphanedPaper: readonly PaperPosition[];

    // Summary
    readonly isHealthy: boolean;
    readonly totalPaperPositions: number;
    readonly totalExchangePositions: number;
    readonly totalMismatches: number;
    readonly worstMismatchPct: number;
}

// ============================================================================
// Report Builder
// ============================================================================

export function createReconciliationReport(params: {
    exchange: string;
    testnet: boolean;
    matches: PositionMatch[];
    mismatches: PositionMismatch[];
    orphanedExchange: ExchangePositionSnapshot[];
    orphanedPaper: PaperPosition[];
}): ReconciliationReport {
    const isHealthy =
        params.mismatches.length === 0 &&
        params.orphanedExchange.length === 0 &&
        params.orphanedPaper.length === 0;

    const worstMismatchPct = params.mismatches.length > 0
        ? Math.max(...params.mismatches.map(m => m.qtyDiffPct))
        : 0;

    return {
        timestamp: Date.now(),
        exchange: params.exchange,
        testnet: params.testnet,
        matches: params.matches,
        mismatches: params.mismatches,
        orphanedExchange: params.orphanedExchange,
        orphanedPaper: params.orphanedPaper,
        isHealthy,
        totalPaperPositions: params.matches.length + params.mismatches.length + params.orphanedPaper.length,
        totalExchangePositions: params.matches.length + params.mismatches.length + params.orphanedExchange.length,
        totalMismatches: params.mismatches.length,
        worstMismatchPct
    };
}

// ============================================================================
// Report Formatting
// ============================================================================

export function formatReportSummary(report: ReconciliationReport): string {
    const lines: string[] = [
        `=== Reconciliation Report ===`,
        `Exchange: ${report.exchange} (${report.testnet ? "testnet" : "production"})`,
        `Time: ${new Date(report.timestamp).toISOString()}`,
        `Status: ${report.isHealthy ? "HEALTHY" : "MISMATCH DETECTED"}`,
        ``,
        `Positions:`,
        `  Paper: ${report.totalPaperPositions}`,
        `  Exchange: ${report.totalExchangePositions}`,
        `  Matches: ${report.matches.length}`,
        `  Mismatches: ${report.mismatches.length}`,
        `  Orphaned (exchange): ${report.orphanedExchange.length}`,
        `  Orphaned (paper): ${report.orphanedPaper.length}`
    ];

    if (report.mismatches.length > 0) {
        lines.push(``);
        lines.push(`Mismatches:`);
        for (const m of report.mismatches) {
            lines.push(
                `  ${m.symbol}: paper=${m.paperQty}, exchange=${m.exchangeQty}, ` +
                `diff=${m.qtyDiffPct.toFixed(2)}% [${m.severity}] - ${m.reason}`
            );
        }
    }

    if (report.orphanedExchange.length > 0) {
        lines.push(``);
        lines.push(`Orphaned on Exchange (unexpected):`);
        for (const p of report.orphanedExchange) {
            lines.push(
                `  ${p.symbol}: ${p.side} ${p.quantity} @ ${p.entryPrice}`
            );
        }
    }

    if (report.orphanedPaper.length > 0) {
        lines.push(``);
        lines.push(`Orphaned in Paper (missing on exchange):`);
        for (const p of report.orphanedPaper) {
            lines.push(
                `  ${p.symbol}: ${p.quantity > 0 ? "LONG" : "SHORT"} ${Math.abs(p.quantity)} @ ${p.avgEntryPrice}`
            );
        }
    }

    return lines.join("\n");
}
