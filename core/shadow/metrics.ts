import { ExecutionResult, ReasonCode } from "../events/execution_event";

export interface ShadowMetrics {
    total_decisions: number;
    would_execute_count: number;
    rejected_count: number;
    skipped_count: number;
    by_reason_code: Record<ReasonCode, number>;
}

/**
 * Aggregates ExecutionResults into ShadowMetrics.
 * Pure function: takes current metrics and a new result, returns updated metrics.
 */
export function aggregateMetrics(
    current: ShadowMetrics,
    result: ExecutionResult
): ShadowMetrics {
    const updated = { ...current };

    updated.total_decisions++;

    if (result.outcome === "WOULD_EXECUTE") {
        updated.would_execute_count++;
    } else if (result.outcome === "REJECTED") {
        updated.rejected_count++;
    } else if (result.outcome === "SKIPPED") {
        updated.skipped_count++;
    }

    // Reason code breakdown
    updated.by_reason_code[result.reason_code] = (updated.by_reason_code[result.reason_code] || 0) + 1;

    return updated;
}

export function createInitialMetrics(): ShadowMetrics {
    return {
        total_decisions: 0,
        would_execute_count: 0,
        rejected_count: 0,
        skipped_count: 0,
        by_reason_code: {} as Record<ReasonCode, number>
    };
}
