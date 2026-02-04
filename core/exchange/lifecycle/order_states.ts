/**
 * Order Lifecycle States and Types
 *
 * Defines the state machine for order lifecycle tracking
 * from intent creation through execution completion.
 */

// ============================================================================
// Lifecycle States
// ============================================================================

export type OrderLifecycleState =
    | "INTENT_CREATED"      // LiveExecutionIntent created from paper execution
    | "GATE_PASSED"         // Passed gateToLive() safety checks
    | "SUBMITTING"          // Submission to exchange in progress
    | "SUBMITTED"           // Exchange confirmed receipt
    | "PARTIALLY_FILLED"    // Some quantity filled
    | "FILLED"              // Completely filled
    | "CANCELLED"           // Cancelled by user or system
    | "REJECTED"            // Rejected by exchange
    | "FAILED"              // Submission error (network, auth, etc.)
    | "EXPIRED";            // Time-in-force expired

/**
 * Terminal states - no further transitions allowed.
 */
export const TERMINAL_STATES: readonly OrderLifecycleState[] = [
    "FILLED",
    "CANCELLED",
    "REJECTED",
    "FAILED",
    "EXPIRED"
];

/**
 * Valid state transitions.
 */
export const STATE_TRANSITIONS: Readonly<Record<OrderLifecycleState, readonly OrderLifecycleState[]>> = {
    "INTENT_CREATED": ["GATE_PASSED", "CANCELLED"],
    "GATE_PASSED": ["SUBMITTING", "CANCELLED"],
    "SUBMITTING": ["SUBMITTED", "FAILED", "REJECTED"],
    "SUBMITTED": ["PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "EXPIRED"],
    "PARTIALLY_FILLED": ["PARTIALLY_FILLED", "FILLED", "CANCELLED"],
    "FILLED": [],
    "CANCELLED": [],
    "REJECTED": [],
    "FAILED": [],
    "EXPIRED": []
};

// ============================================================================
// Interfaces
// ============================================================================

export interface Fill {
    readonly fillId: string;          // Unique fill ID
    readonly quantity: number;         // Filled quantity
    readonly price: number;            // Fill price
    readonly fee: number;              // Fee amount
    readonly feeAsset: string;         // Fee asset (e.g., "USDT")
    readonly timestamp: number;        // Fill timestamp (Unix ms)
}

export interface OrderLifecycleEntry {
    readonly bridgeId: string;                    // Unique bridge ID (from gateToLive)
    readonly sourceExecutionId: string;           // Paper execution ID
    readonly state: OrderLifecycleState;
    readonly exchangeOrderId?: string;            // Set after submission
    readonly clientOrderId?: string;              // Our order ID sent to exchange
    readonly symbol: string;
    readonly side: "BUY" | "SELL";
    readonly requestedQty: number;
    readonly filledQty: number;
    readonly avgFillPrice?: number;               // Weighted average of fills
    readonly fills: readonly Fill[];
    readonly createdAt: number;                   // Entry creation time
    readonly updatedAt: number;                   // Last update time
    readonly error?: string;                      // Error message if failed/rejected
    readonly metadata?: Readonly<Record<string, unknown>>;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if a state is terminal (no further transitions).
 */
export function isTerminalState(state: OrderLifecycleState): boolean {
    return TERMINAL_STATES.includes(state);
}

/**
 * Check if a state transition is valid.
 */
export function isValidTransition(from: OrderLifecycleState, to: OrderLifecycleState): boolean {
    return STATE_TRANSITIONS[from]?.includes(to) ?? false;
}

/**
 * Get all valid next states from current state.
 */
export function getValidNextStates(state: OrderLifecycleState): readonly OrderLifecycleState[] {
    return STATE_TRANSITIONS[state] ?? [];
}

/**
 * Create a new lifecycle entry from LiveExecutionIntent.
 */
export function createLifecycleEntry(
    bridgeId: string,
    sourceExecutionId: string,
    symbol: string,
    side: "BUY" | "SELL",
    requestedQty: number
): OrderLifecycleEntry {
    const now = Date.now();
    return {
        bridgeId,
        sourceExecutionId,
        state: "INTENT_CREATED",
        symbol,
        side,
        requestedQty,
        filledQty: 0,
        fills: [],
        createdAt: now,
        updatedAt: now
    };
}

/**
 * Calculate weighted average fill price from fills.
 */
export function calculateAvgFillPrice(fills: readonly Fill[]): number {
    if (fills.length === 0) return 0;

    const totalNotional = fills.reduce((sum, f) => sum + f.quantity * f.price, 0);
    const totalQty = fills.reduce((sum, f) => sum + f.quantity, 0);

    return totalQty > 0 ? totalNotional / totalQty : 0;
}

/**
 * Calculate total fees from fills.
 */
export function calculateTotalFees(fills: readonly Fill[]): number {
    return fills.reduce((sum, f) => sum + f.fee, 0);
}

/**
 * Check if order is fully filled.
 */
export function isFullyFilled(entry: OrderLifecycleEntry): boolean {
    return entry.filledQty >= entry.requestedQty;
}

/**
 * Get fill percentage.
 */
export function getFillPercentage(entry: OrderLifecycleEntry): number {
    if (entry.requestedQty === 0) return 0;
    return Math.min(100, (entry.filledQty / entry.requestedQty) * 100);
}
