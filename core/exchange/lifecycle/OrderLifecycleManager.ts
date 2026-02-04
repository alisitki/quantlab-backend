/**
 * Order Lifecycle Manager
 *
 * Manages order state transitions with validation, persistence, and audit logging.
 * Ensures all state changes are tracked and alerting is triggered on failures.
 */

import { EventEmitter } from "node:events";
import {
    OrderLifecycleEntry,
    OrderLifecycleState,
    Fill,
    isValidTransition,
    isTerminalState,
    createLifecycleEntry,
    calculateAvgFillPrice
} from "./order_states.js";
import { OrderStateStore } from "./OrderStateStore.js";
import { LiveExecutionIntent } from "../../bridge/live_execution_intent.js";

// ============================================================================
// Types
// ============================================================================

export interface StateChangeEvent {
    readonly entry: OrderLifecycleEntry;
    readonly previousState: OrderLifecycleState | null;
    readonly timestamp: number;
}

export interface LifecycleManagerConfig {
    readonly storeDir?: string;
    readonly enableAudit?: boolean;
    readonly enableAlerts?: boolean;
}

// ============================================================================
// Order Lifecycle Manager
// ============================================================================

export class OrderLifecycleManager extends EventEmitter {
    readonly #store: OrderStateStore;
    readonly #enableAudit: boolean;
    readonly #enableAlerts: boolean;

    constructor(config: LifecycleManagerConfig = {}) {
        super();
        this.#store = new OrderStateStore(config.storeDir);
        this.#enableAudit = config.enableAudit ?? true;
        this.#enableAlerts = config.enableAlerts ?? true;
    }

    /**
     * Initialize the manager and underlying store.
     */
    async init(): Promise<void> {
        await this.#store.init();
    }

    /**
     * Create a new lifecycle entry from a LiveExecutionIntent.
     */
    async createFromIntent(intent: LiveExecutionIntent): Promise<OrderLifecycleEntry> {
        const entry = createLifecycleEntry(
            intent.bridge_id,
            intent.source_execution_id,
            intent.symbol,
            intent.side,
            intent.quantity
        );

        await this.#store.save(entry);
        await this.emitStateChange(entry, null);

        return entry;
    }

    /**
     * Transition an order to a new state.
     */
    async transition(
        bridgeId: string,
        newState: OrderLifecycleState,
        metadata?: Partial<Pick<OrderLifecycleEntry, "exchangeOrderId" | "clientOrderId" | "error" | "metadata">>
    ): Promise<OrderLifecycleEntry> {
        const current = await this.#store.get(bridgeId);

        if (!current) {
            throw new Error(`Order not found: ${bridgeId}`);
        }

        // Validate transition
        if (!isValidTransition(current.state, newState)) {
            throw new Error(
                `Invalid state transition: ${current.state} -> ${newState} for order ${bridgeId}`
            );
        }

        const updated: OrderLifecycleEntry = {
            ...current,
            state: newState,
            updatedAt: Date.now(),
            ...(metadata?.exchangeOrderId && { exchangeOrderId: metadata.exchangeOrderId }),
            ...(metadata?.clientOrderId && { clientOrderId: metadata.clientOrderId }),
            ...(metadata?.error && { error: metadata.error }),
            ...(metadata?.metadata && {
                metadata: { ...current.metadata, ...metadata.metadata }
            })
        };

        await this.#store.save(updated);
        await this.emitStateChange(updated, current.state);

        // Alert on terminal failure states
        if (this.#enableAlerts && (newState === "REJECTED" || newState === "FAILED")) {
            await this.sendFailureAlert(updated);
        }

        return updated;
    }

    /**
     * Add a fill to an order.
     * Automatically transitions to PARTIALLY_FILLED or FILLED.
     */
    async addFill(bridgeId: string, fill: Fill): Promise<OrderLifecycleEntry> {
        const current = await this.#store.get(bridgeId);

        if (!current) {
            throw new Error(`Order not found: ${bridgeId}`);
        }

        // Can only add fills in SUBMITTED or PARTIALLY_FILLED state
        if (current.state !== "SUBMITTED" && current.state !== "PARTIALLY_FILLED") {
            throw new Error(
                `Cannot add fill in state ${current.state} for order ${bridgeId}`
            );
        }

        const newFilledQty = current.filledQty + fill.quantity;
        const newFills = [...current.fills, fill];
        const avgFillPrice = calculateAvgFillPrice(newFills);

        // Determine new state
        const newState: OrderLifecycleState =
            newFilledQty >= current.requestedQty ? "FILLED" : "PARTIALLY_FILLED";

        const updated: OrderLifecycleEntry = {
            ...current,
            state: newState,
            filledQty: newFilledQty,
            avgFillPrice,
            fills: newFills,
            updatedAt: Date.now()
        };

        await this.#store.save(updated);
        await this.emitStateChange(updated, current.state);

        return updated;
    }

    /**
     * Get an order by bridge ID.
     */
    async get(bridgeId: string): Promise<OrderLifecycleEntry | null> {
        return this.#store.get(bridgeId);
    }

    /**
     * Get all orders.
     */
    getAll(): OrderLifecycleEntry[] {
        return this.#store.getAll();
    }

    /**
     * Get orders by state.
     */
    getByState(state: OrderLifecycleState): OrderLifecycleEntry[] {
        return this.#store.getByState(state);
    }

    /**
     * Get orders by symbol.
     */
    getBySymbol(symbol: string): OrderLifecycleEntry[] {
        return this.#store.getBySymbol(symbol);
    }

    /**
     * Get active (non-terminal) orders.
     */
    getActiveOrders(): OrderLifecycleEntry[] {
        return this.getAll().filter(e => !isTerminalState(e.state));
    }

    /**
     * Get orders created today.
     */
    getTodayOrders(): OrderLifecycleEntry[] {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const startOfDay = today.getTime();
        const endOfDay = startOfDay + 24 * 60 * 60 * 1000;

        return this.#store.getByTimeRange(startOfDay, endOfDay);
    }

    /**
     * Get state counts for metrics.
     */
    getStateCounts(): Record<string, number> {
        return this.#store.getStateCounts();
    }

    /**
     * Cleanup old terminal orders.
     */
    async cleanup(olderThanDays: number = 30): Promise<number> {
        return this.#store.cleanupOldEntries(olderThanDays);
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private async emitStateChange(
        entry: OrderLifecycleEntry,
        previousState: OrderLifecycleState | null
    ): Promise<void> {
        const event: StateChangeEvent = {
            entry,
            previousState,
            timestamp: Date.now()
        };

        this.emit("stateChange", event);

        // Audit logging
        if (this.#enableAudit) {
            await this.writeAuditLog(entry, previousState);
        }
    }

    private async writeAuditLog(
        entry: OrderLifecycleEntry,
        previousState: OrderLifecycleState | null
    ): Promise<void> {
        try {
            // Dynamic import to avoid circular dependency
            const { emitAudit } = await import("../../audit/AuditWriter.js");

            await emitAudit({
                actor: "lifecycle_manager",
                action: "ORDER_STATE_CHANGE",
                target_type: "bridge_order",
                target_id: entry.bridgeId,
                metadata: {
                    from_state: previousState,
                    to_state: entry.state,
                    symbol: entry.symbol,
                    side: entry.side,
                    requested_qty: entry.requestedQty,
                    filled_qty: entry.filledQty,
                    exchange_order_id: entry.exchangeOrderId,
                    error: entry.error
                }
            });
        } catch {
            // Audit write failure should not break order flow
            console.error(`[LifecycleManager] Audit write failed for ${entry.bridgeId}`);
        }
    }

    private async sendFailureAlert(entry: OrderLifecycleEntry): Promise<void> {
        try {
            const { sendAlert, AlertType, AlertSeverity } = await import("../../alerts/index.js");

            await sendAlert({
                type: AlertType.RISK_REJECTION,
                severity: entry.state === "FAILED" ? AlertSeverity.ERROR : AlertSeverity.WARNING,
                message: `Bridge order ${entry.state}: ${entry.bridgeId}`,
                metadata: {
                    bridge_id: entry.bridgeId,
                    symbol: entry.symbol,
                    side: entry.side,
                    error: entry.error,
                    requested_qty: entry.requestedQty
                }
            });
        } catch {
            // Alert failure should not break order flow
            console.error(`[LifecycleManager] Alert failed for ${entry.bridgeId}`);
        }
    }
}
