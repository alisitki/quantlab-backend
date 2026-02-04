/**
 * Execution Bridge
 *
 * Main orchestrator for bridging paper executions to live exchange orders.
 * Integrates gate logic, lifecycle management, and reconciliation.
 *
 * Flow:
 * 1. Receive PaperExecutionResult
 * 2. Check kill switch
 * 3. Run gateToLive() safety checks
 * 4. Create lifecycle entry
 * 5. Submit to exchange (if not SHADOW)
 * 6. Track fill and update lifecycle
 * 7. Update daily limits
 */

import { EventEmitter } from "node:events";
import { ExchangeAdapter, SubmitOrderParams } from "../adapters/base/ExchangeAdapter.js";
import { OrderLifecycleManager } from "../lifecycle/OrderLifecycleManager.js";
import { PositionReconciler, ReconciliationScheduler } from "../reconciliation/PositionReconciler.js";
import { PaperPosition } from "../reconciliation/ReconciliationReport.js";
import { PaperExecutionResult } from "../../paper/paper_execution_result.js";
import { gateToLive } from "../../bridge/gate.js";
import { BridgeConfig, BridgeLimitsState } from "../../bridge/bridge_config.js";
import { LiveExecutionIntent } from "../../bridge/live_execution_intent.js";
import { loadKillSwitchFromEnv } from "../../futures/kill_switch.js";
import {
    ExecutionBridgeConfig,
    loadExecutionConfigFromEnv,
    validateExecutionConfig,
    BridgeMode
} from "./ExecutionConfig.js";

// ============================================================================
// Types
// ============================================================================

export type BridgeExecutionStatus =
    | "GATED"           // Failed gate checks
    | "SHADOW"          // Shadow mode - not submitted
    | "SUBMITTING"      // Submission in progress
    | "SUBMITTED"       // Submitted to exchange
    | "FILLED"          // Order filled
    | "REJECTED"        // Exchange rejected
    | "FAILED"          // Submission failed
    | "KILLED";         // Kill switch active

export interface BridgeExecutionResult {
    readonly bridgeId: string;
    readonly status: BridgeExecutionStatus;
    readonly intent?: LiveExecutionIntent;
    readonly exchangeOrderId?: string;
    readonly filledQty?: number;
    readonly avgFillPrice?: number;
    readonly slippageBps?: number;
    readonly latencyMs?: number;
    readonly error?: string;
    readonly timestamp: number;
}

export interface BridgeStats {
    readonly mode: BridgeMode;
    readonly exchange: string;
    readonly testnet: boolean;
    readonly ordersToday: number;
    readonly notionalToday: number;
    readonly maxOrdersPerDay: number;
    readonly maxNotionalPerDay: number;
    readonly isKillSwitchActive: boolean;
    readonly lastReconciliationHealthy?: boolean;
}

// ============================================================================
// Execution Bridge
// ============================================================================

export class ExecutionBridge extends EventEmitter {
    readonly #adapter: ExchangeAdapter;
    readonly #lifecycleManager: OrderLifecycleManager;
    readonly #config: ExecutionBridgeConfig;
    readonly #reconciler?: PositionReconciler;
    #reconciliationScheduler?: ReconciliationScheduler;

    // Daily limits state (resets at midnight UTC)
    #limitsState: BridgeLimitsState;
    #limitsResetDate: string;

    constructor(
        adapter: ExchangeAdapter,
        lifecycleManager: OrderLifecycleManager,
        config?: Partial<ExecutionBridgeConfig>
    ) {
        super();
        this.#adapter = adapter;
        this.#lifecycleManager = lifecycleManager;

        // Load config from env, override with provided config
        const envConfig = loadExecutionConfigFromEnv();
        this.#config = { ...envConfig, ...config };

        // Validate config
        const validation = validateExecutionConfig(this.#config);
        if (!validation.valid) {
            throw new Error(`Invalid bridge config: ${validation.errors.join(", ")}`);
        }
        for (const warning of validation.warnings) {
            console.warn(`[Bridge] Config warning: ${warning}`);
        }

        // Initialize limits
        this.#limitsState = { current_order_count: 0, current_notional_usd: 0 };
        this.#limitsResetDate = this.getTodayUTC();

        // Create reconciler
        this.#reconciler = new PositionReconciler(adapter, {
            tolerancePct: 0.01,
            enableAlerts: true,
            symbols: this.#config.allowedSymbols as string[]
        });
    }

    /**
     * Initialize the bridge.
     */
    async init(): Promise<void> {
        await this.#lifecycleManager.init();
        console.log(`[Bridge] Initialized in ${this.#config.mode} mode for ${this.#config.exchange}`);
    }

    /**
     * Execute a paper execution result through the bridge.
     */
    async execute(paperResult: PaperExecutionResult): Promise<BridgeExecutionResult> {
        const startTime = Date.now();

        // Reset daily limits if needed
        this.checkAndResetDailyLimits();

        // STEP 1: Kill Switch Check (highest priority)
        const killSwitch = loadKillSwitchFromEnv();
        if (killSwitch.global_kill) {
            return this.createResult("KILLED", {
                error: `Kill switch active: ${killSwitch.reason}`
            });
        }

        // Symbol-level kill switch
        if (killSwitch.symbol_kill[paperResult.symbol]) {
            return this.createResult("KILLED", {
                error: `Symbol kill switch active for ${paperResult.symbol}`
            });
        }

        // STEP 2: Gate Check
        const bridgeConfig = this.buildBridgeConfig();
        const intent = gateToLive(paperResult, bridgeConfig, this.#limitsState, Date.now());

        if (!intent) {
            await this.logAudit("GATE_REJECTED", paperResult.execution_id, {
                reason: "Gate check failed",
                limits: this.#limitsState
            });
            return this.createResult("GATED", {
                error: "Gate check failed - check limits or allowed symbols"
            });
        }

        // STEP 3: Create lifecycle entry
        const lifecycleEntry = await this.#lifecycleManager.createFromIntent(intent);
        await this.#lifecycleManager.transition(intent.bridge_id, "GATE_PASSED");

        // STEP 4: Shadow mode - don't submit
        if (this.#config.mode === "SHADOW") {
            await this.logAudit("SHADOW_EXECUTION", intent.bridge_id, {
                symbol: intent.symbol,
                side: intent.side,
                qty: intent.quantity,
                price: intent.price
            });

            return this.createResult("SHADOW", {
                bridgeId: intent.bridge_id,
                intent
            });
        }

        // STEP 5: Submit to exchange
        await this.#lifecycleManager.transition(intent.bridge_id, "SUBMITTING");

        try {
            const orderParams: SubmitOrderParams = {
                clientOrderId: intent.bridge_id,
                symbol: intent.symbol,
                side: intent.side,
                orderType: "MARKET",
                quantity: intent.quantity,
                timeInForce: "IOC",
                reduceOnly: this.#config.reduceOnly
            };

            const exchangeOrder = await this.#adapter.submitOrder(orderParams);
            const latencyMs = Date.now() - startTime;

            // Update lifecycle
            await this.#lifecycleManager.transition(intent.bridge_id, "SUBMITTED", {
                exchangeOrderId: exchangeOrder.exchangeOrderId
            });

            // STEP 6: Handle fill
            if (exchangeOrder.filledQty > 0) {
                await this.#lifecycleManager.addFill(intent.bridge_id, {
                    fillId: exchangeOrder.exchangeOrderId,
                    quantity: exchangeOrder.filledQty,
                    price: exchangeOrder.avgFillPrice,
                    fee: exchangeOrder.fees.reduce((sum, f) => sum + f.amount, 0),
                    feeAsset: exchangeOrder.fees[0]?.asset || "USDT",
                    timestamp: Date.now()
                });
            }

            // STEP 7: Update limits
            this.#limitsState = {
                current_order_count: this.#limitsState.current_order_count + 1,
                current_notional_usd: this.#limitsState.current_notional_usd +
                    (exchangeOrder.filledQty * exchangeOrder.avgFillPrice)
            };

            // Calculate slippage
            const slippageBps = intent.price > 0
                ? Math.round(((exchangeOrder.avgFillPrice - intent.price) / intent.price) * 10000)
                : 0;

            const finalStatus: BridgeExecutionStatus =
                exchangeOrder.filledQty >= intent.quantity ? "FILLED" : "SUBMITTED";

            this.emit("execution", {
                bridgeId: intent.bridge_id,
                status: finalStatus,
                exchangeOrderId: exchangeOrder.exchangeOrderId,
                filledQty: exchangeOrder.filledQty,
                avgFillPrice: exchangeOrder.avgFillPrice,
                slippageBps,
                latencyMs
            });

            return this.createResult(finalStatus, {
                bridgeId: intent.bridge_id,
                intent,
                exchangeOrderId: exchangeOrder.exchangeOrderId,
                filledQty: exchangeOrder.filledQty,
                avgFillPrice: exchangeOrder.avgFillPrice,
                slippageBps,
                latencyMs
            });

        } catch (error) {
            const errorMsg = error instanceof Error ? error.message : String(error);

            await this.#lifecycleManager.transition(intent.bridge_id, "FAILED", {
                error: errorMsg
            });

            return this.createResult("FAILED", {
                bridgeId: intent.bridge_id,
                intent,
                error: errorMsg,
                latencyMs: Date.now() - startTime
            });
        }
    }

    /**
     * Start position reconciliation.
     */
    startReconciliation(getPaperPositions: () => PaperPosition[] | Promise<PaperPosition[]>): void {
        if (!this.#reconciler) return;

        this.#reconciliationScheduler = new ReconciliationScheduler(
            this.#reconciler,
            getPaperPositions,
            this.#config.reconciliationIntervalMs
        );
        this.#reconciliationScheduler.start();
    }

    /**
     * Stop position reconciliation.
     */
    stopReconciliation(): void {
        this.#reconciliationScheduler?.stop();
    }

    /**
     * Get current bridge stats.
     */
    getStats(): BridgeStats {
        const killSwitch = loadKillSwitchFromEnv();
        const lastReport = this.#reconciliationScheduler?.getLastReport();

        return {
            mode: this.#config.mode,
            exchange: this.#config.exchange,
            testnet: this.#config.testnet,
            ordersToday: this.#limitsState.current_order_count,
            notionalToday: this.#limitsState.current_notional_usd,
            maxOrdersPerDay: this.#config.maxOrdersPerDay,
            maxNotionalPerDay: this.#config.maxNotionalPerDay,
            isKillSwitchActive: killSwitch.global_kill,
            lastReconciliationHealthy: lastReport?.isHealthy
        };
    }

    /**
     * Get the current config.
     */
    getConfig(): ExecutionBridgeConfig {
        return { ...this.#config };
    }

    /**
     * Reset daily limits (manual).
     */
    resetDailyLimits(): void {
        this.#limitsState = { current_order_count: 0, current_notional_usd: 0 };
        this.#limitsResetDate = this.getTodayUTC();
        console.log("[Bridge] Daily limits reset");
    }

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    private buildBridgeConfig(): BridgeConfig {
        return {
            live_enabled: this.#config.mode !== "SHADOW",
            allowed_symbols: this.#config.allowedSymbols as string[],
            max_orders_per_day: this.#config.maxOrdersPerDay,
            max_notional_per_day: this.#config.maxNotionalPerDay,
            // Safety: Force CANARY even if config says LIVE
            mode: this.#config.mode === "LIVE" ? "CANARY" : this.#config.mode === "SHADOW" ? "PAPER_ONLY" : "CANARY"
        };
    }

    private checkAndResetDailyLimits(): void {
        const today = this.getTodayUTC();
        if (today !== this.#limitsResetDate) {
            this.#limitsState = { current_order_count: 0, current_notional_usd: 0 };
            this.#limitsResetDate = today;
            console.log("[Bridge] Daily limits auto-reset for new day");
        }
    }

    private getTodayUTC(): string {
        return new Date().toISOString().split("T")[0];
    }

    private createResult(
        status: BridgeExecutionStatus,
        data: Partial<BridgeExecutionResult> = {}
    ): BridgeExecutionResult {
        return {
            bridgeId: data.bridgeId || "",
            status,
            intent: data.intent,
            exchangeOrderId: data.exchangeOrderId,
            filledQty: data.filledQty,
            avgFillPrice: data.avgFillPrice,
            slippageBps: data.slippageBps,
            latencyMs: data.latencyMs,
            error: data.error,
            timestamp: Date.now()
        };
    }

    private async logAudit(
        action: string,
        targetId: string,
        metadata: Record<string, unknown>
    ): Promise<void> {
        try {
            const { emitAudit } = await import("../../audit/AuditWriter.js");
            await emitAudit({
                actor: "execution_bridge",
                action,
                target_type: "bridge_order",
                target_id: targetId,
                metadata
            });
        } catch {
            // Audit failure should not break execution
        }
    }
}
