/**
 * Execution Bridge Configuration
 *
 * Defines configuration for the exchange execution bridge.
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Bridge execution mode.
 * - SHADOW: Log what would be executed, don't submit to exchange
 * - CANARY: Limited symbols, reduce-only orders, strict limits
 * - LIVE: Full execution (structurally blocked in code)
 */
export type BridgeMode = "SHADOW" | "CANARY" | "LIVE";

export interface ExecutionBridgeConfig {
    /** Execution mode */
    readonly mode: BridgeMode;

    /** Exchange to use */
    readonly exchange: string;

    /** Use testnet */
    readonly testnet: boolean;

    /** Allowed symbols (for CANARY mode) */
    readonly allowedSymbols: readonly string[];

    /** Maximum orders per day */
    readonly maxOrdersPerDay: number;

    /** Maximum notional value per day (USD) */
    readonly maxNotionalPerDay: number;

    /** Maximum notional per single order (USD) */
    readonly maxNotionalPerOrder: number;

    /** Position reconciliation interval (ms) */
    readonly reconciliationIntervalMs: number;

    /** Force reduce-only orders */
    readonly reduceOnly: boolean;
}

// ============================================================================
// Defaults
// ============================================================================

export const DEFAULT_EXECUTION_CONFIG: ExecutionBridgeConfig = {
    mode: "SHADOW",
    exchange: "binance",
    testnet: true,
    allowedSymbols: ["BTCUSDT"],
    maxOrdersPerDay: 10,
    maxNotionalPerDay: 1000,
    maxNotionalPerOrder: 100,
    reconciliationIntervalMs: 60000,
    reduceOnly: true
};

// ============================================================================
// Config Loader
// ============================================================================

/**
 * Load execution config from environment variables.
 */
export function loadExecutionConfigFromEnv(): ExecutionBridgeConfig {
    const mode = (process.env.BRIDGE_MODE || "SHADOW") as BridgeMode;

    // Safety: Never allow LIVE mode from env, force to CANARY
    const safeMode: BridgeMode = mode === "LIVE" ? "CANARY" : mode;

    const allowedSymbolsRaw = process.env.BRIDGE_ALLOWED_SYMBOLS || "BTCUSDT";
    const allowedSymbols = allowedSymbolsRaw.split(",").map(s => s.trim().toUpperCase());

    return {
        mode: safeMode,
        exchange: process.env.BRIDGE_EXCHANGE || "binance",
        testnet: process.env.BINANCE_TESTNET !== "0",
        allowedSymbols,
        maxOrdersPerDay: parseInt(process.env.BRIDGE_MAX_ORDERS_PER_DAY || "10"),
        maxNotionalPerDay: parseFloat(process.env.BRIDGE_MAX_NOTIONAL_PER_DAY || "1000"),
        maxNotionalPerOrder: parseFloat(process.env.BRIDGE_MAX_NOTIONAL_PER_ORDER || "100"),
        reconciliationIntervalMs: parseInt(process.env.RECONCILIATION_INTERVAL_MS || "60000"),
        reduceOnly: process.env.BRIDGE_REDUCE_ONLY !== "0"
    };
}

// ============================================================================
// Config Validation
// ============================================================================

export interface ConfigValidationResult {
    readonly valid: boolean;
    readonly errors: readonly string[];
    readonly warnings: readonly string[];
}

export function validateExecutionConfig(config: ExecutionBridgeConfig): ConfigValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Mode validation
    if (config.mode === "LIVE") {
        errors.push("LIVE mode is not allowed - use CANARY for real orders");
    }

    // Symbol validation
    if (config.allowedSymbols.length === 0) {
        errors.push("At least one allowed symbol must be configured");
    }

    // Limit validation
    if (config.maxOrdersPerDay <= 0) {
        errors.push("maxOrdersPerDay must be positive");
    }
    if (config.maxNotionalPerDay <= 0) {
        errors.push("maxNotionalPerDay must be positive");
    }
    if (config.maxNotionalPerOrder <= 0) {
        errors.push("maxNotionalPerOrder must be positive");
    }
    if (config.maxNotionalPerOrder > config.maxNotionalPerDay) {
        warnings.push("maxNotionalPerOrder > maxNotionalPerDay");
    }

    // Safety warnings
    if (!config.testnet && config.mode !== "SHADOW") {
        warnings.push("Running on production exchange - ensure this is intended");
    }
    if (!config.reduceOnly && config.mode === "CANARY") {
        warnings.push("reduceOnly is disabled in CANARY mode - this allows position increases");
    }

    return {
        valid: errors.length === 0,
        errors,
        warnings
    };
}
