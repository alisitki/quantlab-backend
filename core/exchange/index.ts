/**
 * Exchange Module - Public Exports
 *
 * Central export point for the exchange execution bridge module.
 */

// Base adapter types
export {
    ExchangeAdapter,
    SubmitOrderParams,
    ExchangeOrder,
    ExchangePosition,
    AccountBalance,
    ExchangeInfo,
    SymbolInfo,
    OrderStatus,
    OrderSide,
    OrderType,
    TimeInForce,
    PositionSide,
    OrderFee
} from "./adapters/base/ExchangeAdapter.js";

export {
    ExchangeCredentials,
    loadBinanceCredentials,
    loadBybitCredentials,
    loadOkxCredentials,
    loadCredentialsForExchange,
    validateCredentials,
    maskCredentials
} from "./adapters/base/ExchangeCredentials.js";

export {
    ExchangeError,
    ExchangeErrorCode,
    ExchangeErrorDetail,
    createExchangeError,
    isRetryableError
} from "./adapters/base/ExchangeError.js";

// Binance adapter
export { BinanceFuturesAdapter } from "./adapters/binance/BinanceFuturesAdapter.js";
export { BinanceSigner } from "./adapters/binance/BinanceSigner.js";

// Bybit adapter
export { BybitFuturesAdapter } from "./adapters/bybit/BybitFuturesAdapter.js";
export { BybitSigner } from "./adapters/bybit/BybitSigner.js";

// OKX adapter
export { OkxFuturesAdapter } from "./adapters/okx/OkxFuturesAdapter.js";
export { OkxSigner } from "./adapters/okx/OkxSigner.js";

// Lifecycle management
export {
    OrderLifecycleState,
    OrderLifecycleEntry,
    Fill,
    TERMINAL_STATES,
    STATE_TRANSITIONS,
    isTerminalState,
    isValidTransition,
    createLifecycleEntry,
    calculateAvgFillPrice,
    calculateTotalFees
} from "./lifecycle/order_states.js";

export {
    OrderLifecycleManager,
    StateChangeEvent,
    LifecycleManagerConfig
} from "./lifecycle/OrderLifecycleManager.js";

export { OrderStateStore } from "./lifecycle/OrderStateStore.js";

// Reconciliation
export {
    PositionReconciler,
    ReconcilerConfig,
    ReconciliationScheduler
} from "./reconciliation/PositionReconciler.js";

export {
    PaperPosition,
    ExchangePositionSnapshot,
    PositionMatch,
    PositionMismatch,
    MismatchSeverity,
    ReconciliationReport,
    createReconciliationReport,
    formatReportSummary
} from "./reconciliation/ReconciliationReport.js";

// Execution bridge
export {
    ExecutionBridge,
    BridgeExecutionResult,
    BridgeExecutionStatus,
    BridgeStats
} from "./execution/ExecutionBridge.js";

export {
    ExecutionBridgeConfig,
    BridgeMode,
    DEFAULT_EXECUTION_CONFIG,
    loadExecutionConfigFromEnv,
    validateExecutionConfig
} from "./execution/ExecutionConfig.js";

// Monitoring
export {
    ExchangeHealthMonitor,
    HealthStatus,
    HealthMonitorConfig
} from "./monitoring/ExchangeHealthMonitor.js";

export {
    SlippageAnalyzer,
    SlippageRecord,
    SlippageStats,
    SlippageAnalyzerConfig
} from "./monitoring/SlippageAnalyzer.js";
