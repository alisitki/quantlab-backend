/**
 * Exchange Bridge Routes for strategyd
 *
 * POST /v1/bridge/start     - Start the execution bridge
 * POST /v1/bridge/stop      - Stop the execution bridge
 * GET  /v1/bridge/status    - Get bridge status and stats
 * GET  /v1/bridge/orders    - Get recent bridge orders
 * GET  /v1/bridge/orders/:id - Get specific order details
 * POST /v1/bridge/reconcile - Force position reconciliation
 * GET  /v1/bridge/health    - Get exchange health status
 * GET  /v1/bridge/slippage  - Get slippage statistics
 */

import {
  ExecutionBridge,
  BinanceFuturesAdapter,
  BybitFuturesAdapter,
  OkxFuturesAdapter,
  OrderLifecycleManager,
  ExchangeHealthMonitor,
  SlippageAnalyzer,
  loadCredentialsForExchange,
  validateCredentials,
  maskCredentials,
  loadExecutionConfigFromEnv,
  validateExecutionConfig
} from '../../../core/exchange/index.js';
import { emitAudit } from '../../../core/audit/AuditWriter.js';

const SUPPORTED_EXCHANGES = ['binance', 'bybit', 'okx'];

/**
 * Create exchange adapter based on exchange name.
 */
function createAdapter(exchange, credentials) {
  switch (exchange.toLowerCase()) {
    case 'binance':
      return new BinanceFuturesAdapter(credentials);
    case 'bybit':
      return new BybitFuturesAdapter(credentials);
    case 'okx':
      return new OkxFuturesAdapter(credentials);
    default:
      throw new Error(`Unsupported exchange: ${exchange}`);
  }
}

// Singleton instances
let bridge = null;
let healthMonitor = null;
let slippageAnalyzer = null;
let lifecycleManager = null;
let adapter = null;

// Paper positions provider (to be set externally)
let paperPositionsProvider = null;

/**
 * Set the paper positions provider function.
 * This should be called by the main app to connect bridge with paper execution state.
 */
export function setPaperPositionsProvider(provider) {
  paperPositionsProvider = provider;
}

/**
 * Get bridge singleton instances for monitoring.
 * Used by monitor.routes.js to access bridge state.
 */
export function getBridgeSingletons() {
  return { bridge, healthMonitor, slippageAnalyzer, lifecycleManager, adapter };
}

export default async function bridgeRoutes(fastify, options) {

  /**
   * POST /v1/bridge/start - Start the execution bridge
   */
  fastify.post('/v1/bridge/start', async (request, reply) => {
    const { exchange, mode, symbols, reconciliationIntervalMs } = request.body || {};

    // Check if already running
    if (bridge) {
      return reply.code(409).send({
        error: 'BRIDGE_ALREADY_RUNNING',
        message: 'Bridge is already running. Stop it first.'
      });
    }

    // Validate exchange
    const selectedExchange = (exchange || 'binance').toLowerCase();
    if (!SUPPORTED_EXCHANGES.includes(selectedExchange)) {
      return reply.code(400).send({
        error: 'INVALID_EXCHANGE',
        message: `Unsupported exchange: ${selectedExchange}`,
        supported: SUPPORTED_EXCHANGES
      });
    }

    try {
      // Load and validate credentials for selected exchange
      const credentials = loadCredentialsForExchange(selectedExchange);
      if (!credentials) {
        const envVars = {
          binance: 'BINANCE_API_KEY, BINANCE_SECRET_KEY',
          bybit: 'BYBIT_API_KEY, BYBIT_SECRET_KEY',
          okx: 'OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE'
        };
        return reply.code(400).send({
          error: 'MISSING_CREDENTIALS',
          message: `${selectedExchange} API credentials not configured. Set ${envVars[selectedExchange]}.`
        });
      }

      const credValidation = validateCredentials(credentials);
      if (!credValidation.valid) {
        return reply.code(400).send({
          error: 'INVALID_CREDENTIALS',
          errors: credValidation.errors
        });
      }

      // Load and validate config
      const config = loadExecutionConfigFromEnv();
      const overrides = { exchange: selectedExchange };
      if (mode && ['SHADOW', 'CANARY', 'LIVE'].includes(mode)) {
        overrides.mode = mode;
      }
      if (symbols && Array.isArray(symbols)) {
        overrides.allowedSymbols = symbols;
      }
      if (reconciliationIntervalMs) {
        overrides.reconciliationIntervalMs = reconciliationIntervalMs;
      }

      const finalConfig = { ...config, ...overrides };
      const configValidation = validateExecutionConfig(finalConfig);
      if (!configValidation.valid) {
        return reply.code(400).send({
          error: 'INVALID_CONFIG',
          errors: configValidation.errors
        });
      }

      // Initialize components
      adapter = createAdapter(selectedExchange, credentials);
      lifecycleManager = new OrderLifecycleManager();
      slippageAnalyzer = new SlippageAnalyzer();
      healthMonitor = new ExchangeHealthMonitor(adapter);

      bridge = new ExecutionBridge(adapter, lifecycleManager, finalConfig);

      await bridge.init();
      healthMonitor.start();

      // Start reconciliation if provider is set
      if (paperPositionsProvider) {
        bridge.startReconciliation(paperPositionsProvider);
      }

      // Audit log
      await emitAudit({
        actor: request.ip || 'unknown',
        action: 'BRIDGE_STARTED',
        target_type: 'bridge',
        target_id: 'execution_bridge',
        metadata: {
          mode: finalConfig.mode,
          exchange: finalConfig.exchange,
          testnet: finalConfig.testnet,
          symbols: finalConfig.allowedSymbols,
          credentials: maskCredentials(credentials)
        }
      });

      return reply.code(200).send({
        status: 'STARTED',
        mode: finalConfig.mode,
        exchange: finalConfig.exchange,
        testnet: finalConfig.testnet,
        allowedSymbols: finalConfig.allowedSymbols,
        maxOrdersPerDay: finalConfig.maxOrdersPerDay,
        maxNotionalPerDay: finalConfig.maxNotionalPerDay
      });

    } catch (error) {
      // Cleanup on failure
      bridge = null;
      healthMonitor?.stop();
      healthMonitor = null;

      return reply.code(500).send({
        error: 'BRIDGE_START_FAILED',
        message: error.message
      });
    }
  });

  /**
   * POST /v1/bridge/stop - Stop the execution bridge
   */
  fastify.post('/v1/bridge/stop', async (request, reply) => {
    if (!bridge) {
      return reply.code(404).send({
        error: 'BRIDGE_NOT_RUNNING',
        message: 'Bridge is not currently running'
      });
    }

    try {
      bridge.stopReconciliation();
      healthMonitor?.stop();

      // Audit log
      await emitAudit({
        actor: request.ip || 'unknown',
        action: 'BRIDGE_STOPPED',
        target_type: 'bridge',
        target_id: 'execution_bridge',
        metadata: {
          stats: bridge.getStats()
        }
      });

      // Clear references
      bridge = null;
      healthMonitor = null;
      adapter = null;

      return reply.code(200).send({
        status: 'STOPPED'
      });

    } catch (error) {
      return reply.code(500).send({
        error: 'BRIDGE_STOP_FAILED',
        message: error.message
      });
    }
  });

  /**
   * GET /v1/bridge/status - Get bridge status and stats
   */
  fastify.get('/v1/bridge/status', async (request, reply) => {
    if (!bridge) {
      return reply.code(200).send({
        running: false,
        message: 'Bridge is not running'
      });
    }

    const stats = bridge.getStats();
    const config = bridge.getConfig();
    const healthStatus = healthMonitor?.getLastStatus();

    return reply.code(200).send({
      running: true,
      stats,
      config: {
        mode: config.mode,
        exchange: config.exchange,
        testnet: config.testnet,
        allowedSymbols: config.allowedSymbols
      },
      health: healthStatus ? {
        healthy: healthStatus.healthy,
        pingMs: healthStatus.pingMs,
        driftMs: healthStatus.serverTimeDriftMs,
        lastChecked: healthStatus.checkedAt
      } : null
    });
  });

  /**
   * GET /v1/bridge/orders - Get recent bridge orders
   */
  fastify.get('/v1/bridge/orders', async (request, reply) => {
    if (!lifecycleManager) {
      return reply.code(200).send({
        orders: [],
        message: 'Bridge not initialized'
      });
    }

    const { state, symbol, limit } = request.query;
    let orders = lifecycleManager.getAll();

    // Filter by state
    if (state) {
      orders = orders.filter(o => o.state === state);
    }

    // Filter by symbol
    if (symbol) {
      orders = orders.filter(o => o.symbol === symbol.toUpperCase());
    }

    // Sort by createdAt descending
    orders.sort((a, b) => b.createdAt - a.createdAt);

    // Apply limit
    const limitNum = parseInt(limit) || 100;
    orders = orders.slice(0, limitNum);

    return reply.code(200).send({
      orders,
      total: orders.length,
      stateCounts: lifecycleManager.getStateCounts()
    });
  });

  /**
   * GET /v1/bridge/orders/:id - Get specific order details
   */
  fastify.get('/v1/bridge/orders/:id', async (request, reply) => {
    if (!lifecycleManager) {
      return reply.code(404).send({
        error: 'BRIDGE_NOT_INITIALIZED'
      });
    }

    const { id } = request.params;
    const order = await lifecycleManager.get(id);

    if (!order) {
      return reply.code(404).send({
        error: 'ORDER_NOT_FOUND',
        bridgeId: id
      });
    }

    return reply.code(200).send(order);
  });

  /**
   * POST /v1/bridge/reconcile - Force position reconciliation
   */
  fastify.post('/v1/bridge/reconcile', async (request, reply) => {
    if (!bridge) {
      return reply.code(404).send({
        error: 'BRIDGE_NOT_RUNNING'
      });
    }

    if (!paperPositionsProvider) {
      return reply.code(400).send({
        error: 'NO_POSITIONS_PROVIDER',
        message: 'Paper positions provider not configured'
      });
    }

    try {
      // Get current paper positions
      const paperPositions = await paperPositionsProvider();

      // Import reconciler dynamically
      const { PositionReconciler } = await import('../../../core/exchange/reconciliation/PositionReconciler.js');
      const reconciler = new PositionReconciler(adapter);
      const report = await reconciler.reconcile(paperPositions);

      return reply.code(200).send({
        report,
        summary: {
          healthy: report.isHealthy,
          matches: report.matches.length,
          mismatches: report.mismatches.length,
          orphanedExchange: report.orphanedExchange.length,
          orphanedPaper: report.orphanedPaper.length
        }
      });

    } catch (error) {
      return reply.code(500).send({
        error: 'RECONCILIATION_FAILED',
        message: error.message
      });
    }
  });

  /**
   * GET /v1/bridge/health - Get exchange health status
   */
  fastify.get('/v1/bridge/health', async (request, reply) => {
    if (!healthMonitor) {
      return reply.code(200).send({
        monitored: false,
        message: 'Health monitor not running'
      });
    }

    const status = healthMonitor.getLastStatus();

    return reply.code(200).send({
      monitored: true,
      status: status || { message: 'No health check performed yet' }
    });
  });

  /**
   * GET /v1/bridge/slippage - Get slippage statistics
   */
  fastify.get('/v1/bridge/slippage', async (request, reply) => {
    if (!slippageAnalyzer) {
      return reply.code(200).send({
        tracked: false,
        message: 'Slippage analyzer not running'
      });
    }

    const { symbol, limit } = request.query;

    const aggregate = slippageAnalyzer.getAggregateStats();
    const bySymbol = slippageAnalyzer.getAllStats();
    const recent = slippageAnalyzer.getRecentRecords(parseInt(limit) || 50);

    let filteredRecent = recent;
    if (symbol) {
      filteredRecent = recent.filter(r => r.symbol === symbol.toUpperCase());
    }

    return reply.code(200).send({
      tracked: true,
      aggregate,
      bySymbol,
      recentRecords: filteredRecent
    });
  });

}
