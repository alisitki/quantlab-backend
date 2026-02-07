/**
 * Strategy Factory Configuration
 *
 * Constants and defaults for strategy generation from edges.
 */

export const FACTORY_CONFIG = {
  // Backtest requirements
  backtest: {
    minTrades: 10,           // Minimum trades in backtest to pass
    minSharpe: 0.5,          // Minimum Sharpe ratio
    maxDrawdownPct: 5        // Maximum drawdown percentage
  },

  // Strategy parameters (defaults derived from edge)
  parameters: {
    baseQuantity: 10,        // Base position size
    maxQuantity: 50,         // Maximum position size
    defaultCooldownMs: 5000  // Default cooldown between trades
  },

  // Template-specific defaults
  templateDefaults: {
    mean_reversion: {
      profitTargetPct: 0.0005,     // 0.05% profit target
      maxVolatilityRatio: 2.0       // Don't enter above this volatility
    },
    momentum: {
      trailingStopPct: 0.015,       // 1.5% trailing stop
      minTrendStrength: 0.3         // Minimum trend strength to enter
    },
    breakout: {
      activationDelay: 5,           // Events to wait before confirming breakout
      maxNoProgressEvents: 100      // Exit if no progress within N events
    }
  },

  // Deployment settings
  deployment: {
    initialStage: 'CANDIDATE',    // Initial stage in promotion lifecycle
    requireBacktestPass: true     // Require successful backtest before deploy
  }
};
