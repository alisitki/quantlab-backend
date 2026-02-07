/**
 * Strategy Lifecycle System Configuration
 *
 * Defines stage-specific criteria, thresholds, and system parameters
 * for the strategy promotion/demotion lifecycle.
 */

/**
 * Stage-specific configuration
 *
 * Each stage defines:
 * - capitalPct: Target capital allocation (0-100)
 * - mode: Execution mode (backtest, paper, live, none)
 * - minRuns: Minimum number of runs before promotion eligible
 * - minDays: Minimum calendar days before promotion eligible
 * - criteria: Performance requirements for promotion
 */
export const LIFECYCLE_CONFIG = {
  stages: {
    CANDIDATE: {
      capitalPct: 0,
      mode: 'backtest',
      minRuns: 1,
      minDays: 0,
      criteria: {
        minSharpe: 0.5,
        maxDrawdownPct: 5,
        minTrades: 10
      }
    },
    PAPER: {
      capitalPct: 0,
      mode: 'paper',
      minRuns: 5,
      minDays: 3,
      criteria: {
        minSharpe: 0.3,
        maxDrawdownPct: 10,
        minWinRate: 0.35,
        minTrades: 50
      }
    },
    CANARY: {
      capitalPct: 1,
      mode: 'live',
      minRuns: 10,
      minDays: 7,
      criteria: {
        minSharpe: 0.3,
        maxDrawdownPct: 8,
        minWinRate: 0.35,
        requireApproval: true
      }
    },
    SHADOW: {
      capitalPct: 5,
      mode: 'live',
      minRuns: 20,
      minDays: 14,
      criteria: {
        minSharpe: 0.3,
        maxDrawdownPct: 10,
        minConsistency: 0.6,
        requireApproval: true
      }
    },
    LIVE: {
      capitalPct: 100,
      mode: 'live'
    },
    RETIRED: {
      capitalPct: 0,
      mode: 'none'
    }
  },

  /**
   * Demotion and retirement rules
   */
  demotion: {
    maxConsecutiveLossDays: 5,
    maxDrawdownMultiplier: 2.0,   // 2x backtest drawdown triggers retire
    minSharpe: -0.5,               // Below this → immediate retire
    edgeDecayTrigger: true,        // Edge health < 0.2 → retire
    killSwitchRetire: true         // KillSwitch activation → auto-retire
  },

  /**
   * Persistence configuration
   */
  persistence: {
    storeDir: 'data/lifecycle',
    filename: 'lifecycle-state.json'
  },

  /**
   * Evaluation parameters
   */
  evaluation: {
    rollingWindowDays: 30,         // Rolling performance window
    maxConcurrentLive: 5           // Max strategies in LIVE stage
  }
};
