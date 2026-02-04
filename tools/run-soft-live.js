#!/usr/bin/env node
import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';
import { StrategyLoader } from '../core/strategy/interface/StrategyLoader.js';
import { observerRegistry } from '../core/observer/ObserverRegistry.js';
import { writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../core/package.json', import.meta.url));
const dotenv = require('dotenv');

dotenv.config({ path: new URL('../core/.env', import.meta.url).pathname, override: true });

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function numEnv(val, fallback) {
  const n = Number(val);
  return Number.isFinite(n) ? n : fallback;
}

function parseVeryHigh(value, fallback) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === 'string' && value.toUpperCase() === 'VERY_HIGH') return 1e12;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function resolveStrategyPath(path) {
  if (!path) return null;
  if (path.startsWith('core/')) return path;
  if (path.startsWith('strategy/')) return `core/${path}`;
  return path;
}

function buildStrategyConfig() {
  const raw = process.env.GO_LIVE_STRATEGY_CONFIG || '';
  let config = {};
  if (raw) {
    try {
      config = JSON.parse(raw);
    } catch {
      console.error('SOFT_LIVE_CONFIG_ERROR: invalid GO_LIVE_STRATEGY_CONFIG JSON');
      process.exit(1);
    }
  }

  const mode = process.env.STRATEGY_MODE || 'OBSERVE_ONLY';
  const sizeMode = process.env.POSITION_SIZE_MODE || 'ZERO';
  if (mode === 'OBSERVE_ONLY' || sizeMode === 'ZERO') {
    if (config.orderQty === undefined) config.orderQty = 0;
  }

  return config;
}

function buildExecutionEngine() {
  const mode = process.env.STRATEGY_MODE || 'OBSERVE_ONLY';
  const sizeMode = process.env.POSITION_SIZE_MODE || 'ZERO';
  if (mode !== 'OBSERVE_ONLY' && sizeMode !== 'ZERO') return null;

  return {
    onOrder(intent) {
      return {
        id: `soft_fill_${Date.now()}`,
        side: intent.side,
        fillPrice: 0,
        qty: 0,
        ts_event: intent.ts_event
      };
    },
    snapshot() {
      return { fills: [], totalRealizedPnl: 0, totalUnrealizedPnl: 0 };
    }
  };
}

function buildBudgetConfig() {
  const maxRunSeconds = numEnv(process.env.RUN_MAX_DURATION_SEC, 86400);
  const maxEvents = numEnv(process.env.RUN_MAX_EVENTS, 2_000_000);
  const maxDecisionsPerSec = numEnv(process.env.RUN_MAX_DECISION_RATE, 2);
  const maxEventsEnabled = envBool(process.env.RUN_BUDGET_MAX_EVENTS_ENABLED ?? '1');
  const maxDecisionRateEnabled = envBool(process.env.RUN_BUDGET_MAX_DECISION_RATE_ENABLED ?? '1');

  return {
    enabled: true,
    maxDurationEnabled: true,
    maxRunSeconds,
    maxEventsEnabled,
    maxEvents,
    maxDecisionRateEnabled,
    maxDecisionsPerMin: maxDecisionsPerSec * 60
  };
}

function buildGuardConfig() {
  const minDecisions = numEnv(process.env.GUARD_MIN_DECISIONS, 5);
  const maxLoss = parseVeryHigh(process.env.GUARD_MAX_LOSS, 1e12);
  const lossStreak = parseVeryHigh(process.env.GUARD_LOSS_STREAK, 1e9);

  return {
    enabled: true,
    minDecisionEnabled: true,
    minDecisions,
    maxLossEnabled: true,
    maxLoss,
    lossStreakEnabled: true,
    lossStreak
  };
}

async function main() {
  const exchange = process.env.GO_LIVE_EXCHANGE;
  const symbols = process.env.GO_LIVE_SYMBOLS ? process.env.GO_LIVE_SYMBOLS.split(',').map(s => s.trim()).filter(Boolean) : [];
  const strategyPathRaw = process.env.GO_LIVE_STRATEGY;
  const strategyPath = resolveStrategyPath(strategyPathRaw);

  if (!exchange || symbols.length === 0 || !strategyPath) {
    console.error('SOFT_LIVE_CONFIG_ERROR: GO_LIVE_EXCHANGE, GO_LIVE_SYMBOLS, GO_LIVE_STRATEGY required');
    process.exit(1);
  }

  const datasetParquet = process.env.GO_LIVE_DATASET_PARQUET || 'live';
  const datasetMeta = process.env.GO_LIVE_DATASET_META || 'live';
  const lagWarnMs = numEnv(process.env.LAG_WARN_MS, 2000);
  const lagErrorMs = numEnv(process.env.LAG_ERROR_MS, 10000);
  const orderingMode = process.env.GO_LIVE_ORDERING_MODE || 'WARN';

  const strategy = await StrategyLoader.loadFromFile(strategyPath, {
    config: buildStrategyConfig(),
    autoAdapt: true
  });

  const runner = new LiveStrategyRunner({
    dataset: { parquet: datasetParquet, meta: datasetMeta },
    exchange,
    symbols,
    strategy,
    strategyConfig: buildStrategyConfig(),
    orderingMode,
    executionEngine: buildExecutionEngine(),
    maxLagMs: lagWarnMs,
    guardConfig: buildGuardConfig(),
    budgetConfig: buildBudgetConfig()
  });

  const heartbeatMs = numEnv(process.env.SOFT_LIVE_HEARTBEAT_MS, 30000);
  const heartbeat = setInterval(() => {
    const runs = observerRegistry.listRuns();
    const current = runs.find(r => r.live_run_id === runner.liveRunId);
    const now = Date.now();
    const lastEventAgeMs = current?.last_event_ts ? now - current.last_event_ts : null;
    const payload = {
      event: 'soft_live_heartbeat',
      live_run_id: runner.liveRunId,
      status: current?.status || 'UNKNOWN',
      decision_count: runner.decisionCount,
      last_event_age_ms: lastEventAgeMs,
      budget_pressure: current?.budget_pressure || 'LOW'
    };
    if (lastEventAgeMs !== null && lastEventAgeMs > lagErrorMs) {
      payload.lag_error = true;
    }
    console.log(JSON.stringify(payload));
  }, heartbeatMs);

  const handleStop = () => runner.stop();
  process.on('SIGINT', handleStop);
  process.on('SIGTERM', handleStop);

  try {
    const result = await runner.run({ handleSignals: false });
    clearInterval(heartbeat);
    const output = {
      live_run_id: result.live_run_id,
      started_at: result.started_at,
      finished_at: result.finished_at,
      stop_reason: result.stop_reason
    };
    await writeFile('/tmp/quantlab-soft-live.json', JSON.stringify(output));
    console.log(JSON.stringify({ event: 'soft_live_done', ...output }));
  } catch (err) {
    clearInterval(heartbeat);
    console.error(JSON.stringify({ event: 'soft_live_error', error: err.message || String(err) }));
    process.exit(1);
  }
}

main();
