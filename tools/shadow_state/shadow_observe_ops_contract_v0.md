# Shadow Observe Ops Contract v0

Scope:
- Selected watchlist candidate -> existing `tools/run-shadow-watchlist-v0.js` -> existing `tools/run-soft-live.js`
- Observe-only usage
- No OMS / capital / execution routing changes

## Canonical Run Path

Dry-run preflight:

```bash
GO_LIVE_STRATEGY=core/strategy/strategies/PrintHeadTailStrategy.js \
node tools/run-shadow-watchlist-v0.js --dry-run
```

Real observe-only launch:

```bash
CORE_LIVE_WS_ENABLED=1 \
STRATEGY_MODE=OBSERVE_ONLY \
POSITION_SIZE_MODE=ZERO \
GO_LIVE_STRATEGY=core/strategy/strategies/PrintHeadTailStrategy.js \
node tools/run-shadow-watchlist-v0.js
```

## Selection Rules

- Default selection: `rank=1`
- Override by rank:

```bash
GO_LIVE_STRATEGY=core/strategy/strategies/PrintHeadTailStrategy.js \
node tools/run-shadow-watchlist-v0.js --rank 2 --dry-run
```

- Override by pack id:

```bash
GO_LIVE_STRATEGY=core/strategy/strategies/PrintHeadTailStrategy.js \
node tools/run-shadow-watchlist-v0.js --pack-id "<PACK_ID>" --dry-run
```

- If `--rank` and `--pack-id` are both passed, they must resolve to the same item.

## Required Env

Dry-run:
- `GO_LIVE_STRATEGY`

Real run:
- `GO_LIVE_STRATEGY`
- `CORE_LIVE_WS_ENABLED=1`

Wrapper-provided:
- `GO_LIVE_EXCHANGE`
- `GO_LIVE_SYMBOLS`
- `SHADOW_WATCH_PACK_ID`
- `SHADOW_WATCH_SELECTION_SLOT`
- `SHADOW_WATCH_DECISION_TIER`
- `SHADOW_WATCH_RANK`

## Optional Env

- `GO_LIVE_DATASET_PARQUET`
- `GO_LIVE_DATASET_META`
- `GO_LIVE_STRATEGY_CONFIG`
- `GO_LIVE_ORDERING_MODE`
- `RUN_MAX_DURATION_SEC`
- `RUN_MAX_EVENTS`
- `RUN_MAX_DECISION_RATE`
- `LAG_WARN_MS`
- `LAG_ERROR_MS`
- archive / audit envs already used by `tools/run-soft-live.js`

## Strategy Binding Decision

Operational default:
- `core/strategy/strategies/PrintHeadTailStrategy.js`

Reason:
- config-free
- symbol-agnostic
- observe-only behavior
- lowest-risk starting point for shadow connectivity and event flow verification

Not selected as default:
- `core/strategy/baseline/BaselineStrategy.js`
  - requires symbol-aware config
  - defaults to one symbol (`btcusdt`)
  - not a safe global default for multi-symbol watchlist items
- `core/strategy/strategies/SlowStrategy.js`
  - test-only
  - intentionally slows/stops processing

## Verify Contract

Preflight:

```bash
GO_LIVE_STRATEGY=core/strategy/strategies/PrintHeadTailStrategy.js \
node tools/run-shadow-watchlist-v0.js --dry-run
```

Launch:
- Watch stdout for selected item mapping
- Then watch `soft_live_heartbeat` JSON lines from `tools/run-soft-live.js`

Ongoing health:
- `observerRegistry` heartbeat output on stdout
- audit spool receives `RUN_START`

Finished:
- `/tmp/quantlab-soft-live.json` exists
- `node tools/verify-soft-live.js` returns `PASS`
- audit spool contains `RUN_STOP`
- archive stats exist if archive is enabled

## Known Limits

- `GO_LIVE_STRATEGY` remains external; watchlist does not encode strategy binding
- Watchlist is not an execution intent and is not converted into `Decision[]`
- `stream` in the watchlist is provenance metadata, not a live runtime control input
