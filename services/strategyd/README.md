# Strategyd

Strategy runner service with SSE replay ingestion.

## Runtime Modes

- Default is **StrategyRuntime v2** (feature-flagged).
- Legacy mode is available via env flag.

### Flags

- `STRATEGY_RUNTIME_V2`
  - Unset or any value except `0` → v2 enabled (default)
  - `0` → legacy SSEStrategyRunner

### Backpressure & Yield (v2)

- `STRATEGYD_YIELD_EVERY` (default: `1000`)
  - Cooperative event-loop yield cadence.
- `STRATEGYD_BACKPRESSURE_HIGH` (default: `1500`)
- `STRATEGYD_BACKPRESSURE_LOW` (default: `500`)
  - Hysteresis thresholds for throttling SSE intake.
- `STRATEGYD_MAX_QUEUE_CAPACITY` (default: `2000`)
  - Hard overflow threshold (legacy + v2).

## Quick Health Checks

Assuming `STRATEGYD_TOKEN=mytoken` and `AUTH_REQUIRED=true`:

```bash
curl http://localhost:3031/health
curl -H "Authorization: Bearer mytoken" http://localhost:3031/state
curl -H "Authorization: Bearer mytoken" http://localhost:3031/metrics
curl -H "Authorization: Bearer mytoken" http://localhost:3031/metrics | grep strategyd_queue_overflow_disconnects_total
```

## Example

```bash
# Default (v2)
STRATEGYD_TOKEN=mytoken AUTH_REQUIRED=true node services/strategyd/server.js

# Legacy
STRATEGY_RUNTIME_V2=0 STRATEGYD_TOKEN=mytoken AUTH_REQUIRED=true node services/strategyd/server.js
```

## Smoke Gates

Runs determinism and overflow parity checks (fails fast):

```bash
node services/strategyd/runtime/smoke.js
```

Or via npm:

```bash
cd services/strategyd
npm run smoke
```

## Overflow Parity Test

```bash
REPLAYD_URL=http://localhost:3036 REPLAYD_TOKEN=test-secret \\
DATASET=bbo SYMBOL=ADAUSDT DATE=2026-01-04 \\
node services/strategyd/runtime/overflow-parity.js
```

## Systemd Env Guidance (Production)

Recommended explicit envs (avoid relying on "unset"):

```bash
STRATEGY_RUNTIME_V2=1
STRATEGYD_YIELD_EVERY=1000
STRATEGYD_BACKPRESSURE_HIGH=1500
STRATEGYD_BACKPRESSURE_LOW=500
STRATEGYD_MAX_QUEUE_CAPACITY=2000
AUTH_REQUIRED=true
STRATEGYD_TOKEN=your-secret-token
REPLAYD_URL=http://127.0.0.1:3036
REPLAYD_TOKEN=your-replayd-token
```

Confirm runtime mode in logs:

```
Runtime mode=v2 yield_every=1000 backpressure_high=1500 backpressure_low=500 max_queue=2000
```
