# Terminology

A glossary of terms used throughout the QuantLab codebase.

---

## Core Concepts

### Replay

The process of streaming historical market events through a strategy in deterministic order. Replay produces the same results every time given the same input.

**Key Property:** Deterministic — same input always produces same output.

**Related Files:** `core/replay/ReplayEngine.js`

---

### Run

A single execution of a strategy against a dataset. Each run has a unique `run_id` and produces a manifest with results.

**Identifier:** `run_{hash}` (16 character hash derived from dataset + config + seed)

**Related Files:** `core/strategy/runtime/StrategyRuntime.js`

---

### Cursor

An encoded position in an event stream used for resuming replay. Contains `ts_event` and `seq` values encoded as base64.

**Format:** Base64-encoded JSON `{"v":1,"ts_event":"123456789","seq":"1"}`

**Property:** Exclusive — resume starts from the event AFTER the cursor.

**Related Files:** `core/replay/CursorCodec.js`

---

### Guard

A safety mechanism that enforces constraints during strategy execution.

**Types:**
- **OrderingGuard** — Enforces monotonic event ordering
- **PromotionGuard** — Validates conditions before live trading
- **RiskGuard** — (Not integrated) Enforces risk limits

**Related Files:** `core/strategy/safety/OrderingGuard.js`

---

### Determinism

The guarantee that identical inputs produce identical outputs. QuantLab achieves determinism through:
- Canonical JSON serialization
- Fixed event ordering
- Counter-based IDs
- No wall-clock dependencies

**Related Files:** `core/strategy/safety/DeterminismValidator.js`

---

### Archive

Persistent storage of run results in S3. Contains manifest, decisions, and stats.

**Structure:**
```
replay_runs/replay_run_id={id}/
├── manifest.json
├── decisions.jsonl
└── stats.json
```

**Related Files:** `core/run-archive/RunArchiveWriter.js`

---

### StrategyRuntime

The main orchestrator that executes strategies. Manages lifecycle, event processing, and state.

**Lifecycle States:** CREATED → INITIALIZING → READY → RUNNING → FINALIZING → DONE

**Related Files:** `core/strategy/runtime/StrategyRuntime.js`

---

### ReplayEngine

Reads events from parquet files and streams them in deterministic order.

**Capabilities:**
- Cursor-based resume
- Batch pagination
- Multi-partition support

**Related Files:** `core/replay/ReplayEngine.js`

---

## Event Model

### ts_event

Event timestamp in nanoseconds from the exchange. Primary ordering field.

**Type:** BigInt (JavaScript) / int64 (Parquet)

---

### seq

Sequence number for tie-breaking when multiple events share the same `ts_event`.

**Type:** BigInt (JavaScript) / int64 (Parquet)

---

### Stream

A type of market data feed.

**Types:**
- `bbo` — Best Bid/Offer (Level 1)
- `trade` — Trade executions
- `mark_price` — Mark/index prices
- `funding` — Funding rates
- `open_interest` — Open interest

---

### BBO (Best Bid/Offer)

The highest bid and lowest ask prices at any moment.

**Fields:** `bid_price`, `bid_qty`, `ask_price`, `ask_qty`

---

## Execution Model

### Fill

The result of executing an order.

**Fields:** `id`, `orderId`, `symbol`, `side`, `qty`, `fillPrice`, `fillValue`, `fee`, `ts_event`

**Semantics (v1):**
- Zero latency (same-tick)
- Zero slippage (exact BBO)
- BUY fills at ask, SELL fills at bid

---

### Order Intent

A request to place an order.

**Fields:** `symbol`, `side` (BUY/SELL), `qty`

---

### Position

The current holding in a symbol.

**Tracked:** Quantity, average entry price, unrealized P&L

---

## Serialization

### Canonical JSON

JSON serialization with sorted keys for deterministic hashing.

**Rules:**
- Keys sorted alphabetically
- BigInt → `"123n"` string
- Undefined omitted
- Null preserved

**Related Files:** `core/strategy/state/StateSerializer.js`

---

### Hash

SHA256 digest of canonically serialized data. Used for:
- State fingerprinting
- Fills verification
- Run identification
- Twin-run comparison

---

## Data Pipeline

### Collector

Python service that collects real-time market data from exchanges via WebSocket.

**Output:** Parquet files in spool directory

**Related Files:** `collector/collector.py`

---

### Spool

Local directory where collector writes before S3 upload.

**Location:** `/opt/quantlab/spool`

**Format:** Hive-partitioned parquet files

---

### Compact

Processed, deduplicated data in S3 ready for replay.

**Location:** `s3://quantlab-compact/`

**Format:** `exchange={X}/stream={Y}/symbol={Z}/date={YYYYMMDD}/data.parquet`

---

### meta.json

Metadata file accompanying each parquet file.

**Contains:** Row count, schema version, manifest ID, time range

---

## Services

### replayd

HTTP service exposing ReplayEngine via SSE (Server-Sent Events).

**Port:** 3030

---

### strategyd

HTTP service managing strategy runs.

**Port:** 3031

---

### backtestd

HTTP service orchestrating determinism backtests.

**Port:** 3041

---

### featurexd

HTTP service for feature extraction.

**Port:** 3051

---

### labeld

HTTP service for label generation.

**Port:** 3061

---

## ML Components

### Advisory Mode

ML model provides signals but does not execute trades automatically.

**Flag:** `ml_mode: "ADVISORY_ONLY"`

---

### Autonomous Mode

ML model can execute trades automatically. Not currently enabled.

**Flag:** `ml_mode: "AUTONOMOUS"`

---

### Feature

Derived data point used as ML model input.

**Examples:** Moving averages, volatility, spread ratios

---

### Promotion

Process of replacing production model with a better-performing candidate.

**Criteria:** Higher directional hit rate, lower drawdown

---

## Safety Concepts

### Twin-Run Verification

Running a strategy twice and comparing hashes to verify determinism.

**Pass Condition:** `stateHash`, `fillsHash`, `eventCount` all match

---

### Error Containment

Policy for handling strategy errors during execution.

**Modes:**
- `FAIL_FAST` — Stop on first error
- `SKIP_AND_LOG` — Skip event, continue
- `QUARANTINE` — Move to quarantine, continue

---

### Budget Manager

Enforces limits on run activity.

**Limits:** Decision count, execution time

---

### Audit Trail

Record of all system actions for compliance and debugging.

**Location:** S3 archive bucket

---

## Operational Terms

### Health Check

Endpoint returning service status.

**Standard Response:** `{"status":"ok"}`

---

### Quality Ledger

15-minute window quality assessments from collector.

**Grades:** GOOD, DEGRADED, BAD

---

### Backpressure

Queue management when events arrive faster than processing.

**Modes:** Normal, High (80%), Critical (95%)

---

### Drain Mode

Writer flush acceleration when queue is filling.

**Modes:** Normal (5s gap), Accelerated (1s gap)

---

## Lifecycle States

| State | Description |
|-------|-------------|
| CREATED | Runtime instantiated, not initialized |
| INITIALIZING | Strategy.onInit() running |
| READY | Initialization complete, waiting to start |
| RUNNING | Processing events |
| PAUSED | Temporarily stopped, can resume |
| FINALIZING | Strategy.onFinalize() running |
| DONE | Completed successfully |
| FAILED | Terminated due to error |
| CANCELED | Terminated by user request |

---

## Abbreviations

| Abbr | Full Form |
|------|-----------|
| BBO | Best Bid/Offer |
| SSE | Server-Sent Events |
| ML | Machine Learning |
| P&L | Profit and Loss |
| MTM | Mark-to-Market |
| OI | Open Interest |
| WS | WebSocket |
| API | Application Programming Interface |
| S3 | Simple Storage Service (AWS) |
| CLI | Command Line Interface |

---

*This glossary is derived from codebase terminology and comments.*
