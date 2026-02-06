# QUANTLAB SYSTEM RUNBOOK
## Frozen System State — 2026-02-03
## Identity Update — 2026-02-05

---

# SECTION 0 — SYSTEM IDENTITY (CRITICAL)

> **QuantLab is NOT a strategy runner. QuantLab is an edge discovery & strategy factory system.**

## Architecture Mental Model

```
OLD (Deprecated):
Data → Features → Strategy → ML → Execution

NEW (Active):
Data → Behavior → Regime → EDGE → Strategy → Risk → Execution
```

**Core Principle:** Strategy is the OUTPUT layer, not the CENTER. Edge discovery is the engine.

## What QuantLab Does

1. Collects multi-exchange market data
2. Extracts market behavior patterns
3. Discovers exploitable edges from behaviors
4. Generates strategies from validated edges
5. Manages strategy lifecycle (promote/demote/retire)
6. Executes with risk controls

## What QuantLab Does NOT Do

- Run fixed, manually-written strategies as the primary function
- Use ML for blind signal prediction
- Treat backtest PnL as proof of edge

---

# SECTION 1 — PROJECT IDENTITY

## System Type
**Edge Discovery & Strategy Factory System**
(Infrastructure: Deterministic Market Data Replay & Algorithmic Trading)

## Primary Purpose
A production-grade system for:
1. Collecting real-time market data from cryptocurrency exchanges (Binance, Bybit, OKX)
2. Extracting market behavior and detecting regime changes
3. Discovering and validating statistical edges
4. Generating and managing strategy lifecycle from edges
5. Replaying historical market data deterministically for edge validation
6. Executing strategies with paper/live execution capabilities

## Core Subsystems

| Subsystem | Location | Purpose |
|-----------|----------|---------|
| **Collector** | `collector/` | Multi-exchange WebSocket data ingestion (Python) |
| **Replay Engine** | `core/replay/` | Deterministic parquet-based market data replay |
| **Strategy Runtime** | `core/strategy/runtime/` | Strategy lifecycle management with determinism guarantees |
| **Execution Engine** | `core/execution/` | Paper trading with same-tick fill simulation |
| **ML Pipeline** | `core/ml/` | XGBoost model training and inference |
| **Safety Guards** | `core/strategy/safety/`, `guards/`, `limits/` | Ordering, promotion, and budget enforcement |
| **Run Archive** | `core/run-archive/` | S3-based run result persistence |
| **Scheduler** | `core/scheduler/` | Cron-based automation for ML and ops |
| **Services** | `services/` | Fastify microservices (strategyd, replayd, featurexd, backtestd, labeld) |

## System Maturity Level

| Component | Maturity | Evidence |
|-----------|----------|----------|
| Replay Engine | **Production** | Deterministic ordering contract, cursor-based resume, DuckDB integration |
| Execution Engine | **Production** | Tested determinism, hash verification |
| Strategy Runtime | **Production** | Full lifecycle, checkpoint support, audit trail |
| Collector | **Production** | Multi-exchange, backpressure handling, atomic writes |
| ML Pipeline | **Beta** | XGBoost integration works, but advisory-only |
| Live Trading | **Experimental** | LiveStrategyRunner exists but not in main service path |
| Risk Management | **Not Integrated** | Code exists, zero runtime references |

---

# SECTION 2 — CURRENT SYSTEM STATE (TRUTH SNAPSHOT)

| Subsystem | Status | Evidence Files | Notes |
|-----------|--------|----------------|-------|
| **Data Ingestion** | STABLE | `collector/*.py` | 3 exchanges, atomic parquet writes, fsync protocol |
| **Data Storage** | STABLE | `collector/writer.py` | Local spool → S3 (separate uploader) |
| **Replay Engine** | STABLE | `core/replay/ReplayEngine.js`, `ParquetReader.js` | DuckDB backend, ORDERING_CONTRACT enforced |
| **Strategy Runtime Core** | STABLE | `core/strategy/runtime/StrategyRuntime.js` | Full lifecycle, determinism validation |
| **Execution Engine** | STABLE | `core/execution/engine.js` | Paper trading, BBO-only fills, memory-optimized (99.998% reduction, 2026-02-05) |
| **Ordering Guard** | STABLE | `core/strategy/safety/OrderingGuard.js` | STRICT/WARN modes, integrated |
| **Determinism Validator** | STABLE | `core/strategy/safety/DeterminismValidator.js` | Hash computation, imported by 4+ modules |
| **Error Containment** | STABLE | `core/strategy/safety/ErrorContainment.js` | Policy-based error handling |
| **ML Pipeline (Offline)** | PARTIAL | `core/ml/models/XGBoostModel.js`, `runtime/` | Training works, inference integrated |
| **ML Advisory (Online)** | PARTIAL | `services/strategyd/runtime/MLDecisionAdapter.js` | Weight adjustment, gate-controlled |
| **Promotion Guards** | EXPERIMENTAL | `core/strategy/guards/PromotionGuardManager.js` | Only used by LiveStrategyRunner |
| **Budget Manager** | EXPERIMENTAL | `core/strategy/limits/RunBudgetManager.js` | Only used by LiveStrategyRunner |
| **Live Trading Path** | EXPERIMENTAL | `core/strategy/live/LiveStrategyRunner.js` | Exists, not in main service |
| **Risk Management** | LEGACY/DETACHED | `core/risk/RiskManager.js`, `rules/*.js` | Zero imports in runtime |
| **Scheduler** | STABLE | `core/scheduler/*.js` | 10+ scripts with shell/cron integration |
| **Ops & Monitoring** | STABLE | `core/observer/`, `core/audit/` | AuditWriter, ObserverRegistry active |
| **Feature Extraction** | PARTIAL | `core/features/`, `services/featurexd/` | V1 (batch) and V2 (streaming) coexist |

---

# SECTION 3 — ACTIVE RUNTIME PATH

## Production Execution Chain

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ACTIVE RUNTIME PATH                                   │
└──────────────────────────────────────────────────────────────────────────────┘

  COLLECTOR                STORAGE              REPLAY              STRATEGY
 ┌─────────┐            ┌─────────┐         ┌─────────┐         ┌──────────┐
 │ Binance │            │  Local  │         │ Replay  │         │ Strategy │
 │ Bybit   │──Events───▶│  Spool  │──S3────▶│ Engine  │──Events▶│ Runtime  │
 │ OKX     │            │(Parquet)│         │(DuckDB) │         │          │
 └─────────┘            └─────────┘         └─────────┘         └────┬─────┘
                                                                     │
                                                                     ▼
  ARCHIVE                EXECUTION             GUARDS              DECISION
 ┌─────────┐            ┌─────────┐         ┌─────────┐         ┌──────────┐
 │   S3    │◀──Results──│Execution│◀─Fills──│Ordering │◀─Check──│ Signal   │
 │ Archive │            │ Engine  │         │ Guard   │         │ Engine   │
 └─────────┘            └─────────┘         └─────────┘         └──────────┘
```

## Stage Details

### Stage 1: DATA INGESTION
| Component | File | Failure Impact |
|-----------|------|----------------|
| Binance Handler | `collector/binance_handler.py` | Binance data loss |
| Bybit Handler | `collector/bybit_handler.py` | Bybit data loss |
| OKX Handler | `collector/okx_handler.py` | OKX data loss |
| Writer | `collector/writer.py` | All data loss |
| **Critical Dependency** | PyArrow, asyncio | System halt |

### Stage 2: REPLAY ENGINE
| Component | File | Failure Impact |
|-----------|------|----------------|
| ReplayEngine | `core/replay/ReplayEngine.js` | No strategy execution possible |
| ParquetReader | `core/replay/ParquetReader.js` | Cannot read market data |
| MetaLoader | `core/replay/MetaLoader.js` | Cannot load dataset metadata |
| CursorCodec | `core/replay/CursorCodec.js` | Cannot resume replays |
| ORDERING_CONTRACT | `core/replay/ORDERING_CONTRACT.js` | Non-deterministic replay |
| **Critical Dependency** | DuckDB | Query engine failure |

### Stage 3: STRATEGY RUNTIME
| Component | File | Failure Impact |
|-----------|------|----------------|
| StrategyRuntime | `core/strategy/runtime/StrategyRuntime.js` | No strategy lifecycle |
| RuntimeConfig | `core/strategy/runtime/RuntimeConfig.js` | No configuration validation |
| RuntimeState | `core/strategy/runtime/RuntimeState.js` | No state tracking |
| StateSerializer | `core/strategy/state/StateSerializer.js` | Non-deterministic state |
| DeterminismValidator | `core/strategy/safety/DeterminismValidator.js` | No hash verification |
| **Critical Dependency** | Node.js crypto | Hash computation failure |

### Stage 4: EXECUTION ENGINE
| Component | File | Failure Impact |
|-----------|------|----------------|
| ExecutionEngine | `core/execution/engine.js` | No order execution |
| Order/Fill/Position | `core/execution/*.js` | No trade tracking |
| **Critical Dependency** | None external | Self-contained |

### Stage 5: ARCHIVE
| Component | File | Failure Impact |
|-----------|------|----------------|
| RunArchiveWriter | `core/run-archive/RunArchiveWriter.js` | No run persistence |
| AuditWriter | `core/audit/AuditWriter.js` | No audit trail |
| **Critical Dependency** | AWS S3 SDK | Archive failure |

---

# SECTION 4 — CONTROL & DECISION LAYERS

## Runtime Control Gates

| Gate | File | Purpose | Failure Consequence |
|------|------|---------|---------------------|
| **OrderingGuard** | `core/strategy/safety/OrderingGuard.js` | Enforce ts_event/seq monotonicity | STRICT: Throws error, halts run. WARN: Logs, continues |
| **DeterminismValidator** | `core/strategy/safety/DeterminismValidator.js` | Compute state/decision hashes | Non-deterministic runs, audit failure |
| **ErrorContainment** | `core/strategy/safety/ErrorContainment.js` | Strategy exception handling | FAIL_FAST: Halts. CONTINUE: Logs and proceeds |
| **PromotionGuardManager** | `core/strategy/guards/PromotionGuardManager.js` | PnL/streak/parity validation | Run marked as failed, no promotion |
| **RunBudgetManager** | `core/strategy/limits/RunBudgetManager.js` | Duration/event/rate limits | Run stopped at budget breach |
| **MLActiveGate** | `services/strategyd/runtime/MLActiveGate.js` | ML feature flag | ML weight returns 1.0 (no adjustment) |
| **MLDecisionAdapter** | `services/strategyd/runtime/MLDecisionAdapter.js` | Confidence → weight mapping | Falls back to no ML influence |

## Gate Integration Points

```
Event Received
     │
     ▼
┌────────────────┐
│ OrderingGuard  │──FAIL──▶ Stop Run (STRICT) or Log (WARN)
└───────┬────────┘
        │ PASS
        ▼
┌────────────────┐
│ BudgetManager  │──FAIL──▶ Stop Run (BUDGET_EXCEEDED)
└───────┬────────┘
        │ PASS
        ▼
┌────────────────┐
│ Strategy.onEvent│──Signal──▶ SignalEngine
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ MLActiveGate   │──Disabled──▶ Skip ML
└───────┬────────┘
        │ Enabled
        ▼
┌────────────────┐
│MLDecisionAdapter│──Weight──▶ Position Sizing
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ExecutionEngine │──Fill──▶ Archive
└────────────────┘
```

---

# SECTION 5 — PARALLEL / DUPLICATE LOGIC

## Identified Duplications

| Component | Locations | Status | Rationale |
|-----------|-----------|--------|-----------|
| **canonicalStringify** | `core/strategy/state/StateSerializer.js` | AUTHORITATIVE | BigInt-safe, deterministic |
| | ~~`services/*/Orchestrator.js`~~ | CONSOLIDATED | Removed in consolidation phase |
| **Auth Middleware** | `core/common/authMiddlewareFactory.js` | AUTHORITATIVE | Shared factory |
| | `services/*/middleware/auth.js` | CONSUMERS | Import from factory |
| **RuntimeConfig** | `core/strategy/runtime/RuntimeConfig.js` | AUTHORITATIVE | Strategy execution config (259 lines) |
| | `services/strategyd/runtime/RuntimeConfig.js` | INTENTIONAL SEPARATION | ML gating config (66 lines) - different purpose |
| **FeatureBuilder** | `core/features/FeatureBuilderV1.js` | BATCH PROCESSING | Deterministic offline feature extraction |
| | `core/features/FeatureBuilder.js` | STREAMING | Real-time feature computation |
| | `services/featurexd/extractors/FeatureSetV1.js` | SERVICE-SPECIFIC | Replay-based feature extraction |
| **Clock Implementations** | `core/replay/clock/AsapClock.js` | FASTEST | No delays |
| | `core/replay/clock/RealtimeClock.js` | REALTIME | Actual time delays |
| | `core/replay/clock/ScaledClock.js` | CONFIGURABLE | Speed multiplier |

## Why Duplications Exist

1. **RuntimeConfig (2 files)**: Core version manages strategy execution parameters with validation and hashing. Service version manages ML feature gating flags. Different domains, same name.

2. **FeatureBuilder (3 implementations)**: V1 is deterministic batch with forward-looking labels (supervised learning). V2 is streaming real-time (live trading). Service version is replay-integrated.

3. **Clock implementations**: Not duplication - polymorphic strategy pattern for timing control.

---

# SECTION 6 — UNUSED OR DETACHED SUBSYSTEMS

| Module | Status | Risk | Recommendation |
|--------|--------|------|----------------|
| `core/risk/RiskManager.js` | ORPHAN | LOW | Integrate into StrategyRuntime or remove |
| `core/risk/rules/*.js` (4 files) | ORPHAN | LOW | Depends on RiskManager decision |
| `core/ml/dataset/LabelBuilder.js` | ORPHAN | LOW | JSDoc reference only, evaluate need |
| `core/features/FeatureBuilderV1.js` | CLI_ONLY | NONE | Used by `run_build_features_v1.js` |
| `core/read_metrics.js` | UTILITY | NONE | Manual analysis script |
| `core/read_s3_file.js` | UTILITY | NONE | Manual analysis script |
| `core/verify_*.js` | UTILITY | NONE | Manual verification scripts |
| `core/worker/migrate_v3.js` | LEGACY | LOW | Migration complete, can archive |
| `core/strategy/strategies/FaultyStrategy.js` | TEST | NONE | Test fixture |
| `core/strategy/strategies/SlowStrategy.js` | TEST | NONE | Test fixture |
| `core/strategy/strategies/PrintHeadTailStrategy.js` | TEST | NONE | Test fixture |
| `core/research/ResearchRunner.js` | TEST_ONLY | LOW | Only test-research.js uses it |
| `core/research/samplers/*.js` | TEST_ONLY | LOW | Research utilities |
| `core/release/ConfigCheck.js` | SUBPROCESS | NONE | Spawned by go-live-check.js |
| `core/release/SelfTest.js` | SUBPROCESS | NONE | Spawned by go-live-check.js |

---

# SECTION 7 — PHASE MAP (ENGINEERING ROADMAP FRAME)

| Phase | Description | Status | Blocking Gaps |
|-------|-------------|--------|---------------|
| **Phase 0** | Data Integrity & Replay Determinism | **STABLE** | None |
| **Phase 1** | Strategy Runtime Core | **STABLE** | None |
| **Phase 2** | Safety & Guard Systems | **STABLE** | PromotionGuard/BudgetManager not in main service path |
| **Phase 3** | ML Advisory Layer | **PARTIAL** | Advisory-only, no autonomous trading |
| **Phase 4** | Live Trading Orchestration | **EXPERIMENTAL** | LiveStrategyRunner not integrated into strategyd |
| **Phase 5** | Operations, Monitoring, Cost Control | **PARTIAL** | Observer API separate from observer module |

## Phase Details

### Phase 0 — Data Integrity & Replay Determinism
- **Status**: STABLE
- **Key Files**: `ORDERING_CONTRACT.js`, `ParquetReader.js`, `CursorCodec.js`
- **Guarantees**: `ORDER BY ts_event ASC, seq ASC` enforced at query level
- **Blocking Gaps**: None

### Phase 1 — Strategy Runtime Core
- **Status**: STABLE
- **Key Files**: `StrategyRuntime.js`, `StateSerializer.js`, `ExecutionEngine.js`
- **Guarantees**: Deterministic state, hash verification, audit trail
- **Blocking Gaps**: None

### Phase 2 — Safety & Guard Systems
- **Status**: STABLE (core), EXPERIMENTAL (live guards)
- **Key Files**: `OrderingGuard.js`, `DeterminismValidator.js`, `ErrorContainment.js`
- **Blocking Gaps**: `PromotionGuardManager` and `RunBudgetManager` only used by `LiveStrategyRunner`, not `RuntimeAdapterV2`

### Phase 3 — ML Advisory Layer
- **Status**: PARTIAL
- **Key Files**: `XGBoostModel.js`, `MLDecisionAdapter.js`, `MLActiveGate.js`
- **Current State**: Weight adjustment on signals, not autonomous decisions
- **Blocking Gaps**: No direct trade generation from ML output

### Phase 4 — Live Trading Orchestration
- **Status**: EXPERIMENTAL
- **Key Files**: `LiveStrategyRunner.js`, `LiveWSConsumer.js`, `LiveEventSequencer.js`
- **Current State**: Code exists, tested, but not wired into main service
- **Blocking Gaps**: `services/strategyd/server.js` uses `SSEStrategyRunner`/`RuntimeAdapterV2`, not `LiveStrategyRunner`

### Phase 5 — Operations, Monitoring, Cost Control
- **Status**: PARTIAL
- **Key Files**: `AuditWriter.js`, `ObserverRegistry.js`, `core/scheduler/*.js`
- **Current State**: Audit trail works, scheduler active
- **Blocking Gaps**: `core/observer/` and `core/observer-api/` are separate implementations

---

# SECTION 8 — CRITICAL DEPENDENCY MAP

## System Spine (Failure = System Collapse)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CRITICAL DEPENDENCY CHAIN                             │
└─────────────────────────────────────────────────────────────────────────────┘

Level 0 (Foundation):
  ├── ORDERING_CONTRACT.js     ← Determinism source of truth
  ├── StateSerializer.js       ← Canonical serialization (BigInt-safe)
  └── DeterminismValidator.js  ← Hash computation

Level 1 (Data Access):
  ├── ParquetReader.js         ← DuckDB query interface
  ├── MetaLoader.js            ← Dataset metadata
  └── ReplayCache.js           ← LRU caching

Level 2 (Orchestration):
  ├── ReplayEngine.js          ← Event streaming
  ├── StrategyRuntime.js       ← Strategy lifecycle
  └── ExecutionEngine.js       ← Order execution

Level 3 (Safety):
  ├── OrderingGuard.js         ← Monotonicity enforcement
  └── ErrorContainment.js      ← Exception handling

Level 4 (Services):
  ├── RuntimeAdapterV2.js      ← Service-level orchestration
  ├── SSEStrategyRunner.js     ← SSE event processing
  └── authMiddlewareFactory.js ← Auth for all endpoints

Level 5 (Persistence):
  ├── AuditWriter.js           ← Audit trail
  └── RunArchiveWriter.js      ← S3 archival
```

## External Dependencies

| Dependency | Used By | Failure Impact |
|------------|---------|----------------|
| **DuckDB** | ParquetReader | Cannot query parquet files |
| **AWS S3 SDK** | MetaLoader, RunArchiveWriter, AuditWriter | No S3 access |
| **Fastify** | All services | No HTTP endpoints |
| **PyArrow** | collector/writer.py | No data persistence |
| **asyncio** | collector/*.py | Collector halts |

---

# SECTION 9 — NEXT ENGINEERING PRIORITIES (EVIDENCE-BASED)

## Priority 1: Risk Management Integration

**Finding**: `core/risk/RiskManager.js` with 4 rule implementations exists but has zero runtime imports.

**Evidence**:
- File exists: `core/risk/RiskManager.js` (methods: onEvent, checkForExit, allow)
- Grep for imports: Zero results in StrategyRuntime, RuntimeAdapterV2, SSEStrategyRunner
- Rules defined: MaxPositionRule, CooldownRule, MaxDailyLossRule, StopLossTakeProfitRule

**System Risk**: HIGH — Live trading without position limits or stop-loss

**Engineering Action**: Integrate RiskManager into StrategyRuntime.#placeOrder() or RuntimeAdapterV2

---

## Priority 2: Live Trading Path Activation

**Finding**: LiveStrategyRunner is complete but not connected to strategyd service.

**Evidence**:
- `core/strategy/live/LiveStrategyRunner.js` imports PromotionGuardManager, RunBudgetManager
- `services/strategyd/server.js` imports SSEStrategyRunner, RuntimeAdapterV2
- No import of LiveStrategyRunner in any service

**System Risk**: MEDIUM — Live guards (promotion, budget) not active in production path

**Engineering Action**: Create route in strategyd that uses LiveStrategyRunner for live-mode runs

---

## Priority 3: Dual Observer Implementation

**Finding**: Two separate observer implementations exist.

**Evidence**:
- `core/observer/index.js` — Express app with ObserverRegistry
- `core/observer-api/index.ts` — TypeScript Express app (systemd service)
- Different auth middleware, different routes

**System Risk**: LOW — Confusion, maintenance burden

**Engineering Action**: Consolidate or clearly document which is authoritative

---

## Priority 4: Test File Breakage

**Finding**: Two test files broken by dead export removal.

**Evidence**:
- `core/strategy/state/StrategyStateContainer.test.js` imports removed `createStateContainerFactory`
- `core/promotion/test-promotion-guard.js` imports removed `logDecision`, `PROMOTION_RULES`

**System Risk**: LOW — Tests fail, not runtime

**Engineering Action**: Update test files to not rely on removed exports, or restore exports as test-only

---

# SECTION 10 — AGENT OPERATING INSTRUCTIONS

## MUST NOT Modify Blindly

| File/Module | Reason |
|-------------|--------|
| `core/replay/ORDERING_CONTRACT.js` | Determinism source of truth. Change requires migration. |
| `core/strategy/state/StateSerializer.js` | Affects all hash computations. Breaking change = non-reproducible runs. |
| `core/strategy/safety/DeterminismValidator.js` | Hash algorithm changes break audit verification. |
| `core/execution/engine.js` | Fill logic must remain deterministic. Same input = same output. |
| `collector/writer.py` | Atomic write protocol (fsync + rename) is critical for data durability. |

## Determinism Constraints

1. **Event Ordering**: All replay must follow `ORDER BY ts_event ASC, seq ASC`. This is defined in `ORDERING_CONTRACT.js` and must not change without version bump.

2. **State Serialization**: `canonicalStringify` sorts keys alphabetically and converts BigInt to `"123n"` format. All state hashing depends on this.

3. **Execution Engine**: Fills are deterministic — BUY fills at ask, SELL fills at bid, same-tick execution. No randomness allowed.

4. **Cursor Encoding**: `CursorCodec.js` encodes `{v, ts_event, seq}` as base64 JSON. Format changes break resume capability.

## Intentional Duplication

| Pattern | Reason |
|---------|--------|
| RuntimeConfig (2 files) | Core = strategy execution. Service = ML gating. Different domains. |
| FeatureBuilder (V1/V2) | V1 = deterministic batch with labels. V2 = streaming real-time. Different use cases. |
| Clock implementations | Polymorphic timing control, not duplication. |

## Safe Experimentation Zones

| Area | Constraint |
|------|------------|
| `core/research/` | Not in production path. Free to experiment. |
| `core/strategy/strategies/*.js` | Test strategies only. Safe to modify for testing. |
| `services/*/routes/*.js` | API routes can be added. Don't modify auth flow. |
| `tools/*.js` | Verification scripts. Safe to add/modify. |
| `core/scheduler/test-*.js` | Test files. Safe to modify. |

## Before Modifying Any Safety Code

1. **Read the guard's test file** to understand expected behavior
2. **Check import graph** — who depends on this module?
3. **Verify determinism** — does change affect hash computation?
4. **Run existing tests** — `node <module>.test.js` if exists

## Service Startup Dependency Order

```
1. replayd       (no dependencies)
2. strategyd     (depends on replayd for SSE)
3. featurexd     (depends on replayd for replay)
4. backtestd     (depends on strategyd, replayd)
5. labeld        (depends on featurexd output)
6. observer-api  (standalone)
7. console-ui    (frontend, depends on all APIs)
```

---

# APPENDIX A — FILE REFERENCE

## Entry Points
- `services/strategyd/server.js` — Strategy execution service
- `services/replayd/server.js` — Replay streaming service
- `services/featurexd/server.js` — Feature extraction service
- `services/backtestd/server.js` — Backtesting service
- `services/labeld/server.js` — Label generation service
- `core/index.js` — Core Express API
- `core/worker/job_worker.js` — Background job worker
- `core/observer-api/index.ts` — Observer API service
- `collector/collector.py` — Data collector
- `core/compressor/run.py` — Daily compaction job

## Critical Contracts
- `core/replay/ORDERING_CONTRACT.js` — Event ordering definition
- `core/strategy/state/StateSerializer.js` — Canonical serialization
- `core/strategy/interface/types.js` — Type definitions

## Safety Modules
- `core/strategy/safety/OrderingGuard.js`
- `core/strategy/safety/DeterminismValidator.js`
- `core/strategy/safety/ErrorContainment.js`
- `core/strategy/guards/PromotionGuardManager.js`
- `core/strategy/limits/RunBudgetManager.js`

---

# APPENDIX B — ENVIRONMENT VARIABLES

## Collector
- `SPOOL_DIR` — Local parquet output directory
- `STORAGE_BACKEND` — Storage type (default: "spool")

## Replay
- `S3_COMPACT_ENDPOINT` — S3/Minio endpoint for compact data
- `S3_COMPACT_ACCESS_KEY` — S3 access key
- `S3_COMPACT_SECRET_KEY` — S3 secret key
- `S3_COMPACT_REGION` — S3 region (default: us-east-1)

## Archive
- `RUN_ARCHIVE_ENABLED` — Enable run archiving (1/0)
- `RUN_ARCHIVE_S3_BUCKET` — Archive bucket name
- `RUN_ARCHIVE_S3_ENDPOINT` — Archive S3 endpoint
- `RUN_ARCHIVE_S3_ACCESS_KEY` — Archive access key
- `RUN_ARCHIVE_S3_SECRET_KEY` — Archive secret key

## Services
- `STRATEGYD_TOKEN` — Auth token for strategyd
- `REPLAYD_TOKEN` — Auth token for replayd
- `FEATUREXD_TOKEN` — Auth token for featurexd
- `BACKTESTD_TOKEN` — Auth token for backtestd
- `LABELD_TOKEN` — Auth token for labeld
- `AUTH_REQUIRED` — Enable auth (default: true, set "false" to disable)

---

*Document generated: 2026-02-03*
*Source: Forensic code analysis of quantlab-backend repository*
*Status: FROZEN SYSTEM STATE — Do not modify without version bump*
