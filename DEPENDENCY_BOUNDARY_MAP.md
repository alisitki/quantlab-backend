# Dependency Boundary Map

This document classifies repository areas by modification risk level.

---

## Risk Classification

| Zone | Risk Level | Description |
|------|------------|-------------|
| **Determinism Core** | CRITICAL | Modifications break replay integrity |
| **Strategy Runtime** | HIGH | Modifications affect all strategies |
| **Safety Guards** | HIGH | Modifications affect safety guarantees |
| **Execution Engine** | HIGH | Modifications affect fill semantics |
| **Service Routes** | MEDIUM | Modifications affect API contracts |
| **Scheduler Scripts** | LOW | Isolated batch jobs |
| **Tools** | LOW | Standalone utilities |
| **Research** | SAFE | Experimental, no runtime impact |
| **Documentation** | SAFE | No runtime impact |

---

## CRITICAL Zone

**Files in this zone are determinism-critical. Any modification can break replay integrity, twin-run verification, and state hashing.**

| File | Purpose | Why Critical |
|------|---------|--------------|
| `core/replay/ORDERING_CONTRACT.js` | Event ordering rules | Defines global event order (ts_event, seq) |
| `core/strategy/state/StateSerializer.js` | Canonical JSON serialization | Deterministic hashing depends on this |
| `core/strategy/safety/DeterminismValidator.js` | Hash computation | State fingerprinting for twin-run |
| `core/replay/CursorCodec.js` | Cursor encoding/decoding | Resume capability |
| `collector/writer.py` | Atomic write protocol | Data integrity guarantee |

### Modification Rules

1. **NEVER** modify without explicit instruction
2. Any change requires twin-run verification
3. Version numbers must be incremented
4. Changes must be backward compatible

---

## HIGH Risk Zone

**Files in this zone affect runtime behavior across all strategies and services.**

### Strategy Runtime

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `core/strategy/runtime/StrategyRuntime.js` | Main orchestrator | All strategy execution affected |
| `core/strategy/runtime/RuntimeContext.js` | Context object | Strategy API contract changes |
| `core/strategy/runtime/RuntimeState.js` | State management | Snapshot/checkpoint breaks |
| `core/strategy/runtime/RuntimeLifecycle.js` | Lifecycle state machine | Run status transitions break |

### Execution Engine

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `core/execution/engine.js` | Execution engine | Fill semantics change |
| `core/execution/fill.js` | Fill model | Fill format changes |
| `core/execution/order.js` | Order model | Order format changes |
| `core/execution/state.js` | Execution state | Position tracking breaks |

### Safety Guards

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `core/strategy/safety/OrderingGuard.js` | Event ordering enforcement | Ordering violations undetected |
| `core/strategy/safety/ErrorContainment.js` | Error policy | Error handling changes |
| `core/strategy/guards/PromotionGuardManager.js` | Live trading guards | Safety checks bypassed |
| `core/strategy/limits/RunBudgetManager.js` | Budget enforcement | Budget limits ineffective |

### Replay Engine

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `core/replay/ReplayEngine.js` | Event streaming | Replay behavior changes |
| `core/replay/ParquetReader.js` | Parquet reading | Data loading changes |
| `core/replay/MetaLoader.js` | Meta.json loading | Metadata handling changes |

---

## MEDIUM Risk Zone

**Files in this zone affect specific services or APIs.**

### Service Routes

| Directory | Purpose | Impact if Modified |
|-----------|---------|-------------------|
| `services/strategyd/routes/*.js` | Strategyd API | Client integrations break |
| `services/replayd/routes/*.js` | Replayd API | SSE clients break |
| `services/backtestd/routes/*.js` | Backtestd API | Backtest workflow breaks |
| `services/featurexd/routes/*.js` | Featurexd API | Feature extraction breaks |
| `services/labeld/routes/*.js` | Labeld API | Label generation breaks |

### Auth Middleware

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `core/common/authMiddlewareFactory.js` | Shared auth | All service auth affected |
| `services/*/middleware/auth.js` | Service auth config | Specific service auth affected |

### Service Servers

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `services/*/server.js` | Service entry points | Service startup/shutdown affected |
| `services/*/config.js` | Service configuration | Service behavior changes |

---

## LOW Risk Zone

**Files in this zone are isolated and have limited blast radius.**

### Scheduler Scripts

| Directory | Purpose | Impact if Modified |
|-----------|---------|-------------------|
| `core/scheduler/*.js` | Batch job scripts | Only affects scheduled jobs |
| `core/worker/job_worker.js` | Job worker | Only affects job processing |

### Tools

| Directory | Purpose | Impact if Modified |
|-----------|---------|-------------------|
| `tools/*.js` | CLI utilities | Only affects specific tool |
| `core/replay/tools/*.js` | Verification scripts | Only affects verification |

### Collector Components

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `collector/config.py` | Collector config | Only affects collector |
| `collector/status_api.py` | Status API | Only affects metrics |
| `collector/*_handler.py` | Exchange handlers | Only affects specific exchange |

---

## SAFE Zone

**Files in this zone have no runtime impact.**

### Research

| Directory | Purpose | Safety |
|-----------|---------|--------|
| `core/research/` | Experiment files | Not used in production |
| `services/strategyd/experiments/` | Experiment results | Read-only data |

### Documentation

| File | Purpose | Safety |
|------|---------|--------|
| `*.md` | Documentation | No runtime impact |
| `docs/` | Documentation | No runtime impact |

### Build Artifacts

| Directory | Purpose | Safety |
|-----------|---------|--------|
| `.next/` | Next.js build | Regenerated on build |
| `node_modules/` | Dependencies | Regenerated on install |
| `__pycache__/` | Python cache | Regenerated on run |

---

## Modification Guidelines by Zone

### CRITICAL Zone Checklist

- [ ] Explicit user instruction received
- [ ] Impact analysis completed
- [ ] Version number incremented
- [ ] Twin-run verification planned
- [ ] Backward compatibility verified
- [ ] Documentation updated

### HIGH Zone Checklist

- [ ] Impact on existing strategies assessed
- [ ] API contract changes documented
- [ ] Test coverage verified
- [ ] Rollback plan prepared

### MEDIUM Zone Checklist

- [ ] Client integration impact assessed
- [ ] API versioning considered
- [ ] Service restart planned

### LOW Zone Checklist

- [ ] Single service/tool impact confirmed
- [ ] No cross-service dependencies

### SAFE Zone

- No checklist required
- Modify freely for improvements

---

## Dependency Graph

```
CRITICAL
└── ORDERING_CONTRACT.js
    └── CursorCodec.js
        └── ReplayEngine.js
            └── StrategyRuntime.js
                └── Services (strategyd, backtestd)

└── StateSerializer.js
    └── DeterminismValidator.js
        └── StrategyRuntime.js
        └── RunArchiveWriter.js

└── writer.py
    └── collector.py (not used by Node services)

HIGH
└── StrategyRuntime.js
    └── SSEStrategyRunner.js
    └── LiveStrategyRunner.js (not wired to services)

└── ExecutionEngine
    └── StrategyRuntime.js

MEDIUM
└── authMiddlewareFactory.js
    └── services/*/middleware/auth.js
        └── services/*/server.js

LOW
└── scheduler/*.js (isolated)
└── tools/*.js (isolated)
```

---

*This classification is based on codebase analysis and import graph tracing.*
