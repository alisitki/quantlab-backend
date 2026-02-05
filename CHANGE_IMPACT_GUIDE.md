# Change Impact Guide

This document maps critical files to their system-wide impact if modified.

---

## Critical Module Impact Matrix

| File | If Modified, What Breaks? |
|------|---------------------------|
| `core/replay/ORDERING_CONTRACT.js` | Replay determinism, cursor resume, twin-run verification, all hash comparisons |
| `core/strategy/state/StateSerializer.js` | State hashing, fills hashing, run ID generation, archive integrity |
| `core/execution/engine.js` | Fill prices, fill timing, position tracking, all backtest results |
| `core/replay/CursorCodec.js` | Resume from cursor, SSE streaming resume, checkpoint restoration |
| `core/strategy/runtime/StrategyRuntime.js` | All strategy execution, event processing, lifecycle management |
| `core/replay/ReplayEngine.js` | Event streaming, batch pagination, multi-partition replay |
| `core/common/authMiddlewareFactory.js` | Authentication on all 5 Fastify services |
| `collector/writer.py` | Data integrity, spool file format, fsync protocol |

---

## Detailed Impact Analysis

### ORDERING_CONTRACT.js

**Location:** `core/replay/ORDERING_CONTRACT.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change `ORDERING_COLUMNS` | All cursors become incompatible; resume fails |
| Change `SQL_ORDER_CLAUSE` | Replay returns events in different order; determinism broken |
| Change `ORDERING_VERSION` | All existing cursors rejected as version mismatch |
| Change `compareOrdering()` | Event validation logic changes; ordering violations undetected |
| Change `buildCursorWhereClause()` | Resume logic changes; events skipped or duplicated |

**Consumers:**
- `CursorCodec.js` (cursor version validation)
- `ReplayEngine.js` (SQL clause)
- `OrderingGuard.js` (ordering comparison)
- `DeterminismValidator.js` (ordering validation)

---

### StateSerializer.js

**Location:** `core/strategy/state/StateSerializer.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change key sorting | All hashes change; twin-run verification fails |
| Change BigInt serialization | BigInt values hash differently; state mismatch |
| Change null/undefined handling | Sparse objects hash differently |
| Change `canonicalStringify()` | Every hash in the system changes |

**Consumers:**
- `DeterminismValidator.js` (hash computation)
- `StrategyRuntime.js` (decision hash)
- `RunArchiveWriter.js` (archive serialization)
- `FeatureOrchestrator.js`, `BacktestOrchestrator.js`, `LabelOrchestrator.js`

**Critical Invariant:**
```javascript
canonicalStringify(canonicalParse(x)) === x
```
Breaking this invariant breaks all determinism guarantees.

---

### ExecutionEngine (engine.js)

**Location:** `core/execution/engine.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change fill price logic | All backtest P&L changes; strategy comparisons invalid |
| Change fee calculation | All fee-sensitive metrics change |
| Change counter reset | Fill IDs non-deterministic; hash verification fails |
| Remove BBO validation | Strategies can fill at invalid prices |
| Change same-tick semantics | Latency model changes; all results differ |

**Consumers:**
- `StrategyRuntime.js` (via `attachExecutionEngine`)
- `SSEStrategyRunner.js` (via runtime)
- `LiveStrategyRunner.js` (via runtime)

**Current Semantics (v1 - DO NOT CHANGE):**
- Zero latency (same-tick)
- Zero slippage (exact BBO)
- BUY fills at ask, SELL fills at bid
- Counter-based deterministic IDs

---

### ExecutionState (state.js)

**Location:** `core/execution/state.js`

**Modified:** 2026-02-05 (Memory optimization update)

**If Modified:**

| Change | Impact |
|--------|--------|
| Change maxDD calculation | Backtest metrics differ; A/B comparisons invalid |
| Change fills storage format | Metrics computation breaks; fills hash changes |
| Modify snapshot() structure | Consumers expecting old fields break |
| Remove backward compatibility | Legacy backtests can't run |
| Change BigInt serialization | Timestamp precision loss; hash mismatch |

**Recent Changes (2026-02-05):**

1. **Streaming MaxDrawdown (O(1) Memory)**
   - Added: `#peakEquity`, `#maxDrawdown`, `#equityHistory` private fields
   - Removed: `equityCurve` array (legacy mode still supported)
   - Impact: 88.8 MB → 24 bytes (99.99% reduction)
   - Accuracy: 0% loss (exact calculation)

2. **Fills Streaming (Disk-Backed)**
   - Added: `#fillsStream`, `#streamingFills` private fields
   - Added: `FillsStream` module (buffered JSONL writer)
   - Impact: 190 MB → 10 KB (99.5% reduction)
   - Accuracy: 0% loss (exact preservation)

3. **Lazy Snapshot**
   - Added: `snapshot({ deepCopy: false })` option
   - Impact: Eliminates +279 MB temporary allocation
   - Safety: `_immutable` flag signals no mutation allowed

**Feature Flags:**
```javascript
new ExecutionState(initialCapital, {
  streamingMaxDD: true,   // ENV: EXECUTION_STREAMING_MAXDD=1
  streamFills: true,      // ENV: EXECUTION_STREAM_FILLS=1
  fillsStreamPath: '/tmp/fills.jsonl'  // Optional custom path
});
```

**Backward Compatibility:**
- Default behavior unchanged (flags default to OFF)
- Legacy `equityCurve` array still populated when streaming disabled
- `snapshot()` with no options behaves identically to old version
- metrics.js handles both streaming and legacy snapshots

**Consumers:**
- `ExecutionEngine.js` (creates ExecutionState instance)
- `metrics.js` (computes backtest metrics)
- `SSEStrategyRunner.js` (calls snapshot())
- `StrategyRuntime.js` (via ExecutionEngine)

**Critical Invariants:**
```javascript
// Streaming maxDD must match legacy calculation
assert(streamingMaxDD === legacyMaxDrawdown(equityCurve));

// Fills streaming must preserve all data
assert(streamedFills.length === inMemoryFills.length);
assert(hash(streamedFills) === hash(inMemoryFills));

// Snapshot deepCopy=false must not be mutated
snapshot({ deepCopy: false })._immutable === true;
```

**Testing:**
```bash
# Verify 0% accuracy loss
node core/execution/tests/test-streaming-maxdd.js        # 6 tests
node core/execution/tests/test-fills-stream.js           # 7 tests
node core/execution/tests/test-fills-streaming-integration.js  # 4 tests
```

**Memory Impact:**
- Baseline: 558 MB (3.7M events, 1.9M fills)
- Optimized: 10 KB (99.998% reduction)
- Peak reduction: 15% (lazy snapshot eliminates copy)

---

### CursorCodec.js

**Location:** `core/replay/CursorCodec.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change encoding format | All existing cursors unparseable |
| Change base64 encoding | SSE cursor strings incompatible |
| Change field validation | Invalid cursors may be accepted |
| Remove BigInt parsing | Precision loss on large timestamps |

**Consumers:**
- `ReplayEngine.js` (cursor decode)
- `services/replayd/routes/stream.js` (cursor parameter)
- `StrategyRuntime.js` (cursor tracking)

---

### StrategyRuntime.js

**Location:** `core/strategy/runtime/StrategyRuntime.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change lifecycle states | Run status tracking breaks |
| Change event processing order | Strategies receive events differently |
| Change context API | All strategies must be updated |
| Change decision tracking | Archive format changes |
| Change run ID generation | Run identification breaks |
| Change checkpoint logic | Resume from checkpoint fails |

**Consumers:**
- `SSEStrategyRunner.js`
- `LiveStrategyRunner.js`
- All strategies via `onEvent(event, context)`

---

### ReplayEngine.js

**Location:** `core/replay/ReplayEngine.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change batch logic | Memory usage changes; OOM risk |
| Change iteration order | Event sequence changes |
| Change meta validation | Invalid datasets may be accepted |
| Change DuckDB queries | Different rows returned |
| Remove cursor enforcement | Resume capability breaks |

**Consumers:**
- `services/replayd/routes/stream.js` (SSE streaming)
- `StrategyRuntime.processReplay()`
- All verification scripts

---

### authMiddlewareFactory.js

**Location:** `core/common/authMiddlewareFactory.js`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change token extraction | All authenticated requests fail |
| Change rate limiting | Rate limits change across all services |
| Change health bypass | Health checks may require auth |
| Change error responses | Client error handling breaks |

**Consumers:**
- `services/strategyd/middleware/auth.js`
- `services/replayd/middleware/auth.js`
- `services/backtestd/middleware/auth.js`
- `services/featurexd/middleware/auth.js`
- `services/labeld/middleware/auth.js`

---

### writer.py

**Location:** `collector/writer.py`

**If Modified:**

| Change | Impact |
|--------|--------|
| Change fsync protocol | Data durability compromised |
| Change atomic rename | Partial files may be read |
| Change partition path | Uploader can't find files |
| Change parquet schema | Downstream readers fail |
| Remove verification | Corrupt files undetected |

**Critical Protocol:**
```python
1. Write to .tmp file
2. fsync the file
3. Verify parquet readable
4. Atomic rename to .parquet
5. fsync parent directory
```

---

## Safe vs Unsafe Changes

### Generally Safe Changes

| Type | Example | Why Safe |
|------|---------|----------|
| Add new field to event | Add `seq` to TradeEvent | Additive, doesn't break existing |
| Add new endpoint | Add `/api/v2/runs` | New endpoint, no existing clients |
| Add optional parameter | Add `?limit=100` | Optional, defaults to current behavior |
| Add logging | Add `console.log` | No logic change |
| Add metrics | Add counter increment | No logic change |

### Unsafe Changes

| Type | Example | Why Unsafe |
|------|---------|------------|
| Change serialization format | Change BigInt → string format | All hashes change |
| Change default values | Change `feeRate` default | All results change |
| Remove field | Remove `seq` from cursor | Backward incompatible |
| Change ordering | Change sort order | Determinism breaks |
| Change timing | Change from same-tick to next-tick | Execution semantics change |

---

## Pre-Modification Checklist

Before modifying any critical file:

- [ ] Is this change explicitly requested by the user?
- [ ] Will this change affect determinism?
- [ ] Will existing cursors/archives remain compatible?
- [ ] What services will need restart?
- [ ] What tests will verify the change?
- [ ] Is the change backward compatible?
- [ ] Is version increment needed?

---

*This guide is derived from import graph analysis and runtime behavior observation.*
