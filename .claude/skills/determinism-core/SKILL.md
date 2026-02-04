---
name: determinism-core
description: Replay engine rules, ordering contracts, and determinism guarantees
---

# Determinism Core

This skill covers the deterministic replay infrastructure that is the foundation of QuantLab.

## Core Principle

**Determinism** = Identical inputs ALWAYS produce identical outputs.

QuantLab achieves this through:
- Canonical JSON serialization (sorted keys)
- Fixed event ordering (`ts_event ASC, seq ASC`)
- Counter-based deterministic IDs
- No wall-clock dependencies in replay path

---

## Critical Files

| File | Purpose | Impact if Modified |
|------|---------|-------------------|
| `ORDERING_CONTRACT.js` | Defines ordering guarantee | All cursors incompatible, resume fails |
| `StateSerializer.js` | Canonical JSON stringify | All hashes change, twin-run fails |
| `CursorCodec.js` | Cursor encode/decode | Existing cursors unparseable |
| `DeterminismValidator.js` | Twin-run comparison | Validation logic changes |

---

## ORDERING_CONTRACT.js

Location: `core/replay/ORDERING_CONTRACT.js`

Defines the **single source of truth** for event ordering:

```javascript
const ORDERING_COLUMNS = ['ts_event', 'seq'];
const SQL_ORDER_CLAUSE = 'ORDER BY ts_event ASC, seq ASC';
const ORDERING_VERSION = 1;
```

### Never Change

| Field | Consequence |
|-------|-------------|
| `ORDERING_COLUMNS` | All cursors become incompatible |
| `SQL_ORDER_CLAUSE` | Replay returns different order |
| `ORDERING_VERSION` | Existing cursors rejected |
| `compareOrdering()` | Ordering violations undetected |
| `buildCursorWhereClause()` | Events skipped or duplicated |

---

## Cursor Format

Cursors encode position in event stream for resume capability.

**Format:** Base64-encoded JSON
```json
{"v":1,"ts_event":"1234567890123456789","seq":"1"}
```

**Key Properties:**
- `ts_event` and `seq` stored as **strings** (BigInt precision)
- Version (`v`) for compatibility checking
- **Exclusive** — resume starts AFTER the cursor position

---

## Twin-Run Verification

Process of running a strategy twice and comparing hashes.

```javascript
const run1 = {
    stateHash: computeStateHash(runtime1.getSnapshot()),
    fillsHash: computeFillsHash(runtime1.getSnapshot().fills),
    eventCount: runtime1.state.eventCount
};

const result = compareTwinRuns(run1, run2);
// result.match === true means determinism verified
```

**Pass Criteria:**
- `stateHash` matches
- `fillsHash` matches
- `eventCount` matches

---

## Canonical Serialization

`StateSerializer.js` provides deterministic JSON:

```javascript
import { canonicalStringify, canonicalParse } from './StateSerializer.js';

// Rules:
// - Keys sorted alphabetically
// - BigInt → "123n" string
// - undefined omitted
// - null preserved
```

**Critical Invariant:**
```javascript
canonicalStringify(canonicalParse(x)) === x
```

Breaking this breaks ALL determinism guarantees.

---

## Verification Commands

```bash
# Full determinism verification
node core/replay/tools/verify-determinism.js

# Replay validation (ordering check)
node core/replay/tools/verify-v1.js

# Cursor resume test
node core/replay/tools/verify-resume.js
```

---

## Safe vs Unsafe Changes

### Safe
- Add new field to event (additive)
- Add logging
- Add metrics

### Unsafe
- Change serialization format
- Change default values
- Remove fields
- Change ordering
- Change timing semantics
