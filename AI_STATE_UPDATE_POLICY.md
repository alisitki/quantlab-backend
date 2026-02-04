# AI State Update Policy

This document defines when and how AI agents must update `SYSTEM_STATE.json`.

---

## CORE PRINCIPLE

`SYSTEM_STATE.json` is the **single source of truth** for system status.

All AI sessions must read it before work and update it after completing engineering tasks that change system status.

---

## WHEN TO UPDATE

After any completed engineering task, AI must evaluate whether the following changed:

| Field | Update When |
|-------|-------------|
| `last_updated` | Any update to the file |
| `current_phase` | Primary development focus shifts to a new phase |
| `phase_status.*` | A phase moves between PARTIAL → STABLE or EXPERIMENTAL → PARTIAL |
| `risk_layer` | RiskManager becomes integrated into runtime |
| `live_trading_path` | Live trading path is activated or deactivated |
| `ml_mode` | ML transitions from ADVISORY_ONLY to another mode |

---

## UPDATE RULES

### 1. Always Update `last_updated`

When modifying `SYSTEM_STATE.json`, set `last_updated` to the current date in `YYYY-MM-DD` format.

### 2. Never Alter `system_state_version` Unless Architecture Changes

The version field tracks structural changes to the state schema itself, not content updates.

Only increment version if:
- New fields are added to the schema
- Fields are removed or renamed
- The meaning of existing fields changes

### 3. Phase Status Values

Use only these status values:

| Status | Meaning |
|--------|---------|
| `STABLE` | Complete and production-ready |
| `PARTIAL` | Partially implemented, some components working |
| `EXPERIMENTAL` | Under development, not production-ready |
| `NOT_STARTED` | No implementation exists |

### 4. Boolean-Like Fields

For fields like `risk_layer`, `live_trading_path`, `ml_mode`:

| Field | Valid Values |
|-------|--------------|
| `risk_layer` | `NOT_INTEGRATED`, `INTEGRATED`, `ACTIVE` |
| `live_trading_path` | `INACTIVE`, `DRY_RUN`, `ACTIVE` |
| `ml_mode` | `DISABLED`, `ADVISORY_ONLY`, `WEIGHT_ADJUSTMENT`, `AUTONOMOUS` |

---

## CHANGELOG REQUIREMENT

When updating `SYSTEM_STATE.json`, AI must add an entry to the commit message or session summary documenting:

- What changed
- Why it changed
- What work triggered the change

Example:

```
STATE UPDATE: phase_status.phase_2_safety_guards → STABLE
Reason: Completed PromotionGuardManager integration and verification
Triggered by: Safety guards consolidation task
```

---

## PROHIBITED UPDATES

AI must **not** update `SYSTEM_STATE.json` to:

- Activate `live_trading_path: "ACTIVE"` without explicit user instruction
- Change `ml_mode` beyond `ADVISORY_ONLY` without explicit user instruction
- Mark a phase as `STABLE` without verification that all components work

---

## CONFLICT RESOLUTION

If AI detects that `SYSTEM_STATE.json` does not match observed system reality:

1. **Do not silently fix it**
2. Report the discrepancy to the user
3. Propose the correct state
4. Wait for user confirmation before updating

---

*This policy ensures persistent, accurate system state across all AI sessions.*
