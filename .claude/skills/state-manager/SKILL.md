---
name: state-manager
description: SYSTEM_STATE.json update policy and state management rules
---

# State Manager

This skill covers the `SYSTEM_STATE.json` update policy.

## Core Principle

`SYSTEM_STATE.json` is the **single source of truth** for system status.

All sessions must:
1. **Read it before work** — Check current phase
2. **Update it after completing tasks** — If status changes

---

## Current State Structure

```json
{
  "system_state_version": "v1.0",
  "last_updated": "2026-02-03",
  "current_phase": 2,
  "phase_status": {
    "phase_0_data_integrity": "STABLE",
    "phase_1_strategy_runtime": "STABLE",
    "phase_2_safety_guards": "STABLE",
    "phase_3_ml_advisory": "PARTIAL",
    "phase_4_live_trading": "EXPERIMENTAL",
    "phase_5_ops_monitoring": "PARTIAL"
  },
  "risk_layer": "NOT_INTEGRATED",
  "live_trading_path": "INACTIVE",
  "ml_mode": "ADVISORY_ONLY"
}
```

---

## When to Update

| Field | Update When |
|-------|-------------|
| `last_updated` | Any update to the file |
| `current_phase` | Primary development focus shifts |
| `phase_status.*` | Phase moves between statuses |
| `risk_layer` | RiskManager integration status changes |
| `live_trading_path` | Live trading activated/deactivated |
| `ml_mode` | ML mode transitions |

---

## Update Rules

### 1. Always Update `last_updated`
Format: `YYYY-MM-DD`

### 2. Never Alter `system_state_version` Unless Schema Changes
Only increment if fields added/removed/renamed.

### 3. Phase Status Values

| Status | Meaning |
|--------|---------|
| `STABLE` | Complete and production-ready |
| `PARTIAL` | Partially implemented |
| `EXPERIMENTAL` | Under development |
| `NOT_STARTED` | No implementation exists |

### 4. Special Fields

| Field | Valid Values |
|-------|--------------|
| `risk_layer` | `NOT_INTEGRATED`, `INTEGRATED`, `ACTIVE` |
| `live_trading_path` | `INACTIVE`, `DRY_RUN`, `ACTIVE` |
| `ml_mode` | `DISABLED`, `ADVISORY_ONLY`, `WEIGHT_ADJUSTMENT`, `AUTONOMOUS` |

---

## Changelog Requirement

When updating, document in commit message:
- What changed
- Why it changed
- What work triggered the change

**Example:**
```
STATE UPDATE: phase_status.phase_2_safety_guards → STABLE
Reason: Completed PromotionGuardManager integration and verification
Triggered by: Safety guards consolidation task
```

---

## Prohibited Updates

**Never do without explicit user instruction:**
- Set `live_trading_path: "ACTIVE"`
- Change `ml_mode` beyond `ADVISORY_ONLY`
- Mark a phase as `STABLE` without verification

---

## Conflict Resolution

If state doesn't match observed reality:

1. **Do NOT silently fix it**
2. Report discrepancy to user
3. Propose the correct state
4. Wait for user confirmation

---

## Quick Commands

```bash
# Read current state
cat SYSTEM_STATE.json | jq .

# Check phase
cat SYSTEM_STATE.json | jq '{phase: .current_phase, status: .phase_status}'

# Validate JSON
cat SYSTEM_STATE.json | jq . > /dev/null && echo "Valid JSON"
```
