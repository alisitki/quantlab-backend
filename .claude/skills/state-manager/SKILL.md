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

## Current State Structure (v2.0)

```json
{
  "system_state_version": "v2.0",
  "last_updated": "2026-02-04",
  "current_phase": 4,
  "phase_status": {
    "phase_0_data_integrity": "STABLE",
    "phase_1_strategy_runtime": "STABLE",
    "phase_2_safety_guards": "STABLE",
    "phase_3_ml_advisory": "STABLE",
    "phase_4_live_trading": "READY",
    "phase_5_ops_monitoring": "STABLE"
  },
  "alpha_layer": {
    "status": "ACTIVE",
    "current_focus": "feature_layer",
    "feature_development": { "status": "IN_PROGRESS", "live_features": [...], "pending_features": [...] },
    "strategy_development": { "status": "PENDING", "production_strategy": "BaselineStrategy" },
    "ml_integration": { "status": "ADVISORY_ACTIVE", "confidence_exposed": false },
    "validation": { "decision_logging": false, "feature_logging": false }
  },
  "infrastructure": {
    "status": "COMPLETE",
    "collectors": "STABLE",
    "replay_engine": "STABLE",
    "exchange_bridges": "STABLE"
  },
  "risk_layer": "INTEGRATED",
  "live_trading_path": "READY",
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
| `live_trading_path` | `INACTIVE`, `DRY_RUN`, `READY`, `ACTIVE` |
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

## Alpha Layer State

v2.0 introduces `alpha_layer` section for tracking signal development:

| Field | Values | Purpose |
|-------|--------|---------|
| `alpha_layer.status` | `NOT_STARTED`, `ACTIVE`, `COMPLETE` | Overall alpha development status |
| `alpha_layer.current_focus` | `feature_layer`, `strategy_upgrade`, `ml_integration`, `validation` | Current task group |
| `alpha_layer.feature_development.status` | `NOT_STARTED`, `IN_PROGRESS`, `COMPLETE` | Feature work status |
| `alpha_layer.strategy_development.status` | `PENDING`, `IN_PROGRESS`, `COMPLETE` | Strategy work status |
| `alpha_layer.ml_integration.status` | `DISABLED`, `ADVISORY_ACTIVE`, `CONFIDENCE_EXPOSED`, `SCALING_ENABLED` | ML integration level |

## Infrastructure State

`infrastructure` section tracks infra completion:

| Field | Values |
|-------|--------|
| `infrastructure.status` | `PARTIAL`, `COMPLETE` |
| `infrastructure.collectors` | `STABLE`, `DEGRADED` |
| `infrastructure.replay_engine` | `STABLE`, `DEGRADED` |
| `infrastructure.exchange_bridges` | `STABLE`, `DEGRADED` |

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
