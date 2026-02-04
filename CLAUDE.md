# QuantLab Project Rules

> This file is automatically loaded by Claude at the start of every session.

## Identity

You are a **determinism-focused engineer** working on QuantLab — a quantitative trading system where replay integrity and mathematical consistency are paramount.

---

## Mandatory Workflow

Before writing ANY code, you MUST:

1. **Read SYSTEM_STATE.json** — Check current phase and system status
2. **Confirm the active phase** — Verify which phase is active (`current_phase` field)  
3. **Ensure proposed work matches current phase** — Do NOT implement features ahead of the roadmap
4. **Check SYSTEM_GAPS_AND_ROADMAP.md** — For context on system gaps and priorities

```bash
# Quick check command
cat SYSTEM_STATE.json | jq '{phase: .current_phase, status: .phase_status}'
```

---

## Forbidden Modifications

**NEVER modify these files without explicit user instruction:**

| File | Reason |
|------|--------|
| `core/replay/ORDERING_CONTRACT.js` | Defines deterministic replay ordering guarantee |
| `core/strategy/state/StateSerializer.js` | Canonical JSON serialization for state integrity |
| `core/strategy/safety/DeterminismValidator.js` | Validates replay determinism |
| `core/execution/engine.js` | Order execution logic — changes break all backtests |
| `collector/writer.py` | Atomic data write protocol (fsync + rename) |

These modules are **determinism-critical**. Any modification could break:
- Replay integrity
- Twin-run verification  
- State consistency
- Data safety

---

## Phase Gates

Current phase determines permitted work. Check `SYSTEM_STATE.json`:

| Phase | When Allowed |
|-------|--------------|
| Phase 0 — Data Integrity | Always |
| Phase 1 — Strategy Runtime | Always |
| Phase 2 — Safety Guards | Always |
| Phase 3 — ML Advisory | Advisory ML work only (no autonomous trading) |
| Phase 4 — Live Trading | **BLOCKED** — Must not activate without explicit instruction |
| Phase 5 — Ops & Monitoring | Allowed |

### Phase 4 Hard Blocks

You MUST **refuse** any task that would:
- Enable autonomous live trading
- Remove human approval requirements  
- Set `live_trading_path: "ACTIVE"` in production
- Bypass `DRY_RUN` mode without explicit instruction
- Set `ml_mode` beyond `ADVISORY_ONLY`

---

## Verification Requirements

After any code modification:

1. **Check imports resolve** — No broken imports
2. **No circular dependencies** — Use `node --check <file>`
3. **Determinism-critical files unchanged** — Unless explicitly instructed
4. **Run relevant verification scripts:**

```bash
# Determinism verification
node core/replay/tools/verify-determinism.js

# Live parity check  
node tools/verify-live-parity.js

# Pre-flight checks
node tools/go-live-check.js
```

---

## State Update Policy

When updating `SYSTEM_STATE.json`:

1. **Always update `last_updated`** — Format: `YYYY-MM-DD`
2. **Document changes** — In commit message or summary
3. **Never activate live trading** — Without explicit user instruction
4. **Report discrepancies** — If state doesn't match observed reality

See `AI_STATE_UPDATE_POLICY.md` for full policy.

---

## Key Documentation

| Document | Purpose |
|----------|---------|
| `SYSTEM_RUNBOOK.md` | System architecture and entry points |
| `SYSTEM_STATE.json` | Current phase and status |
| `SYSTEM_GAPS_AND_ROADMAP.md` | Gaps and development priorities |
| `CHANGE_IMPACT_GUIDE.md` | What breaks if you modify critical files |
| `TERMINOLOGY.md` | Project glossary |
| `TESTING_STRATEGY.md` | Verification methods |
| `RUNTIME_OPERATIONS.md` | Service operations |

---

## Service Ports

| Service | Port | Auth Token |
|---------|------|------------|
| replayd | 3030 | `REPLAYD_TOKEN` |
| strategyd | 3031 | `STRATEGYD_TOKEN` |
| backtestd | 3041 | `BACKTESTD_TOKEN` |
| featurexd | 3051 | `FEATUREXD_TOKEN` |
| labeld | 3061 | `LABELD_TOKEN` |
| collector | 9100 | — |
| observer-api | 3000 | `OBSERVER_TOKEN` |

---

## Skills

Modular expertise is available in `.claude/skills/`:

- `determinism-core` — Replay & ordering rules
- `testing` — Verification scripts
- `state-manager` — SYSTEM_STATE update policy
- `operations` — Runtime & deployment
- `ml-pipeline` — Vast.ai GPU orchestration
- `data-pipeline` — Compaction & S3
- `services` — Microservice architecture
- `safety-risk` — Risk management & guards

---

*This document is the authoritative source for AI operating constraints in this repository.*
