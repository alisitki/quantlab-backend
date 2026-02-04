# AI Project Rules

This document defines mandatory operating constraints for all AI agents working on the QuantLab codebase.

---

## MANDATORY AI WORKFLOW

Before writing any code, the AI must:

1. **Read SYSTEM_RUNBOOK.md** — Understand system architecture, entry points, and data flows
2. **Read SYSTEM_STATE.json** — Check current phase and system status
3. **Confirm the system phase** — Verify which phase is active and what work is permitted
4. **Ensure proposed work matches current phase** — Do not implement features ahead of the roadmap
5. **Refuse to modify determinism-critical modules** — Unless explicitly instructed by the user

---

## FORBIDDEN MODIFICATIONS

AI must **never modify** the following files without explicit user instruction:

| File | Reason |
|------|--------|
| `core/replay/ORDERING_CONTRACT.js` | Defines deterministic replay ordering guarantee |
| `core/strategy/state/StateSerializer.js` | Canonical JSON serialization for state integrity |
| `core/strategy/safety/DeterminismValidator.js` | Validates replay determinism |
| `core/execution/ExecutionEngine.js` | Order execution logic |
| `collector/writer.py` | Atomic data write protocol (fsync + rename) |

These modules are **determinism-critical**. Any modification could break replay integrity, state consistency, or data safety.

---

## PHASE GATE

Work must align with the current system phase as defined in `SYSTEM_STATE.json`.

| Phase | Status | AI Permission |
|-------|--------|---------------|
| Phase 0 — Data Integrity | STABLE | Allowed |
| Phase 1 — Strategy Runtime | STABLE | Allowed |
| Phase 2 — Safety Guards | STABLE | Allowed |
| Phase 3 — ML Advisory | PARTIAL | Only advisory ML work (no autonomous trading) |
| Phase 4 — Live Trading | EXPERIMENTAL | **Must not activate live trading without explicit instruction** |
| Phase 5 — Ops & Monitoring | PARTIAL | Allowed |

### Phase 4 Restriction

AI must **refuse** any task that would:

- Enable autonomous live trading
- Remove human approval requirements from execution paths
- Activate the `ACTIVE` gating flag in production
- Bypass `DRY_RUN` mode without explicit instruction

---

## ARCHITECTURE RESPECT

AI must:

- Follow patterns established in `SYSTEM_RUNBOOK.md`
- Not introduce new services without architectural justification
- Not duplicate functionality that already exists in core modules
- Maintain separation between replay (deterministic) and live (real-time) paths

---

## VERIFICATION REQUIREMENT

After any code modification, AI should:

- Verify imports resolve correctly
- Ensure no circular dependencies introduced
- Confirm determinism-critical modules remain unchanged (unless instructed)
- Run relevant verification scripts if available

---

## DOCUMENT MAINTENANCE

If AI work changes system architecture or phase status:

- Update `SYSTEM_STATE.json` per `AI_STATE_UPDATE_POLICY.md`
- Propose updates to `SYSTEM_RUNBOOK.md` if architecture changes

---

*This document is the authoritative source for AI operating constraints in this repository.*
