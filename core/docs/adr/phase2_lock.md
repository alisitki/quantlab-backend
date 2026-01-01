# QuantLab Phase-2: Futures Engineering — Locked Design (ADR)

## SECTION 1 — PURPOSE

QuantLab Phase-2 represents the formal engineering foundation for futures trading capabilities within the QuantLab ecosystem. Its primary purpose is to establish a rigorous, deterministic, and safety-first infrastructure specifically for USDT-margined perpetual futures.

By design, Phase-2 is strictly INACTIVE. Its existence is dedicated to the elimination of futures-specific risks—such as excessive leverage, liquidation proximity, and funding toxicity—before any code is permitted to interact with live exchange environments. Phase-2 prepares the logic for futures execution without ever engaging in it.

## SECTION 2 — SCOPE (WHAT IS INCLUDED)

Phase-2 covers the following four foundational layers:

### Futures Canary & Kill-Switch (Phase-2.1)
Responsibility: Enforces hard structural limits and provides emergency termination mechanisms for futures intent evaluation.
Safety Guarantee: Structural blocks prevent any intent from transitioning to a live state, while global and symbol-specific kill-switches provide an override tier that supersedes all other logic.

### Leverage & Liquidation-Aware Sizing (Phase-2.2)
Responsibility: Calculates position quantity based on a maximum allowed percentage of account equity and market volatility.
Safety Guarantee: Position sizing is computed such that the estimated liquidation price is mathematically guaranteed to fall beyond the specified stop-loss price.

### Funding & Hold-Time Guards (Phase-2.3)
Responsibility: Estimates cumulative funding costs based on expected holding duration and current funding snapshots.
Safety Guarantee: Any intent where the projected funding impact exceeds the predefined equity budget or falls into a toxic rate category is rejected at the gate.

### Exchange Adapter (Phase-2.4)
Responsibility: Performs pure mapping of internal order intents to exchange-formatted JSON payloads (Binance-style).
Safety Guarantee: The adapter is strictly a data-transformation layer with no network capabilities, and it is structurally programmed to throw a critical error if a "LIVE" mode intent is encountered.

## SECTION 3 — EXPLICIT NON-GOALS (CRITICAL)

Phase-2 is defined by what it does not do. The following activities are explicitly out of scope:

- No live trading execution.
- No HTTP or WebSocket exchange API calls.
- No automated cron jobs or scheduler integration.
- No margin borrowing or debt management.
- No funding settlement or PnL realization.
- No active position tracking or management.
- No retry logic, error recovery, or order state synchronization.

Any code enabling the above is OUT OF SCOPE for Phase-2.

## SECTION 4 — SAFETY GUARANTEES (HARD CLAIMS)

The following guarantees have been formally verified through deterministic unit testing:

- Guaranteed: LIVE mode is structurally unreachable across all pipeline layers.
- Guaranteed: reduceOnly is enforced as true for all futures intents to prevent unintended position opening.
- Guaranteed: Worst-case loss is capped by a strict policy percentage of total equity.
- Guaranteed: Estimated liquidation price never precedes the stop-loss price for any calculated position.
- Guaranteed: Cumulative funding cost is bounded by a predefined budget percentage.
- Guaranteed: All outputs are deterministic and hash-stable across repeated evaluations.

## SECTION 5 — WHY PHASE-2 IS LOCKED

Phase-2 must remain inactive to prevent the inherent volatility and complexity of perpetual futures from affecting system stability. Futures amplify mistakes through leverage and liquidation mechanics; therefore, any implementation must be fully hardened and validated against historical and synthetic data before live interaction is considered. Just testing live is strictly forbidden to protect capital and system integrity.

## SECTION 6 — CONDITIONS TO UNLOCK (PHASE-3 GATE)

Phase-2 MAY ONLY be unlocked if ALL of the following conditions are true:

- Explicit operator decision following a full security audit.
- Canary-live bridge design is reviewed and approved.
- Global and symbol-level kill-switches are verified in a production-ready environment.
- Maximum position size for initial live tests is hard-capped at a minimal nominal value.
- Execution requires a manual per-trade trigger; automation remains disabled.
- Deployment is restricted to a single, high-liquidity symbol.
- Full observability via OPS events and real-time telemetry is active.

Unlocking Phase-2 without satisfying these conditions is a protocol violation.

## SECTION 7 — FINAL LOCK STATEMENT

Phase-2 is COMPLETE and LOCKED. Any live futures execution belongs to Phase-3 and beyond.

Date: 2025-12-31
Phase Identifier: PHASE-2-FUTURES-SAFETY-LOCKED
Status: LOCKED / INACTIVE
