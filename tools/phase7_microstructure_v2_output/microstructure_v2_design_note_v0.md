# Microstructure V2 Design Note

`MICROSTRUCTURE_V2` keeps the original local bybit microstructure trigger, but it no longer treats that trigger as sufficient on its own.

## Core Logic

1. `LAYER 1 — LOCAL TRIGGER`
   Bybit `linkusdt` trade pressure is still computed from the selected V1-style local microstructure cell.

2. `LAYER 2 — SAME-SYMBOL CROSS-EXCHANGE CONFIRMATION`
   External `linkusdt` trade pressure is read from `binance` and `okx`.
   Entry or reversal is allowed only when:
   - local signal is present
   - external alignment count is strong enough
   - venue divergence stays below the configured cap

3. `LAYER 3 — OPTIONAL BTC SUPPORT`
   BTC context is implemented as a secondary gate only.
   It is disabled by default in V2-core smoke.

## Entry Decision

Flat state:
- `LONG` only if local trigger exists and external confirmation passes
- `SHORT` only if local trigger exists and external confirmation passes
- otherwise the strategy stays flat and records an explicit rejection reason

Positioned state:
- reversal only on opposite local trigger plus confirmation
- flat exit still uses the existing simple hysteresis-style neutral-band close
- no broad exit redesign was added in this sprint

## New Ex-Ante Fields

Per-trade opening context now captures:
- `venue_alignment_count`
- `external_alignment_count`
- `external_available_count`
- `venue_divergence_score`
- `venue_divergence_pass`
- `venue_pressure_snapshot`
- `market_support_mode`
- `market_support_flag`
- `entry_decision_reason`

## What Changed vs V1

- V1: local bybit pressure could open directly
- V2: local bybit pressure is only a candidate trigger
- V2 requires same-symbol external venue confirmation first
- BTC is present only as an optional secondary modifier
