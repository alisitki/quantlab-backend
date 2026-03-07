# Shadow Execution Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- trigger trades
- auto-run anything
- replace cadence, scheduler, or batch policy
- replace deeper execution or PnL analysis

Use it to read the current pack-level execution/PnL surface faster.

## Authoritative Surface

Primary artifact:
- `tools/shadow_state/shadow_execution_pack_summary_v0.json`

Authoritative builder:
- `tools/shadow_execution_pack_summary_v0.py`

This surface is derived from:
- `tools/shadow_state/shadow_execution_ledger_v0.jsonl`

## Field Reading Order

Read pack execution state in this order:

1. `pnl_interpretation`
2. `pnl_attention_flag`
3. `last_pnl_state`
4. `latest_unrealized_sign`, `last_total_unrealized_pnl`
5. `latest_realized_sign`, `last_total_realized_pnl`
6. `last_positions_count`, `last_fills_count`
7. `last_snapshot_present`
8. `last_equity`, `last_max_position_value`

Why this order:
- first read the shortest operator-facing interpretation
- then see whether attention is needed
- then inspect the raw state and signs behind that interpretation
- only then inspect size/equity details if needed

## Field Semantics

### `last_pnl_state`

Current enum:
- `NO_SNAPSHOT`
- `FLAT_NO_FILLS`
- `ACTIVE_POSITION`
- `REALIZED_GAIN`
- `REALIZED_LOSS`
- `REALIZED_FLAT`

Read:
- raw latest state from the persisted ledger

### `latest_realized_sign`

Current enum:
- `GAIN`
- `LOSS`
- `FLAT`
- `UNKNOWN`

Read:
- sign-only summary of `last_total_realized_pnl`

### `latest_unrealized_sign`

Current enum:
- `GAIN`
- `LOSS`
- `FLAT`
- `UNKNOWN`

Read:
- sign-only summary of `last_total_unrealized_pnl`

### `pnl_interpretation`

Current enum:
- `NO_SNAPSHOT`
- `ACTIVE_GAINING`
- `ACTIVE_LOSING`
- `ACTIVE_FLAT`
- `ACTIVE_UNKNOWN`
- `REALIZED_GAIN`
- `REALIZED_LOSS`
- `REALIZED_FLAT`
- `FLAT_NO_FILLS`
- `UNKNOWN`

Read:
- shortest operator-facing interpretation of the latest execution/PnL state

### `pnl_attention_flag`

Values:
- `true`
- `false`

Read:
- `true` means latest execution/PnL state is in an attention bucket:
  - `NO_SNAPSHOT`
  - `ACTIVE_LOSING`
  - `ACTIVE_UNKNOWN`
  - `REALIZED_LOSS`
  - `UNKNOWN`

## Deterministic Interpretation Patterns

1. `NO_SNAPSHOT`
   Read as: latest run did not provide a usable execution snapshot.

2. `FLAT_NO_FILLS`
   Read as: latest run finished flat and no fills were recorded.

3. `ACTIVE_POSITION` + `ACTIVE_GAINING`
   Read as: an active position exists and unrealized PnL is positive.

4. `ACTIVE_POSITION` + `ACTIVE_LOSING`
   Read as: an active position exists and unrealized PnL is negative.

5. `ACTIVE_POSITION` + `ACTIVE_FLAT`
   Read as: an active position exists but unrealized PnL is flat.

6. `REALIZED_GAIN`
   Read as: latest flat state has positive realized PnL.

7. `REALIZED_LOSS`
   Read as: latest flat state has negative realized PnL.

8. `REALIZED_FLAT`
   Read as: latest flat state has fills, but realized PnL is zero or unavailable.

9. `pnl_attention_flag=true`
   Read as: latest execution/PnL state deserves operator attention before deeper interpretation.

10. `latest_realized_sign` and `latest_unrealized_sign` differ
    Read as: latest state mixes closed and open PnL directions; inspect raw totals instead of relying on sign alone.

## What Not To Infer

Do not treat these fields as:
- an execution recommendation
- a go-live decision
- a replacement for strategy review
- a replacement for observation review
- a scheduling policy

These fields are only a reading contract for human operators.

## Minimal Operator Workflow

1. Check `pnl_interpretation`.
2. Check `pnl_attention_flag`.
3. Check `last_pnl_state`.
4. Check realized/unrealized signs and totals.
5. If needed, inspect counts and equity snapshot.

Stop there unless deeper forensic detail is required.
