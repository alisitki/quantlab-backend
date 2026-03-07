# Shadow Execution Outcome Review Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- trigger trades
- auto-run anything
- replace cadence, scheduler, or batch policy
- replace deeper execution or PnL analysis

Use it to read the compact pack-level shadow execution outcome review faster.

## Authoritative Surface

Primary artifact:
- `tools/shadow_state/shadow_execution_outcome_review_v0.json`

Authoritative builder:
- `tools/shadow_execution_outcome_review_v0.py`

Upstream compact source:
- `tools/shadow_state/shadow_execution_rollup_snapshot_v0.json`
- `tools/shadow_execution_rollup_snapshot_v0.py`

Related reading contracts:
- `tools/shadow_state/shadow_execution_rollup_reading_contract_v0.md`
- `tools/shadow_state/shadow_execution_reading_contract_v0.md`

## Field Reading Order

Read each outcome-review item in this order:

1. `outcome_class`
2. `outcome_attention_flag`
3. `latest_vs_recent_consistency`
4. `outcome_review_short`
5. `last_observed_at`

Why this order:
- first read the compact review class
- then see whether attention is active
- then see whether latest and recent outcomes line up
- then read the short operator note
- only then inspect recency

## Field Semantics

### `outcome_class`

Current enum:
- `NO_RECENT_HISTORY`
- `ATTENTION_REQUIRED`
- `STABLE_GAINING`
- `STABLE_LOSING`
- `STABLE_FLAT`
- `MIXED_RECENT`

### `latest_vs_recent_consistency`

Current enum:
- `CONSISTENT`
- `DIVERGENT`
- `UNKNOWN`

### `outcome_attention_flag`

Values:
- `true`
- `false`

Read:
- `true` means the upstream rollup attention is already active
- or latest outcome and recent bias diverge

### `outcome_review_short`

Short deterministic operator-facing interpretation of the derived outcome state.

## Deterministic Interpretation Patterns

1. `STABLE_GAINING`
   Read as: latest outcome and recent window both lean gaining.

2. `STABLE_LOSING`
   Read as: latest outcome and recent window both lean losing.

3. `STABLE_FLAT`
   Read as: latest outcome and recent window both lean flat.

4. `MIXED_RECENT`
   Read as: latest outcome does not align cleanly with the recent window.

5. `ATTENTION_REQUIRED`
   Read as: latest outcome is attention-class or too unclear to compress further.

6. `NO_RECENT_HISTORY`
   Read as: recent rollup history is absent, so no meaningful outcome trend can be stated.

7. `latest_vs_recent_consistency=CONSISTENT`
   Read as: latest outcome bucket matches the dominant recent bias.

8. `latest_vs_recent_consistency=DIVERGENT`
   Read as: latest outcome and recent bias do not line up; inspect the upstream rollup if needed.

9. `latest_vs_recent_consistency=UNKNOWN`
   Read as: no recent basis or no trustworthy latest bucket exists for a consistency call.

10. `outcome_attention_flag=true`
    Read as: do not rely only on the compact review; inspect upstream execution rollup fields if the pack matters.

## What Not To Infer

Do not treat this artifact as:
- an execution recommendation
- a trading signal
- an automatic go-live decision
- an automatic skip signal
- a replacement for the execution rollup snapshot
- a replacement for deeper pack summary inspection

This contract is only for human operator reading.

## Minimal Operator Workflow

1. Check `outcome_class`.
2. Check `outcome_attention_flag`.
3. Check `latest_vs_recent_consistency`.
4. Check `outcome_review_short`.
5. If needed, inspect the upstream execution rollup snapshot.
