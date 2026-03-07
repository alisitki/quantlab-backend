# Shadow Execution Review Queue Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- trigger trades
- auto-run anything
- replace cadence, scheduler, or batch policy
- replace deeper execution or PnL analysis

Use it to read the compact execution/PnL review queue faster.

## Authoritative Surface

Primary artifact:
- `tools/shadow_state/shadow_execution_review_queue_v0.json`

Authoritative builder:
- `tools/shadow_execution_review_queue_v0.py`

Upstream operator-facing source:
- `tools/shadow_state/shadow_operator_snapshot_v0.json`
- `tools/shadow_operator_snapshot_v0.py`

Related reading contracts:
- `tools/shadow_state/shadow_execution_outcome_review_reading_contract_v0.md`
- `tools/shadow_state/shadow_combined_operator_reading_contract_v0.md`

## Field Reading Order

Read each review-queue item in this order:

1. `review_priority_bucket`
2. `trend_class`
3. `trend_attention_flag`
4. `trend_direction`
5. `review_reason_short`
6. `source_rank`

Why this order:
- first identify which packs need review first
- then read the compact trend class
- then see whether trend attention is active
- then read the trend direction
- then use the short review reason
- only then inspect source ranking context

## Field Semantics

### `review_priority_bucket`

Current enum:
- `HIGH`
- `NORMAL`
- `LOW`

Read:
- `HIGH` means review first in the execution/PnL queue
- `NORMAL` means review after high-priority packs
- `LOW` means compact trend surface is comparatively calm or missing

### `trend_class`

Current enum:
- `STABLE`
- `MIXED`
- `ATTENTION`
- `NO_HISTORY`

### `trend_direction`

Current enum:
- `GAINING`
- `LOSING`
- `FLAT`
- `UNKNOWN`

### `trend_attention_flag`

Values:
- `true`
- `false`

### `review_reason_short`

Short deterministic operator-facing explanation for the review queue position.

## Deterministic Interpretation Patterns

1. `HIGH` + `ATTENTION`
   Read as: latest execution outcome needs explicit operator review.

2. `HIGH` + `STABLE` + `trend_attention_flag=true`
   Read as: trend is mechanically stable, but attention is still active in the upstream outcome surface.

3. `NORMAL` + `MIXED`
   Read as: recent execution outcome is mixed and should be reviewed after high-priority items.

4. `LOW` + `STABLE`
   Read as: current execution trend is stable and does not need to lead the queue.

5. `LOW` + `NO_HISTORY`
   Read as: there is not enough execution trend history to prioritize the pack.

6. `trend_direction=GAINING`
   Read as: latest execution state points positive.

7. `trend_direction=LOSING`
   Read as: latest execution state points negative.

8. `trend_direction=FLAT`
   Read as: latest execution state points flat.

9. `trend_direction=UNKNOWN`
   Read as: latest execution state is too sparse or ambiguous to directionalize.

## What Not To Infer

Do not treat this artifact as:
- an execution recommendation
- a trading signal
- an automatic skip signal
- a replacement for the operator snapshot
- a replacement for deeper execution rollup inspection

This contract is only for human operator review workflow.

## Minimal Operator Workflow

1. Read `review_priority_bucket`.
2. Read `trend_class` and `trend_attention_flag`.
3. Read `trend_direction`.
4. Read `review_reason_short`.
5. If needed, inspect the upstream operator snapshot and outcome review fields.
