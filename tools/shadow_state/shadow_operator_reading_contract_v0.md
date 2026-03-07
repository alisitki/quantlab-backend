# Shadow Operator Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- suppress or skip candidates
- auto-start observation
- change runtime or policy behavior

Use it to read the current review/watchlist surfaces faster.

## Authoritative Surfaces

Primary surface:
- `tools/phase6_state/candidate_review.tsv`
- `tools/phase6_state/candidate_review.json`

Downstream surface:
- `tools/shadow_state/shadow_watchlist_v0.tsv`
- `tools/shadow_state/shadow_watchlist_v0.json`

Authoritative derivation happens in:
- `tools/phase6_candidate_review_v0.py`

The watchlist is pass-through only:
- `tools/shadow_candidate_bridge_v0.py`

## Field Reading Order

Read in this order:

1. `decision_tier`, `score`, `context_flags`
2. `observation_status`
3. `next_action_hint`
4. `reobserve_status`
5. `observation_last_outcome_short`
6. `observation_attention_flag`
7. `last_observation_age_hours`, `observation_recency_bucket`
8. `recent_observation_trail`

Why this order:
- first read candidate quality
- then read latest observation result
- then read the operator-facing hint
- then read recency and recent trail only if needed

## Field Semantics

### `observation_status`

Enum:
- `NEW`
- `OBSERVED_PASS`
- `OBSERVED_PASS_NO_EVENTS`
- `OBSERVED_FAIL`
- `OBSERVED_UNKNOWN`

Read:
- latest known observation outcome only

### `next_action_hint`

Enum:
- `READY_TO_OBSERVE`
- `ALREADY_OBSERVED_GOOD`
- `REOBSERVE_CANDIDATE`
- `NEEDS_ATTENTION`
- `REVIEW_OBSERVATION_STATE`

Read:
- a human reading hint only
- not an automatic action

### `reobserve_status`

Enum:
- `NOT_OBSERVED`
- `RECENTLY_OBSERVED`
- `STALE_OBSERVATION`
- `OBSERVATION_TIME_UNKNOWN`

Read:
- recency awareness only
- not a scheduling decision

### `observation_last_outcome_short`

Examples:
- `NO_HISTORY`
- `PASS(16)`
- `PASS_NO_EVENTS`
- `FAIL`
- `UNKNOWN`

Read:
- shortest single-field summary of the latest observation

### `observation_attention_flag`

Values:
- `true`
- `false`

Read:
- `true` means latest known observation ended in `FAIL` or `UNKNOWN`
- it does not mean auto-suppress

### `recent_observation_trail`

Shape:
- newest-first compact trail
- example:
  - `2026-03-07T07:24:58Z/PASS(16)/STREAM_END`
  - `2026-03-07T07:24:58Z/PASS(16)/STREAM_END | 2026-03-06T10:00:00Z/FAIL/ERROR`

Read:
- short recent line only
- use when you need more than the latest observation

## Deterministic Interpretation Patterns

1. `NEW` + `READY_TO_OBSERVE` + `NOT_OBSERVED`
   Read as: not yet observed in shadow.

2. `OBSERVED_PASS` + `ALREADY_OBSERVED_GOOD` + `RECENTLY_OBSERVED`
   Read as: recently observed and passed with real event flow.

3. `OBSERVED_PASS_NO_EVENTS` + `REOBSERVE_CANDIDATE`
   Read as: previous observe passed lifecycle checks but did not produce positive event count.

4. `OBSERVED_FAIL` + `NEEDS_ATTENTION`
   Read as: last known shadow run failed and needs operator inspection before trusting it again.

5. `OBSERVED_UNKNOWN` + `REVIEW_OBSERVATION_STATE`
   Read as: observation exists but last outcome is not reliable enough to classify cleanly.

6. `observation_attention_flag=true`
   Read as: latest observation ended in `FAIL` or `UNKNOWN`.

7. `observation_recency_bucket=WITHIN_24H`
   Read as: observation is fresh; re-observe urgency is low unless other fields say otherwise.

8. `observation_recency_bucket=OLDER_THAN_7D` or `STALE_OBSERVATION`
   Read as: observation is old enough that recency confidence is weaker.

9. `recent_observation_trail` shows mixed states like `PASS(...) | FAIL | PASS_NO_EVENTS`
   Read as: shadow history is not stable; inspect the trail instead of relying on the latest state alone.

10. `PASS(16)` or another positive `PASS(n)`
    Read as: lifecycle passed and positive processed event count was recorded.

## What Not To Infer

Do not treat these fields as:
- an automatic go-live signal
- an automatic skip signal
- a replacement for `decision_tier`
- a replacement for `context_flags`
- a scheduling policy

These fields are only a reading contract for human operators.

## Minimal Operator Workflow

1. Check `decision_tier` and `score`.
2. Check `observation_status`.
3. Check `next_action_hint`.
4. If needed, check `reobserve_status`.
5. If still unclear, inspect `observation_last_outcome_short` and `recent_observation_trail`.

Stop there unless you need deeper forensic detail.
