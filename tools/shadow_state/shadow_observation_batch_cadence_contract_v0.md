# Shadow Observation Batch Cadence Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- suppress or skip watchlist items
- auto-start observation
- replace scheduler/systemd/cron
- create a daemon or infinite loop
- change runtime policy

It helps a human operator choose between:
- `RUN_TOP1`
- `RUN_TOP3`
- `DO_NOT_RUN`

using the current watchlist and existing bounded batch routine.

## Canonical Inputs

Primary surface:
- `tools/shadow_state/shadow_watchlist_v0.tsv`
- `tools/shadow_state/shadow_watchlist_v0.json`

Supporting runbook:
- `tools/shadow_state/shadow_observation_batch_runbook_v0.md`

Signals used from the top watchlist items:
- `decision_tier`
- `observation_status`
- `next_action_hint`
- `reobserve_status`
- `observation_attention_flag`
- `observation_last_outcome_short`
- `recent_observation_trail`

Batch-size signal:
- current watchlist item count

## Invocation Outcomes

### `RUN_TOP1`

Meaning:
- run only the first watchlist item with the bounded top-1 profile

Use the runbook command:
- top-1 bounded real run

### `RUN_TOP3`

Meaning:
- run the first three watchlist items with the bounded top-3 profile

Use the runbook command:
- top-3 bounded real run

### `DO_NOT_RUN`

Meaning:
- do not start a bounded batch right now
- re-read watchlist/review surfaces later or wait for a fresher candidate state

## Decision Matrix

Apply these rules in order.

1. If watchlist item count is `0`
   -> `DO_NOT_RUN`

2. If top item shows:
   - `observation_status=OBSERVED_PASS`
   - `next_action_hint=ALREADY_OBSERVED_GOOD`
   - `reobserve_status=RECENTLY_OBSERVED`
   - `observation_attention_flag=false`
   -> `DO_NOT_RUN`

3. If top item shows:
   - `observation_attention_flag=true`
   -> `RUN_TOP1`

4. If top item shows:
   - `observation_status=OBSERVED_FAIL`
   or
   - `next_action_hint=NEEDS_ATTENTION`
   -> `RUN_TOP1`

5. If top item shows:
   - `observation_status=OBSERVED_PASS_NO_EVENTS`
   or
   - `next_action_hint=REOBSERVE_CANDIDATE`
   -> `RUN_TOP1`

6. If top item shows:
   - `observation_status=NEW`
   - `next_action_hint=READY_TO_OBSERVE`
   and watchlist item count is `1`
   -> `RUN_TOP1`

7. If the first three watchlist items all show:
   - `next_action_hint=READY_TO_OBSERVE`
   and watchlist item count is at least `3`
   -> `RUN_TOP3`

8. If the first three watchlist items all show:
   - `decision_tier=PROMOTE_STRONG`
   and none of them show `observation_attention_flag=true`
   and at least one of them shows:
     - `reobserve_status=NOT_OBSERVED`
     or
     - `reobserve_status=STALE_OBSERVATION`
   -> `RUN_TOP3`

9. Fallback:
   - if top item is `NEW`
   - or top item is `STALE_OBSERVATION`
   -> `RUN_TOP1`
   - otherwise -> `DO_NOT_RUN`

## Reading Notes

Read the top item first.

Prefer `RUN_TOP1` when:
- you want the smallest bounded check
- the top item needs attention
- the top item has never been observed
- the top item passed before but had no events

Prefer `RUN_TOP3` when:
- the watchlist head is deep enough
- the first three items all look observation-ready
- you want a bounded small-batch refresh instead of a single-item smoke

Prefer `DO_NOT_RUN` when:
- the top item is already fresh and good
- the watchlist is empty

## What This Contract Does Not Mean

This is not:
- a ranking policy
- a selection policy
- a suppression rule
- an automatic observe scheduler
- a go-live policy

It is only a bounded invocation preference contract for a human operator.
