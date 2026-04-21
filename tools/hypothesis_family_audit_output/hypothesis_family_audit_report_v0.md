# Hypothesis Family Audit v0

Generated: `2026-04-06T06:52:48Z`

## Nightly Policy

| family_id | role | status | nightly_mode | reason |
|---|---:|---:|---:|---|
| `momentum_v1` | `TRADING` | `WEAK` | `REDUCED_NIGHTLY` | No current profitability PASS in Phase7 artifacts; prior tightening/OOS weakness requires controlled nightly instead of full-volume generation. |
| `return_reversal_v1` | `TRADING` | `FAILED` | `REDUCED_NIGHTLY` | Profitability-tested subset failed economic edge: gross pnl was non-positive before fees. |
| `spread_reversion_v1` | `CONTEXT` | `ACTIVE` | `REDUCED_NIGHTLY` | Context/guard behavior is useful but should not produce directional candidate spam. |

## Downgrades

- `momentum_v1` -> `REDUCED_NIGHTLY`: No current profitability PASS in Phase7 artifacts; prior tightening/OOS weakness requires controlled nightly instead of full-volume generation.
- `return_reversal_v1` -> `REDUCED_NIGHTLY`: Profitability-tested subset failed economic edge: gross pnl was non-positive before fees.
- `spread_reversion_v1` -> `REDUCED_NIGHTLY`: Context/guard behavior is useful but should not produce directional candidate spam.

## Remain Full Nightly

- None. Current evidence does not justify unrestricted `FULL_NIGHTLY` for any family.

## Cost-Aware Prefilter

- Status: `POLICY_DEFINED_NOT_APPLIED_BY_THIS_SCRIPT`
- Rule: After Phase5, candidates with derivable gross_pnl <= 0 should not advance into Phase6 candidate generation.
- Unknown gross handling: If gross pnl is not derivable from existing artifacts, mark UNKNOWN/needs-review; do not delete or silently fail the candidate.

## Pipeline Impact

- Candidate-family attributions eligible for reduced handling: `199` / `199`.
- Pack rows fully under non-full families: `142`.
- Mixed rows preserving a full-nightly family while reducing another family attribution: `0`.
- No family is deleted; context capability remains; trading discovery is retained through reduced control lanes.

## Profitability Note

`momentum_v1` is downgraded because the current audit has no profitability PASS for it and prior tightening/OOS evidence makes full-volume nightly too optimistic.
`return_reversal_v1` is not a discovery failure. It produced fill-backed Phase7 activity, but the tested profitability subset had non-positive gross PnL before fees, so nightly volume should be reduced until a cost/profitability gate improves the lane.

