## 1) GOAL
- latency_leadlag_v1 sinyalinin genellemesini test etmek: aynı pair ve parametrelerle birden fazla window’da anlamlı mı?

## 2) FIXED CONFIG (no drift)
- pair focus: binance->okx
- dt_ms=25, h_ms=250, tolerance_ms=20
- stream=bbo/top-of-book
- determinism check: ONvsON PASS required
- support threshold: event_count>=200 (window-level pass için)

## 3) WINDOW SELECTION RULE (state-driven)
- Source of truth: compacted state inventory (SUCCESS + day_quality_post in {GOOD, DEGRADED})
- Eligible days: day_quality_post in {GOOD, DEGRADED}
- BAD is excluded (and not compacted).
- Preference order: prefer GOOD first, then fill with DEGRADED (deterministic order: window_id asc).
- Select N windows of 2 consecutive days each, same symbol across 3 exchanges if available.
- N=12 (hard cap). If fewer available, take all and mark N_actual.

## 4) REGIME STRATIFICATION (simple, deterministic)
- Partition candidates into 3 buckets by rows_total from state inventory.
- Exact rule:
- Sort candidate windows by rows_total ascending.
- Define quantile cut points on sorted index:
- LOW: index < floor(N_candidates/3)
- MID: floor(N_candidates/3) <= index < floor(2*N_candidates/3)
- HIGH: index >= floor(2*N_candidates/3)
- Pick 4 windows from each bucket (total 12).
- If a bucket has <4, backfill from MID (then remaining from nearest non-empty bucket with deterministic order rows_total asc, window_id asc).

## 5) CAMPAIGN TIMEBOX / LOOP
- MAX_TRIES=1 (campaign-level)
- MAX_WALL=2h (campaign-level)
- STOP RULE: after N windows processed OR MAX_WALL hit (whichever first). No re-runs.

## 6) PER-WINDOW PASS/FAIL LABEL
For each window produce a row with:
- window_id, event_count, mean_bps, t_stat, determinism_status, window_label

Rules:
- If determinism FAIL -> window_label=FAIL/DETERMINISM_FAIL
- Else if event_count<200 -> window_label=INSUFFICIENT_SUPPORT
- Else if |t_stat|>=3 and mean_bps>0 -> window_label=DIRECTIONAL
- Else if |t_stat|>=3 and mean_bps<0 -> window_label=ANTI_EDGE
- Else -> window_label=NO_EDGE  (criterion: |t_stat|<3)

COVERAGE GUARD:
- If any of the 3 exchanges has event_count_exchange < 200 for the focused pair/window (or missing stream for a day) then set window_label=INSUFFICIENT_SUPPORT.
- Rationale: tolerate short gaps but prevent matching bias.

## 7) CAMPAIGN-LEVEL DECISION (mechanical)
- PASS (family holds) if:
- At least 8 of N_actual windows have window_label=DIRECTIONAL
- And at least 3 windows in each bucket (LOW/MID/HIGH) are DIRECTIONAL (if bucket exists)
- FAIL (family rejected) if:
- In first 5 processed windows, >=4 are NO_EDGE or INSUFFICIENT_SUPPORT (early kill)
- Or DIRECTIONAL count < 6 at end
- If mixed (e.g., many ANTI_EDGE): mark as “ANTI_EDGE dominated” and treat as reject unless explicitly chosen.

## 8) OUTPUT CONTRACT (what files will be produced when run happens)
- results_windows.tsv: one row per window with above fields
- results_summary.txt: campaign label + counts by bucket
- determinism_compare.tsv per window (or aggregated) must exist
- Evidence: one SLIM pack at end (not now)

## 9) NEXT AFTER RESULT (no run here)
- If campaign PASS -> Phase-5 friction/spec (break-even bps) and then Phase-5 controlled execution design (still no Phase-6)
- If FAIL/INSUFF -> Phase-5 hypothesis reset (change stream or broaden pair set)
