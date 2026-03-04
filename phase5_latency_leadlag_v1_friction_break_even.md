# Phase-5 Friction / Break-Even Report (Report-Only)

## Inputs
- pack_id: `multi-hypothesis-phase5-latency-leadlag-v1-bbo-top3-20260213_063414`
- pack_path: `/home/deploy/quantlab-evidence-archive/20260213_slim/multi-hypothesis-phase5-latency-leadlag-v1-bbo-top3-20260213_063414`
- files_used: results_windows.tsv, results_summary.txt, spec_v2.json, label_report.txt
- N_windows: 18

## Summary
- median(mean_bps) = 0.872906 bps
- p10(mean_bps) = 0.756059 bps
- min(mean_bps) = 0.704696 bps
- conservative_cost_bps (>=80% survive) = 0.6 bps
- median_break_even_bps = 0.872906 bps
- note: fee/slippage not measured here; this is thresholding only

## friction_stats.tsv

| scope | group | count | min | p10 | p25 | median | p75 | p90 | max |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | all | 18 | 0.704696 | 0.756059 | 0.793659 | 0.872906 | 1.119656 | 1.209125 | 1.356353 |
| symbol | adausdt | 6 | 1.101261 | 1.113524 | 1.126871 | 1.162177 | 1.231468 | 1.300117 | 1.356353 |
| symbol | avaxusdt | 6 | 0.791002 | 0.796316 | 0.809173 | 0.850146 | 0.931584 | 0.960636 | 0.968657 |
| symbol | linkusdt | 6 | 0.704696 | 0.724302 | 0.748248 | 0.771142 | 0.821165 | 0.855933 | 0.877319 |
| quality_tag | DD | 3 | 0.781018 | 0.798513 | 0.824755 | 0.868493 | 1.056187 | 1.168804 | 1.243882 |
| quality_tag | GD | 3 | 0.834547 | 0.858160 | 0.893581 | 0.952615 | 1.154484 | 1.275605 | 1.356353 |
| quality_tag | GG | 12 | 0.704696 | 0.745645 | 0.783568 | 0.854560 | 1.107393 | 1.129691 | 1.194229 |

## break_even_grid.tsv

| total_cost_bps | survival_rate | avg_net_bps |
|---:|---:|---:|
| 0.0 | 1.000000 | 0.948255 |
| 0.2 | 1.000000 | 0.748255 |
| 0.4 | 1.000000 | 0.548255 |
| 0.6 | 1.000000 | 0.348255 |
| 0.8 | 0.722222 | 0.148255 |
| 1.0 | 0.333333 | -0.051745 |
| 1.2 | 0.111111 | -0.251745 |
| 1.5 | 0.000000 | -0.551745 |
| 2.0 | 0.000000 | -1.051745 |

## Decision Guidance (Phase-5, no live)
- If conservative_cost_bps < 0.4 bps: likely not tradeable with taker fees.
- If conservative_cost_bps >= 0.8 bps: potentially tradeable; proceed to execution feasibility spec.
