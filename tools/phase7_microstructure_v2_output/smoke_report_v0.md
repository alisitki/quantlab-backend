# Phase7 Microstructure V2 Smoke v0

This smoke validates only implementation correctness and compact observability for the new cross-exchange-confirmed V2 runtime.

- Status: `SMOKE_PASS`
- verify_soft_live_pass: `True`
- processed_event_count: `8801`
- decision_count: `0`
- fill_count: `0`
- context_fields_populated: `False`
- Smoke gate: `verify_soft_live_pass && processed_event_count > 0`

**What Changed vs V1**

- Local bybit microstructure trigger still starts the decision path.
- Entry / reversal now require same-symbol external venue confirmation from `binance` and `okx`.
- Low divergence is enforced through a fixed `max_divergence_score` gate.
- BTC support exists as an optional secondary gate; it is disabled in this smoke by default.
- A short smoke may still produce zero fills; that is not treated as an integration failure if the runner processed live events cleanly.

**Artifacts**

- [result json](/home/deploy/quantlab-backend/tools/phase7_microstructure_v2_output/smoke_run/shadow_observation_batch_result_v0.json)
- [summary json](/home/deploy/quantlab-backend/tools/phase7_microstructure_v2_output/smoke_run/batch_out/rank01_microstructure_imbalance_v1_bybit_linkusdt_trade_d100_h500_pt020_v2/summary.json)
- [execution events](/home/deploy/quantlab-backend/tools/phase7_microstructure_v2_output/smoke_run/shadow_state/shadow_execution_events_v1.jsonl)
- [paper ledger](/home/deploy/quantlab-backend/tools/phase7_microstructure_v2_output/smoke_run/shadow_state/shadow_futures_paper_ledger_v1.json)

**Next Sprint Validation Command**

```bash
python3 tools/phase7_microstructure_v2_runtime_v0.py --output-dir tools/phase7_microstructure_v2_output/full_run --result-json tools/phase7_microstructure_v2_output/full_run_result_v0.json --report-md tools/phase7_microstructure_v2_output/full_run_report_v0.md --run-max-duration-sec 21600 --per-run-timeout-sec 21900 --subprocess-timeout-sec 22500 --heartbeat-ms 5000
```
