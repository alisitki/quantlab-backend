#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/deploy/quantlab-backend"
cd "$ROOT"

OUT_DIR="tools/phase7_microstructure_turnover_output/full_linkusdt"
EVIDENCE_DIR="tools/phase7_microstructure_turnover_output/evidence_pack"
STRATEGY_ID="microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/shadow_state" "$EVIDENCE_DIR"

python3 - <<'PY'
from pathlib import Path
import json

root = Path("/home/deploy/quantlab-backend")
out = root / "tools/phase7_microstructure_turnover_output/full_linkusdt"
shortlist = json.loads((root / "tools/microstructure_phase6_shortlist_v0.json").read_text())
items = shortlist.get("shortlist") or shortlist.get("items") or shortlist.get("strategies") or []
strategy_id = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"
row = next(item for item in items if item.get("strategy_id") == strategy_id)
cell = dict(row["selected_cell"])
cell.update({"exchange": "bybit", "symbol": "linkusdt", "stream": "trade"})
watchlist = {
    "schema_version": "phase7_microstructure_turnover_watchlist_v0",
    "generated_ts_utc": "2026-04-07T00:00:00Z",
    "governance": {
        "surface_role": "TASK_LOCAL_LINKUSDT_TURNOVER_VALIDATION",
        "not_authoritative_for": ["global shadow watchlist", "ranking", "promotion"],
        "notes": ["linkusdt-only turnover validation", "global shadow_watchlist is not read or written"],
    },
    "selected_count": 1,
    "items": [{
        "rank": 1,
        "pack_id": strategy_id,
        "pack_path": "",
        "exchange": "bybit",
        "symbols": ["linkusdt"],
        "decision_tier": "PHASE7_TURNOVER_VALIDATION",
        "selection_slot": "bybit/trade/linkusdt",
        "strategy_id": strategy_id,
        "family_id": "microstructure_imbalance_v1",
        "selected_cell": cell,
    }],
}
config = {
    "binding_mode": "PAPER_DIRECTIONAL_V1",
    "family_id": "microstructure_imbalance_v1",
    "source_pack_id": strategy_id,
    "source_decision_tier": "PHASE7_TURNOVER_VALIDATION",
    "exchange": "bybit",
    "stream": "trade",
    "symbols": ["linkusdt"],
    "source_family_report_path": str(row.get("source_family_report_path") or ""),
    "window": "turnover_validation",
    "orderQty": 1,
    "params": {
        "delta_ms_list": [int(cell["delta_ms"])],
        "h_ms_list": [int(cell["h_ms"])],
        "pressure_threshold_list": [float(cell["pressure_threshold"])],
        "tolerance_ms": 0,
    },
    "selected_cell": cell,
}
(out / "input_watchlist.json").write_text(json.dumps(watchlist, indent=2, sort_keys=True) + "\n")
(out / "strategy_config.json").write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
PY

stdout="$EVIDENCE_DIR/0013_run_linkusdt_full_batch_stdout.log"
stderr="$EVIDENCE_DIR/0013_run_linkusdt_full_batch_stderr.log"
timelog="$EVIDENCE_DIR/0013_run_linkusdt_full_batch_time.log"

/usr/bin/time -v -o "$timelog" python3 tools/run-shadow-observation-batch-v0.py \
  --watchlist "$OUT_DIR/input_watchlist.json" \
  --max-items 1 \
  --strategy core/strategy/strategies/MicrostructureImbalanceV1Strategy.js \
  --strategy-config-json "$(cat "$OUT_DIR/strategy_config.json")" \
  --summary-json-path "$OUT_DIR/summary_runtime.json" \
  --history-jsonl "$OUT_DIR/shadow_state/shadow_observation_history_v0.jsonl" \
  --index-json "$OUT_DIR/shadow_state/shadow_observation_index_v0.json" \
  --phase6-state-dir tools/phase6_state \
  --shadow-state-dir "$OUT_DIR/shadow_state" \
  --refresh-result-json "$OUT_DIR/shadow_state/shadow_derived_surface_refresh_v0.json" \
  --execution-ledger-jsonl "$OUT_DIR/shadow_state/shadow_execution_ledger_v0.jsonl" \
  --execution-pack-summary-json "$OUT_DIR/shadow_state/shadow_execution_pack_summary_v0.json" \
  --audit-base-dir "$OUT_DIR/audit" \
  --out-dir "$OUT_DIR/batch_out" \
  --result-json "$OUT_DIR/shadow_observation_batch_result_v0.json" \
  --per-run-timeout-sec 21900 \
  --run-max-duration-sec 21600 \
  --heartbeat-ms 5000 >"$stdout" 2>"$stderr" &
batch_pid=$!
batch_started_at=$(date +%s)
echo "PHASE7_MICROSTRUCTURE_TURNOVER_STARTED strategy_id=$STRATEGY_ID run_max_duration_sec=21600"
while kill -0 "$batch_pid" 2>/dev/null; do
  sleep 60
  if kill -0 "$batch_pid" 2>/dev/null; then
    now=$(date +%s)
    echo "PHASE7_MICROSTRUCTURE_TURNOVER_PROGRESS elapsed_sec=$((now - batch_started_at)) still_running=1"
  fi
done
wait "$batch_pid"

python3 - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json
import math

root = Path("/home/deploy/quantlab-backend")
out = root / "tools/phase7_microstructure_turnover_output/full_linkusdt"
final_json = root / "tools/phase7_microstructure_turnover_v0.json"
final_md = root / "tools/phase7_microstructure_turnover_output/phase7_microstructure_turnover_report_v0.md"
strategy_id = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"

def f(value, default=None):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default

def ratio(a, b):
    return None if a is None or b in (None, 0) else a / b

baseline_doc = json.loads((root / "tools/phase7_microstructure_profitability_v0.json").read_text())
baseline = next(row for row in baseline_doc["per_strategy"] if row["strategy_id"] == strategy_id)
summary = json.loads((out / "summary_runtime.json").read_text())
batch = json.loads((out / "shadow_observation_batch_result_v0.json").read_text())
ledger = json.loads((out / "shadow_state/shadow_futures_paper_ledger_v1.json").read_text())
item = ledger["items"][0]

base = {
    "fill_count": baseline["trade_quality"]["fill_count"],
    "gross_pnl": baseline["gross_performance"]["gross_realized_pnl_quote"],
    "net_pnl": baseline["net_performance"]["net_realized_pnl_quote"],
    "total_fee": baseline["fee_burden"]["total_fee_quote"],
    "processed_event_count": baseline["efficiency_metrics"]["processed_event_count"],
    "gross_pnl_per_fill": baseline["efficiency_metrics"]["gross_pnl_per_fill"],
    "fee_per_fill": baseline["fee_burden"]["fee_per_fill"],
    "gross_pnl_per_1k_events": baseline["efficiency_metrics"]["gross_pnl_per_1k_events"],
}
after = {
    "verify_soft_live_pass": bool((batch.get("results") or [{}])[0].get("verify_soft_live_pass")),
    "processed_event_count": None,
    "decision_count": int(item.get("decision_event_count") or 0),
    "fill_count": int(item.get("fill_event_count") or 0),
    "gross_pnl": f(item.get("replayed_realized_pnl_quote_gross")),
    "net_pnl": f(item.get("replayed_realized_pnl_quote_net")),
    "total_fee": f(item.get("total_fee_quote"), 0.0),
    "funding_cost": f(item.get("funding_cost_quote"), 0.0),
    "turnover": f(item.get("turnover_quote")),
    "gross_pnl_per_fill": ratio(f(item.get("replayed_realized_pnl_quote_gross")), f(item.get("fill_event_count"))),
    "fee_per_fill": ratio(f(item.get("total_fee_quote")), f(item.get("fill_event_count"))),
    "gross_pnl_per_1k_events": None,
    "cost_accounting_status": item.get("cost_accounting_status"),
    "profitability_status": item.get("profitability_status"),
    "stop_reason": summary.get("stop_reason"),
}
history_path = out / "shadow_state/shadow_observation_index_v0.json"
if history_path.exists():
    idx = json.loads(history_path.read_text())
    # Keep this defensive: index schema is not the economic authority.
    after["processed_event_count"] = idx.get("processed_event_count")

fill_reduction_ratio = ratio((base["fill_count"] - after["fill_count"]), base["fill_count"])
gross_per_fill_change_ratio = ratio(
    (after["gross_pnl_per_fill"] or 0) - (base["gross_pnl_per_fill"] or 0),
    abs(base["gross_pnl_per_fill"] or 0),
)
fee_reduction_ratio = ratio((base["total_fee"] - after["total_fee"]), base["total_fee"])
gross_preserved_ratio = ratio(after["gross_pnl"], base["gross_pnl"])

if after["verify_soft_live_pass"] is not True or after["fill_count"] <= 0:
    verdict = "WORSE"
    reason = "invalid or no-fill full run"
elif (fill_reduction_ratio or 0) > 0.2 and (gross_per_fill_change_ratio or 0) > 0 and (gross_preserved_ratio or 0) >= 0.5:
    verdict = "IMPROVED"
    reason = "turnover fell, gross per fill improved, and gross edge did not collapse"
elif after["gross_pnl"] is not None and after["gross_pnl"] <= 0:
    verdict = "WORSE"
    reason = "gross edge became non-positive"
else:
    verdict = "NO_IMPROVEMENT"
    reason = "turnover/gross-per-fill target not met"

payload = {
    "schema_version": "phase7_microstructure_turnover_v0",
    "generated_ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "strategy_id": strategy_id,
    "family_id": "microstructure_imbalance_v1",
    "exchange": "bybit",
    "symbol": "linkusdt",
    "lever": {
        "selected": "MINIMUM_HOLD_ENFORCEMENT",
        "implementation": "runtime effective hold floor: max(selected_cell.h_ms, 5000ms)",
        "signal_math_changed": False,
    },
    "baseline": base,
    "after": after,
    "comparison": {
        "fill_reduction_ratio": fill_reduction_ratio,
        "gross_per_fill_change_ratio": gross_per_fill_change_ratio,
        "fee_reduction_ratio": fee_reduction_ratio,
        "gross_preserved_ratio": gross_preserved_ratio,
    },
    "verdict": verdict,
    "verdict_reason": reason,
    "source_paths": {
        "full_run_dir": str(out),
        "baseline_profitability_json": str(root / "tools/phase7_microstructure_profitability_v0.json"),
        "batch_result_json": str(out / "shadow_observation_batch_result_v0.json"),
        "futures_paper_ledger_json": str(out / "shadow_state/shadow_futures_paper_ledger_v1.json"),
    },
}
final_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
final_md.write_text(
    "# Phase7 Microstructure Turnover v0\n\n"
    f"- strategy: `{strategy_id}`\n"
    "- lever: `MINIMUM_HOLD_ENFORCEMENT`, effective hold floor `5000ms`\n"
    f"- verdict: `{verdict}`\n"
    f"- reason: {reason}\n"
    f"- baseline fills: `{base['fill_count']}` after fills: `{after['fill_count']}`\n"
    f"- baseline gross/fill: `{base['gross_pnl_per_fill']}` after gross/fill: `{after['gross_pnl_per_fill']}`\n"
    f"- baseline fees: `{base['total_fee']}` after fees: `{after['total_fee']}`\n"
    f"- baseline gross: `{base['gross_pnl']}` after gross: `{after['gross_pnl']}`\n\n"
    "No ranking, promotion, Phase5, or signal math changes were made.\n"
)
print("PHASE7_MICROSTRUCTURE_TURNOVER_COMPLETE")
print(f"result_json={final_json}")
print(f"report_md={final_md}")
print(f"verdict={verdict}")
PY

echo "FULL_LINKUSDT_TURNOVER_DONE"
