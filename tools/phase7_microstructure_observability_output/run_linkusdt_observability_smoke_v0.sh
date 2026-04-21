#!/usr/bin/env bash
set -euo pipefail

OUTPUT_ROOT="tools/phase7_microstructure_observability_output"
RUN_ROOT="$OUTPUT_ROOT/smoke_linkusdt"
EVIDENCE_ROOT="$OUTPUT_ROOT/evidence_pack"
WATCHLIST_JSON="$RUN_ROOT/input_watchlist.json"
STRATEGY_CONFIG_JSON="$RUN_ROOT/strategy_config.json"
SUMMARY_JSON="$RUN_ROOT/summary_runtime.json"
SHADOW_STATE_DIR="$RUN_ROOT/shadow_state"
BATCH_OUT_DIR="$RUN_ROOT/batch_out"
AUDIT_DIR="$RUN_ROOT/audit"
RESULT_JSON="$RUN_ROOT/shadow_observation_batch_result_v0.json"
REFRESH_RESULT_JSON="$SHADOW_STATE_DIR/shadow_derived_surface_refresh_v0.json"
HISTORY_JSONL="$SHADOW_STATE_DIR/shadow_observation_history_v0.jsonl"
INDEX_JSON="$SHADOW_STATE_DIR/shadow_observation_index_v0.json"
EXECUTION_LEDGER_JSONL="$SHADOW_STATE_DIR/shadow_execution_ledger_v0.jsonl"
EXECUTION_PACK_SUMMARY_JSON="$SHADOW_STATE_DIR/shadow_execution_pack_summary_v0.json"
TIME_LOG="$EVIDENCE_ROOT/0001_observability_smoke_time.log"
STDOUT_LOG="$EVIDENCE_ROOT/0001_observability_smoke_stdout.log"
STDERR_LOG="$EVIDENCE_ROOT/0001_observability_smoke_stderr.log"

rm -rf "$RUN_ROOT"
mkdir -p "$RUN_ROOT" "$SHADOW_STATE_DIR" "$BATCH_OUT_DIR" "$AUDIT_DIR" "$EVIDENCE_ROOT"

cat >"$WATCHLIST_JSON" <<'JSON'
{
  "schema_version": "phase7_microstructure_observability_watchlist_v0",
  "generated_ts_utc": "2026-04-09T00:00:00Z",
  "governance": {
    "surface_role": "TASK_LOCAL_LINKUSDT_OBSERVABILITY_SMOKE",
    "authoritative_scope": "Single-strategy task-local microstructure observability smoke only.",
    "not_authoritative_for": ["global shadow watchlist", "ranking", "promotion"]
  },
  "source_shortlist_json": "/home/deploy/quantlab-backend/tools/microstructure_phase6_shortlist_v0.json",
  "selected_count": 1,
  "items": [
    {
      "rank": 1,
      "pack_id": "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020",
      "pack_path": "",
      "exchange": "bybit",
      "symbols": ["linkusdt"],
      "decision_tier": "PHASE6_MICROSTRUCTURE_SHORTLIST",
      "selection_slot": "bybit/trade/linkusdt",
      "strategy_id": "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020",
      "family_id": "microstructure_imbalance_v1",
      "selected_cell": {
        "delta_ms": 100,
        "event_count": 638846,
        "h_ms": 500,
        "mean_signed_fwd_return_bps": 0.8139515752445161,
        "pressure_threshold": 0.2,
        "t_stat": 129.41468382493628
      }
    }
  ]
}
JSON

cat >"$STRATEGY_CONFIG_JSON" <<'JSON'
{
  "binding_mode": "PAPER_DIRECTIONAL_V1",
  "exchange": "bybit",
  "family_id": "microstructure_imbalance_v1",
  "orderQty": 1,
  "params": {
    "delta_ms_list": [100],
    "h_ms_list": [500],
    "pressure_threshold_list": [0.2],
    "tolerance_ms": 0
  },
  "selected_cell": {
    "delta_ms": 100,
    "event_count": 638846,
    "exchange": "bybit",
    "h_ms": 500,
    "mean_signed_fwd_return_bps": 0.8139515752445161,
    "pressure_threshold": 0.2,
    "stream": "trade",
    "symbol": "linkusdt",
    "t_stat": 129.41468382493628
  },
  "source_decision_tier": "PHASE6_MICROSTRUCTURE_SHORTLIST",
  "source_family_report_path": "/home/deploy/quantlab-backend/tools/microstructure_imbalance_fullscan_output/fullscan_trade/artifacts/microstructure_imbalance/family_microstructure_imbalance_report.json",
  "source_pack_id": "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020",
  "stream": "trade",
  "symbols": ["linkusdt"],
  "window": "20260324,20260325,20260326,20260327"
}
JSON

STRATEGY_CONFIG_COMPACT="$(tr -d '\n' < "$STRATEGY_CONFIG_JSON")"

/usr/bin/time -v -o "$TIME_LOG" \
  python3 tools/run-shadow-observation-batch-v0.py \
    --watchlist "$WATCHLIST_JSON" \
    --max-items 1 \
    --strategy core/strategy/strategies/MicrostructureImbalanceV1Strategy.js \
    --strategy-config-json "$STRATEGY_CONFIG_COMPACT" \
    --summary-json-path "$SUMMARY_JSON" \
    --history-jsonl "$HISTORY_JSONL" \
    --index-json "$INDEX_JSON" \
    --phase6-state-dir tools/phase6_state \
    --shadow-state-dir "$SHADOW_STATE_DIR" \
    --refresh-result-json "$REFRESH_RESULT_JSON" \
    --execution-ledger-jsonl "$EXECUTION_LEDGER_JSONL" \
    --execution-pack-summary-json "$EXECUTION_PACK_SUMMARY_JSON" \
    --audit-base-dir "$AUDIT_DIR" \
    --out-dir "$BATCH_OUT_DIR" \
    --result-json "$RESULT_JSON" \
    --per-run-timeout-sec 900 \
    --run-max-duration-sec 600 \
    --heartbeat-ms 5000 \
    >"$STDOUT_LOG" 2>"$STDERR_LOG"

code=$?
printf 'exit_code=%s\n' "$code" >> "$TIME_LOG"
exit "$code"
