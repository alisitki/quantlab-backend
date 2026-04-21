#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path("/home/deploy/quantlab-backend")
SHORTLIST_JSON = ROOT / "tools/microstructure_phase6_shortlist_v0.json"
BASELINE_JSON = ROOT / "tools/phase7_microstructure_hysteresis_v0.json"
STRATEGY_PATH = ROOT / "core/strategy/strategies/MicrostructureImbalanceV1Strategy.js"
BASE_STRATEGY_ID = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"
ENTRY_THRESHOLD = 0.2
EXIT_THRESHOLD = 0.1
EXPECTED_EDGE_SCALE = 0.006095348835
FEE_RATE = 0.0004


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    return json.loads(path.read_text())


def finite_or_none(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def threshold_token(threshold: float) -> str:
    return f"{int(round(threshold * 100)):03d}"


def compose_strategy_id(base_row: dict) -> str:
    cell = base_row["selected_cell"]
    return (
        f"microstructure_imbalance_v1__{base_row['exchange']}__{base_row['symbol']}__{base_row['stream']}"
        f"__d{int(cell['delta_ms'])}__h{int(cell['h_ms'])}__pt{threshold_token(float(cell['pressure_threshold']))}"
        f"__xt{threshold_token(EXIT_THRESHOLD)}__fa"
    )


@dataclass
class PreparedRun:
    strategy_id: str
    output_dir: Path
    watchlist_path: Path
    strategy_config_path: Path


def find_base_shortlist_row() -> dict:
    shortlist = load_json(SHORTLIST_JSON)
    items = shortlist.get("shortlist") or shortlist.get("items") or shortlist.get("strategies") or []
    for row in items:
        if row.get("strategy_id") == BASE_STRATEGY_ID:
            return row
    raise SystemExit(f"missing shortlist row for {BASE_STRATEGY_ID}")


def action_metrics_from_paper_ledger(path: Path, selected_pack_id: str | None = None) -> dict:
    doc = load_json(path)
    items = doc.get("items") or []
    if selected_pack_id:
        for item in items:
            if item.get("selected_pack_id") == selected_pack_id:
                items = [item]
                break
    item = items[0] if items else {}
    action_sequence = item.get("action_sequence") or []
    open_count = 0
    close_count = 0
    reversal_count = 0
    for action_row in action_sequence:
        action = str(action_row.get("action") or "")
        if action.endswith("_OPEN"):
            open_count += 1
        elif "REVERSAL" in action:
            reversal_count += 1
        elif action in {"LONG_CLOSE", "SHORT_CLOSE"} or action.endswith("_LOW_EDGE_OPPOSITE"):
            close_count += 1
    return {
        "open_count": open_count,
        "close_count": close_count,
        "reversal_count": reversal_count,
        "fill_count": int(item.get("fill_event_count") or 0),
        "decision_count": int(item.get("decision_event_count") or 0),
        "gross_pnl": finite_or_none(item.get("replayed_realized_pnl_quote_gross")),
        "net_pnl": finite_or_none(item.get("replayed_realized_pnl_quote_net")),
        "total_fee": finite_or_none(item.get("total_fee_quote")) or 0.0,
        "funding_cost": finite_or_none(item.get("funding_cost_quote")) or 0.0,
        "turnover": finite_or_none(item.get("turnover_quote")),
        "cost_accounting_status": item.get("cost_accounting_status"),
        "profitability_status": item.get("profitability_status"),
    }


def prepare_run(output_dir: Path) -> PreparedRun:
    row = find_base_shortlist_row()
    strategy_id = compose_strategy_id(row)
    cell = dict(row["selected_cell"])
    cell.update({
        "exchange": "bybit",
        "symbol": "linkusdt",
        "stream": "trade",
        "pressure_threshold": ENTRY_THRESHOLD,
    })
    if output_dir.exists():
        shutil.rmtree(output_dir)
    (output_dir / "shadow_state").mkdir(parents=True, exist_ok=True)

    watchlist = {
        "schema_version": "phase7_microstructure_feeaware_watchlist_v0",
        "generated_ts_utc": iso_utc_now(),
        "governance": {
            "surface_role": "TASK_LOCAL_LINKUSDT_FEEAWARE_VALIDATION",
            "not_authoritative_for": ["global shadow watchlist", "ranking", "promotion"],
            "notes": [
                "linkusdt-only fee-aware validation",
                "global shadow_watchlist is not read or written",
                "single lever only: fee-aware entry gating on top of hysteresis",
            ],
        },
        "selected_count": 1,
        "items": [{
            "rank": 1,
            "pack_id": strategy_id,
            "pack_path": "",
            "exchange": "bybit",
            "symbols": ["linkusdt"],
            "decision_tier": "PHASE7_FEEAWARE_VALIDATION",
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
        "source_decision_tier": "PHASE7_FEEAWARE_VALIDATION",
        "exchange": "bybit",
        "stream": "trade",
        "symbols": ["linkusdt"],
        "source_family_report_path": "",
        "window": "feeaware_validation",
        "orderQty": 1,
        "params": {
            "delta_ms_list": [int(cell["delta_ms"])],
            "h_ms_list": [int(cell["h_ms"])],
            "pressure_threshold_list": [ENTRY_THRESHOLD],
            "exit_pressure_threshold": EXIT_THRESHOLD,
            "fee_aware_expected_edge_scale": EXPECTED_EDGE_SCALE,
            "fee_aware_fee_rate": FEE_RATE,
            "tolerance_ms": 0,
        },
        "selected_cell": cell,
    }

    watchlist_path = output_dir / "input_watchlist.json"
    strategy_config_path = output_dir / "strategy_config.json"
    watchlist_path.write_text(json.dumps(watchlist, indent=2, sort_keys=True) + "\n")
    strategy_config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    return PreparedRun(strategy_id=strategy_id, output_dir=output_dir, watchlist_path=watchlist_path, strategy_config_path=strategy_config_path)


def run_batch(prepared: PreparedRun, run_max_duration_sec: int, per_run_timeout_sec: int, heartbeat_ms: int, progress_interval_sec: int) -> int:
    command = [
        "python3",
        "tools/run-shadow-observation-batch-v0.py",
        "--watchlist", str(prepared.watchlist_path),
        "--max-items", "1",
        "--strategy", str(STRATEGY_PATH),
        "--strategy-config-json", prepared.strategy_config_path.read_text(),
        "--summary-json-path", str(prepared.output_dir / "summary_runtime.json"),
        "--history-jsonl", str(prepared.output_dir / "shadow_state/shadow_observation_history_v0.jsonl"),
        "--index-json", str(prepared.output_dir / "shadow_state/shadow_observation_index_v0.json"),
        "--phase6-state-dir", "tools/phase6_state",
        "--shadow-state-dir", str(prepared.output_dir / "shadow_state"),
        "--refresh-result-json", str(prepared.output_dir / "shadow_state/shadow_derived_surface_refresh_v0.json"),
        "--execution-ledger-jsonl", str(prepared.output_dir / "shadow_state/shadow_execution_ledger_v0.jsonl"),
        "--execution-pack-summary-json", str(prepared.output_dir / "shadow_state/shadow_execution_pack_summary_v0.json"),
        "--audit-base-dir", str(prepared.output_dir / "audit"),
        "--out-dir", str(prepared.output_dir / "batch_out"),
        "--result-json", str(prepared.output_dir / "shadow_observation_batch_result_v0.json"),
        "--per-run-timeout-sec", str(per_run_timeout_sec),
        "--run-max-duration-sec", str(run_max_duration_sec),
        "--heartbeat-ms", str(heartbeat_ms),
    ]
    print(
        "PHASE7_MICROSTRUCTURE_FEEAWARE_STARTED "
        f"strategy_id={prepared.strategy_id} run_max_duration_sec={run_max_duration_sec}",
        flush=True,
    )
    started_at = time.time()
    proc = subprocess.Popen(command)
    next_progress = started_at + progress_interval_sec
    while True:
        try:
            exit_code = proc.wait(timeout=max(1, int(next_progress - time.time())))
            break
        except subprocess.TimeoutExpired:
            elapsed = int(time.time() - started_at)
            print(
                "PHASE7_MICROSTRUCTURE_FEEAWARE_PROGRESS "
                f"elapsed_sec={elapsed} still_running=1",
                flush=True,
            )
            next_progress += progress_interval_sec
    print(
        "PHASE7_MICROSTRUCTURE_FEEAWARE_DONE "
        f"strategy_id={prepared.strategy_id} exit_code={exit_code}",
        flush=True,
    )
    return exit_code


def analyze_run(prepared: PreparedRun, result_json: Path, report_md: Path, run_mode: str) -> dict:
    baseline_doc = load_json(BASELINE_JSON)
    baseline_after = baseline_doc["after"]
    batch = load_json(prepared.output_dir / "shadow_observation_batch_result_v0.json")
    batch_result = (batch.get("results") or [{}])[0]
    summary_runtime = load_json(prepared.output_dir / "summary_runtime.json")
    summary_json = load_json(Path(batch_result["summary_json_path"]))
    baseline_action = action_metrics_from_paper_ledger(
        Path(baseline_doc["source_paths"]["futures_paper_ledger_json"]),
        baseline_doc["strategy_id"],
    )
    after_action = action_metrics_from_paper_ledger(
        prepared.output_dir / "shadow_state/shadow_futures_paper_ledger_v1.json",
        prepared.strategy_id,
    )

    base = {
        "fill_count": int(baseline_after["fill_count"]),
        "decision_count": int(baseline_after["decision_count"]),
        "gross_pnl": float(baseline_after["gross_pnl"]),
        "net_pnl": float(baseline_after["net_pnl"]),
        "total_fee": float(baseline_after["total_fee"]),
        "processed_event_count": int(baseline_after["processed_event_count"]),
        "gross_pnl_per_fill": float(baseline_after["gross_pnl_per_fill"]),
        "fee_per_fill": float(baseline_after["fee_per_fill"]),
        "gross_pnl_per_1k_events": float(baseline_after["gross_pnl_per_1k_events"]),
        "open_count": int(baseline_after["open_count"]),
        "close_count": int(baseline_after["close_count"]),
        "reversal_count": int(baseline_after["reversal_count"]),
        "reversal_per_1k_events": float(baseline_after["reversal_per_1k_events"]),
    }
    after = {
        "verify_soft_live_pass": bool(batch_result.get("verify_soft_live_pass")),
        "processed_event_count": summary_json.get("processed_event_count"),
        "decision_count": after_action["decision_count"],
        "fill_count": after_action["fill_count"],
        "gross_pnl": after_action["gross_pnl"],
        "net_pnl": after_action["net_pnl"],
        "total_fee": after_action["total_fee"],
        "funding_cost": after_action["funding_cost"],
        "turnover": after_action["turnover"],
        "gross_pnl_per_fill": ratio(after_action["gross_pnl"], after_action["fill_count"]),
        "fee_per_fill": ratio(after_action["total_fee"], after_action["fill_count"]),
        "gross_pnl_per_1k_events": ratio((after_action["gross_pnl"] or 0) * 1000, summary_json.get("processed_event_count")),
        "cost_accounting_status": after_action["cost_accounting_status"],
        "profitability_status": after_action["profitability_status"],
        "stop_reason": summary_runtime.get("stop_reason"),
        "run_duration_sec": summary_json.get("run_duration_sec"),
        "open_count": after_action["open_count"],
        "close_count": after_action["close_count"],
        "reversal_count": after_action["reversal_count"],
        "reversal_per_1k_events": ratio(after_action["reversal_count"] * 1000, summary_json.get("processed_event_count")),
    }

    fill_reduction_ratio = ratio(base["fill_count"] - after["fill_count"], base["fill_count"])
    fee_reduction_ratio = ratio(base["total_fee"] - after["total_fee"], base["total_fee"])
    gross_per_fill_change_ratio = ratio((after["gross_pnl_per_fill"] or 0) - (base["gross_pnl_per_fill"] or 0), abs(base["gross_pnl_per_fill"] or 0))
    gross_preserved_ratio = ratio(after["gross_pnl"], base["gross_pnl"])
    net_improvement_quote = None if after["net_pnl"] is None else after["net_pnl"] - base["net_pnl"]

    if after["verify_soft_live_pass"] is not True or (after["processed_event_count"] or 0) <= 0:
        verdict = "WORSE"
        reason = "smoke/full run invalid or no processed events"
    elif run_mode == "smoke":
        verdict = "SMOKE_PASS"
        reason = "verify_soft_live_pass=true and processed_event_count>0"
    elif after["fill_count"] <= 0:
        verdict = "WORSE"
        reason = "signal killed or no fills"
    elif after["gross_pnl"] is not None and after["gross_pnl"] <= 0:
        verdict = "WORSE"
        reason = "gross edge became non-positive"
    elif (
        (fill_reduction_ratio or 0) > 0.2
        and (fee_reduction_ratio or 0) > 0.2
        and (gross_per_fill_change_ratio or 0) > 0
        and (net_improvement_quote or 0) > 0
        and (gross_preserved_ratio or 0) >= 0.5
    ):
        verdict = "IMPROVED"
        reason = "fewer economically filtered trades improved pnl per fill and net outcome without collapsing gross edge"
    else:
        verdict = "NO_IMPROVEMENT"
        reason = "critical fee-aware trade-quality target not met"

    payload = {
        "schema_version": "phase7_microstructure_feeaware_v0",
        "generated_ts_utc": iso_utc_now(),
        "run_mode": run_mode,
        "strategy_id": prepared.strategy_id,
        "family_id": "microstructure_imbalance_v1",
        "exchange": "bybit",
        "symbol": "linkusdt",
        "lever": {
            "selected": "FEE_AWARE_ENTRY_GATING",
            "implementation": "keep hysteresis, gate flat entries and reversals when abs(pressure) * scale <= estimated entry fee",
            "signal_math_changed": False,
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "expected_edge_scale": EXPECTED_EDGE_SCALE,
            "fee_rate": FEE_RATE,
            "fee_reference": "dynamic price * orderQty * fee_rate",
        },
        "baseline": base,
        "after": after,
        "comparison": {
            "fill_reduction_ratio": fill_reduction_ratio,
            "fee_reduction_ratio": fee_reduction_ratio,
            "gross_per_fill_change_ratio": gross_per_fill_change_ratio,
            "gross_preserved_ratio": gross_preserved_ratio,
            "net_improvement_quote": net_improvement_quote,
        },
        "verdict": verdict,
        "verdict_reason": reason,
        "source_paths": {
            "baseline_hysteresis_json": str(BASELINE_JSON),
            "full_run_dir": str(prepared.output_dir),
            "batch_result_json": str(prepared.output_dir / "shadow_observation_batch_result_v0.json"),
            "futures_paper_ledger_json": str(prepared.output_dir / "shadow_state/shadow_futures_paper_ledger_v1.json"),
            "summary_runtime_json": str(prepared.output_dir / "summary_runtime.json"),
        },
    }
    result_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    report_md.write_text(
        "# Phase7 Microstructure Fee-Aware v0\n\n"
        f"- strategy: `{prepared.strategy_id}`\n"
        "- lever: `FEE_AWARE_ENTRY_GATING`, hysteresis kept, fee-aware gate enabled\n"
        f"- verdict: `{verdict}`\n"
        f"- reason: {reason}\n"
        f"- baseline fills: `{base['fill_count']}` after fills: `{after['fill_count']}`\n"
        f"- baseline gross/fill: `{base['gross_pnl_per_fill']}` after gross/fill: `{after['gross_pnl_per_fill']}`\n"
        f"- baseline fees: `{base['total_fee']}` after fees: `{after['total_fee']}`\n"
        f"- baseline net: `{base['net_pnl']}` after net: `{after['net_pnl']}`\n"
        f"- baseline gross: `{base['gross_pnl']}` after gross: `{after['gross_pnl']}`\n\n"
        "No ranking, promotion, Phase5, hold, threshold, or extra filtering levers were changed.\n"
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["smoke", "full"], required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--result-json", required=True)
    parser.add_argument("--report-md", required=True)
    parser.add_argument("--run-max-duration-sec", type=int, required=True)
    parser.add_argument("--per-run-timeout-sec", type=int, required=True)
    parser.add_argument("--heartbeat-ms", type=int, default=5000)
    parser.add_argument("--progress-interval-sec", type=int, default=60)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = ROOT / args.output_dir
    result_json = ROOT / args.result_json
    report_md = ROOT / args.report_md
    result_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)

    prepared = prepare_run(output_dir)
    exit_code = run_batch(
        prepared,
        run_max_duration_sec=args.run_max_duration_sec,
        per_run_timeout_sec=args.per_run_timeout_sec,
        heartbeat_ms=args.heartbeat_ms,
        progress_interval_sec=args.progress_interval_sec,
    )
    payload = analyze_run(prepared, result_json, report_md, args.mode)
    print("PHASE7_MICROSTRUCTURE_FEEAWARE_COMPLETE", flush=True)
    print(f"result_json={result_json}", flush=True)
    print(f"report_md={report_md}", flush=True)
    print(f"run_exit_code={exit_code}", flush=True)
    print(f"verdict={payload['verdict']}", flush=True)
    return 0 if exit_code == 0 else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
