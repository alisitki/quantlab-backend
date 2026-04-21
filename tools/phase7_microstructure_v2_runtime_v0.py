#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
PHASE6 = TOOLS / "phase6_state"
SOURCE_RESULT_JSON = TOOLS / "phase7_microstructure_shadow_result_v1.json"
BATCH_TOOL = TOOLS / "run-shadow-observation-batch-v0.py"
STRATEGY_FILE = "core/strategy/strategies/MicrostructureImbalanceV2Strategy.js"
DEFAULT_OUTPUT_DIR = TOOLS / "phase7_microstructure_v2_output" / "smoke_run"
DEFAULT_RESULT_JSON = TOOLS / "phase7_microstructure_v2_output" / "smoke_result_v0.json"
DEFAULT_REPORT_MD = TOOLS / "phase7_microstructure_v2_output" / "smoke_report_v0.md"
FAMILY_ID = "microstructure_imbalance_v2"
SOURCE_STRATEGY_ID = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"


class Phase7MicrostructureV2Error(RuntimeError):
    pass


def fail(message: str) -> None:
    raise Phase7MicrostructureV2Error(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{label}_missing:{path}")
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run microstructure V2 smoke / bounded validation for linkusdt")
    parser.add_argument("--source-result-json", type=Path, default=SOURCE_RESULT_JSON)
    parser.add_argument("--batch-tool", type=Path, default=BATCH_TOOL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--result-json", type=Path, default=DEFAULT_RESULT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--run-max-duration-sec", type=int, default=600)
    parser.add_argument("--per-run-timeout-sec", type=int, default=660)
    parser.add_argument("--subprocess-timeout-sec", type=int, default=900)
    parser.add_argument("--heartbeat-ms", type=int, default=5000)
    parser.add_argument("--exit-pressure-threshold", type=float, default=0.1)
    parser.add_argument("--max-venue-divergence-score", type=float, default=0.35)
    parser.add_argument("--require-external-alignment-count", type=int, default=2)
    parser.add_argument("--min-external-available-count", type=int, default=2)
    parser.add_argument("--enable-btc-support", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.run_max_duration_sec <= 0:
        fail("invalid_run_max_duration_sec")
    if args.per_run_timeout_sec < args.run_max_duration_sec:
        fail("per_run_timeout_must_cover_run_max_duration")
    if args.subprocess_timeout_sec < args.per_run_timeout_sec:
        fail("subprocess_timeout_must_cover_per_run_timeout")
    if args.require_external_alignment_count <= 0:
        fail("invalid_require_external_alignment_count")
    if args.min_external_available_count <= 0:
        fail("invalid_min_external_available_count")
    return args


def select_source_row(source_doc: dict[str, Any]) -> dict[str, Any]:
    items = source_doc.get("results")
    if not isinstance(items, list):
        fail("source_results_missing")
    for row in items:
        if not isinstance(row, dict):
            continue
        if str(row.get("strategy_id") or "").strip() == SOURCE_STRATEGY_ID:
            return row
    fail(f"source_strategy_not_found:{SOURCE_STRATEGY_ID}")


def build_runtime_strategy_config(source_row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    cell = dict(source_row.get("source_shortlist_cell") or {})
    if not cell:
        fail("source_shortlist_cell_missing")
    cell["exchange"] = "bybit"
    cell["stream"] = "trade"
    cell["symbol"] = "linkusdt"
    confirmation = {
        "window_ms": int(cell["delta_ms"]),
        "venues": ["binance", "okx"],
        "required_alignment_count": int(args.require_external_alignment_count),
        "min_available_count": int(args.min_external_available_count),
        "max_divergence_score": float(args.max_venue_divergence_score),
    }
    btc_support = {
        "mode": "REQUIRE_SUPPORTIVE" if args.enable_btc_support else "DISABLED",
        "exchanges": ["bybit", "okx"],
        "window_ms": int(cell["delta_ms"]),
    }
    return {
        "binding_mode": "PAPER_DIRECTIONAL_V1",
        "family_id": FAMILY_ID,
        "source_pack_id": f"{SOURCE_STRATEGY_ID}__v2",
        "source_decision_tier": "PHASE7_MICROSTRUCTURE_V2_CORE",
        "exchange": "bybit",
        "stream": "trade",
        "symbols": ["linkusdt"],
        "source_family_report_path": "",
        "window": "live",
        "orderQty": 1,
        "auxiliary_feeds": [
            {"exchange": "binance", "symbols": ["linkusdt"]},
            {"exchange": "okx", "symbols": ["linkusdt"]},
            *(
                [
                    {"exchange": "bybit", "symbols": ["btcusdt"]},
                    {"exchange": "okx", "symbols": ["btcusdt"]},
                ]
                if args.enable_btc_support
                else []
            ),
        ],
        "params": {
            "delta_ms_list": [int(cell["delta_ms"])],
            "h_ms_list": [int(cell["h_ms"])],
            "pressure_threshold_list": [float(cell["pressure_threshold"])],
            "tolerance_ms": 0,
            "exit_pressure_threshold": float(args.exit_pressure_threshold),
            "confirmation": confirmation,
            "btc_support": btc_support,
        },
        "selected_cell": cell,
    }


def build_watchlist(strategy_id: str) -> dict[str, Any]:
    return {
        "schema_version": "phase7_microstructure_v2_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "surface_role": "TASK_LOCAL_MICROSTRUCTURE_V2_SMOKE",
            "authoritative_scope": "Single-strategy linkusdt V2 smoke only.",
            "not_authoritative_for": ["global shadow watchlist", "ranking", "promotion"],
        },
        "selected_count": 1,
        "items": [
            {
                "rank": 1,
                "pack_id": strategy_id,
                "pack_path": "",
                "exchange": "bybit",
                "symbols": ["linkusdt"],
                "decision_tier": "PHASE7_MICROSTRUCTURE_V2_CORE",
                "selection_slot": "bybit/trade/linkusdt",
                "strategy_id": strategy_id,
                "family_id": FAMILY_ID,
            }
        ],
    }


def count_execution_events(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, int]:
    counts = {"DECISION": 0, "FILL": 0, "RISK_REJECT": 0}
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if str(row.get("live_run_id") or "").strip() != live_run_id:
            continue
        event_type = str(row.get("event_type") or "").strip().upper()
        if event_type in counts:
            counts[event_type] += 1
    return counts


def find_context_presence(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, Any]:
    matched = 0
    populated = 0
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if str(row.get("live_run_id") or "").strip() != live_run_id:
            continue
        matched += 1
        trade_context = row.get("trade_context")
        if not isinstance(trade_context, dict):
            continue
        opening = trade_context.get("opening_trade")
        if not isinstance(opening, dict):
            continue
        if opening.get("venue_alignment_count") is None:
            continue
        if opening.get("venue_divergence_score") is None:
            continue
        if opening.get("entry_decision_reason") is None:
            continue
        populated += 1
    return {
        "matched_event_rows": matched,
        "opening_trade_context_rows_with_v2_fields": populated,
        "context_fields_populated": populated > 0,
    }


def resolve_processed_event_count(item: dict[str, Any], summary: dict[str, Any]) -> int:
    for source in (item, summary):
        value = source.get("processed_event_count")
        if value in (None, "", "unknown"):
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return 0


def run_batch(args: argparse.Namespace, strategy_config: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    watchlist_path = output_dir / "input_watchlist.json"
    shadow_state_dir = output_dir / "shadow_state"
    batch_out_dir = output_dir / "batch_out"
    audit_base_dir = output_dir / "audit"
    summary_json_path = output_dir / "summary_runtime.json"
    history_jsonl = shadow_state_dir / "shadow_observation_history_v0.jsonl"
    index_json = shadow_state_dir / "shadow_observation_index_v0.json"
    result_json = output_dir / "shadow_observation_batch_result_v0.json"
    refresh_result_json = shadow_state_dir / "shadow_derived_surface_refresh_v0.json"
    execution_ledger_jsonl = shadow_state_dir / "shadow_execution_ledger_v0.jsonl"
    execution_pack_summary_json = shadow_state_dir / "shadow_execution_pack_summary_v0.json"
    top_stdout_log = output_dir / "batch_command_stdout.log"
    top_stderr_log = output_dir / "batch_command_stderr.log"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    shadow_state_dir.mkdir(parents=True, exist_ok=True)
    write_json(watchlist_path, build_watchlist(strategy_config["source_pack_id"]))

    cmd = [
        sys.executable,
        str(Path(args.batch_tool).resolve()),
        "--watchlist",
        str(watchlist_path),
        "--max-items",
        "1",
        "--strategy",
        STRATEGY_FILE,
        "--strategy-config-json",
        json.dumps(strategy_config, sort_keys=True),
        "--summary-json-path",
        str(summary_json_path),
        "--history-jsonl",
        str(history_jsonl),
        "--index-json",
        str(index_json),
        "--phase6-state-dir",
        str(PHASE6.resolve()),
        "--shadow-state-dir",
        str(shadow_state_dir),
        "--refresh-result-json",
        str(refresh_result_json),
        "--execution-ledger-jsonl",
        str(execution_ledger_jsonl),
        "--execution-pack-summary-json",
        str(execution_pack_summary_json),
        "--audit-base-dir",
        str(audit_base_dir),
        "--out-dir",
        str(batch_out_dir),
        "--result-json",
        str(result_json),
        "--per-run-timeout-sec",
        str(int(args.per_run_timeout_sec)),
        "--run-max-duration-sec",
        str(int(args.run_max_duration_sec)),
        "--heartbeat-ms",
        str(int(args.heartbeat_ms)),
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    with top_stdout_log.open("w", encoding="utf-8") as stdout_handle, top_stderr_log.open("w", encoding="utf-8") as stderr_handle:
        try:
            completed = subprocess.run(
                cmd,
                cwd=str(ROOT),
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
                timeout=int(args.subprocess_timeout_sec),
            )
            exit_code: int | str = int(completed.returncode)
            timed_out = False
        except subprocess.TimeoutExpired:
            exit_code = "timeout"
            timed_out = True
    return {
        "command": " ".join(cmd),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "watchlist_path": str(watchlist_path.resolve()),
        "shadow_state_dir": str(shadow_state_dir.resolve()),
        "batch_out_dir": str(batch_out_dir.resolve()),
        "batch_result_json_path": str(result_json.resolve()),
        "summary_json_path": str(summary_json_path.resolve()),
        "top_stdout_log": str(top_stdout_log.resolve()),
        "top_stderr_log": str(top_stderr_log.resolve()),
    }


def build_result(args: argparse.Namespace, source_row: dict[str, Any], strategy_config: dict[str, Any], run_meta: dict[str, Any]) -> dict[str, Any]:
    if run_meta["timed_out"]:
        return {
            "status": "INVALID_RUN",
            "verify_soft_live_pass": False,
            "processed_event_count": 0,
            "decision_count": 0,
            "fill_count": 0,
            "context_fields_populated": False,
            "reason": "subprocess_timeout",
            "artifacts": run_meta,
        }
    if run_meta["exit_code"] != 0:
        return {
            "status": "INVALID_RUN",
            "verify_soft_live_pass": False,
            "processed_event_count": 0,
            "decision_count": 0,
            "fill_count": 0,
            "context_fields_populated": False,
            "reason": f"batch_exit_{run_meta['exit_code']}",
            "artifacts": run_meta,
        }
    batch_result = load_json(Path(run_meta["batch_result_json_path"]), "batch_result_json")
    results = batch_result.get("results")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        fail("batch_result_invalid")
    item = results[0]
    summary_path = Path(str(item.get("summary_json_path") or ""))
    summary = load_json(summary_path, "summary_json") if summary_path.exists() else {}
    shadow_state_dir = Path(run_meta["shadow_state_dir"])
    execution_events_rows = load_jsonl(shadow_state_dir / "shadow_execution_events_v1.jsonl")
    pack_id = str(item.get("pack_id") or strategy_config["source_pack_id"])
    live_run_id = str(item.get("live_run_id") or "")
    event_counts = count_execution_events(execution_events_rows, pack_id, live_run_id)
    context_presence = find_context_presence(execution_events_rows, pack_id, live_run_id)

    metrics = {
        "verify_soft_live_pass": item.get("verify_soft_live_pass") is True,
        "processed_event_count": resolve_processed_event_count(item, summary),
        "decision_count": int(event_counts["DECISION"]),
        "fill_count": int(event_counts["FILL"]),
        "risk_reject_count": int(event_counts["RISK_REJECT"]),
        "run_duration_sec": summary.get("run_duration_sec"),
        "stop_reason": summary.get("stop_reason"),
        "heartbeat_count": summary.get("heartbeat_count"),
        **context_presence,
    }
    smoke_pass = (
        metrics["verify_soft_live_pass"] is True
        and metrics["processed_event_count"] > 0
    )
    return {
        "schema_version": "phase7_microstructure_v2_smoke_result_v0",
        "generated_ts_utc": utc_now_iso(),
        "authoritative_sources": {
            "source_result_json": str(Path(args.source_result_json).resolve()),
            "strategy_file": str((ROOT / STRATEGY_FILE).resolve()),
            "batch_tool": str(Path(args.batch_tool).resolve()),
        },
        "source_strategy_id": str(source_row.get("strategy_id") or ""),
        "strategy_id": strategy_config["source_pack_id"],
        "family_id": FAMILY_ID,
        "exchange": "bybit",
        "symbol": "linkusdt",
        "runtime_strategy_file": STRATEGY_FILE,
        "runtime_strategy_config_excerpt": {
            "selected_cell": strategy_config["selected_cell"],
            "confirmation": strategy_config["params"]["confirmation"],
            "btc_support": strategy_config["params"]["btc_support"],
            "auxiliary_feeds": strategy_config["auxiliary_feeds"],
        },
        "status": "SMOKE_PASS" if smoke_pass else "SMOKE_FAIL",
        "smoke_gate_pass": smoke_pass,
        "metrics": metrics,
        "artifacts": {
            **run_meta,
            "summary_json_path": str(summary_path.resolve()) if summary_path.exists() else None,
            "execution_events_jsonl": str((shadow_state_dir / "shadow_execution_events_v1.jsonl").resolve()),
            "futures_paper_ledger_json": str((shadow_state_dir / "shadow_futures_paper_ledger_v1.json").resolve()),
        },
    }


def write_report(path: Path, result: dict[str, Any]) -> None:
    metrics = result["metrics"]
    lines = [
        "# Phase7 Microstructure V2 Smoke v0",
        "",
        "This smoke validates only implementation correctness and compact observability for the new cross-exchange-confirmed V2 runtime.",
        "",
        f"- Status: `{result['status']}`",
        f"- verify_soft_live_pass: `{metrics['verify_soft_live_pass']}`",
        f"- processed_event_count: `{metrics['processed_event_count']}`",
        f"- decision_count: `{metrics['decision_count']}`",
        f"- fill_count: `{metrics['fill_count']}`",
        f"- context_fields_populated: `{metrics['context_fields_populated']}`",
        "- Smoke gate: `verify_soft_live_pass && processed_event_count > 0`",
        "",
        "**What Changed vs V1**",
        "",
        "- Local bybit microstructure trigger still starts the decision path.",
        "- Entry / reversal now require same-symbol external venue confirmation from `binance` and `okx`.",
        "- Low divergence is enforced through a fixed `max_divergence_score` gate.",
        "- BTC support exists as an optional secondary gate; it is disabled in this smoke by default.",
        "- A short smoke may still produce zero fills; that is not treated as an integration failure if the runner processed live events cleanly.",
        "",
        "**Artifacts**",
        "",
        f"- [result json]({result['artifacts']['batch_result_json_path']})",
        f"- [summary json]({result['artifacts']['summary_json_path']})",
        f"- [execution events]({result['artifacts']['execution_events_jsonl']})",
        f"- [paper ledger]({result['artifacts']['futures_paper_ledger_json']})",
        "",
        "**Next Sprint Validation Command**",
        "",
        "```bash",
        "python3 tools/phase7_microstructure_v2_runtime_v0.py "
        "--output-dir tools/phase7_microstructure_v2_output/full_run "
        "--result-json tools/phase7_microstructure_v2_output/full_run_result_v0.json "
        "--report-md tools/phase7_microstructure_v2_output/full_run_report_v0.md "
        "--run-max-duration-sec 21600 "
        "--per-run-timeout-sec 21900 "
        "--subprocess-timeout-sec 22500 "
        "--heartbeat-ms 5000",
        "```",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_doc = load_json(Path(args.source_result_json), "source_result_json")
    source_row = select_source_row(source_doc)
    strategy_config = build_runtime_strategy_config(source_row, args)
    run_meta = run_batch(args, strategy_config, Path(args.output_dir).resolve())
    result = build_result(args, source_row, strategy_config, run_meta)
    write_json(Path(args.result_json).resolve(), result)
    write_report(Path(args.report_md).resolve(), result)
    print("PHASE7_MICROSTRUCTURE_V2_RUNTIME_COMPLETE")
    print(f"status={result['status']}")
    print(f"result_json={Path(args.result_json).resolve()}")
    print(f"report_md={Path(args.report_md).resolve()}")
    return 0 if result["smoke_gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
