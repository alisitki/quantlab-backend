#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
PHASE6 = TOOLS / "phase6_state"
EXPECTATION_OUTPUT = TOOLS / "phase7_expectation_audit_output"
DEFAULT_SHORTLIST_JSON = PHASE6 / "shadow_shortlist_v0.json"
DEFAULT_EXPECTATION_REPORT_JSON = EXPECTATION_OUTPUT / "expectation_audit_report_v0.json"
DEFAULT_WINDOW_PLAN_JSON = EXPECTATION_OUTPUT / "shadow_window_plan_v0.json"
DEFAULT_BINDING_JSON = PHASE6 / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_CANONICAL_TRUTH_REGISTRY = TOOLS / "system_state" / "canonical_truth_registry_v0.json"
DEFAULT_BATCH_TOOL = TOOLS / "run-shadow-observation-batch-v0.py"
DEFAULT_OUTPUT_DIR = TOOLS / "phase7_medium_shadow_validation_output" / "full_run"
DEFAULT_RESULT_JSON = TOOLS / "phase7_medium_shadow_result_v0.json"
EXPECTED_SYMBOLS = ("avaxusdt", "ethusdt", "linkusdt", "xrpusdt")
TARGET_BAND = "MEDIUM_FREQUENCY"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
RUN_SEMANTICS = "ISOLATED_PAPER_DIRECTIONAL_SHADOW"
SCHEMA_VERSION = "phase7_medium_shadow_result_v0"
DEFAULT_MAX_PARALLEL = 4
DEFAULT_RUN_MAX_DURATION_SEC = 43_200
DEFAULT_PER_RUN_TIMEOUT_SEC = 43_500
DEFAULT_SUBPROCESS_TIMEOUT_SEC = 44_400
DEFAULT_HEARTBEAT_MS = 5_000
DEFAULT_MAX_CONTINUATION = 2
DEFAULT_MAX_TRADE_TRANSITIONS_PER_1K_EVENTS = 2.5
DEFAULT_PROGRESS_INTERVAL_SEC = 60
VERDICT_PRIORITY = {
    "CONTINUE": 0,
    "WEAK": 1,
    "NO_SIGNAL": 2,
    "FAIL_CHURN": 3,
    "INVALID_RUN": 4,
}


class MediumShadowValidationError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise MediumShadowValidationError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run isolated medium-frequency Phase7 shadow validation.")
    parser.add_argument("--shortlist-json", default=str(DEFAULT_SHORTLIST_JSON))
    parser.add_argument("--expectation-report-json", default=str(DEFAULT_EXPECTATION_REPORT_JSON))
    parser.add_argument("--window-plan-json", default=str(DEFAULT_WINDOW_PLAN_JSON))
    parser.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    parser.add_argument("--canonical-truth-registry", default=str(DEFAULT_CANONICAL_TRUTH_REGISTRY))
    parser.add_argument("--batch-tool", default=str(DEFAULT_BATCH_TOOL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--max-parallel", type=int, default=DEFAULT_MAX_PARALLEL)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--subprocess-timeout-sec", type=int, default=DEFAULT_SUBPROCESS_TIMEOUT_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--progress-interval-sec", type=int, default=DEFAULT_PROGRESS_INTERVAL_SEC)
    parser.add_argument("--max-continuation", type=int, default=DEFAULT_MAX_CONTINUATION)
    parser.add_argument(
        "--max-trade-transitions-per-1k-events",
        type=float,
        default=DEFAULT_MAX_TRADE_TRANSITIONS_PER_1K_EVENTS,
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.max_parallel <= 0:
        fail(f"invalid_max_parallel:{args.max_parallel}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.subprocess_timeout_sec <= 0:
        fail(f"invalid_subprocess_timeout_sec:{args.subprocess_timeout_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    if args.progress_interval_sec <= 0:
        fail(f"invalid_progress_interval_sec:{args.progress_interval_sec}")
    if args.max_continuation < 0:
        fail(f"invalid_max_continuation:{args.max_continuation}")
    if args.max_trade_transitions_per_1k_events <= 0:
        fail(f"invalid_max_trade_transitions_per_1k_events:{args.max_trade_transitions_per_1k_events}")
    if args.subprocess_timeout_sec < args.per_run_timeout_sec:
        fail("subprocess_timeout_sec_must_cover_per_run_timeout_sec")
    return args


def load_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require_registry_path(registry: dict[str, Any], concept_name: str, expected_path: Path) -> dict[str, Any]:
    expected_resolved = str(expected_path.resolve())
    for item in registry.get("concepts", []):
        if str(item.get("concept") or "").strip() != concept_name:
            continue
        authoritative_now = item.get("authoritative_now")
        if not isinstance(authoritative_now, list):
            fail(f"canonical_registry_concept_missing_authoritative_now:{concept_name}")
        for entry in authoritative_now:
            if not isinstance(entry, dict):
                continue
            entry_path = str(entry.get("path") or "").strip()
            if not entry_path:
                continue
            if str(Path(entry_path).resolve()) == expected_resolved:
                return item
        fail(f"canonical_registry_path_mismatch:{concept_name}:{expected_path}")
    fail(f"canonical_registry_concept_missing:{concept_name}")


def shortlist_rows(shortlist_doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows = shortlist_doc.get("strategies")
    if not isinstance(rows, list):
        fail("shortlist_missing_strategies")
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            fail("shortlist_row_not_object")
        out.append(row)
    if not out:
        fail("shortlist_empty")
    return out


def expectation_rows(expectation_doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows = expectation_doc.get("rows")
    if not isinstance(rows, list):
        fail("expectation_report_missing_rows")
    return [row for row in rows if isinstance(row, dict)]


def binding_by_strategy_id(binding_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = binding_doc.get("items")
    if not isinstance(items, list):
        fail("binding_missing_items")
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        strategy_id = str(item.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = item
    return out


def normalize_symbols(binding_row: dict[str, Any]) -> list[str]:
    values = binding_row.get("symbols")
    if not isinstance(values, list):
        fail(f"binding_symbols_invalid:{binding_row.get('strategy_id')}")
    symbols = [str(value or "").strip().upper() for value in values if str(value or "").strip()]
    if not symbols:
        fail(f"binding_symbols_empty:{binding_row.get('strategy_id')}")
    return symbols


def slugify(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value or ""))
    compact = "_".join(part for part in cleaned.split("_") if part)
    return compact[:80] or "item"


def selected_targets(
    shortlist_doc: dict[str, Any],
    expectation_doc: dict[str, Any],
    window_plan_doc: dict[str, Any],
    binding_doc: dict[str, Any],
) -> list[dict[str, Any]]:
    shortlist = shortlist_rows(shortlist_doc)
    shortlist_by_id = {
        str(row.get("strategy_id") or "").strip(): row
        for row in shortlist
        if str(row.get("strategy_id") or "").strip()
    }
    expectation_map = {
        str(row.get("strategy_id") or "").strip(): row
        for row in expectation_rows(expectation_doc)
        if str(row.get("strategy_id") or "").strip() and str(row.get("classification_band") or "").strip() == TARGET_BAND
    }
    multi_hour_ids = window_plan_doc.get("multi_hour_strategy_ids")
    if not isinstance(multi_hour_ids, list):
        fail("window_plan_missing_multi_hour_strategy_ids")
    multi_hour_set = {str(value or "").strip() for value in multi_hour_ids if str(value or "").strip()}
    binding_map = binding_by_strategy_id(binding_doc)
    target_ids = sorted(set(shortlist_by_id) & set(expectation_map) & multi_hour_set & set(binding_map))
    targets: list[dict[str, Any]] = []
    for strategy_id in target_ids:
        shortlist_row = shortlist_by_id[strategy_id]
        binding_row = binding_map[strategy_id]
        if str(binding_row.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
            continue
        symbol = str(shortlist_row.get("selected_symbol") or binding_row.get("selected_symbol") or "").strip().lower()
        if not symbol:
            fail(f"target_symbol_missing:{strategy_id}")
        rank = int(shortlist_row.get("rank") or 0)
        targets.append(
            {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "rank": rank,
                "shortlist_row": shortlist_row,
                "expectation_row": expectation_map[strategy_id],
                "binding_row": binding_row,
            }
        )
    targets.sort(key=lambda item: (int(item["rank"]), str(item["symbol"]), str(item["strategy_id"])))
    actual_symbols = tuple(sorted(item["symbol"] for item in targets))
    if actual_symbols != EXPECTED_SYMBOLS:
        fail(f"medium_symbol_assertion_failed:expected={EXPECTED_SYMBOLS}:actual={actual_symbols}")
    if len(targets) != len(EXPECTED_SYMBOLS):
        fail(f"medium_target_count_mismatch:{len(targets)}")
    return targets


def build_watchlist(target: dict[str, Any], shortlist_json_path: Path, binding_json_path: Path) -> dict[str, Any]:
    shortlist_row = target["shortlist_row"]
    binding_row = target["binding_row"]
    exchange = str(binding_row.get("exchange") or shortlist_row.get("exchange") or "").strip()
    stream = str(binding_row.get("stream") or shortlist_row.get("stream") or "").strip()
    runtime_cfg = binding_row.get("runtime_strategy_config") if isinstance(binding_row.get("runtime_strategy_config"), dict) else {}
    decision_tier = str(runtime_cfg.get("source_decision_tier") or binding_row.get("decision_tier") or "").strip()
    return {
        "schema_version": "phase7_medium_shadow_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "surface_role": "TASK_LOCAL_MEDIUM_FREQUENCY_SHADOW_SELECTION",
            "authoritative_scope": "Single-strategy task-local selection only.",
            "not_authoritative_for": [
                "global shadow watchlist",
                "ranking",
                "promotion",
            ],
            "notes": [
                "This watchlist is isolated to one medium-frequency Phase7 validation run.",
                "It intentionally does not read or mutate the global shadow watchlist.",
            ],
        },
        "source_shadow_shortlist_json": str(shortlist_json_path.resolve()),
        "source_candidate_strategy_runtime_binding_json": str(binding_json_path.resolve()),
        "selected_count": 1,
        "items": [
            {
                "rank": int(target["rank"]),
                "pack_id": str(binding_row.get("pack_id") or "").strip(),
                "pack_path": str(binding_row.get("pack_path") or "").strip(),
                "exchange": exchange,
                "symbols": normalize_symbols(binding_row),
                "decision_tier": decision_tier,
                "selection_slot": f"{exchange}/{stream}" if exchange and stream else "",
            }
        ],
    }


def run_batch_for_target(args: argparse.Namespace, target: dict[str, Any], run_root: Path) -> dict[str, Any]:
    shortlist_row = target["shortlist_row"]
    binding_row = target["binding_row"]
    watchlist_path = run_root / "input_watchlist.json"
    shadow_state_dir = run_root / "shadow_state"
    batch_out_dir = run_root / "batch_out"
    audit_base_dir = run_root / "audit"
    summary_json_path = run_root / "summary_runtime.json"
    history_jsonl = shadow_state_dir / "shadow_observation_history_v0.jsonl"
    index_json = shadow_state_dir / "shadow_observation_index_v0.json"
    result_json = run_root / "shadow_observation_batch_result_v0.json"
    refresh_result_json = shadow_state_dir / "shadow_derived_surface_refresh_v0.json"
    execution_ledger_jsonl = shadow_state_dir / "shadow_execution_ledger_v0.jsonl"
    execution_pack_summary_json = shadow_state_dir / "shadow_execution_pack_summary_v0.json"
    top_stdout_log = run_root / "batch_command_stdout.log"
    top_stderr_log = run_root / "batch_command_stderr.log"
    if run_root.exists():
        shutil.rmtree(run_root)
    run_root.mkdir(parents=True, exist_ok=True)
    shadow_state_dir.mkdir(parents=True, exist_ok=True)
    write_json(watchlist_path, build_watchlist(target, Path(args.shortlist_json), Path(args.binding_json)))

    runtime_strategy_file = str(binding_row.get("runtime_strategy_file") or "").strip()
    runtime_strategy_config = binding_row.get("runtime_strategy_config")
    if not runtime_strategy_file:
        fail(f"binding_runtime_strategy_file_missing:{target['strategy_id']}")
    if not isinstance(runtime_strategy_config, dict):
        fail(f"binding_runtime_strategy_config_missing:{target['strategy_id']}")

    cmd = [
        sys.executable,
        str(Path(args.batch_tool).resolve()),
        "--watchlist",
        str(watchlist_path),
        "--max-items",
        "1",
        "--strategy",
        runtime_strategy_file,
        "--strategy-config-json",
        json.dumps(runtime_strategy_config, sort_keys=True),
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
        "watchlist_path": str(watchlist_path),
        "shadow_state_dir": str(shadow_state_dir),
        "batch_out_dir": str(batch_out_dir),
        "audit_base_dir": str(audit_base_dir),
        "summary_json_path": str(summary_json_path),
        "batch_result_json_path": str(result_json),
        "refresh_result_json_path": str(refresh_result_json),
        "execution_ledger_jsonl": str(execution_ledger_jsonl),
        "execution_pack_summary_json": str(execution_pack_summary_json),
        "top_stdout_log": str(top_stdout_log),
        "top_stderr_log": str(top_stderr_log),
    }


def non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def non_negative_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def count_execution_events(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, int]:
    counts = {"DECISION": 0, "RISK_REJECT": 0, "FILL": 0}
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if str(row.get("live_run_id") or "").strip() != live_run_id:
            continue
        event_type = str(row.get("event_type") or "").strip().upper()
        if event_type in counts:
            counts[event_type] += 1
    return counts


def count_trade_rows(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, int]:
    counts = {"OPEN": 0, "CLOSED": 0}
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if live_run_id not in {
            str(row.get("open_live_run_id") or "").strip(),
            str(row.get("last_live_run_id") or "").strip(),
        }:
            continue
        status = str(row.get("status") or "").strip().upper()
        if status in counts:
            counts[status] += 1
    return counts


def latest_pack_summary_row(pack_summary: dict[str, Any], pack_id: str) -> dict[str, Any] | None:
    latest_by_pack_id = pack_summary.get("latest_by_pack_id")
    if isinstance(latest_by_pack_id, dict):
        row = latest_by_pack_id.get(pack_id)
        if isinstance(row, dict):
            return row
    return None


def futures_paper_item(futures_paper_ledger: dict[str, Any], pack_id: str, live_run_id: str) -> dict[str, Any] | None:
    items = futures_paper_ledger.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if str(item.get("live_run_id") or "").strip() != live_run_id:
            continue
        return item
    return None


def reversal_count_from_summary(summary: dict[str, Any], futures_item: dict[str, Any] | None) -> int | None:
    for source in (summary, futures_item or {}):
        for key in ("trade_reversal_count", "reversal_count"):
            value = non_negative_int(source.get(key))
            if value is not None:
                return value
    return None


def verdict_from_metrics(metrics: dict[str, Any], invalid_run: bool, max_trade_transitions_per_1k_events: float) -> tuple[str, str]:
    if invalid_run:
        return "INVALID_RUN", str(metrics.get("invalid_reason") or "invalid_run")
    verify_pass = metrics.get("verify_soft_live_pass") is True
    if not verify_pass:
        return "INVALID_RUN", "verify_soft_live_pass != true"
    processed = int(metrics.get("processed_event_count") or 0)
    if processed == 0:
        return "NO_SIGNAL", "processed_event_count == 0"
    churn = metrics.get("trade_transitions_per_1k_events")
    if isinstance(churn, (int, float)) and float(churn) > float(max_trade_transitions_per_1k_events):
        return "FAIL_CHURN", f"trade_transitions_per_1k_events>{float(max_trade_transitions_per_1k_events):.3f}"
    decision_count = int(metrics.get("decision_count") or 0)
    fill_count = int(metrics.get("fill_count") or 0)
    if fill_count > 0:
        return "CONTINUE", "fill_count > 0 with bounded churn"
    if decision_count > 0:
        return "WEAK", "decision_count > 0 but fill_count == 0"
    return "NO_SIGNAL", "decision_count == 0 and fill_count == 0"


def continuation_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    churn = metrics.get("trade_transitions_per_1k_events")
    churn_value = float(churn) if isinstance(churn, (int, float)) else 10**9
    return (
        VERDICT_PRIORITY.get(str(row.get("verdict") or "INVALID_RUN"), 99),
        int(metrics.get("risk_reject_count") or 0),
        -int(metrics.get("fill_count") or 0),
        -int(metrics.get("decision_count") or 0),
        churn_value,
        int(row.get("rank") or 10**9),
        str(row.get("strategy_id") or ""),
    )


def invalid_result_row(target: dict[str, Any], run_meta: dict[str, Any], reason: str) -> dict[str, Any]:
    shortlist_row = target["shortlist_row"]
    metrics = {
        "verify_soft_live_pass": False,
        "processed_event_count": 0,
        "decision_count": 0,
        "fill_count": 0,
        "open_count": 0,
        "exit_count": 0,
        "reversal_count": None,
        "trade_transitions": 0,
        "trade_transitions_per_1k_events": None,
        "risk_reject_count": 0,
        "run_duration_sec": None,
        "stop_reason": reason,
        "heartbeat_count": None,
        "heartbeat_seen": None,
        "fills_count_snapshot": None,
        "positions_count": None,
        "total_realized_pnl": None,
        "total_unrealized_pnl": None,
        "equity": None,
        "invalid_reason": reason,
    }
    verdict, verdict_reason = verdict_from_metrics(metrics, True, 0.0)
    return {
        "strategy_id": target["strategy_id"],
        "rank": int(target["rank"]),
        "family_id": str(shortlist_row.get("family_id") or "").strip(),
        "symbol": str(target["symbol"]),
        "exchange": str(shortlist_row.get("exchange") or "").strip(),
        "status": "INVALID_RUN",
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "metrics": metrics,
        "artifacts": run_meta,
        "eliminated": True,
        "elimination_reason": verdict_reason,
    }


def build_result_row(target: dict[str, Any], run_meta: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    shortlist_row = target["shortlist_row"]
    try:
        batch_result_path = Path(str(run_meta["batch_result_json_path"]))
        if run_meta["timed_out"]:
            return invalid_result_row(target, run_meta, "subprocess_timeout")
        if run_meta["exit_code"] != 0:
            note = f"batch_exit_{run_meta['exit_code']}"
            if batch_result_path.exists():
                batch_result = load_json(batch_result_path, "batch_result_json")
                results = batch_result.get("results")
                if isinstance(results, list) and results and isinstance(results[0], dict):
                    maybe_note = str(results[0].get("note") or "").strip()
                    if maybe_note:
                        note = maybe_note
            return invalid_result_row(target, run_meta, note)
        batch_result = load_json(batch_result_path, "batch_result_json")
        results = batch_result.get("results")
        if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
            return invalid_result_row(target, run_meta, "batch_result_invalid")
        item = results[0]
        if item.get("run_executed") is not True:
            return invalid_result_row(target, run_meta, "run_not_executed")
        if args.dry_run:
            metrics = {
                "verify_soft_live_pass": None,
                "processed_event_count": 0,
                "decision_count": 0,
                "fill_count": 0,
                "open_count": 0,
                "exit_count": 0,
                "reversal_count": None,
                "trade_transitions": 0,
                "trade_transitions_per_1k_events": None,
                "risk_reject_count": 0,
                "run_duration_sec": None,
                "stop_reason": "DRY_RUN",
                "heartbeat_count": None,
                "heartbeat_seen": None,
                "fills_count_snapshot": None,
                "positions_count": None,
                "total_realized_pnl": None,
                "total_unrealized_pnl": None,
                "equity": None,
            }
            return {
                "strategy_id": target["strategy_id"],
                "rank": int(target["rank"]),
                "family_id": str(shortlist_row.get("family_id") or "").strip(),
                "symbol": str(target["symbol"]),
                "exchange": str(shortlist_row.get("exchange") or "").strip(),
                "status": "DRY_RUN_ONLY",
                "verdict": "NO_SIGNAL",
                "verdict_reason": "dry_run only",
                "metrics": metrics,
                "artifacts": {
                    **run_meta,
                    "summary_json_path": str(Path(run_meta["summary_json_path"]).resolve()),
                    "refresh_result_json_path": str(Path(run_meta["refresh_result_json_path"]).resolve()),
                    "execution_events_jsonl": str((Path(run_meta["shadow_state_dir"]) / "shadow_execution_events_v1.jsonl").resolve()),
                    "trade_ledger_jsonl": str((Path(run_meta["shadow_state_dir"]) / "shadow_trade_ledger_v1.jsonl").resolve()),
                    "futures_paper_ledger_json": str((Path(run_meta["shadow_state_dir"]) / "shadow_futures_paper_ledger_v1.json").resolve()),
                    "execution_pack_summary_json": str((Path(run_meta["shadow_state_dir"]) / "shadow_execution_pack_summary_v0.json").resolve()),
                },
                "eliminated": True,
                "elimination_reason": "dry_run only",
            }
        if item.get("verify_soft_live_pass") is not True:
            return invalid_result_row(target, run_meta, "verify_soft_live_failed")
        if item.get("summary_generated") is not True:
            return invalid_result_row(target, run_meta, "summary_not_generated")
        if item.get("history_updated") is not True:
            return invalid_result_row(target, run_meta, "history_not_updated")
        summary_json_path = Path(str(item.get("summary_json_path") or "")).resolve()
        if not summary_json_path.exists():
            return invalid_result_row(target, run_meta, "summary_json_missing")
        shadow_state_dir = Path(str(run_meta["shadow_state_dir"])).resolve()
        summary = load_json(summary_json_path, "summary_json")
        refresh_result = load_json(Path(str(run_meta["refresh_result_json_path"])).resolve(), "refresh_result_json")
        execution_events_rows = load_jsonl(shadow_state_dir / "shadow_execution_events_v1.jsonl")
        trade_rows = load_jsonl(shadow_state_dir / "shadow_trade_ledger_v1.jsonl")
        futures_paper_ledger = load_json(shadow_state_dir / "shadow_futures_paper_ledger_v1.json", "futures_paper_ledger")
        execution_pack_summary = load_json(shadow_state_dir / "shadow_execution_pack_summary_v0.json", "execution_pack_summary")

        pack_id = str(item.get("pack_id") or target["binding_row"].get("pack_id") or "").strip()
        live_run_id = str(summary.get("live_run_id") or "").strip()
        processed_event_count = non_negative_int(summary.get("processed_event_count")) or 0
        event_counts = count_execution_events(execution_events_rows, pack_id, live_run_id)
        trade_counts = count_trade_rows(trade_rows, pack_id, live_run_id)
        trade_transitions = int(trade_counts["OPEN"]) + int(trade_counts["CLOSED"])
        trade_transitions_per_1k_events = (
            round((1000.0 * trade_transitions) / processed_event_count, 6) if processed_event_count > 0 else None
        )
        pack_summary_row = latest_pack_summary_row(execution_pack_summary, pack_id)
        futures_item = futures_paper_item(futures_paper_ledger, pack_id, live_run_id)
        execution_summary = summary.get("execution_summary")
        execution_summary = execution_summary if isinstance(execution_summary, dict) else {}

        metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": processed_event_count,
            "decision_count": int(event_counts["DECISION"]),
            "fill_count": int(event_counts["FILL"]),
            "open_count": int(trade_counts["OPEN"]),
            "exit_count": int(trade_counts["CLOSED"]),
            "reversal_count": reversal_count_from_summary(summary, futures_item),
            "trade_transitions": trade_transitions,
            "trade_transitions_per_1k_events": trade_transitions_per_1k_events,
            "risk_reject_count": int(event_counts["RISK_REJECT"]),
            "run_duration_sec": non_negative_float(summary.get("run_duration_sec")),
            "stop_reason": str(summary.get("stop_reason") or ""),
            "heartbeat_count": non_negative_int(summary.get("heartbeat_count")),
            "heartbeat_seen": summary.get("heartbeat_seen"),
            "fills_count_snapshot": non_negative_int(execution_summary.get("fills_count")),
            "positions_count": non_negative_int(execution_summary.get("positions_count")),
            "total_realized_pnl": execution_summary.get("total_realized_pnl"),
            "total_unrealized_pnl": execution_summary.get("total_unrealized_pnl"),
            "equity": execution_summary.get("equity"),
            "refresh_sync_ok": bool(refresh_result.get("sync_ok")),
            "pnl_interpretation": str((pack_summary_row or {}).get("pnl_interpretation") or "UNKNOWN"),
            "paper_run_status": str((futures_item or {}).get("paper_run_status") or "UNKNOWN"),
        }
        verdict, verdict_reason = verdict_from_metrics(
            metrics,
            False,
            float(args.max_trade_transitions_per_1k_events),
        )
        status = "OK" if verdict in {"CONTINUE", "WEAK", "NO_SIGNAL"} else verdict
        return {
            "strategy_id": target["strategy_id"],
            "rank": int(target["rank"]),
            "family_id": str(shortlist_row.get("family_id") or "").strip(),
            "symbol": str(target["symbol"]),
            "exchange": str(shortlist_row.get("exchange") or "").strip(),
            "status": status,
            "verdict": verdict,
            "verdict_reason": verdict_reason,
            "metrics": metrics,
            "artifacts": {
                **run_meta,
                "summary_json_path": str(summary_json_path),
                "refresh_result_json_path": str(Path(run_meta["refresh_result_json_path"]).resolve()),
                "execution_events_jsonl": str((shadow_state_dir / "shadow_execution_events_v1.jsonl").resolve()),
                "trade_ledger_jsonl": str((shadow_state_dir / "shadow_trade_ledger_v1.jsonl").resolve()),
                "futures_paper_ledger_json": str((shadow_state_dir / "shadow_futures_paper_ledger_v1.json").resolve()),
                "execution_pack_summary_json": str((shadow_state_dir / "shadow_execution_pack_summary_v0.json").resolve()),
            },
            "eliminated": True,
            "elimination_reason": verdict_reason,
        }
    except MediumShadowValidationError as exc:
        return invalid_result_row(target, run_meta, f"artifact_parse_error:{exc}")


def apply_reduction(rows: list[dict[str, Any]], max_continuation: int) -> list[dict[str, Any]]:
    eligible = [row for row in rows if str(row.get("verdict") or "") in {"CONTINUE", "WEAK"}]
    eligible.sort(key=continuation_sort_key)
    selected = eligible[:max_continuation]
    selected_ids = {str(row.get("strategy_id") or "") for row in selected}
    continuation_rows: list[dict[str, Any]] = []
    for row in rows:
        strategy_id = str(row.get("strategy_id") or "")
        if strategy_id in selected_ids:
            row["eliminated"] = False
            row["elimination_reason"] = ""
        else:
            row["eliminated"] = True
            if strategy_id in {str(candidate.get("strategy_id") or "") for candidate in eligible}:
                row["elimination_reason"] = "lower_priority_than_top2"
        if strategy_id in selected_ids:
            metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
            continuation_rows.append(
                {
                    "strategy_id": strategy_id,
                    "rank": row.get("rank"),
                    "family_id": row.get("family_id"),
                    "symbol": row.get("symbol"),
                    "exchange": row.get("exchange"),
                    "verdict": row.get("verdict"),
                    "reason": row.get("verdict_reason"),
                    "metrics": {
                        "processed_event_count": metrics.get("processed_event_count"),
                        "decision_count": metrics.get("decision_count"),
                        "fill_count": metrics.get("fill_count"),
                        "trade_transitions_per_1k_events": metrics.get("trade_transitions_per_1k_events"),
                        "risk_reject_count": metrics.get("risk_reject_count"),
                    },
                }
            )
    continuation_rows.sort(key=lambda row: (int(row["rank"] or 10**9), str(row["strategy_id"] or "")))
    return continuation_rows


def summary_payload(rows: list[dict[str, Any]], continuation_rows: list[dict[str, Any]]) -> dict[str, Any]:
    verdict_counts = {
        verdict: sum(1 for row in rows if str(row.get("verdict") or "") == verdict)
        for verdict in ("CONTINUE", "WEAK", "NO_SIGNAL", "FAIL_CHURN", "INVALID_RUN")
    }
    lane_result = "NEGATIVE_LANE" if verdict_counts["NO_SIGNAL"] == len(rows) else "MIXED_LANE"
    if continuation_rows:
        lane_result = "POSITIVE_LANE"
    return {
        "target_count": len(rows),
        "continuation_count": len(continuation_rows),
        "verdict_counts": verdict_counts,
        "lane_result": lane_result,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry_path = Path(args.canonical_truth_registry).resolve()
    shortlist_path = Path(args.shortlist_json).resolve()
    expectation_path = Path(args.expectation_report_json).resolve()
    window_plan_path = Path(args.window_plan_json).resolve()
    binding_path = Path(args.binding_json).resolve()
    output_dir = Path(args.output_dir).resolve()
    result_json_path = Path(args.result_json).resolve()

    registry = load_json(registry_path, "canonical_truth_registry")
    shortlist_doc = load_json(shortlist_path, "shadow_shortlist_json")
    expectation_doc = load_json(expectation_path, "expectation_report_json")
    window_plan_doc = load_json(window_plan_path, "window_plan_json")
    binding_doc = load_json(binding_path, "binding_json")

    require_registry_path(registry, "runtime_binding", binding_path)
    targets = selected_targets(shortlist_doc, expectation_doc, window_plan_doc, binding_doc)
    if output_dir.exists() and not args.dry_run:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    run_roots: list[Path] = []
    target_by_id: dict[str, dict[str, Any]] = {}
    for idx, target in enumerate(targets, start=1):
        run_root = output_dir / "runs" / f"{idx:02d}_{slugify(target['symbol'])}"
        target["run_root"] = run_root
        run_roots.append(run_root)
        target_by_id[target["strategy_id"]] = target

    run_meta_by_id: dict[str, dict[str, Any]] = {}
    started_at = monotonic()
    print(
        f"PHASE7_MEDIUM_SHADOW_VALIDATION_STARTED target_count={len(targets)} max_parallel={int(args.max_parallel)} "
        f"progress_interval_sec={int(args.progress_interval_sec)} run_max_duration_sec={int(args.run_max_duration_sec)}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=int(args.max_parallel)) as executor:
        pending = {
            executor.submit(run_batch_for_target, args, target, target["run_root"]): target["strategy_id"]
            for target in targets
        }
        while pending:
            done, not_done = wait(
                pending,
                timeout=float(args.progress_interval_sec),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                elapsed = int(monotonic() - started_at)
                print(
                    f"PHASE7_MEDIUM_SHADOW_VALIDATION_PROGRESS elapsed_sec={elapsed} "
                    f"completed={len(run_meta_by_id)} remaining={len(pending)}",
                    flush=True,
                )
                continue
            for future in done:
                strategy_id = pending[future]
                try:
                    run_meta_by_id[strategy_id] = future.result()
                except Exception as exc:
                    run_meta_by_id[strategy_id] = {
                        "command": "",
                        "exit_code": "internal_error",
                        "timed_out": False,
                        "watchlist_path": str((target_by_id[strategy_id]["run_root"] / "input_watchlist.json").resolve()),
                        "shadow_state_dir": str((target_by_id[strategy_id]["run_root"] / "shadow_state").resolve()),
                        "batch_out_dir": str((target_by_id[strategy_id]["run_root"] / "batch_out").resolve()),
                        "audit_base_dir": str((target_by_id[strategy_id]["run_root"] / "audit").resolve()),
                        "summary_json_path": str((target_by_id[strategy_id]["run_root"] / "summary_runtime.json").resolve()),
                        "batch_result_json_path": str((target_by_id[strategy_id]["run_root"] / "shadow_observation_batch_result_v0.json").resolve()),
                        "refresh_result_json_path": str((target_by_id[strategy_id]["run_root"] / "shadow_state" / "shadow_derived_surface_refresh_v0.json").resolve()),
                        "execution_ledger_jsonl": str((target_by_id[strategy_id]["run_root"] / "shadow_state" / "shadow_execution_ledger_v0.jsonl").resolve()),
                        "execution_pack_summary_json": str((target_by_id[strategy_id]["run_root"] / "shadow_state" / "shadow_execution_pack_summary_v0.json").resolve()),
                        "top_stdout_log": str((target_by_id[strategy_id]["run_root"] / "batch_command_stdout.log").resolve()),
                        "top_stderr_log": str((target_by_id[strategy_id]["run_root"] / "batch_command_stderr.log").resolve()),
                        "internal_error": str(exc),
                    }
                elapsed = int(monotonic() - started_at)
                exit_code = run_meta_by_id[strategy_id].get("exit_code")
                timed_out = 1 if run_meta_by_id[strategy_id].get("timed_out") else 0
                print(
                    f"PHASE7_MEDIUM_SHADOW_VALIDATION_STRATEGY_DONE elapsed_sec={elapsed} "
                    f"completed={len(run_meta_by_id)}/{len(targets)} strategy_id={strategy_id} "
                    f"exit_code={exit_code} timed_out={timed_out}",
                    flush=True,
                )
                del pending[future]

    rows = [build_result_row(target_by_id[strategy_id], run_meta_by_id[strategy_id], args) for strategy_id in sorted(run_meta_by_id)]
    rows.sort(key=lambda row: (int(row.get("rank") or 10**9), str(row.get("symbol") or ""), str(row.get("strategy_id") or "")))
    continuation_rows = [] if args.dry_run else apply_reduction(rows, int(args.max_continuation))
    if args.dry_run:
        for row in rows:
            row["eliminated"] = True
            row["elimination_reason"] = "dry_run only"

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "authoritative_inputs": [
                {
                    "concept": "runtime_binding",
                    "path": str(binding_path),
                    "file_generated_ts_utc": binding_doc.get("generated_ts_utc"),
                }
            ],
            "task_local_inputs": [
                {
                    "concept": "shadow_shortlist",
                    "path": str(shortlist_path),
                    "generated_ts_utc": shortlist_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "expectation_audit_report",
                    "path": str(expectation_path),
                    "generated_ts_utc": expectation_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "shadow_window_plan",
                    "path": str(window_plan_path),
                    "generated_ts_utc": window_plan_doc.get("generated_ts_utc"),
                },
            ],
            "notes": [
                "This sprint only evaluates the medium-frequency lane.",
                "Global ranking, promotion state, and shadow watchlist remain unchanged.",
                "Execution uses isolated task-local watchlists and task-local shadow state directories.",
            ],
        },
        "run_policy": {
            "target_band": TARGET_BAND,
            "target_strategy_ids": [target["strategy_id"] for target in targets],
            "target_symbols": [target["symbol"] for target in targets],
            "run_semantics": RUN_SEMANTICS,
            "max_parallel": int(args.max_parallel),
            "run_max_duration_sec": int(args.run_max_duration_sec),
            "per_run_timeout_sec": int(args.per_run_timeout_sec),
            "subprocess_timeout_sec": int(args.subprocess_timeout_sec),
            "heartbeat_ms": int(args.heartbeat_ms),
            "progress_interval_sec": int(args.progress_interval_sec),
            "max_continuation": int(args.max_continuation),
            "extreme_churn_max_trade_transitions_per_1k_events": float(args.max_trade_transitions_per_1k_events),
            "dry_run": bool(args.dry_run),
        },
        "summary": summary_payload(rows, continuation_rows if not args.dry_run else []),
        "continuation_candidates": continuation_rows,
        "results": rows,
    }
    write_json(result_json_path, payload)
    print("PHASE7_MEDIUM_SHADOW_VALIDATION_COMPLETE")
    print(f"result_json={result_json_path}")
    print(f"target_count={len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MediumShadowValidationError as exc:
        print(f"PHASE7_MEDIUM_SHADOW_VALIDATION_ERROR: {exc}")
        raise SystemExit(1)
