#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import importlib.util
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
PHASE7_CONTINUATION_OUTPUT = TOOLS / "phase7_continuation_validation_output"
DEFAULT_MEDIUM_RESULT_JSON = TOOLS / "phase7_medium_shadow_result_v0.json"
DEFAULT_MEDIUM_REPORT_MD = TOOLS / "phase7_medium_shadow_validation_output" / "phase7_medium_shadow_report_v0.md"
DEFAULT_BINDING_JSON = TOOLS / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_CANONICAL_TRUTH_REGISTRY = TOOLS / "system_state" / "canonical_truth_registry_v0.json"
DEFAULT_BATCH_TOOL = TOOLS / "run-shadow-observation-batch-v0.py"
DEFAULT_OUTPUT_DIR = PHASE7_CONTINUATION_OUTPUT / "full_run"
DEFAULT_RESULT_JSON = TOOLS / "phase7_continuation_validation_result_v1.json"
DEFAULT_REPORT_MD = PHASE7_CONTINUATION_OUTPUT / "phase7_continuation_validation_report_v1.md"
EXPECTED_SYMBOLS = ("avaxusdt", "linkusdt")
SCHEMA_VERSION = "phase7_continuation_validation_result_v1"
DEFAULT_RUN_POSTURE = "paper_directional"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
DEFAULT_MAX_PARALLEL = 2
DEFAULT_RUN_MAX_DURATION_SEC = 86_400
DEFAULT_PER_RUN_TIMEOUT_SEC = 86_700
DEFAULT_SUBPROCESS_TIMEOUT_SEC = 87_600
DEFAULT_HEARTBEAT_MS = 5_000
DEFAULT_PROGRESS_INTERVAL_SEC = 60
DEFAULT_MAX_TRADE_TRANSITIONS_PER_1K_EVENTS = 2.5
VERDICT_PRIORITY = {
    "KEEP_ADVANCING": 0,
    "WEAK_CONTINUE": 1,
    "DROP": 2,
    "INVALID_RUN": 3,
}


def load_support_module() -> Any:
    module_path = TOOLS / "phase7_medium_shadow_validation_v0.py"
    spec = importlib.util.spec_from_file_location("phase7_medium_shadow_validation_v0", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable_to_load_support_module:{module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SUPPORT = load_support_module()


class ContinuationValidationError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise ContinuationValidationError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path, label: str) -> dict[str, Any]:
    return SUPPORT.load_json(path, label)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return SUPPORT.load_jsonl(path)


def load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    SUPPORT.write_json(path, payload)


def read_text(path: Path, label: str) -> str:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    return path.read_text(encoding="utf-8")


def normalize_symbols(binding_row: dict[str, Any]) -> list[str]:
    return SUPPORT.normalize_symbols(binding_row)


def slugify(value: str) -> str:
    return SUPPORT.slugify(value)


def non_negative_int(value: Any) -> int | None:
    return SUPPORT.non_negative_int(value)


def non_negative_float(value: Any) -> float | None:
    return SUPPORT.non_negative_float(value)


def count_execution_events(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, int]:
    return SUPPORT.count_execution_events(rows, pack_id, live_run_id)


def count_trade_rows(rows: list[dict[str, Any]], pack_id: str, live_run_id: str) -> dict[str, int]:
    return SUPPORT.count_trade_rows(rows, pack_id, live_run_id)


def latest_pack_summary_row(pack_summary: dict[str, Any] | None, pack_id: str) -> dict[str, Any] | None:
    if not isinstance(pack_summary, dict):
        return None
    return SUPPORT.latest_pack_summary_row(pack_summary, pack_id)


def futures_paper_item(futures_paper_ledger: dict[str, Any] | None, pack_id: str, live_run_id: str) -> dict[str, Any] | None:
    if not isinstance(futures_paper_ledger, dict):
        return None
    return SUPPORT.futures_paper_item(futures_paper_ledger, pack_id, live_run_id)


def reversal_count_from_summary(summary: dict[str, Any], futures_item: dict[str, Any] | None) -> int | None:
    return SUPPORT.reversal_count_from_summary(summary, futures_item)


def require_registry_path(registry: dict[str, Any], concept_name: str, expected_path: Path) -> dict[str, Any]:
    return SUPPORT.require_registry_path(registry, concept_name, expected_path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run isolated Phase7 continuation validation for the surviving pair.")
    parser.add_argument("--medium-result-json", default=str(DEFAULT_MEDIUM_RESULT_JSON))
    parser.add_argument("--medium-report-md", default=str(DEFAULT_MEDIUM_REPORT_MD))
    parser.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    parser.add_argument("--canonical-truth-registry", default=str(DEFAULT_CANONICAL_TRUTH_REGISTRY))
    parser.add_argument("--batch-tool", default=str(DEFAULT_BATCH_TOOL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT_MD))
    parser.add_argument("--max-parallel", type=int, default=DEFAULT_MAX_PARALLEL)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--subprocess-timeout-sec", type=int, default=DEFAULT_SUBPROCESS_TIMEOUT_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--progress-interval-sec", type=int, default=DEFAULT_PROGRESS_INTERVAL_SEC)
    parser.add_argument("--run-posture", choices=("paper_directional", "observe_only"), default=DEFAULT_RUN_POSTURE)
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
    if args.max_trade_transitions_per_1k_events <= 0:
        fail(f"invalid_max_trade_transitions_per_1k_events:{args.max_trade_transitions_per_1k_events}")
    if args.subprocess_timeout_sec < args.per_run_timeout_sec:
        fail("subprocess_timeout_sec_must_cover_per_run_timeout_sec")
    return args


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


def selected_targets(medium_result_doc: dict[str, Any], binding_doc: dict[str, Any]) -> list[dict[str, Any]]:
    summary = medium_result_doc.get("summary")
    if not isinstance(summary, dict):
        fail("medium_result_missing_summary")
    candidates = medium_result_doc.get("continuation_candidates")
    if not isinstance(candidates, list):
        fail("medium_result_missing_continuation_candidates")
    if int(summary.get("continuation_count") or 0) != 2:
        fail(f"medium_result_continuation_count_mismatch:{summary.get('continuation_count')}")
    binding_map = binding_by_strategy_id(binding_doc)
    targets: list[dict[str, Any]] = []
    for row in candidates:
        if not isinstance(row, dict):
            fail("continuation_candidate_not_object")
        strategy_id = str(row.get("strategy_id") or "").strip()
        if not strategy_id:
            fail("continuation_candidate_missing_strategy_id")
        binding_row = binding_map.get(strategy_id)
        if not isinstance(binding_row, dict):
            fail(f"continuation_binding_missing:{strategy_id}")
        if str(binding_row.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
            fail(f"continuation_binding_not_runnable:{strategy_id}")
        symbol = str(row.get("symbol") or binding_row.get("selected_symbol") or "").strip().lower()
        if not symbol:
            fail(f"continuation_symbol_missing:{strategy_id}")
        targets.append(
            {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "rank": int(row.get("rank") or 0),
                "medium_row": row,
                "binding_row": binding_row,
            }
        )
    targets.sort(key=lambda item: (int(item["rank"]), str(item["symbol"]), str(item["strategy_id"])))
    actual_symbols = tuple(sorted(item["symbol"] for item in targets))
    if actual_symbols != EXPECTED_SYMBOLS:
        fail(f"continuation_symbol_assertion_failed:expected={EXPECTED_SYMBOLS}:actual={actual_symbols}")
    if len(targets) != len(EXPECTED_SYMBOLS):
        fail(f"continuation_target_count_mismatch:{len(targets)}")
    return targets


def runtime_config_for_run(binding_row: dict[str, Any], run_posture: str) -> dict[str, Any]:
    runtime_config = binding_row.get("runtime_strategy_config")
    if not isinstance(runtime_config, dict):
        fail(f"binding_runtime_strategy_config_missing:{binding_row.get('strategy_id')}")
    if run_posture == "paper_directional":
        return copy.deepcopy(runtime_config)
    override = copy.deepcopy(runtime_config)
    override["binding_mode"] = "OBSERVE_ONLY"
    return override


def run_semantics(run_posture: str) -> str:
    if run_posture == "paper_directional":
        return "ISOLATED_PAPER_DIRECTIONAL_CONTINUATION_SHADOW"
    return "ISOLATED_OBSERVE_ONLY_CONTINUATION_SHADOW"


def build_watchlist(target: dict[str, Any], medium_result_json_path: Path, binding_json_path: Path) -> dict[str, Any]:
    binding_row = target["binding_row"]
    medium_row = target["medium_row"]
    exchange = str(binding_row.get("exchange") or medium_row.get("exchange") or "").strip()
    stream = str(binding_row.get("stream") or "").strip()
    return {
        "schema_version": "phase7_continuation_shadow_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "surface_role": "TASK_LOCAL_CONTINUATION_VALIDATION_SELECTION",
            "authoritative_scope": "Single-strategy task-local continuation validation only.",
            "not_authoritative_for": [
                "global shadow watchlist",
                "ranking",
                "promotion",
            ],
            "notes": [
                "This watchlist is isolated to one continuation validation run.",
                "It intentionally does not read or mutate the global shadow watchlist.",
                "Execution posture is task-local only and does not mutate canonical runtime binding.",
            ],
        },
        "source_phase7_medium_shadow_result_json": str(medium_result_json_path.resolve()),
        "source_candidate_strategy_runtime_binding_json": str(binding_json_path.resolve()),
        "selected_count": 1,
        "items": [
            {
                "rank": int(target["rank"]),
                "pack_id": str(binding_row.get("pack_id") or "").strip(),
                "pack_path": str(binding_row.get("pack_path") or "").strip(),
                "exchange": exchange,
                "symbols": normalize_symbols(binding_row),
                "decision_tier": str(binding_row.get("decision_tier") or "").strip(),
                "selection_slot": f"{exchange}/{stream}" if exchange and stream else "",
            }
        ],
    }


def run_batch_for_target(args: argparse.Namespace, target: dict[str, Any], run_root: Path) -> dict[str, Any]:
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
    write_json(watchlist_path, build_watchlist(target, Path(args.medium_result_json), Path(args.binding_json)))

    runtime_strategy_file = str(binding_row.get("runtime_strategy_file") or "").strip()
    runtime_strategy_config = runtime_config_for_run(binding_row, str(args.run_posture))
    if not runtime_strategy_file:
        fail(f"binding_runtime_strategy_file_missing:{target['strategy_id']}")
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
        str((TOOLS / "phase6_state").resolve()),
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
        "audit_base_dir": str(audit_base_dir.resolve()),
        "summary_json_path": str(summary_json_path.resolve()),
        "batch_result_json_path": str(result_json.resolve()),
        "refresh_result_json_path": str(refresh_result_json.resolve()),
        "execution_ledger_jsonl": str(execution_ledger_jsonl.resolve()),
        "execution_pack_summary_json": str(execution_pack_summary_json.resolve()),
        "top_stdout_log": str(top_stdout_log.resolve()),
        "top_stderr_log": str(top_stderr_log.resolve()),
        "effective_run_semantics": run_semantics(str(args.run_posture)),
    }


def cost_proxy_metrics(futures_item: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(futures_item, dict):
        return {
            "available": False,
            "note": "not available in current artifact surface",
            "paper_run_status": "not available in current artifact surface",
            "cost_accounting_status": "not available in current artifact surface",
            "profitability_status": "not available in current artifact surface",
            "effective_fee_rate": None,
            "estimated_exit_fee_quote": None,
            "total_fee_quote": None,
            "funding_cost_quote": None,
            "mark_to_market_pnl_quote_net_paid_fees": None,
            "fee_support_status": "not available in current artifact surface",
        }
    return {
        "available": True,
        "note": "",
        "paper_run_status": str(futures_item.get("paper_run_status") or "UNKNOWN"),
        "cost_accounting_status": str(futures_item.get("cost_accounting_status") or "UNKNOWN"),
        "profitability_status": str(futures_item.get("profitability_status") or "UNKNOWN"),
        "effective_fee_rate": futures_item.get("effective_fee_rate"),
        "estimated_exit_fee_quote": futures_item.get("estimated_exit_fee_quote"),
        "total_fee_quote": futures_item.get("total_fee_quote"),
        "funding_cost_quote": futures_item.get("funding_cost_quote"),
        "mark_to_market_pnl_quote_net_paid_fees": futures_item.get("mark_to_market_pnl_quote_net_paid_fees"),
        "fee_support_status": str(futures_item.get("fee_support_status") or "UNKNOWN"),
    }


def stderr_failure_reason(stderr_log_path: str | Path | None) -> str | None:
    if stderr_log_path is None:
        return None
    path = Path(stderr_log_path)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    if "binding_mode must be PAPER_DIRECTIONAL_V1" in text:
        return "observe_only_unsupported_by_strategy_contract"
    return None


def invalid_result_row(target: dict[str, Any], run_meta: dict[str, Any], reason: str) -> dict[str, Any]:
    medium_row = target["medium_row"]
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
        "bounded_churn": False,
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
        "cost_slippage_proxy": {
            "available": False,
            "note": "not available in current artifact surface",
            "paper_run_status": "not available in current artifact surface",
            "cost_accounting_status": "not available in current artifact surface",
            "profitability_status": "not available in current artifact surface",
            "effective_fee_rate": None,
            "estimated_exit_fee_quote": None,
            "total_fee_quote": None,
            "funding_cost_quote": None,
            "mark_to_market_pnl_quote_net_paid_fees": None,
            "fee_support_status": "not available in current artifact surface",
        },
        "invalid_reason": reason,
    }
    return {
        "strategy_id": target["strategy_id"],
        "rank": int(target["rank"]),
        "family_id": str(medium_row.get("family_id") or binding_family_id(target)).strip(),
        "symbol": str(target["symbol"]),
        "exchange": str(medium_row.get("exchange") or target["binding_row"].get("exchange") or "").strip(),
        "status": "INVALID_RUN",
        "verdict": "INVALID_RUN",
        "verdict_reason": reason,
        "metrics": metrics,
        "artifacts": run_meta,
        "eliminated": True,
        "elimination_reason": reason,
    }


def binding_family_id(target: dict[str, Any]) -> str:
    return str(target["binding_row"].get("family_id") or "").strip()


def base_verdict_from_metrics(metrics: dict[str, Any], max_trade_transitions_per_1k_events: float) -> tuple[str, str]:
    if metrics.get("verify_soft_live_pass") is not True:
        return "INVALID_RUN", "verify_soft_live_pass != true"
    processed = int(metrics.get("processed_event_count") or 0)
    if processed == 0:
        return "DROP", "processed_event_count == 0"
    churn = metrics.get("trade_transitions_per_1k_events")
    if isinstance(churn, (int, float)) and float(churn) > float(max_trade_transitions_per_1k_events):
        return "DROP", f"trade_transitions_per_1k_events>{float(max_trade_transitions_per_1k_events):.3f}"
    fill_count = int(metrics.get("fill_count") or 0)
    decision_count = int(metrics.get("decision_count") or 0)
    if fill_count > 0:
        return "KEEP_ADVANCING", "fill_count > 0 with bounded churn"
    if decision_count > 0:
        return "WEAK_CONTINUE", "decision_count > 0 but fill_count == 0"
    return "DROP", "decision_count == 0 and fill_count == 0"


def comparison_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    churn = metrics.get("trade_transitions_per_1k_events")
    churn_value = float(churn) if isinstance(churn, (int, float)) else 10**9
    return (
        VERDICT_PRIORITY.get(str(row.get("verdict") or "INVALID_RUN"), 99),
        int(metrics.get("risk_reject_count") or 0),
        -int(metrics.get("fill_count") or 0),
        -int(metrics.get("decision_count") or 0),
        -int(metrics.get("processed_event_count") or 0),
        churn_value,
        int(row.get("rank") or 10**9),
        str(row.get("strategy_id") or ""),
    )


def build_result_row(target: dict[str, Any], run_meta: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    medium_row = target["medium_row"]
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
                    stderr_reason = stderr_failure_reason(results[0].get("stderr_log_path"))
                    if stderr_reason:
                        note = stderr_reason
            return invalid_result_row(target, run_meta, note)
        batch_result = load_json(batch_result_path, "batch_result_json")
        results = batch_result.get("results")
        if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
            return invalid_result_row(target, run_meta, "batch_result_invalid")
        item = results[0]
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
                "bounded_churn": None,
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
                "cost_slippage_proxy": {
                    "available": False,
                    "note": "not available in current artifact surface",
                    "paper_run_status": "not available in current artifact surface",
                    "cost_accounting_status": "not available in current artifact surface",
                    "profitability_status": "not available in current artifact surface",
                    "effective_fee_rate": None,
                    "estimated_exit_fee_quote": None,
                    "total_fee_quote": None,
                    "funding_cost_quote": None,
                    "mark_to_market_pnl_quote_net_paid_fees": None,
                    "fee_support_status": "not available in current artifact surface",
                },
            }
            return {
                "strategy_id": target["strategy_id"],
                "rank": int(target["rank"]),
                "family_id": str(medium_row.get("family_id") or binding_family_id(target)).strip(),
                "symbol": str(target["symbol"]),
                "exchange": str(medium_row.get("exchange") or target["binding_row"].get("exchange") or "").strip(),
                "status": "DRY_RUN_ONLY",
                "verdict": "DROP",
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
        if item.get("run_executed") is not True:
            return invalid_result_row(target, run_meta, "run_not_executed")
        if item.get("verify_soft_live_pass") is not True:
            return invalid_result_row(
                target,
                run_meta,
                stderr_failure_reason(item.get("stderr_log_path")) or "verify_soft_live_failed",
            )
        if item.get("summary_generated") is not True:
            return invalid_result_row(target, run_meta, "summary_not_generated")
        if item.get("history_updated") is not True:
            return invalid_result_row(target, run_meta, "history_not_updated")
        summary_json_path = Path(str(item.get("summary_json_path") or "")).resolve()
        if not summary_json_path.exists():
            return invalid_result_row(target, run_meta, "summary_json_missing")
        shadow_state_dir = Path(str(run_meta["shadow_state_dir"])).resolve()
        summary = load_json(summary_json_path, "summary_json")
        refresh_result = load_optional_json(Path(str(run_meta["refresh_result_json_path"])).resolve()) or {}
        execution_events_rows = load_jsonl(shadow_state_dir / "shadow_execution_events_v1.jsonl")
        trade_rows = load_jsonl(shadow_state_dir / "shadow_trade_ledger_v1.jsonl")
        futures_paper_ledger = load_optional_json(shadow_state_dir / "shadow_futures_paper_ledger_v1.json")
        execution_pack_summary = load_optional_json(shadow_state_dir / "shadow_execution_pack_summary_v0.json")

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
        proxy = cost_proxy_metrics(futures_item)
        bounded_churn = (
            isinstance(trade_transitions_per_1k_events, (int, float))
            and float(trade_transitions_per_1k_events) <= float(args.max_trade_transitions_per_1k_events)
        )

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
            "bounded_churn": bounded_churn,
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
            "cost_slippage_proxy": proxy,
        }
        verdict, verdict_reason = base_verdict_from_metrics(
            metrics,
            float(args.max_trade_transitions_per_1k_events),
        )
        status = "OK" if verdict != "INVALID_RUN" else "INVALID_RUN"
        return {
            "strategy_id": target["strategy_id"],
            "rank": int(target["rank"]),
            "family_id": str(medium_row.get("family_id") or binding_family_id(target)).strip(),
            "symbol": str(target["symbol"]),
            "exchange": str(medium_row.get("exchange") or target["binding_row"].get("exchange") or "").strip(),
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
    except ContinuationValidationError:
        raise
    except Exception as exc:
        return invalid_result_row(target, run_meta, f"artifact_parse_error:{exc}")


def compare_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid_rows = [row for row in rows if str(row.get("verdict") or "") != "INVALID_RUN"]
    stronger = None
    weaker = None
    comparison_notes: list[str] = []
    if valid_rows:
        ordered = sorted(valid_rows, key=comparison_sort_key)
        stronger = ordered[0]
        if len(ordered) > 1:
            weaker = ordered[1]
    if stronger and weaker:
        comparison_notes.append(
            f"stronger={stronger.get('symbol')} weaker={weaker.get('symbol')} based on verdict tier, risk rejects, fills, decisions, processed events, churn, and prior rank"
        )
    surviving = [row for row in rows if str(row.get("verdict") or "") in {"KEEP_ADVANCING", "WEAK_CONTINUE"}]
    invalid_count = sum(1 for row in rows if str(row.get("verdict") or "") == "INVALID_RUN")
    if invalid_count == len(rows):
        final_recommendation = "NEITHER_ADVANCE"
    elif len(surviving) == 2:
        final_recommendation = "BOTH_ADVANCE"
    elif len(surviving) == 1:
        symbol = str(surviving[0].get("symbol") or "").lower()
        if symbol == "avaxusdt":
            final_recommendation = "AVAX_ONLY"
        elif symbol == "linkusdt":
            final_recommendation = "LINK_ONLY"
        else:
            final_recommendation = "NEITHER_ADVANCE"
    else:
        final_recommendation = "NEITHER_ADVANCE"
    return {
        "stronger_candidate": (
            {
                "strategy_id": stronger.get("strategy_id"),
                "symbol": stronger.get("symbol"),
                "verdict": stronger.get("verdict"),
            }
            if stronger
            else None
        ),
        "weaker_candidate": (
            {
                "strategy_id": weaker.get("strategy_id"),
                "symbol": weaker.get("symbol"),
                "verdict": weaker.get("verdict"),
            }
            if weaker
            else None
        ),
        "both_survive": len(surviving) == 2,
        "only_one_survives": len(surviving) == 1,
        "surviving_symbols": [str(row.get("symbol") or "") for row in surviving],
        "comparison_notes": comparison_notes,
        "final_recommendation": final_recommendation,
        "stop_condition": "INVALID_LANE" if invalid_count == len(rows) else "",
    }


def apply_pairwise_reduction(rows: list[dict[str, Any]], comparison: dict[str, Any]) -> None:
    surviving_ids = {
        str(row.get("strategy_id") or "")
        for row in rows
        if str(row.get("verdict") or "") in {"KEEP_ADVANCING", "WEAK_CONTINUE"}
    }
    stronger_id = str((comparison.get("stronger_candidate") or {}).get("strategy_id") or "")
    for row in rows:
        strategy_id = str(row.get("strategy_id") or "")
        if strategy_id in surviving_ids:
            row["eliminated"] = False
            row["elimination_reason"] = ""
            continue
        row["eliminated"] = True
        verdict = str(row.get("verdict") or "")
        if verdict == "INVALID_RUN":
            row["elimination_reason"] = "invalid_run"
        elif verdict == "DROP":
            if stronger_id and stronger_id != strategy_id:
                row["elimination_reason"] = "weaker_than_peer_or_no_signal"
            else:
                row["elimination_reason"] = "no_signal_or_churn"
        else:
            row["elimination_reason"] = str(row.get("verdict_reason") or "not_selected")


def summary_payload(rows: list[dict[str, Any]], comparison: dict[str, Any]) -> dict[str, Any]:
    verdict_counts = {
        verdict: sum(1 for row in rows if str(row.get("verdict") or "") == verdict)
        for verdict in ("KEEP_ADVANCING", "WEAK_CONTINUE", "DROP", "INVALID_RUN")
    }
    lane_result = "VALID_LANE"
    if verdict_counts["INVALID_RUN"] == len(rows):
        lane_result = "INVALID_LANE"
    elif verdict_counts["DROP"] == len(rows):
        lane_result = "NEGATIVE_LANE"
    elif verdict_counts["KEEP_ADVANCING"] or verdict_counts["WEAK_CONTINUE"]:
        lane_result = "POSITIVE_LANE"
    return {
        "target_count": len(rows),
        "verdict_counts": verdict_counts,
        "lane_result": lane_result,
        "final_recommendation": comparison.get("final_recommendation"),
    }


def report_markdown(payload: dict[str, Any], medium_result_path: Path, medium_report_path: Path) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    comparison = payload.get("pairwise_comparison") if isinstance(payload.get("pairwise_comparison"), dict) else {}
    lines = [
        "# Phase7 Continuation Validation Report",
        "",
        "Generated from:",
        f"- `{medium_result_path}`",
        f"- `{medium_report_path}`",
        f"- `{payload.get('run_policy', {}).get('target_symbols', [])}`",
        "",
        "## Summary",
        "",
        f"- `target_count={summary.get('target_count')}`",
        f"- `lane_result={summary.get('lane_result')}`",
        f"- `final_recommendation={summary.get('final_recommendation')}`",
        "- verdicts:",
        f"- `KEEP_ADVANCING={summary.get('verdict_counts', {}).get('KEEP_ADVANCING')}`",
        f"- `WEAK_CONTINUE={summary.get('verdict_counts', {}).get('WEAK_CONTINUE')}`",
        f"- `DROP={summary.get('verdict_counts', {}).get('DROP')}`",
        f"- `INVALID_RUN={summary.get('verdict_counts', {}).get('INVALID_RUN')}`",
        "",
        "## Per-Strategy Results",
        "",
    ]
    for row in payload.get("results", []):
        if not isinstance(row, dict):
            continue
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        proxy = metrics.get("cost_slippage_proxy") if isinstance(metrics.get("cost_slippage_proxy"), dict) else {}
        lines.extend(
            [
                f"### `{row.get('symbol')}`",
                f"- `verdict={row.get('verdict')}`",
                f"- `reason={row.get('verdict_reason')}`",
                f"- `processed_event_count={metrics.get('processed_event_count')}`",
                f"- `decision_count={metrics.get('decision_count')}`",
                f"- `fill_count={metrics.get('fill_count')}`",
                f"- `open_count={metrics.get('open_count')}`",
                f"- `exit_count={metrics.get('exit_count')}`",
                f"- `trade_transitions_per_1k_events={metrics.get('trade_transitions_per_1k_events')}`",
                f"- `run_duration_sec={metrics.get('run_duration_sec')}`",
                f"- `stop_reason={metrics.get('stop_reason')}`",
                (
                    f"- `cost_proxy={proxy.get('profitability_status')}`"
                    if proxy.get("available")
                    else "- `cost_proxy=not available in current artifact surface`"
                ),
                "",
            ]
        )
    lines.extend(
        [
            "## Pairwise Comparison",
            "",
            f"- `stronger_candidate={((comparison.get('stronger_candidate') or {}).get('symbol'))}`",
            f"- `weaker_candidate={((comparison.get('weaker_candidate') or {}).get('symbol'))}`",
            f"- `both_survive={comparison.get('both_survive')}`",
            f"- `only_one_survives={comparison.get('only_one_survives')}`",
            f"- `final_recommendation={comparison.get('final_recommendation')}`",
            (
                f"- `stop_condition={comparison.get('stop_condition')}`"
                if comparison.get("stop_condition")
                else ""
            ),
            "",
            "## Next Phase",
            "",
            (
                "- The selected continuation posture is still blocked; resolve posture-vs-contract in a separate sprint before retrying this lane."
                if summary.get("lane_result") == "INVALID_LANE"
                else "- Continue only the surviving pair decision from this artifact."
            ),
            "- Do not mutate ranking, promotion, or the global watchlist from this sprint alone.",
            "",
        ]
    )
    return "\n".join(line for line in lines if line != "")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry_path = Path(args.canonical_truth_registry).resolve()
    medium_result_path = Path(args.medium_result_json).resolve()
    medium_report_path = Path(args.medium_report_md).resolve()
    binding_path = Path(args.binding_json).resolve()
    output_dir = Path(args.output_dir).resolve()
    result_json_path = Path(args.result_json).resolve()
    report_md_path = Path(args.report_md).resolve()

    registry = load_json(registry_path, "canonical_truth_registry")
    medium_result_doc = load_json(medium_result_path, "phase7_medium_shadow_result_json")
    _medium_report_text = read_text(medium_report_path, "phase7_medium_shadow_report_md")
    binding_doc = load_json(binding_path, "binding_json")

    require_registry_path(registry, "runtime_binding", binding_path)
    targets = selected_targets(medium_result_doc, binding_doc)
    if output_dir.exists() and not args.dry_run:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target_by_id: dict[str, dict[str, Any]] = {}
    for idx, target in enumerate(targets, start=1):
        run_root = output_dir / "runs" / f"{idx:02d}_{slugify(target['symbol'])}"
        target["run_root"] = run_root
        target_by_id[target["strategy_id"]] = target

    run_meta_by_id: dict[str, dict[str, Any]] = {}
    started_at = monotonic()
    print(
        f"PHASE7_CONTINUATION_VALIDATION_STARTED target_count={len(targets)} max_parallel={int(args.max_parallel)} "
        f"progress_interval_sec={int(args.progress_interval_sec)} run_max_duration_sec={int(args.run_max_duration_sec)}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=int(args.max_parallel)) as executor:
        pending = {
            executor.submit(run_batch_for_target, args, target, target["run_root"]): target["strategy_id"]
            for target in targets
        }
        while pending:
            done, _not_done = wait(
                pending,
                timeout=float(args.progress_interval_sec),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                elapsed = int(monotonic() - started_at)
                print(
                    f"PHASE7_CONTINUATION_VALIDATION_PROGRESS elapsed_sec={elapsed} "
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
                        "effective_run_semantics": run_semantics(str(args.run_posture)),
                    }
                elapsed = int(monotonic() - started_at)
                exit_code = run_meta_by_id[strategy_id].get("exit_code")
                timed_out = 1 if run_meta_by_id[strategy_id].get("timed_out") else 0
                print(
                    f"PHASE7_CONTINUATION_VALIDATION_STRATEGY_DONE elapsed_sec={elapsed} "
                    f"completed={len(run_meta_by_id)}/{len(targets)} strategy_id={strategy_id} "
                    f"exit_code={exit_code} timed_out={timed_out}",
                    flush=True,
                )
                del pending[future]

    rows = [build_result_row(target_by_id[strategy_id], run_meta_by_id[strategy_id], args) for strategy_id in sorted(run_meta_by_id)]
    rows.sort(key=lambda row: (int(row.get("rank") or 10**9), str(row.get("symbol") or ""), str(row.get("strategy_id") or "")))
    comparison = compare_rows(rows) if not args.dry_run else {
        "stronger_candidate": None,
        "weaker_candidate": None,
        "both_survive": False,
        "only_one_survives": False,
        "surviving_symbols": [],
        "comparison_notes": ["dry_run only"],
        "final_recommendation": "NEITHER_ADVANCE",
    }
    apply_pairwise_reduction(rows, comparison)
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
                    "concept": "phase7_medium_shadow_result",
                    "path": str(medium_result_path),
                    "generated_ts_utc": medium_result_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "phase7_medium_shadow_report",
                    "path": str(medium_report_path),
                },
            ],
            "notes": [
                "This sprint only evaluates the two continuation candidates from the medium-frequency lane.",
                "Global ranking, promotion state, and shadow watchlist remain unchanged.",
                "Execution uses isolated task-local watchlists and task-local shadow state directories only.",
            ],
        },
        "run_policy": {
            "target_symbols": [target["symbol"] for target in targets],
            "target_strategy_ids": [target["strategy_id"] for target in targets],
            "run_posture": str(args.run_posture),
            "run_semantics": run_semantics(str(args.run_posture)),
            "max_parallel": int(args.max_parallel),
            "run_max_duration_sec": int(args.run_max_duration_sec),
            "per_run_timeout_sec": int(args.per_run_timeout_sec),
            "subprocess_timeout_sec": int(args.subprocess_timeout_sec),
            "heartbeat_ms": int(args.heartbeat_ms),
            "progress_interval_sec": int(args.progress_interval_sec),
            "extreme_churn_max_trade_transitions_per_1k_events": float(args.max_trade_transitions_per_1k_events),
            "dry_run": bool(args.dry_run),
        },
        "summary": summary_payload(rows, comparison),
        "pairwise_comparison": comparison,
        "final_recommendation": comparison.get("final_recommendation"),
        "results": rows,
    }
    if args.dry_run:
        payload["summary"]["lane_result"] = "DRY_RUN_ONLY"
        payload["summary"]["final_recommendation"] = "NEITHER_ADVANCE"
    write_json(result_json_path, payload)
    report_md_path.parent.mkdir(parents=True, exist_ok=True)
    report_md_path.write_text(report_markdown(payload, medium_result_path, medium_report_path) + "\n", encoding="utf-8")
    print("PHASE7_CONTINUATION_VALIDATION_COMPLETE")
    print(f"result_json={result_json_path}")
    print(f"report_md={report_md_path}")
    print(f"target_count={len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContinuationValidationError as exc:
        print(f"PHASE7_CONTINUATION_VALIDATION_ERROR: {exc}")
        raise SystemExit(1)
