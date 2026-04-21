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
DEFAULT_SHORTLIST_JSON = ROOT / "tools" / "phase6_state" / "shadow_shortlist_v0.json"
DEFAULT_BINDING_JSON = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_CANONICAL_TRUTH_REGISTRY = ROOT / "tools" / "system_state" / "canonical_truth_registry_v0.json"
DEFAULT_OUTPUT_DIR = ROOT / "tools" / "phase7_shadow_validation_output"
DEFAULT_RESULT_JSON = ROOT / "tools" / "phase7_shadow_result_v0.json"
DEFAULT_BATCH_TOOL = ROOT / "tools" / "run-shadow-observation-batch-v0.py"
SCHEMA_VERSION = "phase7_shortlist_shadow_validation_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"


class ShadowValidationError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise ShadowValidationError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run isolated bounded shadow validation for a shortlist.")
    parser.add_argument("--shortlist-json", default=str(DEFAULT_SHORTLIST_JSON))
    parser.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    parser.add_argument("--canonical-truth-registry", default=str(DEFAULT_CANONICAL_TRUTH_REGISTRY))
    parser.add_argument("--batch-tool", default=str(DEFAULT_BATCH_TOOL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--max-strategies", type=int, default=10)
    parser.add_argument("--run-max-duration-sec", type=int, default=12)
    parser.add_argument("--per-run-timeout-sec", type=int, default=45)
    parser.add_argument("--heartbeat-ms", type=int, default=2000)
    parser.add_argument("--subprocess-timeout-sec", type=int, default=180)
    parser.add_argument("--max-continuation", type=int, default=2)
    parser.add_argument("--max-trade-transitions", type=int, default=20)
    parser.add_argument("--max-fill-events", type=int, default=40)
    parser.add_argument("--max-trade-transitions-per-1k-events", type=float, default=2.5)
    args = parser.parse_args(argv)
    if args.max_strategies <= 0:
        fail(f"invalid_max_strategies:{args.max_strategies}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    if args.subprocess_timeout_sec <= 0:
        fail(f"invalid_subprocess_timeout_sec:{args.subprocess_timeout_sec}")
    if args.max_continuation < 0:
        fail(f"invalid_max_continuation:{args.max_continuation}")
    if args.max_trade_transitions < 0:
        fail(f"invalid_max_trade_transitions:{args.max_trade_transitions}")
    if args.max_fill_events < 0:
        fail(f"invalid_max_fill_events:{args.max_fill_events}")
    if args.max_trade_transitions_per_1k_events <= 0:
        fail(f"invalid_max_trade_transitions_per_1k_events:{args.max_trade_transitions_per_1k_events}")
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


def require_registry_path(registry: dict[str, Any], concept_name: str, expected_path: str) -> dict[str, Any]:
    expected_resolved = str(Path(expected_path).resolve())
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
            if entry_path == expected_path or str(Path(entry_path).resolve()) == expected_resolved:
                return item
        fail(f"canonical_registry_path_mismatch:{concept_name}:{expected_path}")
    fail(f"canonical_registry_concept_missing:{concept_name}")


def binding_by_strategy_id(binding_artifact: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = binding_artifact.get("items")
    if not isinstance(items, list):
        fail("binding_artifact_missing_items")
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        strategy_id = str(item.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = item
    return out


def shortlist_rows(shortlist_doc: dict[str, Any], max_strategies: int) -> list[dict[str, Any]]:
    rows = shortlist_doc.get("strategies")
    if not isinstance(rows, list):
        fail("shortlist_missing_strategies")
    selected: list[dict[str, Any]] = []
    for row in rows[:max_strategies]:
        if not isinstance(row, dict):
            fail("shortlist_row_not_object")
        selected.append(row)
    if not selected:
        fail("shortlist_empty")
    return selected


def slugify(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value or ""))
    compact = "_".join(part for part in cleaned.split("_") if part)
    return compact[:80] or "item"


def normalize_symbols(binding_row: dict[str, Any]) -> list[str]:
    values = binding_row.get("symbols")
    if not isinstance(values, list):
        fail(f"binding_symbols_invalid:{binding_row.get('strategy_id')}")
    symbols = [str(value or "").strip().upper() for value in values if str(value or "").strip()]
    if not symbols:
        fail(f"binding_symbols_empty:{binding_row.get('strategy_id')}")
    return symbols


def build_watchlist(strategy_row: dict[str, Any], binding_row: dict[str, Any]) -> dict[str, Any]:
    exchange = str(binding_row.get("exchange") or "").strip()
    stream = str(binding_row.get("stream") or "").strip()
    runtime_strategy_config = (
        binding_row.get("runtime_strategy_config") if isinstance(binding_row.get("runtime_strategy_config"), dict) else {}
    )
    decision_tier = str(
        runtime_strategy_config.get("source_decision_tier")
        or binding_row.get("decision_tier")
        or ""
    ).strip()
    return {
        "schema_version": "phase7_shortlist_shadow_validation_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "source_shadow_shortlist_json": str(DEFAULT_SHORTLIST_JSON),
        "source_candidate_strategy_runtime_binding_json": str(DEFAULT_BINDING_JSON),
        "selected_count": 1,
        "items": [
            {
                "rank": int(strategy_row.get("rank") or 0),
                "pack_id": str(binding_row.get("pack_id") or "").strip(),
                "pack_path": str(binding_row.get("pack_path") or "").strip(),
                "exchange": exchange,
                "symbols": normalize_symbols(binding_row),
                "decision_tier": decision_tier,
                "selection_slot": f"{exchange}/{stream}" if exchange and stream else "",
            }
        ],
    }


def run_batch_for_strategy(
    args: argparse.Namespace,
    strategy_row: dict[str, Any],
    binding_row: dict[str, Any],
    run_root: Path,
) -> dict[str, Any]:
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

    write_json(watchlist_path, build_watchlist(strategy_row, binding_row))

    runtime_strategy_file = str(binding_row.get("runtime_strategy_file") or "").strip()
    runtime_strategy_config = (
        binding_row.get("runtime_strategy_config") if isinstance(binding_row.get("runtime_strategy_config"), dict) else None
    )
    if not runtime_strategy_file:
        fail(f"binding_runtime_strategy_file_missing:{binding_row.get('strategy_id')}")
    if runtime_strategy_config is None:
        fail(f"binding_runtime_strategy_config_missing:{binding_row.get('strategy_id')}")

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
        str((ROOT / "tools" / "phase6_state").resolve()),
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


def parse_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
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


def compute_churn_flag(
    *,
    processed_event_count: int,
    trade_counts: dict[str, int],
    fill_event_count: int,
    args: argparse.Namespace,
) -> tuple[bool, str]:
    trade_transitions = int(trade_counts.get("OPEN", 0)) + int(trade_counts.get("CLOSED", 0))
    churn_per_1k = (1000.0 * trade_transitions / processed_event_count) if processed_event_count > 0 else None
    if trade_transitions > int(args.max_trade_transitions):
        return True, f"trade_transitions>{int(args.max_trade_transitions)}"
    if fill_event_count > int(args.max_fill_events):
        return True, f"fill_events>{int(args.max_fill_events)}"
    if churn_per_1k is not None and churn_per_1k > float(args.max_trade_transitions_per_1k_events):
        return True, f"trade_transitions_per_1k_events>{float(args.max_trade_transitions_per_1k_events):.3f}"
    return False, ""


def classify_continuation(metrics: dict[str, Any], rank: int) -> tuple[bool, str]:
    if metrics["extreme_churn"]:
        return False, "Extreme churn threshold exceeded."
    if metrics["verify_soft_live_pass"] is not True:
        return False, "verify_soft_live failed."
    if int(metrics["processed_event_count"]) <= 0:
        return False, "No processed events."
    if int(metrics["fill_event_count"]) <= 0:
        return False, "No synthetic fill activity in bounded run."
    if int(metrics["risk_reject_event_count"]) > 0:
        return False, "Risk rejects observed in bounded run."
    return True, f"Observed fill-backed activity with bounded churn at shortlist rank {rank}."


def continuation_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    churn_per_1k = metrics.get("trade_transitions_per_1k_events")
    churn_value = float(churn_per_1k) if isinstance(churn_per_1k, (int, float)) else 10**9
    return (
        int(metrics.get("risk_reject_event_count", 0) or 0),
        churn_value,
        -int(metrics.get("fill_event_count", 0) or 0),
        -int(metrics.get("processed_event_count", 0) or 0),
        int(row.get("rank", 10**9) or 10**9),
        str(row.get("strategy_id") or ""),
    )


def build_result_row(
    strategy_row: dict[str, Any],
    binding_row: dict[str, Any],
    run_meta: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    batch_result_path = Path(run_meta["batch_result_json_path"])
    if run_meta["timed_out"]:
        return {
            "strategy_id": str(strategy_row.get("strategy_id") or "").strip(),
            "rank": int(strategy_row.get("rank") or 0),
            "family_id": str(strategy_row.get("family_id") or "").strip(),
            "selected_symbol": str(strategy_row.get("selected_symbol") or "").strip(),
            "exchange": str(strategy_row.get("exchange") or "").strip(),
            "status": "STOPPED_INFRA",
            "stop_reason": "subprocess_timeout",
            "metrics": {},
            "artifacts": run_meta,
            "continue_candidate": False,
            "continue_reason": "Run subprocess timed out.",
        }
    if run_meta["exit_code"] != 0:
        batch_result = load_json(batch_result_path, "batch_result_json") if batch_result_path.exists() else {}
        item_note = ""
        if isinstance(batch_result.get("results"), list) and batch_result["results"]:
            first = batch_result["results"][0]
            if isinstance(first, dict):
                item_note = str(first.get("note") or "").strip()
        return {
            "strategy_id": str(strategy_row.get("strategy_id") or "").strip(),
            "rank": int(strategy_row.get("rank") or 0),
            "family_id": str(strategy_row.get("family_id") or "").strip(),
            "selected_symbol": str(strategy_row.get("selected_symbol") or "").strip(),
            "exchange": str(strategy_row.get("exchange") or "").strip(),
            "status": "STOPPED_INFRA",
            "stop_reason": item_note or f"batch_exit_{run_meta['exit_code']}",
            "metrics": {},
            "artifacts": run_meta,
            "continue_candidate": False,
            "continue_reason": "Batch runner exited non-zero.",
        }

    batch_result = load_json(batch_result_path, "batch_result_json")
    results = batch_result.get("results")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        fail(f"batch_result_invalid_results:{batch_result_path}")
    item = results[0]
    summary_json_path = Path(str(item.get("summary_json_path") or "")).resolve()
    refresh_result_json_path = Path(str(run_meta["refresh_result_json_path"])).resolve()
    shadow_state_dir = Path(str(run_meta["shadow_state_dir"])).resolve()

    summary = load_json(summary_json_path, "summary_json")
    refresh_result = load_json(refresh_result_json_path, "refresh_result_json")
    execution_events_rows = load_jsonl(shadow_state_dir / "shadow_execution_events_v1.jsonl")
    trade_rows = load_jsonl(shadow_state_dir / "shadow_trade_ledger_v1.jsonl")
    futures_paper_ledger = load_json(
        shadow_state_dir / "shadow_futures_paper_ledger_v1.json",
        "futures_paper_ledger_json",
    )
    pack_summary = load_json(
        shadow_state_dir / "shadow_execution_pack_summary_v0.json",
        "execution_pack_summary_json",
    )

    verify_soft_live_pass = bool(item.get("verify_soft_live_pass"))
    processed_event_count = parse_positive_int(summary.get("processed_event_count"))
    if not verify_soft_live_pass:
        return {
            "strategy_id": str(strategy_row.get("strategy_id") or "").strip(),
            "rank": int(strategy_row.get("rank") or 0),
            "family_id": str(strategy_row.get("family_id") or "").strip(),
            "selected_symbol": str(strategy_row.get("selected_symbol") or "").strip(),
            "exchange": str(strategy_row.get("exchange") or "").strip(),
            "status": "STOPPED_INFRA",
            "stop_reason": "verify_soft_live_failed",
            "metrics": {
                "verify_soft_live_pass": verify_soft_live_pass,
                "processed_event_count": processed_event_count if processed_event_count is not None else "unknown",
            },
            "artifacts": run_meta,
            "continue_candidate": False,
            "continue_reason": "verify_soft_live failed.",
        }
    if processed_event_count is None or processed_event_count <= 0:
        return {
            "strategy_id": str(strategy_row.get("strategy_id") or "").strip(),
            "rank": int(strategy_row.get("rank") or 0),
            "family_id": str(strategy_row.get("family_id") or "").strip(),
            "selected_symbol": str(strategy_row.get("selected_symbol") or "").strip(),
            "exchange": str(strategy_row.get("exchange") or "").strip(),
            "status": "STOPPED_INFRA",
            "stop_reason": "processed_event_count_not_positive",
            "metrics": {
                "verify_soft_live_pass": verify_soft_live_pass,
                "processed_event_count": processed_event_count if processed_event_count is not None else "unknown",
            },
            "artifacts": run_meta,
            "continue_candidate": False,
            "continue_reason": "processed_event_count was not positive.",
        }

    live_run_id = str(summary.get("live_run_id") or "").strip()
    pack_id = str(item.get("pack_id") or binding_row.get("pack_id") or "").strip()
    event_counts = count_execution_events(execution_events_rows, pack_id, live_run_id)
    trade_counts = count_trade_rows(trade_rows, pack_id, live_run_id)
    trade_transitions = int(trade_counts["OPEN"]) + int(trade_counts["CLOSED"])
    churn_per_1k = round((1000.0 * trade_transitions) / processed_event_count, 6)
    extreme_churn, extreme_churn_reason = compute_churn_flag(
        processed_event_count=processed_event_count,
        trade_counts=trade_counts,
        fill_event_count=int(event_counts["FILL"]),
        args=args,
    )

    pack_summary_row = latest_pack_summary_row(pack_summary, pack_id)
    futures_item = futures_paper_item(futures_paper_ledger, pack_id, live_run_id)
    execution_summary = summary.get("execution_summary")
    execution_summary = execution_summary if isinstance(execution_summary, dict) else {}

    metrics = {
        "verify_soft_live_pass": verify_soft_live_pass,
        "processed_event_count": processed_event_count,
        "heartbeat_seen": summary.get("heartbeat_seen"),
        "heartbeat_count": summary.get("heartbeat_count"),
        "run_duration_sec": summary.get("run_duration_sec"),
        "stop_reason": summary.get("stop_reason"),
        "decision_event_count": int(event_counts["DECISION"]),
        "risk_reject_event_count": int(event_counts["RISK_REJECT"]),
        "fill_event_count": int(event_counts["FILL"]),
        "trade_open_count": int(trade_counts["OPEN"]),
        "trade_closed_count": int(trade_counts["CLOSED"]),
        "trade_transitions": trade_transitions,
        "trade_transitions_per_1k_events": churn_per_1k,
        "snapshot_present": execution_summary.get("snapshot_present"),
        "positions_count": execution_summary.get("positions_count"),
        "fills_count": execution_summary.get("fills_count"),
        "total_realized_pnl": execution_summary.get("total_realized_pnl"),
        "total_unrealized_pnl": execution_summary.get("total_unrealized_pnl"),
        "equity": execution_summary.get("equity"),
        "refresh_sync_ok": bool(refresh_result.get("sync_ok")),
        "execution_artifacts_synced": bool(batch_result.get("execution_artifacts_synced")),
        "pnl_interpretation": str((pack_summary_row or {}).get("pnl_interpretation") or "UNKNOWN"),
        "recent_pnl_bias": str((pack_summary_row or {}).get("recent_pnl_bias") or "NO_HISTORY"),
        "paper_run_status": str((futures_item or {}).get("paper_run_status") or "UNKNOWN"),
        "profitability_status": str((futures_item or {}).get("profitability_status") or "UNKNOWN"),
        "cost_accounting_status": str((futures_item or {}).get("cost_accounting_status") or "UNKNOWN"),
        "extreme_churn": extreme_churn,
        "extreme_churn_reason": extreme_churn_reason,
    }

    continue_candidate, continue_reason = classify_continuation(metrics, int(strategy_row.get("rank") or 0))
    status = "FAIL_EXTREME_CHURN" if extreme_churn else "OK"
    return {
        "strategy_id": str(strategy_row.get("strategy_id") or "").strip(),
        "rank": int(strategy_row.get("rank") or 0),
        "family_id": str(strategy_row.get("family_id") or "").strip(),
        "selected_symbol": str(strategy_row.get("selected_symbol") or "").strip(),
        "exchange": str(strategy_row.get("exchange") or "").strip(),
        "status": status,
        "stop_reason": "",
        "metrics": metrics,
        "artifacts": {
            **run_meta,
            "summary_json_path": str(summary_json_path),
            "refresh_result_json_path": str(refresh_result_json_path),
            "execution_events_jsonl": str((shadow_state_dir / "shadow_execution_events_v1.jsonl").resolve()),
            "trade_ledger_jsonl": str((shadow_state_dir / "shadow_trade_ledger_v1.jsonl").resolve()),
            "futures_paper_ledger_json": str((shadow_state_dir / "shadow_futures_paper_ledger_v1.json").resolve()),
            "execution_pack_summary_json": str((shadow_state_dir / "shadow_execution_pack_summary_v0.json").resolve()),
        },
        "continue_candidate": continue_candidate,
        "continue_reason": continue_reason,
    }


def build_continuation(rows: list[dict[str, Any]], max_continuation: int) -> list[dict[str, Any]]:
    eligible = [row for row in rows if row.get("continue_candidate") is True]
    eligible.sort(key=continuation_sort_key)
    selected: list[dict[str, Any]] = []
    for row in eligible[:max_continuation]:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        selected.append(
            {
                "strategy_id": row.get("strategy_id"),
                "rank": row.get("rank"),
                "family_id": row.get("family_id"),
                "selected_symbol": row.get("selected_symbol"),
                "exchange": row.get("exchange"),
                "reason": row.get("continue_reason"),
                "metrics": {
                    "processed_event_count": metrics.get("processed_event_count"),
                    "fill_event_count": metrics.get("fill_event_count"),
                    "trade_transitions": metrics.get("trade_transitions"),
                    "trade_transitions_per_1k_events": metrics.get("trade_transitions_per_1k_events"),
                    "risk_reject_event_count": metrics.get("risk_reject_event_count"),
                    "paper_run_status": metrics.get("paper_run_status"),
                    "pnl_interpretation": metrics.get("pnl_interpretation"),
                },
            }
        )
    return selected


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    shortlist_path = Path(args.shortlist_json).resolve()
    binding_path = Path(args.binding_json).resolve()
    registry_path = Path(args.canonical_truth_registry).resolve()
    output_dir = Path(args.output_dir).resolve()
    result_json_path = Path(args.result_json).resolve()

    registry = load_json(registry_path, "canonical_truth_registry")
    binding_truth = require_registry_path(registry, "runtime_binding", args.binding_json)

    shortlist_doc = load_json(shortlist_path, "shadow_shortlist_json")
    binding_artifact = load_json(binding_path, "binding_json")
    shortlist = shortlist_rows(shortlist_doc, int(args.max_strategies))
    bindings = binding_by_strategy_id(binding_artifact)

    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    stopped_reason = ""
    stop_after_strategy_id = ""

    for index, strategy_row in enumerate(shortlist, start=1):
        strategy_id = str(strategy_row.get("strategy_id") or "").strip()
        if not strategy_id:
            fail("shortlist_strategy_id_missing")
        binding_row = bindings.get(strategy_id)
        if binding_row is None:
            fail(f"shortlist_binding_missing:{strategy_id}")
        if str(binding_row.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
            fail(f"shortlist_binding_not_bound_shadow_runnable:{strategy_id}")

        run_dir = output_dir / "runs" / f"{index:02d}_{slugify(strategy_row.get('selected_symbol') or strategy_id)}"
        run_meta = run_batch_for_strategy(args, strategy_row, binding_row, run_dir)
        row = build_result_row(strategy_row, binding_row, run_meta, args)
        results.append(row)
        if row["status"] == "STOPPED_INFRA":
            stopped_reason = str(row.get("stop_reason") or "infra_stop")
            stop_after_strategy_id = strategy_id
            break

    continuation = build_continuation(results, int(args.max_continuation))
    overall_status = "STOPPED_INFRA" if stopped_reason else "OK"
    hypothesis_result = "PASS" if continuation else "FAIL"

    summary = {
        "attempted_count": len(results),
        "completed_ok_count": sum(1 for row in results if row.get("status") == "OK"),
        "extreme_churn_fail_count": sum(1 for row in results if row.get("status") == "FAIL_EXTREME_CHURN"),
        "continuation_candidate_count": len(continuation),
        "verify_fail_count": sum(
            1
            for row in results
            if str(row.get("stop_reason") or "").strip() == "verify_soft_live_failed"
        ),
        "processed_event_zero_fail_count": sum(
            1
            for row in results
            if str(row.get("stop_reason") or "").strip() == "processed_event_count_not_positive"
        ),
    }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "status": overall_status,
        "hypothesis_result": hypothesis_result,
        "stopped_reason": stopped_reason,
        "stop_after_strategy_id": stop_after_strategy_id,
        "governance": {
            "authoritative_inputs": [
                {
                    "concept": "runtime_binding",
                    "path": str(binding_path),
                    "registry_entry": binding_truth,
                }
            ],
            "task_local_inputs": [
                {
                    "concept": "shadow_shortlist",
                    "path": str(shortlist_path),
                    "role": "shortlist-only advisory input from Phase6 tradability triage",
                }
            ],
            "execution_mode": "isolated_one_item_shadow_observation_batches",
            "notes": [
                "Runs use isolated per-strategy shadow_state directories under tools/phase7_shadow_validation_output.",
                "Canonical global shadow watchlist/ranking is not modified by this validation runner.",
            ],
        },
        "run_policy": {
            "max_strategies": int(args.max_strategies),
            "run_max_duration_sec": int(args.run_max_duration_sec),
            "per_run_timeout_sec": int(args.per_run_timeout_sec),
            "heartbeat_ms": int(args.heartbeat_ms),
            "subprocess_timeout_sec": int(args.subprocess_timeout_sec),
            "stop_rules": {
                "verify_soft_live_fail": "STOP_ENTIRE_RUN",
                "processed_event_count_zero": "STOP_ENTIRE_RUN",
                "extreme_churn": "MARK_STRATEGY_FAIL",
            },
            "extreme_churn_thresholds": {
                "max_trade_transitions": int(args.max_trade_transitions),
                "max_fill_events": int(args.max_fill_events),
                "max_trade_transitions_per_1k_events": float(args.max_trade_transitions_per_1k_events),
            },
        },
        "summary": summary,
        "continuation_candidates": continuation,
        "results": results,
    }

    write_json(result_json_path, payload)

    print(f"status={payload['status']}")
    print(f"hypothesis_result={payload['hypothesis_result']}")
    print(f"attempted_count={summary['attempted_count']}")
    print(f"continuation_candidate_count={summary['continuation_candidate_count']}")
    print(f"result_json={result_json_path}")
    return 0 if overall_status == "OK" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ShadowValidationError as exc:
        print(f"PHASE7_SHORTLIST_SHADOW_VALIDATION_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
