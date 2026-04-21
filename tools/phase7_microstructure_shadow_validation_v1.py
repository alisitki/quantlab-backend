#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
PHASE6 = TOOLS / "phase6_state"
DEFAULT_SHORTLIST_JSON = TOOLS / "microstructure_phase6_shortlist_v0.json"
DEFAULT_BATCH_TOOL = TOOLS / "run-shadow-observation-batch-v0.py"
DEFAULT_OUTPUT_DIR = TOOLS / "phase7_microstructure_shadow_v1_output" / "full_run"
DEFAULT_RESULT_JSON = TOOLS / "phase7_microstructure_shadow_result_v1.json"
DEFAULT_REPORT_MD = TOOLS / "phase7_microstructure_shadow_v1_output" / "phase7_microstructure_shadow_report_v1.md"
RUNTIME_STRATEGY_FILE = "core/strategy/strategies/MicrostructureImbalanceV1Strategy.js"
FAMILY_ID = "microstructure_imbalance_v1"
RUN_SEMANTICS = "ISOLATED_PAPER_DIRECTIONAL_MICROSTRUCTURE_SHADOW"
VERDICT_PRIORITY = {
    "KEEP_ADVANCING": 0,
    "WEAK_CONTINUE": 1,
    "DROP": 2,
    "INVALID_RUN": 3,
}


class Phase7MicrostructureShadowError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise Phase7MicrostructureShadowError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase7 microstructure shortlist shadow validation v1")
    parser.add_argument("--shortlist-json", type=Path, default=DEFAULT_SHORTLIST_JSON)
    parser.add_argument("--batch-tool", type=Path, default=DEFAULT_BATCH_TOOL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--result-json", type=Path, default=DEFAULT_RESULT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--max-parallel", type=int, default=3)
    parser.add_argument("--run-max-duration-sec", type=int, default=21600)
    parser.add_argument("--per-run-timeout-sec", type=int, default=21900)
    parser.add_argument("--subprocess-timeout-sec", type=int, default=22500)
    parser.add_argument("--heartbeat-ms", type=int, default=5000)
    parser.add_argument("--progress-interval-sec", type=int, default=60)
    parser.add_argument("--max-continuation", type=int, default=3)
    parser.add_argument("--max-trade-transitions-per-1k-events", type=float, default=25.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.max_parallel <= 0:
        fail(f"invalid_max_parallel:{args.max_parallel}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.subprocess_timeout_sec < args.per_run_timeout_sec:
        fail("subprocess_timeout_sec_must_cover_per_run_timeout_sec")
    if args.max_continuation <= 0 or args.max_continuation > 3:
        fail(f"invalid_max_continuation:{args.max_continuation}")
    return args


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
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
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


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().lower()


def slugify(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value or ""))
    compact = "_".join(part for part in cleaned.split("_") if part)
    return compact[:80] or "item"


def selected_targets(shortlist_doc: dict[str, Any]) -> list[dict[str, Any]]:
    items = shortlist_doc.get("shortlist")
    if not isinstance(items, list) or not items:
        fail("shortlist_missing_items")
    targets: list[dict[str, Any]] = []
    for idx, row in enumerate(items, start=1):
        if not isinstance(row, dict):
            fail("shortlist_item_not_object")
        strategy_id = str(row.get("strategy_id") or "").strip()
        family_id = str(row.get("family_id") or "").strip()
        exchange = str(row.get("exchange") or "").strip().lower()
        symbol = normalize_symbol(row.get("symbol"))
        stream = str(row.get("stream") or "").strip().lower()
        cell = row.get("selected_cell")
        if family_id != FAMILY_ID:
            fail(f"shortlist_family_mismatch:{strategy_id}:{family_id}")
        if exchange != "bybit":
            fail(f"shortlist_exchange_not_bybit:{strategy_id}:{exchange}")
        if stream != "trade":
            fail(f"shortlist_stream_not_trade:{strategy_id}:{stream}")
        if not strategy_id or not symbol:
            fail(f"shortlist_missing_strategy_or_symbol:{idx}")
        if not isinstance(cell, dict):
            fail(f"shortlist_selected_cell_missing:{strategy_id}")
        targets.append(
            {
                "rank": idx,
                "strategy_id": strategy_id,
                "family_id": family_id,
                "exchange": exchange,
                "symbol": symbol,
                "stream": stream,
                "shortlist_row": row,
            }
        )
    if len(targets) != 10:
        fail(f"shortlist_count_mismatch_expected_10_actual_{len(targets)}")
    return targets


def build_runtime_strategy_config(target: dict[str, Any]) -> dict[str, Any]:
    row = target["shortlist_row"]
    cell = dict(row["selected_cell"])
    cell["exchange"] = target["exchange"]
    cell["symbol"] = target["symbol"]
    cell["stream"] = target["stream"]
    return {
        "binding_mode": "PAPER_DIRECTIONAL_V1",
        "family_id": FAMILY_ID,
        "source_pack_id": target["strategy_id"],
        "source_decision_tier": "PHASE6_MICROSTRUCTURE_SHORTLIST",
        "exchange": target["exchange"],
        "stream": target["stream"],
        "symbols": [target["symbol"]],
        "source_family_report_path": str(row.get("source_family_report_path") or ""),
        "window": ",".join(row.get("selection_evidence", {}).get("source_dates", [])),
        "orderQty": 1,
        "params": {
            "delta_ms_list": [int(cell["delta_ms"])],
            "h_ms_list": [int(cell["h_ms"])],
            "pressure_threshold_list": [float(cell["pressure_threshold"])],
            "tolerance_ms": 0,
        },
        "selected_cell": cell,
    }


def build_watchlist(target: dict[str, Any], shortlist_json_path: Path) -> dict[str, Any]:
    return {
        "schema_version": "phase7_microstructure_shadow_watchlist_v1",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "surface_role": "TASK_LOCAL_MICROSTRUCTURE_PHASE7_SELECTION",
            "authoritative_scope": "Single-strategy task-local validation only.",
            "not_authoritative_for": ["global shadow watchlist", "ranking", "promotion"],
            "notes": [
                "This watchlist is isolated to one microstructure Phase7 validation run.",
                "It intentionally does not read or mutate the global shadow watchlist.",
            ],
        },
        "source_shortlist_json": str(shortlist_json_path.resolve()),
        "selected_count": 1,
        "items": [
            {
                "rank": int(target["rank"]),
                "pack_id": target["strategy_id"],
                "pack_path": "",
                "exchange": target["exchange"],
                "symbols": [target["symbol"]],
                "decision_tier": "PHASE6_MICROSTRUCTURE_SHORTLIST",
                "selection_slot": f"{target['exchange']}/{target['stream']}/{target['symbol']}",
                "strategy_id": target["strategy_id"],
                "family_id": FAMILY_ID,
                "selected_cell": target["shortlist_row"].get("selected_cell"),
            }
        ],
    }


def run_batch_for_target(args: argparse.Namespace, target: dict[str, Any], run_root: Path) -> dict[str, Any]:
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
    write_json(watchlist_path, build_watchlist(target, Path(args.shortlist_json)))

    cmd = [
        sys.executable,
        str(Path(args.batch_tool).resolve()),
        "--watchlist",
        str(watchlist_path),
        "--max-items",
        "1",
        "--strategy",
        RUNTIME_STRATEGY_FILE,
        "--strategy-config-json",
        json.dumps(build_runtime_strategy_config(target), sort_keys=True),
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
        "audit_base_dir": str(audit_base_dir.resolve()),
        "summary_json_path": str(summary_json_path.resolve()),
        "batch_result_json_path": str(result_json.resolve()),
        "refresh_result_json_path": str(refresh_result_json.resolve()),
        "execution_ledger_jsonl": str(execution_ledger_jsonl.resolve()),
        "execution_pack_summary_json": str(execution_pack_summary_json.resolve()),
        "top_stdout_log": str(top_stdout_log.resolve()),
        "top_stderr_log": str(top_stderr_log.resolve()),
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


def latest_pack_summary_row(pack_summary: dict[str, Any] | None, pack_id: str) -> dict[str, Any] | None:
    if not isinstance(pack_summary, dict):
        return None
    latest_by_pack_id = pack_summary.get("latest_by_pack_id")
    if isinstance(latest_by_pack_id, dict):
        row = latest_by_pack_id.get(pack_id)
        if isinstance(row, dict):
            return row
    return None


def futures_paper_item(futures_paper_ledger: dict[str, Any] | None, pack_id: str, live_run_id: str) -> dict[str, Any] | None:
    if not isinstance(futures_paper_ledger, dict):
        return None
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


def cost_proxy_metrics(futures_item: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(futures_item, dict):
        return {
            "available": False,
            "note": "not available in current artifact surface",
            "paper_run_status": "not available in current artifact surface",
            "cost_accounting_status": "not available in current artifact surface",
            "profitability_status": "not available in current artifact surface",
            "effective_fee_rate": None,
            "total_fee_quote": None,
            "funding_cost_quote": None,
            "mark_to_market_pnl_quote_net_paid_fees": None,
        }
    return {
        "available": True,
        "note": "",
        "paper_run_status": str(futures_item.get("paper_run_status") or "UNKNOWN"),
        "cost_accounting_status": str(futures_item.get("cost_accounting_status") or "UNKNOWN"),
        "profitability_status": str(futures_item.get("profitability_status") or "UNKNOWN"),
        "effective_fee_rate": futures_item.get("effective_fee_rate"),
        "total_fee_quote": futures_item.get("total_fee_quote"),
        "funding_cost_quote": futures_item.get("funding_cost_quote"),
        "mark_to_market_pnl_quote_net_paid_fees": futures_item.get("mark_to_market_pnl_quote_net_paid_fees"),
    }


def invalid_result_row(target: dict[str, Any], run_meta: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "strategy_id": target["strategy_id"],
        "rank": int(target["rank"]),
        "family_id": FAMILY_ID,
        "symbol": target["symbol"],
        "exchange": target["exchange"],
        "status": "INVALID_RUN",
        "verdict": "INVALID_RUN",
        "verdict_reason": reason,
        "metrics": {
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
            "basic_pnl_proxy": {"available": False, "note": "not available in current artifact surface"},
            "cost_proxy": cost_proxy_metrics(None),
        },
        "artifacts": run_meta,
        "eliminated": True,
        "elimination_reason": reason,
    }


def verdict_from_metrics(metrics: dict[str, Any], max_churn: float) -> tuple[str, str]:
    if metrics.get("verify_soft_live_pass") is not True:
        return "INVALID_RUN", "verify_soft_live_pass != true"
    processed = int(metrics.get("processed_event_count") or 0)
    if processed == 0:
        return "DROP", "processed_event_count == 0"
    churn = metrics.get("trade_transitions_per_1k_events")
    if isinstance(churn, (int, float)) and float(churn) > float(max_churn):
        return "DROP", f"trade_transitions_per_1k_events>{float(max_churn):.3f}"
    fill_count = int(metrics.get("fill_count") or 0)
    decision_count = int(metrics.get("decision_count") or 0)
    if fill_count > 0:
        return "KEEP_ADVANCING", "fill_count > 0 with bounded churn"
    if decision_count > 0:
        return "DROP", "decision_count > 0 but fill_count == 0"
    return "DROP", "decision_count == 0 and fill_count == 0"


def build_result_row(target: dict[str, Any], run_meta: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
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
        if args.dry_run:
            return invalid_result_row(target, run_meta, "dry_run_only")
        if item.get("run_executed") is not True:
            return invalid_result_row(target, run_meta, "run_not_executed")
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
        refresh_result = load_optional_json(Path(str(run_meta["refresh_result_json_path"])).resolve()) or {}
        execution_events_rows = load_jsonl(shadow_state_dir / "shadow_execution_events_v1.jsonl")
        trade_rows = load_jsonl(shadow_state_dir / "shadow_trade_ledger_v1.jsonl")
        futures_paper_ledger = load_optional_json(shadow_state_dir / "shadow_futures_paper_ledger_v1.json")
        execution_pack_summary = load_optional_json(shadow_state_dir / "shadow_execution_pack_summary_v0.json")

        pack_id = str(item.get("pack_id") or target["strategy_id"]).strip()
        live_run_id = str(summary.get("live_run_id") or "").strip()
        processed_event_count = non_negative_int(summary.get("processed_event_count")) or 0
        event_counts = count_execution_events(execution_events_rows, pack_id, live_run_id)
        trade_counts = count_trade_rows(trade_rows, pack_id, live_run_id)
        trade_transitions = int(trade_counts["OPEN"]) + int(trade_counts["CLOSED"])
        trade_transitions_per_1k_events = (
            round((1000.0 * trade_transitions) / processed_event_count, 6) if processed_event_count > 0 else None
        )
        futures_item = futures_paper_item(futures_paper_ledger, pack_id, live_run_id)
        pack_summary_row = latest_pack_summary_row(execution_pack_summary, pack_id)
        execution_summary = summary.get("execution_summary")
        execution_summary = execution_summary if isinstance(execution_summary, dict) else {}
        basic_pnl_proxy = {
            "available": any(key in execution_summary for key in ("total_realized_pnl", "total_unrealized_pnl", "equity")),
            "total_realized_pnl": execution_summary.get("total_realized_pnl"),
            "total_unrealized_pnl": execution_summary.get("total_unrealized_pnl"),
            "equity": execution_summary.get("equity"),
            "pnl_interpretation": str((pack_summary_row or {}).get("pnl_interpretation") or "UNKNOWN"),
        }
        metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": processed_event_count,
            "decision_count": int(event_counts["DECISION"]),
            "fill_count": int(event_counts["FILL"]),
            "open_count": int(trade_counts["OPEN"]),
            "exit_count": int(trade_counts["CLOSED"]),
            "reversal_count": None,
            "trade_transitions": trade_transitions,
            "trade_transitions_per_1k_events": trade_transitions_per_1k_events,
            "bounded_churn": (
                isinstance(trade_transitions_per_1k_events, (int, float))
                and float(trade_transitions_per_1k_events) <= float(args.max_trade_transitions_per_1k_events)
            ),
            "risk_reject_count": int(event_counts["RISK_REJECT"]),
            "run_duration_sec": non_negative_float(summary.get("run_duration_sec")),
            "stop_reason": str(summary.get("stop_reason") or ""),
            "heartbeat_count": non_negative_int(summary.get("heartbeat_count")),
            "heartbeat_seen": summary.get("heartbeat_seen"),
            "fills_count_snapshot": non_negative_int(execution_summary.get("fills_count")),
            "positions_count": non_negative_int(execution_summary.get("positions_count")),
            "basic_pnl_proxy": basic_pnl_proxy,
            "cost_proxy": cost_proxy_metrics(futures_item),
            "refresh_sync_ok": bool(refresh_result.get("sync_ok")),
        }
        verdict, reason = verdict_from_metrics(metrics, float(args.max_trade_transitions_per_1k_events))
        return {
            "strategy_id": target["strategy_id"],
            "rank": int(target["rank"]),
            "family_id": FAMILY_ID,
            "symbol": target["symbol"],
            "exchange": target["exchange"],
            "status": "OK" if verdict != "INVALID_RUN" else "INVALID_RUN",
            "verdict": verdict,
            "verdict_reason": reason,
            "metrics": metrics,
            "source_shortlist_cell": target["shortlist_row"].get("selected_cell"),
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
            "elimination_reason": reason,
        }
    except Phase7MicrostructureShadowError:
        raise
    except Exception as exc:
        return invalid_result_row(target, run_meta, f"artifact_parse_error:{exc}")


def reduction_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    churn = metrics.get("trade_transitions_per_1k_events")
    churn_value = float(churn) if isinstance(churn, (int, float)) else 10**9
    source_cell = row.get("source_shortlist_cell") if isinstance(row.get("source_shortlist_cell"), dict) else {}
    source_score = row.get("source_shortlist_score")
    return (
        VERDICT_PRIORITY.get(str(row.get("verdict") or "INVALID_RUN"), 99),
        int(metrics.get("risk_reject_count") or 0),
        -int(metrics.get("fill_count") or 0),
        -int(metrics.get("decision_count") or 0),
        churn_value,
        -float(source_score if isinstance(source_score, (int, float)) else 0.0),
        -float(source_cell.get("t_stat") or 0.0),
        int(row.get("rank") or 10**9),
        str(row.get("strategy_id") or ""),
    )


def apply_reduction(rows: list[dict[str, Any]], targets: dict[str, dict[str, Any]], max_continuation: int) -> list[dict[str, Any]]:
    for row in rows:
        target = targets.get(str(row.get("strategy_id") or ""))
        if target:
            row["source_shortlist_score"] = target["shortlist_row"].get("score")
    eligible = [row for row in rows if str(row.get("verdict") or "") == "KEEP_ADVANCING"]
    eligible.sort(key=reduction_sort_key)
    selected = eligible[:max_continuation]
    selected_ids = {str(row.get("strategy_id") or "") for row in selected}
    continuation: list[dict[str, Any]] = []
    for row in rows:
        sid = str(row.get("strategy_id") or "")
        if sid in selected_ids:
            row["eliminated"] = False
            row["elimination_reason"] = ""
            metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
            continuation.append(
                {
                    "strategy_id": sid,
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
        elif str(row.get("verdict") or "") == "KEEP_ADVANCING":
            row["eliminated"] = True
            row["elimination_reason"] = f"lower_priority_than_top{max_continuation}"
    continuation.sort(key=lambda row: (int(row.get("rank") or 10**9), str(row.get("strategy_id") or "")))
    return continuation


def summary_payload(rows: list[dict[str, Any]], continuation: list[dict[str, Any]]) -> dict[str, Any]:
    verdict_counts = {
        verdict: sum(1 for row in rows if str(row.get("verdict") or "") == verdict)
        for verdict in ("KEEP_ADVANCING", "WEAK_CONTINUE", "DROP", "INVALID_RUN")
    }
    lane_result = "INVALID_LANE" if verdict_counts["INVALID_RUN"] == len(rows) else "MIXED_LANE"
    if verdict_counts["KEEP_ADVANCING"] > 0:
        lane_result = "POSITIVE_LANE"
    elif verdict_counts["DROP"] == len(rows):
        lane_result = "NEGATIVE_LANE"
    top = continuation[0] if continuation else None
    return {
        "total_strategy_count": len(rows),
        "verify_pass_count": sum(1 for row in rows if row.get("metrics", {}).get("verify_soft_live_pass") is True),
        "fill_positive_count": sum(1 for row in rows if int(row.get("metrics", {}).get("fill_count") or 0) > 0),
        "continuation_count": len(continuation),
        "survivor_strategy_ids": [row["strategy_id"] for row in continuation],
        "top_candidate": top,
        "verdict_counts": verdict_counts,
        "lane_result": lane_result,
    }


def report_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Phase7 Microstructure Shadow Result v1",
        "",
        f"- generated_ts_utc: `{payload['generated_ts_utc']}`",
        "- validation_scope: `bybit-only validation`",
        f"- lane_result: `{summary['lane_result']}`",
        f"- strategies run: `{summary['total_strategy_count']}`",
        f"- strategies with fills: `{summary['fill_positive_count']}`",
        f"- survivors: `{summary['continuation_count']}`",
        "- profitability verdict: `not evaluated in this sprint`",
        "",
        "## Survivors",
    ]
    if payload["continuation_candidates"]:
        for row in payload["continuation_candidates"]:
            metrics = row.get("metrics", {})
            lines.append(
                "- `{}` symbol={} fills={} decisions={} churn_per_1k={}".format(
                    row["strategy_id"],
                    row["symbol"],
                    metrics.get("fill_count"),
                    metrics.get("decision_count"),
                    metrics.get("trade_transitions_per_1k_events"),
                )
            )
    else:
        lines.append("- none")
    lines += ["", "## Dropped / Invalid"]
    for row in payload["results"]:
        if row.get("eliminated") is False:
            continue
        metrics = row.get("metrics", {})
        lines.append(
            "- `{}` symbol={} verdict={} reason={} fills={} decisions={}".format(
                row.get("strategy_id"),
                row.get("symbol"),
                row.get("verdict"),
                row.get("elimination_reason") or row.get("verdict_reason"),
                metrics.get("fill_count"),
                metrics.get("decision_count"),
            )
        )
    lines += [
        "",
        "## Note",
        "This is bybit-only validation for `microstructure_imbalance_v1`. No profitability verdict is issued here.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    shortlist_doc = load_json(Path(args.shortlist_json).resolve(), "shortlist_json")
    targets = selected_targets(shortlist_doc)
    target_by_id = {target["strategy_id"]: target for target in targets}
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for idx, target in enumerate(targets, start=1):
        target["run_root"] = output_dir / "runs" / f"{idx:02d}_{slugify(target['symbol'])}_{slugify(target['strategy_id'])}"

    run_meta_by_id: dict[str, dict[str, Any]] = {}
    started_at = time.monotonic()
    print(
        f"PHASE7_MICROSTRUCTURE_SHADOW_STARTED target_count={len(targets)} max_parallel={int(args.max_parallel)} "
        f"progress_interval_sec={int(args.progress_interval_sec)} run_max_duration_sec={int(args.run_max_duration_sec)}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=int(args.max_parallel)) as executor:
        future_map = {
            executor.submit(run_batch_for_target, args, target, target["run_root"]): target["strategy_id"]
            for target in targets
        }
        pending = set(future_map)
        last_progress = time.monotonic()
        while pending:
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            now = time.monotonic()
            if now - last_progress >= float(args.progress_interval_sec):
                print(
                    "PHASE7_MICROSTRUCTURE_SHADOW_PROGRESS "
                    f"elapsed_sec={int(now - started_at)} completed={len(run_meta_by_id)} remaining={len(pending) + len(done)}",
                    flush=True,
                )
                last_progress = now
            for future in done:
                sid = future_map[future]
                try:
                    run_meta_by_id[sid] = future.result()
                except Exception as exc:
                    target = target_by_id[sid]
                    run_meta_by_id[sid] = {
                        "command": "",
                        "exit_code": "exception",
                        "timed_out": False,
                        "exception": repr(exc),
                        "watchlist_path": str((target["run_root"] / "input_watchlist.json").resolve()),
                        "shadow_state_dir": str((target["run_root"] / "shadow_state").resolve()),
                        "batch_out_dir": str((target["run_root"] / "batch_out").resolve()),
                        "audit_base_dir": str((target["run_root"] / "audit").resolve()),
                        "summary_json_path": str((target["run_root"] / "summary_runtime.json").resolve()),
                        "batch_result_json_path": str((target["run_root"] / "shadow_observation_batch_result_v0.json").resolve()),
                        "refresh_result_json_path": str((target["run_root"] / "shadow_state" / "shadow_derived_surface_refresh_v0.json").resolve()),
                        "execution_ledger_jsonl": str((target["run_root"] / "shadow_state" / "shadow_execution_ledger_v0.jsonl").resolve()),
                        "execution_pack_summary_json": str((target["run_root"] / "shadow_state" / "shadow_execution_pack_summary_v0.json").resolve()),
                    }
                print(
                    "PHASE7_MICROSTRUCTURE_SHADOW_STRATEGY_DONE "
                    f"completed={len(run_meta_by_id)}/{len(targets)} strategy_id={sid} "
                    f"exit_code={run_meta_by_id[sid].get('exit_code')} timed_out={1 if run_meta_by_id[sid].get('timed_out') else 0}",
                    flush=True,
                )

    rows = [build_result_row(target_by_id[sid], run_meta_by_id[sid], args) for sid in sorted(run_meta_by_id)]
    rows.sort(key=lambda row: (int(row.get("rank") or 10**9), str(row.get("strategy_id") or "")))
    continuation = apply_reduction(rows, target_by_id, int(args.max_continuation))
    payload = {
        "schema_version": "phase7_microstructure_shadow_result_v1",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "bybit_only_validation": True,
            "ranking_mutation": False,
            "promotion_mutation": False,
            "global_shadow_watchlist_mutation": False,
            "strategy_logic_mutation": False,
            "new_candidate_generation": False,
            "notes": [
                "This result uses only tools/microstructure_phase6_shortlist_v0.json.",
                "No profitability verdict is issued in this sprint.",
            ],
        },
        "run_policy": {
            "family_id": FAMILY_ID,
            "runtime_strategy_file": RUNTIME_STRATEGY_FILE,
            "run_semantics": RUN_SEMANTICS,
            "posture": "PAPER_DIRECTIONAL_V1",
            "max_parallel": int(args.max_parallel),
            "run_max_duration_sec": int(args.run_max_duration_sec),
            "per_run_timeout_sec": int(args.per_run_timeout_sec),
            "subprocess_timeout_sec": int(args.subprocess_timeout_sec),
            "heartbeat_ms": int(args.heartbeat_ms),
            "max_trade_transitions_per_1k_events": float(args.max_trade_transitions_per_1k_events),
            "max_continuation": int(args.max_continuation),
            "dry_run": bool(args.dry_run),
        },
        "source_shortlist_json": str(Path(args.shortlist_json).resolve()),
        "summary": summary_payload(rows, continuation),
        "continuation_candidates": continuation,
        "results": rows,
    }
    write_json(Path(args.result_json).resolve(), payload)
    Path(args.report_md).resolve().parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_md).resolve().write_text(report_markdown(payload), encoding="utf-8")
    print(
        "PHASE7_MICROSTRUCTURE_SHADOW_COMPLETE "
        f"result_json={Path(args.result_json).resolve()} report_md={Path(args.report_md).resolve()} "
        f"survivors={len(continuation)} lane_result={payload['summary']['lane_result']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
