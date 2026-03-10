#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WATCHLIST = ROOT / "tools" / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_WRAPPER = ROOT / "tools" / "run-shadow-watchlist-v0.js"
DEFAULT_SUMMARY_TOOL = ROOT / "tools" / "shadow_observation_summary_v0.py"
DEFAULT_HISTORY_TOOL = ROOT / "tools" / "shadow_observation_history_v0.py"
DEFAULT_VERIFY_SCRIPT = ROOT / "tools" / "verify-soft-live.js"
DEFAULT_OUT_DIR = ROOT / "tools" / "shadow_state" / "shadow_observation_batch_v0"
DEFAULT_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_observation_batch_result_v0.json"
DEFAULT_HISTORY_JSONL = ROOT / "tools" / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_INDEX_JSON = ROOT / "tools" / "shadow_state" / "shadow_observation_index_v0.json"
DEFAULT_PHASE6_STATE_DIR = ROOT / "tools" / "phase6_state"
DEFAULT_SHADOW_STATE_DIR = ROOT / "tools" / "shadow_state"
DEFAULT_REFRESH_TOOL = ROOT / "tools" / "refresh-shadow-derived-surfaces-v0.py"
DEFAULT_REFRESH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_derived_surface_refresh_v0.json"
DEFAULT_EXECUTION_LEDGER_TOOL = ROOT / "tools" / "shadow_execution_ledger_v0.py"
DEFAULT_EXECUTION_PACK_SUMMARY_TOOL = ROOT / "tools" / "shadow_execution_pack_summary_v0.py"
DEFAULT_EXECUTION_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_ledger_v0.jsonl"
DEFAULT_EXECUTION_PACK_SUMMARY_JSON = ROOT / "tools" / "shadow_state" / "shadow_execution_pack_summary_v0.json"
DEFAULT_SUMMARY_JSON_PATH = Path("/tmp/quantlab-soft-live.json")
DEFAULT_AUDIT_BASE_DIR = Path("/tmp/quantlab-shadow-batch-audit")
DEFAULT_STRATEGY = "core/strategy/strategies/PrintHeadTailStrategy.js"
DEFAULT_MAX_ITEMS = 3
DEFAULT_PER_RUN_TIMEOUT_SEC = 45
DEFAULT_RUN_MAX_DURATION_SEC = 20
DEFAULT_HEARTBEAT_MS = 4000
SCHEMA_VERSION = "shadow_observation_batch_result_v0"


class BatchError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise BatchError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finite shadow observation batch routine v0")
    parser.add_argument("--watchlist", default=str(DEFAULT_WATCHLIST))
    parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS)
    parser.add_argument("--strategy", default=DEFAULT_STRATEGY)
    parser.add_argument("--strategy-config-json", default="{}")
    parser.add_argument("--wrapper-script", default=str(DEFAULT_WRAPPER))
    parser.add_argument("--summary-tool", default=str(DEFAULT_SUMMARY_TOOL))
    parser.add_argument("--history-tool", default=str(DEFAULT_HISTORY_TOOL))
    parser.add_argument("--verify-script", default=str(DEFAULT_VERIFY_SCRIPT))
    parser.add_argument("--summary-json-path", default=str(DEFAULT_SUMMARY_JSON_PATH))
    parser.add_argument("--history-jsonl", default=str(DEFAULT_HISTORY_JSONL))
    parser.add_argument("--index-json", default=str(DEFAULT_INDEX_JSON))
    parser.add_argument("--phase6-state-dir", default=str(DEFAULT_PHASE6_STATE_DIR))
    parser.add_argument("--shadow-state-dir", default=str(DEFAULT_SHADOW_STATE_DIR))
    parser.add_argument("--refresh-tool", default=str(DEFAULT_REFRESH_TOOL))
    parser.add_argument("--refresh-result-json", default=str(DEFAULT_REFRESH_RESULT_JSON))
    parser.add_argument("--execution-ledger-tool", default=str(DEFAULT_EXECUTION_LEDGER_TOOL))
    parser.add_argument("--execution-pack-summary-tool", default=str(DEFAULT_EXECUTION_PACK_SUMMARY_TOOL))
    parser.add_argument("--execution-ledger-jsonl", default=str(DEFAULT_EXECUTION_LEDGER_JSONL))
    parser.add_argument("--execution-pack-summary-json", default=str(DEFAULT_EXECUTION_PACK_SUMMARY_JSON))
    parser.add_argument("--audit-base-dir", default=str(DEFAULT_AUDIT_BASE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.max_items <= 0:
        fail(f"invalid_max_items:{args.max_items}")
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    try:
        parsed_strategy_config = json.loads(str(args.strategy_config_json))
    except json.JSONDecodeError as exc:
        fail(f"invalid_strategy_config_json:{exc}")
    if not isinstance(parsed_strategy_config, dict):
        fail("strategy_config_json_not_object")
    args.strategy_config_json = json.dumps(parsed_strategy_config, sort_keys=True)
    return args


def load_watchlist(path: Path) -> dict[str, Any]:
    if not path.exists():
        fail(f"watchlist_missing:{path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"watchlist_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"watchlist_not_object:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"watchlist_missing_items:{path}")
    if not items:
        fail(f"watchlist_empty:{path}")
    return obj


def selected_items(watchlist: dict[str, Any], max_items: int) -> list[dict[str, Any]]:
    items = []
    for item in list(watchlist.get("items") or [])[:max_items]:
        if not isinstance(item, dict):
            fail("watchlist_item_not_object")
        pack_id = str(item.get("pack_id", "")).strip()
        exchange = str(item.get("exchange", "")).strip()
        symbols = item.get("symbols")
        if not pack_id:
            fail("watchlist_item_missing_pack_id")
        if not exchange:
            fail(f"watchlist_item_missing_exchange:{pack_id}")
        if not isinstance(symbols, list) or not [str(v or "").strip() for v in symbols if str(v or "").strip()]:
            fail(f"watchlist_item_missing_symbols:{pack_id}")
        items.append(dict(item))
    return items


def slugify(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value or ""))
    compact = "_".join(part for part in cleaned.split("_") if part)
    return compact[:80] or "item"


def item_dir(base_dir: Path, item: dict[str, Any]) -> Path:
    rank = int(item.get("rank", 0))
    pack_id = str(item.get("pack_id", "")).strip()
    return base_dir / f"rank{rank:02d}_{slugify(pack_id)}"


def audit_dir(base_dir: Path, item: dict[str, Any]) -> Path:
    rank = int(item.get("rank", 0))
    pack_id = str(item.get("pack_id", "")).strip()
    return base_dir / f"rank{rank:02d}_{slugify(pack_id)}"


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_command(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: Path,
    stdout_path: Path | None = None,
    stderr_path: Path | None = None,
    timeout_sec: int | None = None,
) -> dict[str, Any]:
    stdout_handle = None
    stderr_handle = None
    try:
        if stdout_path is not None:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_handle = stdout_path.open("w", encoding="utf-8")
        if stderr_path is not None:
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_handle = stderr_path.open("w", encoding="utf-8")
        completed = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=stdout_handle if stdout_handle is not None else subprocess.PIPE,
            stderr=stderr_handle if stderr_handle is not None else subprocess.PIPE,
            text=True,
            timeout=timeout_sec,
        )
        return {
            "exit_code": int(completed.returncode),
            "stdout": "" if stdout_handle is not None else str(completed.stdout or ""),
            "stderr": "" if stderr_handle is not None else str(completed.stderr or ""),
            "timed_out": False,
        }
    except subprocess.TimeoutExpired:
        return {
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "timed_out": True,
        }
    finally:
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def item_commands(
    args: argparse.Namespace,
    watchlist_path: Path,
    item: dict[str, Any],
    item_root: Path,
    audit_root: Path,
) -> dict[str, list[str]]:
    pack_id = str(item["pack_id"]).strip()
    summary_path = item_root / "summary.json"
    stdout_log = item_root / "stdout.log"
    audit_path = audit_root
    return {
        "wrapper": [
            "node",
            str(Path(args.wrapper_script).resolve()),
            "--watchlist",
            str(watchlist_path),
            "--pack-id",
            pack_id,
        ],
        "verify": [
            "node",
            str(Path(args.verify_script).resolve()),
        ],
        "summary": [
            sys.executable,
            str(Path(args.summary_tool).resolve()),
            "--watchlist",
            str(watchlist_path),
            "--pack-id",
            pack_id,
            "--summary-json",
            str(Path(args.summary_json_path).resolve()),
            "--audit-spool-dir",
            str(audit_path),
            "--stdout-log",
            str(stdout_log),
            "--out-json",
            str(summary_path),
        ],
        "history": [
            sys.executable,
            str(Path(args.history_tool).resolve()),
            "--summary-json",
            str(summary_path),
            "--history-jsonl",
            str(Path(args.history_jsonl).resolve()),
            "--index-json",
            str(Path(args.index_json).resolve()),
        ],
    }


def build_wrapper_env(args: argparse.Namespace, audit_root: Path) -> dict[str, str]:
    strategy_config = json.loads(str(args.strategy_config_json))
    binding_mode = str(strategy_config.get("binding_mode") or "").strip()
    order_qty = strategy_config.get("orderQty")
    position_size_mode = "ZERO"
    strategy_mode = "OBSERVE_ONLY"
    try:
        order_qty_value = float(order_qty)
    except (TypeError, ValueError):
        order_qty_value = 0.0
    if binding_mode == "PAPER_DIRECTIONAL_V1" and order_qty_value > 0:
        strategy_mode = binding_mode
        position_size_mode = "FIXED"
    return {
        **os.environ,
        "AUDIT_ENABLED": "1",
        "AUDIT_SPOOL_DIR": str(audit_root),
        "RUN_ARCHIVE_ENABLED": "0",
        "CORE_LIVE_WS_ENABLED": "1",
        "STRATEGY_MODE": strategy_mode,
        "POSITION_SIZE_MODE": position_size_mode,
        "GO_LIVE_STRATEGY": str(args.strategy),
        "GO_LIVE_STRATEGY_CONFIG": str(args.strategy_config_json),
        "GO_LIVE_DATASET_PARQUET": "live",
        "GO_LIVE_DATASET_META": "live",
        "PROMOTION_GUARD_MIN_DECISIONS_ENABLED": "0",
        "RUN_MAX_DURATION_SEC": str(int(args.run_max_duration_sec)),
        "RUN_BUDGET_MAX_EVENTS_ENABLED": "0",
        "RUN_BUDGET_MAX_DECISION_RATE_ENABLED": "0",
        "SOFT_LIVE_HEARTBEAT_MS": str(int(args.heartbeat_ms)),
        "SHADOW_BATCH_SUMMARY_JSON": str(Path(args.summary_json_path).resolve()),
    }


def build_verify_env(audit_root: Path) -> dict[str, str]:
    return {
        **os.environ,
        "AUDIT_SPOOL_DIR": str(audit_root),
        "RUN_ARCHIVE_ENABLED": "0",
    }


def base_result(
    item: dict[str, Any],
    item_root: Path,
    audit_root: Path,
    commands: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "rank": int(item.get("rank", 0)),
        "pack_id": str(item.get("pack_id", "")).strip(),
        "exchange": str(item.get("exchange", "")).strip(),
        "symbols": [str(v or "").strip() for v in list(item.get("symbols") or []) if str(v or "").strip()],
        "run_executed": False,
        "run_exit_code": "not_run",
        "verify_soft_live_pass": "unknown",
        "summary_generated": False,
        "summary_json_path": str(item_root / "summary.json"),
        "history_updated": False,
        "stdout_log_path": str(item_root / "stdout.log"),
        "stderr_log_path": str(item_root / "stderr.log"),
        "audit_spool_dir": str(audit_root),
        "wrapper_command": command_preview(commands["wrapper"]),
        "summary_command": command_preview(commands["summary"]),
        "history_command": command_preview(commands["history"]),
        "verify_command": command_preview(commands["verify"]),
        "note": "",
    }


def process_item(args: argparse.Namespace, watchlist_path: Path, item: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    item_root = item_dir(out_dir, item)
    audit_root = audit_dir(Path(args.audit_base_dir).resolve(), item)
    item_root.mkdir(parents=True, exist_ok=True)
    commands = item_commands(args, watchlist_path, item, item_root, audit_root)
    result = base_result(item, item_root, audit_root, commands)

    if args.dry_run:
        result["note"] = "dry_run"
        return result

    summary_json_path = Path(args.summary_json_path).resolve()
    default_summary_json_path = DEFAULT_SUMMARY_JSON_PATH.resolve()
    for stale_path in {summary_json_path, default_summary_json_path}:
        if stale_path.exists():
            stale_path.unlink()
    if audit_root.exists():
        shutil.rmtree(audit_root)

    wrapper_env = build_wrapper_env(args, audit_root)
    wrapper_res = run_command(
        commands["wrapper"],
        env=wrapper_env,
        cwd=ROOT,
        stdout_path=item_root / "stdout.log",
        stderr_path=item_root / "stderr.log",
        timeout_sec=int(args.per_run_timeout_sec),
    )
    result["run_executed"] = True
    if wrapper_res["timed_out"]:
        result["run_exit_code"] = "timeout"
        result["note"] = f"wrapper_timeout_after_{int(args.per_run_timeout_sec)}s"
        return result
    result["run_exit_code"] = int(wrapper_res["exit_code"])
    if int(wrapper_res["exit_code"]) != 0:
        result["note"] = f"wrapper_exit_{int(wrapper_res['exit_code'])}"
        return result

    if summary_json_path != default_summary_json_path and summary_json_path.exists():
        default_summary_json_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(summary_json_path, default_summary_json_path)

    verify_res = run_command(
        commands["verify"],
        env=build_verify_env(audit_root),
        cwd=ROOT,
    )
    verify_pass = (not verify_res["timed_out"]) and int(verify_res["exit_code"]) == 0
    result["verify_soft_live_pass"] = bool(verify_pass)

    summary_res = run_command(commands["summary"], cwd=ROOT)
    if summary_res["timed_out"] or int(summary_res["exit_code"]) != 0:
        result["note"] = f"summary_exit_{summary_res['exit_code']}"
        return result
    result["summary_generated"] = True

    history_res = run_command(commands["history"], cwd=ROOT)
    if history_res["timed_out"] or int(history_res["exit_code"]) != 0:
        result["note"] = f"history_exit_{history_res['exit_code']}"
        return result
    result["history_updated"] = True
    result["note"] = ""
    return result


def refresh_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(Path(args.refresh_tool).resolve()),
        "--state-dir",
        str(Path(args.phase6_state_dir).resolve()),
        "--shadow-state-dir",
        str(Path(args.shadow_state_dir).resolve()),
        "--observation-index",
        str(Path(args.index_json).resolve()),
        "--observation-history",
        str(Path(args.history_jsonl).resolve()),
        "--execution-ledger-tool",
        str(Path(args.execution_ledger_tool).resolve()),
        "--execution-pack-summary-tool",
        str(Path(args.execution_pack_summary_tool).resolve()),
        "--execution-ledger-jsonl",
        str(Path(args.execution_ledger_jsonl).resolve()),
        "--execution-pack-summary-json",
        str(Path(args.execution_pack_summary_json).resolve()),
        "--result-json",
        str(Path(args.refresh_result_json).resolve()),
    ]


def load_refresh_result(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def refresh_step(refresh_result: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    if not isinstance(refresh_result, dict):
        return None
    steps = refresh_result.get("steps")
    if not isinstance(steps, list):
        return None
    for step in steps:
        if isinstance(step, dict) and str(step.get("name") or "").strip() == name:
            return step
    return None


def step_executed(step: dict[str, Any] | None) -> bool:
    if not isinstance(step, dict):
        return False
    return str(step.get("status") or "").strip().upper() in {"OK", "FAILED"}


def step_exit_code(step: dict[str, Any] | None) -> int | str:
    if not isinstance(step, dict):
        return "not_run"
    raw = step.get("exit_code", "not_run")
    if isinstance(raw, int):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        return str(raw)


def execution_rebuild_note_from_steps(
    ledger_step: dict[str, Any] | None,
    pack_step: dict[str, Any] | None,
    *,
    refresh_executed: bool,
    refresh_exit_code: int | str,
) -> str:
    if not refresh_executed:
        return "no_history_updates_for_execution_rebuild"
    if refresh_exit_code == "timeout":
        return "refresh_timeout"
    if isinstance(ledger_step, dict):
        ledger_status = str(ledger_step.get("status") or "").strip().upper()
        if ledger_status == "FAILED":
            return f"execution_ledger_exit_{step_exit_code(ledger_step)}"
        if ledger_status == "NOT_RUN":
            return "execution_ledger_not_run"
    if isinstance(pack_step, dict):
        pack_status = str(pack_step.get("status") or "").strip().upper()
        if pack_status == "FAILED":
            return f"execution_pack_summary_exit_{step_exit_code(pack_step)}"
        if pack_status == "NOT_RUN":
            return "execution_pack_summary_not_run"
    return ""


def run_refresh(args: argparse.Namespace, results: list[dict[str, Any]]) -> dict[str, Any]:
    command = refresh_command(args)
    payload = {
        "refresh_executed": False,
        "refresh_exit_code": "not_run",
        "refresh_result_json_path": str(Path(args.refresh_result_json).resolve()),
        "surfaces_synced": False,
        "refresh_command": command_preview(command),
        "refresh_note": "",
        "execution_ledger_rebuild_executed": False,
        "execution_ledger_rebuild_exit_code": "not_run",
        "execution_ledger_path": str(Path(args.execution_ledger_jsonl).resolve()),
        "execution_ledger_rebuild_command": "",
        "execution_pack_summary_rebuild_executed": False,
        "execution_pack_summary_rebuild_exit_code": "not_run",
        "execution_pack_summary_path": str(Path(args.execution_pack_summary_json).resolve()),
        "execution_pack_summary_rebuild_command": "",
        "execution_artifacts_synced": False,
        "execution_rebuild_note": "",
    }
    if args.dry_run:
        payload["execution_rebuild_note"] = "no_history_updates_for_execution_rebuild"
        return payload
    if not any(result.get("history_updated") is True for result in results):
        payload["refresh_note"] = "no_history_updates_to_refresh"
        payload["execution_rebuild_note"] = "no_history_updates_for_execution_rebuild"
        return payload

    refresh_res = run_command(command, cwd=ROOT)
    payload["refresh_executed"] = True
    if refresh_res["timed_out"]:
        payload["refresh_exit_code"] = "timeout"
        payload["refresh_note"] = "refresh_timeout"
        payload["execution_rebuild_note"] = "refresh_timeout"
        return payload

    payload["refresh_exit_code"] = int(refresh_res["exit_code"])
    refresh_result = load_refresh_result(Path(args.refresh_result_json).resolve())
    failed_step = ""
    if isinstance(refresh_result, dict):
        payload["surfaces_synced"] = bool(refresh_result.get("sync_ok"))
        failed_step = str(refresh_result.get("failed_step") or "").strip()

        ledger_step = refresh_step(refresh_result, "execution_ledger")
        pack_step = refresh_step(refresh_result, "execution_pack_summary")

        if isinstance(ledger_step, dict):
            payload["execution_ledger_rebuild_executed"] = step_executed(ledger_step)
            payload["execution_ledger_rebuild_exit_code"] = step_exit_code(ledger_step)
            payload["execution_ledger_path"] = str(
                ledger_step.get("output_path") or payload["execution_ledger_path"]
            )
            payload["execution_ledger_rebuild_command"] = str(ledger_step.get("command") or "")
        if isinstance(pack_step, dict):
            payload["execution_pack_summary_rebuild_executed"] = step_executed(pack_step)
            payload["execution_pack_summary_rebuild_exit_code"] = step_exit_code(pack_step)
            payload["execution_pack_summary_path"] = str(
                pack_step.get("output_path") or payload["execution_pack_summary_path"]
            )
            payload["execution_pack_summary_rebuild_command"] = str(pack_step.get("command") or "")
        payload["execution_artifacts_synced"] = (
            isinstance(ledger_step, dict)
            and str(ledger_step.get("status") or "").strip().upper() == "OK"
            and isinstance(pack_step, dict)
            and str(pack_step.get("status") or "").strip().upper() == "OK"
        )
        payload["execution_rebuild_note"] = execution_rebuild_note_from_steps(
            ledger_step,
            pack_step,
            refresh_executed=True,
            refresh_exit_code=payload["refresh_exit_code"],
        )

    if int(refresh_res["exit_code"]) != 0:
        payload["refresh_note"] = (
            f"refresh_exit_{int(refresh_res['exit_code'])}:{failed_step}"
            if failed_step
            else f"refresh_exit_{int(refresh_res['exit_code'])}"
        )
        return payload
    if not payload["surfaces_synced"]:
        payload["refresh_note"] = f"refresh_failed_step_{failed_step}" if failed_step else "refresh_sync_incomplete"
        return payload
    return payload


def build_batch_result(
    args: argparse.Namespace,
    watchlist_path: Path,
    results: list[dict[str, Any]],
    refresh_payload: dict[str, Any],
) -> dict[str, Any]:
    completed_count = sum(
        1
        for result in results
        if result["run_executed"]
        and result["run_exit_code"] == 0
        and result["verify_soft_live_pass"] is True
        and result["summary_generated"]
        and result["history_updated"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "watchlist_path": str(watchlist_path),
        "max_items": int(args.max_items),
        "attempted_count": len(results),
        "completed_count": completed_count,
        "dry_run": bool(args.dry_run),
        "refresh_executed": bool(refresh_payload["refresh_executed"]),
        "refresh_exit_code": refresh_payload["refresh_exit_code"],
        "refresh_result_json_path": refresh_payload["refresh_result_json_path"],
        "surfaces_synced": bool(refresh_payload["surfaces_synced"]),
        "refresh_command": refresh_payload["refresh_command"],
        "refresh_note": refresh_payload["refresh_note"],
        "execution_ledger_rebuild_executed": bool(refresh_payload["execution_ledger_rebuild_executed"]),
        "execution_ledger_rebuild_exit_code": refresh_payload["execution_ledger_rebuild_exit_code"],
        "execution_ledger_path": refresh_payload["execution_ledger_path"],
        "execution_ledger_rebuild_command": refresh_payload["execution_ledger_rebuild_command"],
        "execution_pack_summary_rebuild_executed": bool(refresh_payload["execution_pack_summary_rebuild_executed"]),
        "execution_pack_summary_rebuild_exit_code": refresh_payload["execution_pack_summary_rebuild_exit_code"],
        "execution_pack_summary_path": refresh_payload["execution_pack_summary_path"],
        "execution_pack_summary_rebuild_command": refresh_payload["execution_pack_summary_rebuild_command"],
        "execution_artifacts_synced": bool(refresh_payload["execution_artifacts_synced"]),
        "execution_rebuild_note": refresh_payload["execution_rebuild_note"],
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    watchlist_path = Path(args.watchlist).resolve()
    out_dir = Path(args.out_dir).resolve()
    result_json_path = Path(args.result_json).resolve()

    watchlist = load_watchlist(watchlist_path)
    items = selected_items(watchlist, int(args.max_items))
    results = [process_item(args, watchlist_path, item, out_dir) for item in items]
    refresh_payload = run_refresh(args, results)
    payload = build_batch_result(args, watchlist_path, results, refresh_payload)
    write_json(result_json_path, payload)

    print(f"batch_result_json={result_json_path}")
    print(f"attempted_count={payload['attempted_count']}")
    print(f"completed_count={payload['completed_count']}")
    print(f"dry_run={1 if payload['dry_run'] else 0}")
    print(f"refresh_executed={1 if payload['refresh_executed'] else 0}")
    print(f"surfaces_synced={1 if payload['surfaces_synced'] else 0}")
    print(f"execution_ledger_rebuild_executed={1 if payload['execution_ledger_rebuild_executed'] else 0}")
    print(
        f"execution_pack_summary_rebuild_executed="
        f"{1 if payload['execution_pack_summary_rebuild_executed'] else 0}"
    )
    print(f"execution_artifacts_synced={1 if payload['execution_artifacts_synced'] else 0}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BatchError as exc:
        print(f"SHADOW_OBSERVATION_BATCH_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
