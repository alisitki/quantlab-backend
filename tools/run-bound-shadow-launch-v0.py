#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDING_ARTIFACT = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_LAUNCH_TOOL = ROOT / "tools" / "run-long-shadow-launch-v0.py"
DEFAULT_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_launch_v0.json"
DEFAULT_GENERATED_WATCHLIST_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_launch_watchlist_v0.json"
DEFAULT_CHILD_LAUNCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_launch_v0.json"
DEFAULT_CHILD_BATCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_result_v0.json"
DEFAULT_REFRESH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_derived_surface_refresh_v0.json"
DEFAULT_OPERATOR_SNAPSHOT_JSON = ROOT / "tools" / "shadow_state" / "shadow_operator_snapshot_v0.json"
DEFAULT_EXECUTION_REVIEW_QUEUE_JSON = ROOT / "tools" / "shadow_state" / "shadow_execution_review_queue_v0.json"
DEFAULT_EXECUTION_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
DEFAULT_TRADE_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_trade_ledger_v1.jsonl"
DEFAULT_BATCH_STDOUT_LOG = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_stdout_v0.log"
DEFAULT_BATCH_STDERR_LOG = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_stderr_v0.log"
DEFAULT_AUDIT_BASE_DIR = Path("/tmp/quantlab-bound-shadow-audit-v0")
DEFAULT_OUT_DIR = Path("/tmp/quantlab-bound-shadow-out-v0")
DEFAULT_PER_RUN_TIMEOUT_SEC = 90
DEFAULT_RUN_MAX_DURATION_SEC = 60
DEFAULT_HEARTBEAT_MS = 5000
SCHEMA_VERSION = "shadow_bound_launch_v0"
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"


class BoundShadowLaunchError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise BoundShadowLaunchError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch one bound runtime strategy in bounded shadow mode.")
    parser.add_argument("--binding-artifact", default=str(DEFAULT_BINDING_ARTIFACT))
    parser.add_argument("--launch-tool", default=str(DEFAULT_LAUNCH_TOOL))
    parser.add_argument("--pack-id", default="")
    parser.add_argument("--strategy-id", default="")
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--generated-watchlist-json", default=str(DEFAULT_GENERATED_WATCHLIST_JSON))
    parser.add_argument("--child-launch-result-json", default=str(DEFAULT_CHILD_LAUNCH_RESULT_JSON))
    parser.add_argument("--child-batch-result-json", default=str(DEFAULT_CHILD_BATCH_RESULT_JSON))
    parser.add_argument("--refresh-result-json", default=str(DEFAULT_REFRESH_RESULT_JSON))
    parser.add_argument("--operator-snapshot-json", default=str(DEFAULT_OPERATOR_SNAPSHOT_JSON))
    parser.add_argument("--execution-review-queue-json", default=str(DEFAULT_EXECUTION_REVIEW_QUEUE_JSON))
    parser.add_argument("--execution-events-jsonl", default=str(DEFAULT_EXECUTION_EVENTS_JSONL))
    parser.add_argument("--trade-ledger-jsonl", default=str(DEFAULT_TRADE_LEDGER_JSONL))
    parser.add_argument("--batch-stdout-log", default=str(DEFAULT_BATCH_STDOUT_LOG))
    parser.add_argument("--batch-stderr-log", default=str(DEFAULT_BATCH_STDERR_LOG))
    parser.add_argument("--audit-base-dir", default=str(DEFAULT_AUDIT_BASE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    args.pack_id = str(args.pack_id or "").strip()
    args.strategy_id = str(args.strategy_id or "").strip()
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    return args


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def load_binding_artifact(path: Path) -> dict[str, Any]:
    obj = load_json(path, "binding_artifact")
    if str(obj.get("schema_version") or "").strip() != BINDING_SCHEMA_VERSION:
        fail(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"binding_artifact_items_invalid:{path}")
    return obj


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def selection_mode(args: argparse.Namespace) -> str:
    if args.strategy_id and args.pack_id:
        return "STRATEGY_ID_AND_PACK_ID"
    if args.strategy_id:
        return "STRATEGY_ID"
    if args.pack_id:
        return "PACK_ID"
    return "FIRST_BOUND"


def row_pack_id(item: dict[str, Any]) -> str:
    return str(item.get("pack_id") or "").strip()


def row_strategy_id(item: dict[str, Any]) -> str:
    return str(item.get("strategy_id") or "").strip()


def select_bound_item(items: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    by_strategy = None
    by_pack = None
    if args.strategy_id:
        by_strategy = next((item for item in items if row_strategy_id(item) == args.strategy_id), None)
        if by_strategy is None:
            fail(f"strategy_id_not_found:{args.strategy_id}")
    if args.pack_id:
        by_pack = next((item for item in items if row_pack_id(item) == args.pack_id), None)
        if by_pack is None:
            fail(f"pack_id_not_found:{args.pack_id}")

    if by_strategy is not None and by_pack is not None:
        if row_strategy_id(by_strategy) != row_strategy_id(by_pack):
            fail(f"selector_conflict:strategy_id={args.strategy_id}:pack_id={args.pack_id}")
        selected = by_strategy
    elif by_strategy is not None:
        selected = by_strategy
    elif by_pack is not None:
        selected = by_pack
    else:
        selected = next(
            (item for item in items if str(item.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE),
            None,
        )
        if selected is None:
            fail("no_bound_shadow_runnable_rows")

    if str(selected.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
        fail(f"selected_row_not_bound:{str(selected.get('runtime_binding_status') or '').strip()}")
    return selected


def build_watchlist_payload(item: dict[str, Any]) -> dict[str, Any]:
    exchange = str(item.get("exchange") or "").strip()
    stream = str(item.get("stream") or "").strip()
    symbols = [str(value or "").strip().upper() for value in list(item.get("symbols") or []) if str(value or "").strip()]
    strategy_config = item.get("runtime_strategy_config") if isinstance(item.get("runtime_strategy_config"), dict) else {}
    decision_tier = str(strategy_config.get("source_decision_tier") or "").strip()
    return {
        "schema_version": "shadow_bound_launch_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "selected_count": 1,
        "items": [
            {
                "rank": int(item.get("rank") or 0),
                "pack_id": row_pack_id(item),
                "pack_path": "",
                "exchange": exchange,
                "symbols": symbols,
                "decision_tier": decision_tier,
                "selection_slot": f"{exchange}/{stream}" if exchange and stream else "",
            }
        ],
    }


def build_launch_command(args: argparse.Namespace, selected: dict[str, Any]) -> list[str]:
    runtime_strategy_config = selected.get("runtime_strategy_config")
    if not isinstance(runtime_strategy_config, dict):
        fail("selected_runtime_strategy_config_invalid")
    runtime_strategy_file = str(selected.get("runtime_strategy_file") or "").strip()
    if not runtime_strategy_file:
        fail("selected_runtime_strategy_file_missing")
    return [
        sys.executable,
        str(Path(args.launch_tool).resolve()),
        "--watchlist",
        str(Path(args.generated_watchlist_json).resolve()),
        "--strategy",
        runtime_strategy_file,
        "--strategy-config-json",
        json.dumps(runtime_strategy_config, sort_keys=True),
        "--batch-result-json",
        str(Path(args.child_batch_result_json).resolve()),
        "--launch-result-json",
        str(Path(args.child_launch_result_json).resolve()),
        "--refresh-result-json",
        str(Path(args.refresh_result_json).resolve()),
        "--operator-snapshot-json",
        str(Path(args.operator_snapshot_json).resolve()),
        "--execution-review-queue-json",
        str(Path(args.execution_review_queue_json).resolve()),
        "--execution-events-jsonl",
        str(Path(args.execution_events_jsonl).resolve()),
        "--trade-ledger-jsonl",
        str(Path(args.trade_ledger_jsonl).resolve()),
        "--batch-stdout-log",
        str(Path(args.batch_stdout_log).resolve()),
        "--batch-stderr-log",
        str(Path(args.batch_stderr_log).resolve()),
        "--audit-base-dir",
        str(Path(args.audit_base_dir).resolve()),
        "--out-dir",
        str(Path(args.out_dir).resolve()),
        "--per-run-timeout-sec",
        str(int(args.per_run_timeout_sec)),
        "--run-max-duration-sec",
        str(int(args.run_max_duration_sec)),
        "--heartbeat-ms",
        str(int(args.heartbeat_ms)),
    ] + (["--dry-run"] if args.dry_run else [])


def run_command(cmd: list[str], *, cwd: Path) -> int:
    completed = subprocess.run(cmd, cwd=str(cwd), text=True)
    return int(completed.returncode)


def build_payload(
    args: argparse.Namespace,
    *,
    selected: dict[str, Any] | None,
    selection_mode_value: str,
    launch_command: list[str],
    launch_exit_code: int | str,
    child_launch_result: dict[str, Any] | None,
    invalid_reason: str,
) -> dict[str, Any]:
    runtime_strategy_config = (
        selected.get("runtime_strategy_config")
        if isinstance(selected, dict) and isinstance(selected.get("runtime_strategy_config"), dict)
        else None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "binding_artifact_path": str(Path(args.binding_artifact).resolve()),
        "launch_tool": str(Path(args.launch_tool).resolve()),
        "selection_mode": selection_mode_value,
        "selected_rank": selected.get("rank") if isinstance(selected, dict) else None,
        "selected_pack_id": row_pack_id(selected or {}) if isinstance(selected, dict) else "",
        "selected_strategy_id": row_strategy_id(selected or {}) if isinstance(selected, dict) else "",
        "selected_family_id": str((selected or {}).get("family_id") or "").strip() if isinstance(selected, dict) else "",
        "runtime_binding_status": str((selected or {}).get("runtime_binding_status") or "").strip() if isinstance(selected, dict) else "",
        "runtime_strategy_file": str((selected or {}).get("runtime_strategy_file") or "").strip() if isinstance(selected, dict) else "",
        "runtime_strategy_config": runtime_strategy_config,
        "generated_watchlist_json": str(Path(args.generated_watchlist_json).resolve()),
        "launch_command": command_preview(launch_command),
        "launch_exit_code": launch_exit_code,
        "child_launch_result_json": str(Path(args.child_launch_result_json).resolve()),
        "child_batch_result_json": str(Path(args.child_batch_result_json).resolve()),
        "refresh_result_json": str(Path(args.refresh_result_json).resolve()),
        "operator_snapshot_json": str(Path(args.operator_snapshot_json).resolve()),
        "execution_review_queue_json": str(Path(args.execution_review_queue_json).resolve()),
        "execution_events_jsonl": str(Path(args.execution_events_jsonl).resolve()),
        "trade_ledger_jsonl": str(Path(args.trade_ledger_jsonl).resolve()),
        "launch_status": str((child_launch_result or {}).get("launch_status") or "INVALID"),
        "valid_run": bool((child_launch_result or {}).get("valid_run")),
        "invalid_reason": str((child_launch_result or {}).get("invalid_reason") or invalid_reason),
        "required_artifacts_ok": bool((child_launch_result or {}).get("required_artifacts_ok")),
        "selected_live_run_id": str((child_launch_result or {}).get("selected_live_run_id") or ""),
        "matched_execution_event_count": int((child_launch_result or {}).get("matched_execution_event_count") or 0),
        "matched_trade_count": int((child_launch_result or {}).get("matched_trade_count") or 0),
        "summary_json_path": str((child_launch_result or {}).get("summary_json_path") or ""),
        "stdout_log_path": str((child_launch_result or {}).get("stdout_log_path") or ""),
        "stderr_log_path": str((child_launch_result or {}).get("stderr_log_path") or ""),
        "audit_spool_dir": str((child_launch_result or {}).get("audit_spool_dir") or ""),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    result_json = Path(args.result_json).resolve()
    selected: dict[str, Any] | None = None
    selection_mode_value = selection_mode(args)
    launch_command: list[str] = []
    launch_exit_code: int | str = "not_run"
    child_launch_result: dict[str, Any] | None = None
    invalid_reason = ""
    try:
        binding_artifact = load_binding_artifact(Path(args.binding_artifact).resolve())
        items = list(binding_artifact.get("items") or [])
        selected = select_bound_item(items, args)
        watchlist_payload = build_watchlist_payload(selected)
        write_json(Path(args.generated_watchlist_json).resolve(), watchlist_payload)
        launch_command = build_launch_command(args, selected)
        launch_exit_code = run_command(launch_command, cwd=ROOT)
        child_launch_path = Path(args.child_launch_result_json).resolve()
        if child_launch_path.exists():
            child_launch_result = load_json(child_launch_path, "child_launch_result_json")
        if launch_exit_code != 0 and not child_launch_result:
            invalid_reason = f"child_launch_exit_{launch_exit_code}"
    except BoundShadowLaunchError as exc:
        invalid_reason = str(exc)

    payload = build_payload(
        args,
        selected=selected,
        selection_mode_value=selection_mode_value,
        launch_command=launch_command,
        launch_exit_code=launch_exit_code,
        child_launch_result=child_launch_result,
        invalid_reason=invalid_reason,
    )
    write_json(result_json, payload)

    print(f"bound_launch_result_json={result_json}")
    print(f"launch_status={payload['launch_status']}")
    print(f"valid_run={1 if payload['valid_run'] else 0}")
    print(f"selected_pack_id={payload['selected_pack_id']}")
    print(f"selected_strategy_id={payload['selected_strategy_id']}")
    print(f"required_artifacts_ok={1 if payload['required_artifacts_ok'] else 0}")
    if payload["launch_status"] == "INVALID":
        return 1
    if isinstance(launch_exit_code, int) and launch_exit_code != 0:
        return launch_exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
