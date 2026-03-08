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
DEFAULT_BATCH_TOOL = ROOT / "tools" / "run-shadow-observation-batch-v0.py"
DEFAULT_WATCHLIST = ROOT / "tools" / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_STRATEGY = "core/strategy/strategies/PrintHeadTailStrategy.js"
DEFAULT_AUDIT_BASE_DIR = Path("/tmp/quantlab-long-shadow-audit-v0")
DEFAULT_OUT_DIR = Path("/tmp/quantlab-long-shadow-out-v0")
DEFAULT_BATCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_long_shadow_batch_result_v0.json"
DEFAULT_LAUNCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_long_shadow_launch_v0.json"
DEFAULT_REFRESH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_derived_surface_refresh_v0.json"
DEFAULT_OPERATOR_SNAPSHOT_JSON = ROOT / "tools" / "shadow_state" / "shadow_operator_snapshot_v0.json"
DEFAULT_EXECUTION_REVIEW_QUEUE_JSON = ROOT / "tools" / "shadow_state" / "shadow_execution_review_queue_v0.json"
DEFAULT_EXECUTION_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
DEFAULT_TRADE_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_trade_ledger_v1.jsonl"
DEFAULT_BATCH_STDOUT_LOG = ROOT / "tools" / "shadow_state" / "shadow_long_shadow_batch_stdout_v0.log"
DEFAULT_BATCH_STDERR_LOG = ROOT / "tools" / "shadow_state" / "shadow_long_shadow_batch_stderr_v0.log"
PROFILE_NAME = "top1_printheadtail_long_shadow_v0"
PROFILE_MAX_ITEMS = 1
DEFAULT_PER_RUN_TIMEOUT_SEC = 90
DEFAULT_RUN_MAX_DURATION_SEC = 60
DEFAULT_HEARTBEAT_MS = 5000
SCHEMA_VERSION = "shadow_long_shadow_launch_v0"


class LongShadowLaunchError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise LongShadowLaunchError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded long-running shadow launch wrapper v0")
    parser.add_argument("--watchlist", default=str(DEFAULT_WATCHLIST))
    parser.add_argument("--strategy", default=DEFAULT_STRATEGY)
    parser.add_argument("--strategy-config-json", default="{}")
    parser.add_argument("--batch-tool", default=str(DEFAULT_BATCH_TOOL))
    parser.add_argument("--audit-base-dir", default=str(DEFAULT_AUDIT_BASE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--batch-result-json", default=str(DEFAULT_BATCH_RESULT_JSON))
    parser.add_argument("--launch-result-json", default=str(DEFAULT_LAUNCH_RESULT_JSON))
    parser.add_argument("--refresh-result-json", default=str(DEFAULT_REFRESH_RESULT_JSON))
    parser.add_argument("--operator-snapshot-json", default=str(DEFAULT_OPERATOR_SNAPSHOT_JSON))
    parser.add_argument("--execution-review-queue-json", default=str(DEFAULT_EXECUTION_REVIEW_QUEUE_JSON))
    parser.add_argument("--execution-events-jsonl", default=str(DEFAULT_EXECUTION_EVENTS_JSONL))
    parser.add_argument("--trade-ledger-jsonl", default=str(DEFAULT_TRADE_LEDGER_JSONL))
    parser.add_argument("--batch-stdout-log", default=str(DEFAULT_BATCH_STDOUT_LOG))
    parser.add_argument("--batch-stderr-log", default=str(DEFAULT_BATCH_STDERR_LOG))
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
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


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def run_command(cmd: list[str], *, cwd: Path, stdout_log: Path, stderr_log: Path) -> int:
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    stderr_log.parent.mkdir(parents=True, exist_ok=True)
    with stdout_log.open("w", encoding="utf-8") as stdout_handle, stderr_log.open("w", encoding="utf-8") as stderr_handle:
        completed = subprocess.run(
            cmd,
            cwd=str(cwd),
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
        )
    return int(completed.returncode)


def parse_positive_processed_count(value: Any) -> tuple[int | None, bool]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None, False
    return parsed, parsed > 0


def build_batch_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(Path(args.batch_tool).resolve()),
        "--watchlist",
        str(Path(args.watchlist).resolve()),
        "--max-items",
        str(PROFILE_MAX_ITEMS),
        "--strategy",
        str(args.strategy),
        "--strategy-config-json",
        str(args.strategy_config_json),
        "--audit-base-dir",
        str(Path(args.audit_base_dir).resolve()),
        "--out-dir",
        str(Path(args.out_dir).resolve()),
        "--result-json",
        str(Path(args.batch_result_json).resolve()),
        "--refresh-result-json",
        str(Path(args.refresh_result_json).resolve()),
        "--per-run-timeout-sec",
        str(int(args.per_run_timeout_sec)),
        "--run-max-duration-sec",
        str(int(args.run_max_duration_sec)),
        "--heartbeat-ms",
        str(int(args.heartbeat_ms)),
    ] + (["--dry-run"] if args.dry_run else [])


def classify_run(
    *,
    args: argparse.Namespace,
    batch_exit_code: int,
    batch_result: dict[str, Any] | None,
    refresh_result: dict[str, Any] | None,
    summary: dict[str, Any] | None,
    operator_snapshot_exists: bool,
    review_queue_exists: bool,
    execution_events_exists: bool,
    trade_ledger_exists: bool,
    events_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> tuple[str, bool, str, dict[str, Any]]:
    diagnostics: dict[str, Any] = {
        "selected_rank": None,
        "selected_pack_id": "",
        "selected_live_run_id": "",
        "summary_heartbeat_seen": "unknown",
        "summary_processed_event_count": "unknown",
        "matched_execution_event_count": 0,
        "matched_trade_count": 0,
        "required_artifacts_ok": False,
    }

    if args.dry_run:
        return "DRY_RUN_ONLY", False, "", diagnostics
    if batch_exit_code != 0:
        return "INVALID", False, f"batch_exit_{batch_exit_code}", diagnostics
    if batch_result is None:
        return "INVALID", False, "batch_result_missing", diagnostics

    attempted_count = batch_result.get("attempted_count")
    completed_count = batch_result.get("completed_count")
    if attempted_count != 1:
        return "INVALID", False, f"attempted_count_{attempted_count}", diagnostics
    if completed_count != 1:
        return "INVALID", False, f"completed_count_{completed_count}", diagnostics
    if batch_result.get("refresh_executed") is not True:
        return "INVALID", False, "refresh_not_executed", diagnostics
    if batch_result.get("refresh_exit_code") != 0:
        return "INVALID", False, f"refresh_exit_{batch_result.get('refresh_exit_code')}", diagnostics
    if batch_result.get("surfaces_synced") is not True:
        return "INVALID", False, "surfaces_not_synced", diagnostics
    if batch_result.get("execution_artifacts_synced") is not True:
        return "INVALID", False, "execution_artifacts_not_synced", diagnostics

    results = batch_result.get("results")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        return "INVALID", False, "batch_results_invalid", diagnostics
    item = results[0]
    diagnostics["selected_rank"] = item.get("rank")
    diagnostics["selected_pack_id"] = str(item.get("pack_id") or "").strip()
    if not diagnostics["selected_pack_id"]:
        return "INVALID", False, "selected_pack_id_missing", diagnostics
    if item.get("run_exit_code") != 0:
        return "INVALID", False, f"item_run_exit_{item.get('run_exit_code')}", diagnostics
    if item.get("verify_soft_live_pass") is not True:
        return "INVALID", False, "verify_soft_live_failed", diagnostics
    if item.get("summary_generated") is not True:
        return "INVALID", False, "summary_not_generated", diagnostics
    if item.get("history_updated") is not True:
        return "INVALID", False, "history_not_updated", diagnostics
    if str(item.get("note") or "").strip():
        return "INVALID", False, f"item_note_{str(item.get('note')).strip()}", diagnostics

    summary_path = Path(str(item.get("summary_json_path") or "")).resolve()
    stdout_path = Path(str(item.get("stdout_log_path") or "")).resolve()
    stderr_path = Path(str(item.get("stderr_log_path") or "")).resolve()
    audit_dir = Path(str(item.get("audit_spool_dir") or "")).resolve()
    for label, path in (
        ("summary_json", summary_path),
        ("stdout_log", stdout_path),
        ("stderr_log", stderr_path),
    ):
        if not path.exists():
            return "INVALID", False, f"missing_{label}", diagnostics
    if not audit_dir.exists():
        return "INVALID", False, "missing_audit_spool_dir", diagnostics

    if refresh_result is None:
        return "INVALID", False, "refresh_result_missing", diagnostics
    if refresh_result.get("sync_ok") is not True:
        return "INVALID", False, f"refresh_sync_{refresh_result.get('failed_step') or 'incomplete'}", diagnostics

    if summary is None:
        return "INVALID", False, "summary_missing", diagnostics
    heartbeat_seen = summary.get("heartbeat_seen")
    diagnostics["summary_heartbeat_seen"] = heartbeat_seen
    processed_event_count, processed_positive = parse_positive_processed_count(summary.get("processed_event_count"))
    diagnostics["summary_processed_event_count"] = processed_event_count if processed_event_count is not None else "unknown"
    diagnostics["selected_live_run_id"] = str(summary.get("live_run_id") or "").strip()
    if not diagnostics["selected_live_run_id"]:
        return "INVALID", False, "live_run_id_missing", diagnostics
    if heartbeat_seen is not True:
        return "INVALID", False, "summary_heartbeat_missing_or_false", diagnostics
    if not processed_positive:
        return "INVALID", False, "processed_event_count_not_positive", diagnostics
    if not operator_snapshot_exists:
        return "INVALID", False, "operator_snapshot_missing", diagnostics
    if not review_queue_exists:
        return "INVALID", False, "execution_review_queue_missing", diagnostics
    if not execution_events_exists:
        return "INVALID", False, "execution_events_missing", diagnostics
    if not trade_ledger_exists:
        return "INVALID", False, "trade_ledger_missing", diagnostics
    diagnostics["required_artifacts_ok"] = True

    selected_pack_id = diagnostics["selected_pack_id"]
    selected_live_run_id = diagnostics["selected_live_run_id"]
    matched_event_count = sum(
        1
        for row in events_rows
        if str(row.get("selected_pack_id") or "").strip() == selected_pack_id
        and str(row.get("live_run_id") or "").strip() == selected_live_run_id
    )
    matched_trade_count = sum(
        1
        for row in trade_rows
        if str(row.get("selected_pack_id") or "").strip() == selected_pack_id
        and selected_live_run_id in {
            str(row.get("open_live_run_id") or "").strip(),
            str(row.get("last_live_run_id") or "").strip(),
        }
    )
    diagnostics["matched_execution_event_count"] = matched_event_count
    diagnostics["matched_trade_count"] = matched_trade_count
    if matched_event_count > 0 or matched_trade_count > 0:
        return "VALID_WITH_EXECUTION_ACTIVITY", True, "", diagnostics
    return "VALID_NO_EXECUTION_ACTIVITY", True, "", diagnostics


def build_launch_payload(
    args: argparse.Namespace,
    *,
    batch_command: list[str],
    batch_exit_code: int,
    batch_result: dict[str, Any] | None,
    refresh_result: dict[str, Any] | None,
    summary: dict[str, Any] | None,
    launch_status: str,
    valid_run: bool,
    invalid_reason: str,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "profile_name": PROFILE_NAME,
        "entrypoint": "tools/run-shadow-observation-batch-v0.py",
        "watchlist_path": str(Path(args.watchlist).resolve()),
        "strategy": str(args.strategy),
        "strategy_config_json": str(args.strategy_config_json),
        "selection_profile": "WATCHLIST_TOP_1_ONLY",
        "max_items": PROFILE_MAX_ITEMS,
        "per_run_timeout_sec": int(args.per_run_timeout_sec),
        "run_max_duration_sec": int(args.run_max_duration_sec),
        "heartbeat_ms": int(args.heartbeat_ms),
        "dry_run": bool(args.dry_run),
        "batch_command": command_preview(batch_command),
        "batch_exit_code": batch_exit_code,
        "batch_result_json": str(Path(args.batch_result_json).resolve()),
        "batch_stdout_log_path": str(Path(args.batch_stdout_log).resolve()),
        "batch_stderr_log_path": str(Path(args.batch_stderr_log).resolve()),
        "refresh_result_json": str(Path(args.refresh_result_json).resolve()),
        "operator_snapshot_json": str(Path(args.operator_snapshot_json).resolve()),
        "execution_review_queue_json": str(Path(args.execution_review_queue_json).resolve()),
        "execution_events_jsonl": str(Path(args.execution_events_jsonl).resolve()),
        "trade_ledger_jsonl": str(Path(args.trade_ledger_jsonl).resolve()),
        "attempted_count": batch_result.get("attempted_count") if isinstance(batch_result, dict) else "unknown",
        "completed_count": batch_result.get("completed_count") if isinstance(batch_result, dict) else "unknown",
        "refresh_sync_ok": bool(refresh_result.get("sync_ok")) if isinstance(refresh_result, dict) else False,
        "launch_status": launch_status,
        "valid_run": bool(valid_run),
        "invalid_reason": invalid_reason,
        "selected_rank": diagnostics["selected_rank"],
        "selected_pack_id": diagnostics["selected_pack_id"],
        "selected_live_run_id": diagnostics["selected_live_run_id"],
        "summary_heartbeat_seen": diagnostics["summary_heartbeat_seen"],
        "summary_processed_event_count": diagnostics["summary_processed_event_count"],
        "matched_execution_event_count": diagnostics["matched_execution_event_count"],
        "matched_trade_count": diagnostics["matched_trade_count"],
        "required_artifacts_ok": diagnostics["required_artifacts_ok"],
        "summary_json_path": "",
        "stdout_log_path": "",
        "stderr_log_path": "",
        "audit_spool_dir": "",
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    launch_result_json = Path(args.launch_result_json).resolve()
    batch_result_json = Path(args.batch_result_json).resolve()
    refresh_result_json = Path(args.refresh_result_json).resolve()
    operator_snapshot_json = Path(args.operator_snapshot_json).resolve()
    review_queue_json = Path(args.execution_review_queue_json).resolve()
    execution_events_jsonl = Path(args.execution_events_jsonl).resolve()
    trade_ledger_jsonl = Path(args.trade_ledger_jsonl).resolve()
    batch_stdout_log = Path(args.batch_stdout_log).resolve()
    batch_stderr_log = Path(args.batch_stderr_log).resolve()

    batch_command = build_batch_command(args)
    batch_exit_code = run_command(batch_command, cwd=ROOT, stdout_log=batch_stdout_log, stderr_log=batch_stderr_log)

    batch_result = json.loads(batch_result_json.read_text(encoding="utf-8")) if batch_result_json.exists() else None
    refresh_result = json.loads(refresh_result_json.read_text(encoding="utf-8")) if refresh_result_json.exists() else None
    summary = None
    if isinstance(batch_result, dict):
        results = batch_result.get("results")
        if isinstance(results, list) and len(results) == 1 and isinstance(results[0], dict):
            summary_path = Path(str(results[0].get("summary_json_path") or "")).resolve()
            if summary_path.exists():
                summary = load_json(summary_path, "summary_json")

    launch_status, valid_run, invalid_reason, diagnostics = classify_run(
        args=args,
        batch_exit_code=batch_exit_code,
        batch_result=batch_result if isinstance(batch_result, dict) else None,
        refresh_result=refresh_result if isinstance(refresh_result, dict) else None,
        summary=summary,
        operator_snapshot_exists=operator_snapshot_json.exists(),
        review_queue_exists=review_queue_json.exists(),
        execution_events_exists=execution_events_jsonl.exists(),
        trade_ledger_exists=trade_ledger_jsonl.exists(),
        events_rows=load_jsonl(execution_events_jsonl),
        trade_rows=load_jsonl(trade_ledger_jsonl),
    )
    payload = build_launch_payload(
        args,
        batch_command=batch_command,
        batch_exit_code=batch_exit_code,
        batch_result=batch_result if isinstance(batch_result, dict) else None,
        refresh_result=refresh_result if isinstance(refresh_result, dict) else None,
        summary=summary,
        launch_status=launch_status,
        valid_run=valid_run,
        invalid_reason=invalid_reason,
        diagnostics=diagnostics,
    )
    if isinstance(batch_result, dict):
        results = batch_result.get("results")
        if isinstance(results, list) and len(results) == 1 and isinstance(results[0], dict):
            payload["summary_json_path"] = str(results[0].get("summary_json_path") or "")
            payload["stdout_log_path"] = str(results[0].get("stdout_log_path") or "")
            payload["stderr_log_path"] = str(results[0].get("stderr_log_path") or "")
            payload["audit_spool_dir"] = str(results[0].get("audit_spool_dir") or "")

    write_json(launch_result_json, payload)

    print(f"launch_result_json={launch_result_json}")
    print(f"launch_status={payload['launch_status']}")
    print(f"valid_run={1 if payload['valid_run'] else 0}")
    print(f"selected_pack_id={payload['selected_pack_id']}")
    print(f"matched_execution_event_count={payload['matched_execution_event_count']}")
    print(f"matched_trade_count={payload['matched_trade_count']}")
    print(f"required_artifacts_ok={1 if payload['required_artifacts_ok'] else 0}")
    if batch_exit_code != 0:
        return batch_exit_code
    return 1 if launch_status == "INVALID" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LongShadowLaunchError as exc:
        print(f"LONG_SHADOW_LAUNCH_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
