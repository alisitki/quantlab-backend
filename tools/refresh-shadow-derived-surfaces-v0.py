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
DEFAULT_STATE_DIR = ROOT / "tools" / "phase6_state"
DEFAULT_SHADOW_STATE_DIR = ROOT / "tools" / "shadow_state"
DEFAULT_CANDIDATE_REVIEW_TOOL = ROOT / "tools" / "phase6_candidate_review_v0.py"
DEFAULT_WATCHLIST_TOOL = ROOT / "tools" / "shadow_candidate_bridge_v0.py"
DEFAULT_EXECUTION_LEDGER_TOOL = ROOT / "tools" / "shadow_execution_ledger_v0.py"
DEFAULT_EXECUTION_EVENTS_TOOL = ROOT / "tools" / "shadow_execution_events_v1.py"
DEFAULT_TRADE_LEDGER_TOOL = ROOT / "tools" / "shadow_trade_ledger_v1.py"
DEFAULT_EXECUTION_PACK_SUMMARY_TOOL = ROOT / "tools" / "shadow_execution_pack_summary_v0.py"
DEFAULT_EXECUTION_ROLLUP_TOOL = ROOT / "tools" / "shadow_execution_rollup_snapshot_v0.py"
DEFAULT_EXECUTION_OUTCOME_REVIEW_TOOL = ROOT / "tools" / "shadow_execution_outcome_review_v0.py"
DEFAULT_OPERATOR_SNAPSHOT_TOOL = ROOT / "tools" / "shadow_operator_snapshot_v0.py"
DEFAULT_EXECUTION_REVIEW_QUEUE_TOOL = ROOT / "tools" / "shadow_execution_review_queue_v0.py"
DEFAULT_OBSERVATION_INDEX = DEFAULT_SHADOW_STATE_DIR / "shadow_observation_index_v0.json"
DEFAULT_OBSERVATION_HISTORY = DEFAULT_SHADOW_STATE_DIR / "shadow_observation_history_v0.jsonl"
DEFAULT_RESULT_JSON = DEFAULT_SHADOW_STATE_DIR / "shadow_derived_surface_refresh_v0.json"
SCHEMA_VERSION = "shadow_derived_surface_refresh_v0"


class RefreshError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise RefreshError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh shadow-facing derived surfaces v0")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR))
    parser.add_argument("--shadow-state-dir", default=str(DEFAULT_SHADOW_STATE_DIR))
    parser.add_argument("--candidate-review-tool", default=str(DEFAULT_CANDIDATE_REVIEW_TOOL))
    parser.add_argument("--watchlist-tool", default=str(DEFAULT_WATCHLIST_TOOL))
    parser.add_argument("--execution-ledger-tool", default=str(DEFAULT_EXECUTION_LEDGER_TOOL))
    parser.add_argument("--execution-events-tool", default=str(DEFAULT_EXECUTION_EVENTS_TOOL))
    parser.add_argument("--trade-ledger-tool", default=str(DEFAULT_TRADE_LEDGER_TOOL))
    parser.add_argument("--execution-pack-summary-tool", default=str(DEFAULT_EXECUTION_PACK_SUMMARY_TOOL))
    parser.add_argument("--execution-rollup-tool", default=str(DEFAULT_EXECUTION_ROLLUP_TOOL))
    parser.add_argument("--execution-outcome-review-tool", default=str(DEFAULT_EXECUTION_OUTCOME_REVIEW_TOOL))
    parser.add_argument("--operator-snapshot-tool", default=str(DEFAULT_OPERATOR_SNAPSHOT_TOOL))
    parser.add_argument("--execution-review-queue-tool", default=str(DEFAULT_EXECUTION_REVIEW_QUEUE_TOOL))
    parser.add_argument("--observation-index", default=str(DEFAULT_OBSERVATION_INDEX))
    parser.add_argument("--observation-history", default=str(DEFAULT_OBSERVATION_HISTORY))
    parser.add_argument("--execution-ledger-jsonl", default="")
    parser.add_argument("--execution-pack-summary-json", default="")
    parser.add_argument("--recent-observation-hours", type=float, default=24.0)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--skip-candidate-review", action="store_true")
    parser.add_argument("--result-json", default=str(DEFAULT_RESULT_JSON))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.top_n <= 0:
        fail(f"invalid_top_n:{args.top_n}")
    if args.recent_observation_hours <= 0:
        fail(f"invalid_recent_observation_hours:{args.recent_observation_hours}")
    return args


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def run_command(cmd: list[str], *, cwd: Path) -> dict[str, Any]:
    completed = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return {
        "exit_code": int(completed.returncode),
        "stdout": str(completed.stdout or ""),
        "stderr": str(completed.stderr or ""),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def stderr_tail(text: str) -> str:
    return "\n".join(str(text or "").splitlines()[-20:])


def build_step(name: str, cmd: list[str], output_path: Path) -> dict[str, Any]:
    return {
        "name": name,
        "command": command_preview(cmd),
        "output_path": str(output_path),
        "exit_code": "not_run",
        "status": "NOT_RUN",
        "note": "",
    }


def mark_skipped(step: dict[str, Any], *, note: str) -> None:
    step["exit_code"] = "skipped"
    step["status"] = "SKIPPED"
    step["note"] = note


def mark_not_run(step: dict[str, Any], *, note: str) -> None:
    step["exit_code"] = "not_run"
    step["status"] = "NOT_RUN"
    step["note"] = note


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    state_dir = Path(args.state_dir).resolve()
    shadow_state_dir = Path(args.shadow_state_dir).resolve()
    candidate_review_tool = Path(args.candidate_review_tool).resolve()
    watchlist_tool = Path(args.watchlist_tool).resolve()
    execution_ledger_tool = Path(args.execution_ledger_tool).resolve()
    execution_events_tool = Path(args.execution_events_tool).resolve()
    trade_ledger_tool = Path(args.trade_ledger_tool).resolve()
    execution_pack_summary_tool = Path(args.execution_pack_summary_tool).resolve()
    execution_rollup_tool = Path(args.execution_rollup_tool).resolve()
    execution_outcome_review_tool = Path(args.execution_outcome_review_tool).resolve()
    operator_snapshot_tool = Path(args.operator_snapshot_tool).resolve()
    execution_review_queue_tool = Path(args.execution_review_queue_tool).resolve()
    observation_index = Path(args.observation_index).resolve()
    observation_history = Path(args.observation_history).resolve()
    result_json = Path(args.result_json).resolve()

    candidate_review_json = state_dir / "candidate_review.json"
    watchlist_json = shadow_state_dir / "shadow_watchlist_v0.json"
    execution_ledger_jsonl = (
        Path(args.execution_ledger_jsonl).resolve()
        if str(args.execution_ledger_jsonl or "").strip()
        else shadow_state_dir / "shadow_execution_ledger_v0.jsonl"
    )
    execution_events_jsonl = shadow_state_dir / "shadow_execution_events_v1.jsonl"
    trade_ledger_jsonl = shadow_state_dir / "shadow_trade_ledger_v1.jsonl"
    execution_pack_summary_json = (
        Path(args.execution_pack_summary_json).resolve()
        if str(args.execution_pack_summary_json or "").strip()
        else shadow_state_dir / "shadow_execution_pack_summary_v0.json"
    )
    execution_rollup_json = shadow_state_dir / "shadow_execution_rollup_snapshot_v0.json"
    execution_outcome_review_json = shadow_state_dir / "shadow_execution_outcome_review_v0.json"
    operator_snapshot_json = shadow_state_dir / "shadow_operator_snapshot_v0.json"
    execution_review_queue_json = shadow_state_dir / "shadow_execution_review_queue_v0.json"

    candidate_review_cmd = [
        sys.executable,
        str(candidate_review_tool),
        "--state-dir",
        str(state_dir),
        "--observation-index",
        str(observation_index),
        "--observation-history",
        str(observation_history),
        "--execution-pack-summary",
        str(execution_pack_summary_json),
        "--recent-observation-hours",
        str(float(args.recent_observation_hours)),
    ]
    watchlist_cmd = [
        sys.executable,
        str(watchlist_tool),
        "--state-dir",
        str(state_dir),
        "--out-dir",
        str(shadow_state_dir),
        "--top-n",
        str(int(args.top_n)),
    ]
    execution_ledger_cmd = [
        sys.executable,
        str(execution_ledger_tool),
        "--history-jsonl",
        str(observation_history),
        "--out-jsonl",
        str(execution_ledger_jsonl),
    ]
    execution_pack_summary_cmd = [
        sys.executable,
        str(execution_pack_summary_tool),
        "--ledger-jsonl",
        str(execution_ledger_jsonl),
        "--out-json",
        str(execution_pack_summary_json),
    ]
    execution_events_cmd = [
        sys.executable,
        str(execution_events_tool),
        "--history-jsonl",
        str(observation_history),
        "--out-jsonl",
        str(execution_events_jsonl),
    ]
    trade_ledger_cmd = [
        sys.executable,
        str(trade_ledger_tool),
        "--execution-ledger-jsonl",
        str(execution_ledger_jsonl),
        "--execution-events-jsonl",
        str(execution_events_jsonl),
        "--out-jsonl",
        str(trade_ledger_jsonl),
    ]
    execution_rollup_cmd = [
        sys.executable,
        str(execution_rollup_tool),
        "--pack-summary",
        str(execution_pack_summary_json),
        "--out-json",
        str(execution_rollup_json),
    ]
    execution_outcome_review_cmd = [
        sys.executable,
        str(execution_outcome_review_tool),
        "--rollup-snapshot",
        str(execution_rollup_json),
        "--out-json",
        str(execution_outcome_review_json),
    ]
    operator_snapshot_cmd = [
        sys.executable,
        str(operator_snapshot_tool),
        "--watchlist",
        str(watchlist_json),
        "--outcome-review",
        str(execution_outcome_review_json),
        "--out-json",
        str(operator_snapshot_json),
    ]
    execution_review_queue_cmd = [
        sys.executable,
        str(execution_review_queue_tool),
        "--operator-snapshot",
        str(operator_snapshot_json),
        "--out-json",
        str(execution_review_queue_json),
    ]

    step_specs = [
        ("candidate_review", candidate_review_cmd, candidate_review_json),
        ("watchlist", watchlist_cmd, watchlist_json),
        ("execution_ledger", execution_ledger_cmd, execution_ledger_jsonl),
        ("execution_events", execution_events_cmd, execution_events_jsonl),
        ("trade_ledger", trade_ledger_cmd, trade_ledger_jsonl),
        ("execution_pack_summary", execution_pack_summary_cmd, execution_pack_summary_json),
        ("execution_rollup_snapshot", execution_rollup_cmd, execution_rollup_json),
        ("execution_outcome_review", execution_outcome_review_cmd, execution_outcome_review_json),
        ("operator_snapshot", operator_snapshot_cmd, operator_snapshot_json),
        ("execution_review_queue", execution_review_queue_cmd, execution_review_queue_json),
    ]
    steps = [build_step(name, cmd, output_path) for name, cmd, output_path in step_specs]

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "dry_run": bool(args.dry_run),
        "state_dir": str(state_dir),
        "shadow_state_dir": str(shadow_state_dir),
        "observation_index": str(observation_index),
        "observation_history": str(observation_history),
        "skip_candidate_review": bool(args.skip_candidate_review),
        "sync_ok": False,
        "failed_step": "",
        "steps": steps,
    }

    if bool(args.skip_candidate_review):
        mark_skipped(steps[0], note="skip_candidate_review")

    if args.dry_run:
        for step in steps:
            if step["status"] == "SKIPPED":
                continue
            mark_not_run(step, note="dry_run")
        write_json(result_json, payload)
        print(f"refresh_result_json={result_json}")
        print("sync_ok=0")
        print("dry_run=1")
        print(f"failed_step={payload['failed_step']}")
        return 0

    for step, (_, cmd, _) in zip(steps, step_specs):
        if step["status"] == "SKIPPED":
            continue
        result = run_command(cmd, cwd=ROOT)
        step["exit_code"] = int(result["exit_code"])
        if int(result["exit_code"]) != 0:
            step["status"] = "FAILED"
            step["note"] = stderr_tail(result["stderr"])
            payload["failed_step"] = str(step["name"])
            write_json(result_json, payload)
            print(f"refresh_result_json={result_json}")
            print("sync_ok=0")
            print("dry_run=0")
            print(f"failed_step={payload['failed_step']}")
            return 2
        output_path = Path(step["output_path"])
        if not output_path.exists():
            step["status"] = "FAILED"
            step["note"] = f"missing_output:{output_path}"
            payload["failed_step"] = str(step["name"])
            write_json(result_json, payload)
            print(f"refresh_result_json={result_json}")
            print("sync_ok=0")
            print("dry_run=0")
            print(f"failed_step={payload['failed_step']}")
            return 2
        step["status"] = "OK"

    payload["sync_ok"] = True
    write_json(result_json, payload)
    print(f"refresh_result_json={result_json}")
    print("sync_ok=1")
    print("dry_run=0")
    print(f"failed_step={payload['failed_step']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RefreshError as exc:
        print(f"SHADOW_DERIVED_SURFACE_REFRESH_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
