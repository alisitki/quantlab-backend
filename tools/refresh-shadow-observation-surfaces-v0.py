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
DEFAULT_OBSERVATION_INDEX = DEFAULT_SHADOW_STATE_DIR / "shadow_observation_index_v0.json"
DEFAULT_OBSERVATION_HISTORY = DEFAULT_SHADOW_STATE_DIR / "shadow_observation_history_v0.jsonl"
DEFAULT_RESULT_JSON = DEFAULT_SHADOW_STATE_DIR / "shadow_observation_surface_refresh_v0.json"
SCHEMA_VERSION = "shadow_observation_surface_refresh_v0"


class RefreshError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise RefreshError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh observation-aware review/watchlist surfaces v0")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR))
    parser.add_argument("--shadow-state-dir", default=str(DEFAULT_SHADOW_STATE_DIR))
    parser.add_argument("--candidate-review-tool", default=str(DEFAULT_CANDIDATE_REVIEW_TOOL))
    parser.add_argument("--watchlist-tool", default=str(DEFAULT_WATCHLIST_TOOL))
    parser.add_argument("--observation-index", default=str(DEFAULT_OBSERVATION_INDEX))
    parser.add_argument("--observation-history", default=str(DEFAULT_OBSERVATION_HISTORY))
    parser.add_argument("--recent-observation-hours", type=float, default=24.0)
    parser.add_argument("--top-n", type=int, default=3)
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    state_dir = Path(args.state_dir).resolve()
    shadow_state_dir = Path(args.shadow_state_dir).resolve()
    candidate_review_tool = Path(args.candidate_review_tool).resolve()
    watchlist_tool = Path(args.watchlist_tool).resolve()
    observation_index = Path(args.observation_index).resolve()
    observation_history = Path(args.observation_history).resolve()
    result_json = Path(args.result_json).resolve()

    candidate_review_cmd = [
        sys.executable,
        str(candidate_review_tool),
        "--state-dir",
        str(state_dir),
        "--observation-index",
        str(observation_index),
        "--observation-history",
        str(observation_history),
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

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "dry_run": bool(args.dry_run),
        "state_dir": str(state_dir),
        "shadow_state_dir": str(shadow_state_dir),
        "candidate_review_command": command_preview(candidate_review_cmd),
        "watchlist_command": command_preview(watchlist_cmd),
        "candidate_review_exit_code": "not_run",
        "watchlist_exit_code": "not_run",
        "sync_ok": False,
        "candidate_review_tsv": str(state_dir / "candidate_review.tsv"),
        "candidate_review_json": str(state_dir / "candidate_review.json"),
        "watchlist_json": str(shadow_state_dir / "shadow_watchlist_v0.json"),
        "watchlist_tsv": str(shadow_state_dir / "shadow_watchlist_v0.tsv"),
    }

    if args.dry_run:
        write_json(result_json, payload)
        print(f"refresh_result_json={result_json}")
        print("sync_ok=0")
        print("dry_run=1")
        return 0

    candidate_review_res = run_command(candidate_review_cmd, cwd=ROOT)
    payload["candidate_review_exit_code"] = int(candidate_review_res["exit_code"])
    if int(candidate_review_res["exit_code"]) != 0:
        payload["candidate_review_stdout"] = candidate_review_res["stdout"]
        payload["candidate_review_stderr"] = candidate_review_res["stderr"]
        write_json(result_json, payload)
        print(f"refresh_result_json={result_json}")
        print("sync_ok=0")
        return 0

    watchlist_res = run_command(watchlist_cmd, cwd=ROOT)
    payload["watchlist_exit_code"] = int(watchlist_res["exit_code"])
    if int(watchlist_res["exit_code"]) != 0:
        payload["watchlist_stdout"] = watchlist_res["stdout"]
        payload["watchlist_stderr"] = watchlist_res["stderr"]
        write_json(result_json, payload)
        print(f"refresh_result_json={result_json}")
        print("sync_ok=0")
        return 0

    payload["sync_ok"] = True
    write_json(result_json, payload)
    print(f"refresh_result_json={result_json}")
    print("sync_ok=1")
    print("dry_run=0")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RefreshError as exc:
        print(f"SHADOW_SURFACE_REFRESH_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
