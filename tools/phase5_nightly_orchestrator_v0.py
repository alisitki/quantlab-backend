#!/usr/bin/env python3
"""Nightly Phase-5/6 orchestrator: enqueue, consume, classify, refresh."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

try:
    from phase5_big_hunt_scheduler_v1 import is_in_active_window, parse_now_utc
except ImportError:  # pragma: no cover
    from tools.phase5_big_hunt_scheduler_v1 import is_in_active_window, parse_now_utc


DEFAULT_STATE_DIR = "tools/phase5_state"
DEFAULT_EXCHANGES = "binance,bybit,okx"
DEFAULT_STREAMS = "trade,bbo"
DEFAULT_INVENTORY_STATE_JSON = "/tmp/compacted__state.json"
DEFAULT_INVENTORY_BUCKET = "quantlab-compact"
DEFAULT_INVENTORY_KEY = "compacted/_state.json"
DEFAULT_INVENTORY_S3_TOOL = "/tmp/s3_compact_tool.py"
DEFAULT_LANE_POLICY = "tools/phase5_state/lane_policy_v0.json"
DEFAULT_PHASE6_STATE_DIR = "tools/phase6_state"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 nightly orchestrator v0")
    p.add_argument("--exchanges", default=DEFAULT_EXCHANGES)
    p.add_argument("--streams", default=DEFAULT_STREAMS)
    p.add_argument("--window-days", type=int, default=1)
    p.add_argument("--all-dates", action="store_true", default=True)
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--per-run-timeout-min", type=int, default=12)
    p.add_argument("--session-wall-budget-min", type=float, default=360.0)
    p.add_argument("--max-jobs", type=int, default=20)
    p.add_argument("--stop-after-failures", type=int, default=1)
    p.add_argument("--sleep-between-jobs-sec", type=float, default=20.0)
    p.add_argument("--sleep-jitter-sec", type=float, default=10.0)
    p.add_argument("--failure-backoff-sec", type=float, default=120.0)
    p.add_argument("--stale-running-min", type=float, default=180.0)
    p.add_argument("--active-window-start", default="20:00")
    p.add_argument("--active-window-end", default="07:00")
    p.add_argument("--active-window-tz", default="Europe/Istanbul")
    p.add_argument("--ignore-active-window", action="store_true")
    p.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    p.add_argument("--inventory-state-json", default=DEFAULT_INVENTORY_STATE_JSON)
    p.add_argument("--inventory-bucket", default=DEFAULT_INVENTORY_BUCKET)
    p.add_argument("--inventory-key", default=DEFAULT_INVENTORY_KEY)
    p.add_argument("--inventory-s3-tool", default=DEFAULT_INVENTORY_S3_TOOL)
    p.add_argument("--lane-policy", default=DEFAULT_LANE_POLICY)
    p.add_argument(
        "--inventory-require-quality-pass",
        dest="inventory_require_quality_pass",
        action="store_true",
        default=True,
    )
    p.add_argument(
        "--inventory-no-quality-pass",
        dest="inventory_require_quality_pass",
        action="store_false",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--now-utc", default="", help="test-only now override")
    return p.parse_args(argv)


def utc_now_iso(dt: Optional[datetime] = None) -> str:
    base = dt or datetime.now(timezone.utc)
    return base.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp(dt: Optional[datetime] = None) -> str:
    base = dt or datetime.now(timezone.utc)
    return base.strftime("%Y%m%d_%H%M%S")


def parse_kv_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in str(text).splitlines():
        line = raw.strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def run_command(cmd: List[str], cwd: Path) -> Dict[str, Any]:
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return {
        "cmd": list(cmd),
        "exit_code": int(proc.returncode),
        "stdout": str(proc.stdout),
        "stderr": str(proc.stderr),
        "kv": parse_kv_lines(proc.stdout),
    }


def candidate_snapshot(repo: Path) -> Dict[str, Any]:
    candidate_index = repo / "tools" / "phase6_state" / "candidate_index.json"
    candidate_review = repo / "tools" / "phase6_state" / "candidate_review.json"
    count_total = 0
    strong_count = 0
    review_count = 0
    top_pack_id = ""
    top_score = ""
    if candidate_index.exists():
        obj = json.loads(candidate_index.read_text(encoding="utf-8"))
        count_total = int(obj.get("record_count", 0) or 0)
        strong_count = int((obj.get("by_tier") or {}).get("PROMOTE_STRONG", 0) or 0)
    if candidate_review.exists():
        obj = json.loads(candidate_review.read_text(encoding="utf-8"))
        review_count = int(obj.get("record_count", 0) or 0)
        top = (obj.get("top_candidates") or [{}])[0]
        if isinstance(top, dict):
            top_pack_id = str(top.get("pack_id", "")).strip()
            top_score = str(top.get("score", "")).strip()
    return {
        "candidate_count_total": count_total,
        "strong_count": strong_count,
        "review_count": review_count,
        "top_pack_id": top_pack_id,
        "top_score": top_score,
    }


def planner_command(args: argparse.Namespace) -> List[str]:
    cmd = [
        "python3",
        "tools/phase5_big_hunt_plan_v2.py",
        "--exchanges",
        str(args.exchanges),
        "--streams",
        str(args.streams),
        "--window-days",
        str(int(args.window_days)),
        "--max-symbols",
        str(int(args.max_symbols)),
        "--per-run-timeout-min",
        str(int(args.per_run_timeout_min)),
        "--state-dir",
        str(args.state_dir),
        "--inventory-state-json",
        str(args.inventory_state_json),
        "--inventory-bucket",
        str(args.inventory_bucket),
        "--inventory-key",
        str(args.inventory_key),
        "--inventory-s3-tool",
        str(args.inventory_s3_tool),
    ]
    if bool(args.all_dates):
        cmd.append("--all-dates")
    if bool(args.inventory_require_quality_pass):
        cmd.append("--require-quality-pass")
    else:
        cmd.append("--allow-bad-quality")
    if bool(args.dry_run):
        cmd.append("--dry-run")
    return cmd


def scheduler_command(args: argparse.Namespace) -> List[str]:
    cmd = [
        "python3",
        "tools/phase5_big_hunt_scheduler_v1.py",
        "--max-jobs",
        str(int(args.max_jobs)),
        "--session-wall-budget-min",
        str(float(args.session_wall_budget_min)),
        "--sleep-between-jobs-sec",
        str(float(args.sleep_between_jobs_sec)),
        "--sleep-jitter-sec",
        str(float(args.sleep_jitter_sec)),
        "--failure-backoff-sec",
        str(float(args.failure_backoff_sec)),
        "--stop-after-failures",
        str(int(args.stop_after_failures)),
        "--stale-running-min",
        str(float(args.stale_running_min)),
        "--active-window-start",
        str(args.active_window_start),
        "--active-window-end",
        str(args.active_window_end),
        "--active-window-tz",
        str(args.active_window_tz),
        "--state-dir",
        str(args.state_dir),
        "--inventory-state-json",
        str(args.inventory_state_json),
        "--inventory-bucket",
        str(args.inventory_bucket),
        "--inventory-key",
        str(args.inventory_key),
        "--inventory-s3-tool",
        str(args.inventory_s3_tool),
    ]
    if bool(args.inventory_require_quality_pass):
        cmd.append("--inventory-require-quality-pass")
    else:
        cmd.append("--inventory-no-quality-pass")
    if bool(args.ignore_active_window):
        cmd.append("--ignore-active-window")
    return cmd


def export_command() -> List[str]:
    return ["python3", "tools/phase6_candidate_export_v0.py"]


def review_command() -> List[str]:
    return ["python3", "tools/phase6_candidate_review_v0.py"]


def v2_command(pack_path: str) -> List[str]:
    return ["python3", "tools/phase6_promotion_guards_v2.py", "--pack", str(pack_path)]


def new_pack_paths_from_batch_report(path: str) -> List[str]:
    raw = str(path or "").strip()
    if not raw:
        return []
    report_path = Path(raw)
    if not report_path.exists() or report_path.is_dir():
        return []
    obj = json.loads(report_path.read_text(encoding="utf-8"))
    processed = list(obj.get("processed") or [])
    pack_paths = []
    for row in processed:
        if not isinstance(row, dict):
            continue
        if str(row.get("final_status", "")).strip() != "DONE":
            continue
        archive_dir = str(row.get("archive_dir", "")).strip()
        if archive_dir:
            pack_paths.append(archive_dir)
    return sorted(set(pack_paths))


def phase6_v2_auto_apply(
    pack_paths: Sequence[str],
    *,
    repo: Path,
    runner: Callable[[List[str], Path], Dict[str, Any]],
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    invoked_count = 0
    record_appended_count = 0
    failed_count = 0
    for pack_path in sorted({str(p).strip() for p in pack_paths if str(p).strip()}):
        invoked_count += 1
        result = runner(v2_command(pack_path), repo)
        kv = dict(result.get("kv") or {})
        exit_code = int(result.get("exit_code", 0) or 0)
        record_appended = str(kv.get("record_appended", "")).strip().lower() == "true"
        if record_appended:
            record_appended_count += 1
        if exit_code != 0:
            failed_count += 1
        results.append(
            {
                "pack_path": pack_path,
                "exit_code": exit_code,
                "decision": str(kv.get("decision", "")).strip(),
                "record_appended": "true" if record_appended else "false",
                "stderr_tail": "\n".join(str(result.get("stderr", "")).splitlines()[-20:]),
            }
        )
    return {
        "pack_count": len(sorted({str(p).strip() for p in pack_paths if str(p).strip()})),
        "invoked_count": invoked_count,
        "record_appended_count": record_appended_count,
        "failed_count": failed_count,
        "packs": results,
    }


def planner_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    kv = dict(result.get("kv") or {})
    return {
        "added_count": int(kv.get("added_count", kv.get("would_add_count", "0")) or 0),
        "skipped_existing_count": int(kv.get("skipped_existing_count", "0") or 0),
        "skipped_done_count": int(kv.get("skipped_done_count", "0") or 0),
        "windows_total": int(kv.get("windows_total", kv.get("windows_considered", "0")) or 0),
    }


def scheduler_summary(result: Optional[Dict[str, Any]], *, dry_run: bool, cmd: List[str]) -> Dict[str, Any]:
    if dry_run:
        return {
            "jobs_processed": 0,
            "done_count": 0,
            "failed_count": 0,
            "promote_new_count": 0,
            "batch_report_path": "",
            "dry_run": True,
            "command_preview": " ".join(cmd),
        }
    kv = dict((result or {}).get("kv") or {})
    return {
        "jobs_processed": int(kv.get("jobs_processed", "0") or 0),
        "done_count": int(kv.get("done_count", "0") or 0),
        "failed_count": int(kv.get("failed_count", "0") or 0),
        "promote_new_count": int(kv.get("promote_new_count", "0") or 0),
        "batch_report_path": str(kv.get("batch_report_path", "")).strip(),
        "dry_run": False,
        "command_preview": "",
    }


def candidate_summary(export_result: Dict[str, Any], review_result: Dict[str, Any], repo: Path) -> Dict[str, Any]:
    snapshot = candidate_snapshot(repo)
    export_kv = dict(export_result.get("kv") or {})
    review_kv = dict(review_result.get("kv") or {})
    return {
        "candidate_count_total": int(
            export_kv.get("candidate_count_total", snapshot["candidate_count_total"]) or snapshot["candidate_count_total"]
        ),
        "strong_count": int(export_kv.get("strong_count", snapshot["strong_count"]) or snapshot["strong_count"]),
        "review_count": int(review_kv.get("review_count", snapshot["review_count"]) or snapshot["review_count"]),
        "top_pack_id": str(review_kv.get("top_pack_id", snapshot["top_pack_id"])).strip(),
        "top_score": str(review_kv.get("top_score", snapshot["top_score"])).strip(),
    }


def write_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_orchestrator(
    args: argparse.Namespace,
    *,
    repo: Path,
    runner: Callable[[List[str], Path], Dict[str, Any]] = run_command,
) -> Tuple[int, Dict[str, Any], Path]:
    started = parse_now_utc(args.now_utc)
    report_path = (Path(args.state_dir).resolve()) / f"nightly_orchestrator_report_{utc_stamp(started)}.json"
    report: Dict[str, Any] = {
        "started_ts_utc": utc_now_iso(started),
        "finished_ts_utc": "",
        "status": "",
        "planner": {
            "added_count": 0,
            "skipped_existing_count": 0,
            "skipped_done_count": 0,
            "windows_total": 0,
        },
        "scheduler": {
            "jobs_processed": 0,
            "done_count": 0,
            "failed_count": 0,
            "promote_new_count": 0,
            "batch_report_path": "",
        },
        "candidate": {
            "candidate_count_total": 0,
            "strong_count": 0,
            "review_count": 0,
            "top_pack_id": "",
            "top_score": "",
        },
        "phase6_v2": {
            "pack_count": 0,
            "invoked_count": 0,
            "record_appended_count": 0,
            "failed_count": 0,
            "packs": [],
        },
        "commands": {
            "planner": planner_command(args),
            "scheduler": scheduler_command(args),
            "phase6_v2": [],
            "candidate_export": export_command(),
            "candidate_review": review_command(),
        },
    }

    if not bool(args.ignore_active_window):
        in_window, local_hhmm = is_in_active_window(
            now_utc=started,
            window_start=str(args.active_window_start),
            window_end=str(args.active_window_end),
            tz_name=str(args.active_window_tz),
        )
        if not in_window:
            report["status"] = "NOOP_OUTSIDE_ACTIVE_WINDOW"
            report["active_window"] = {
                "local_time": local_hhmm,
                "tz": str(args.active_window_tz),
                "window": f"{args.active_window_start}..{args.active_window_end}",
            }
            report["candidate"] = candidate_snapshot(repo)
            report["finished_ts_utc"] = utc_now_iso()
            write_report(report_path, report)
            return 0, report, report_path

    planner_result = runner(planner_command(args), repo)
    report["planner"] = planner_summary(planner_result)
    report["planner"]["exit_code"] = int(planner_result.get("exit_code", 0) or 0)
    if int(planner_result.get("exit_code", 0) or 0) != 0:
        report["status"] = "FAIL_PLANNER"
        report["planner"]["stderr_tail"] = "\n".join(str(planner_result.get("stderr", "")).splitlines()[-40:])
        report["finished_ts_utc"] = utc_now_iso()
        write_report(report_path, report)
        return 2, report, report_path

    scheduler_result: Optional[Dict[str, Any]] = None
    if bool(args.dry_run):
        report["scheduler"] = scheduler_summary(None, dry_run=True, cmd=scheduler_command(args))
    else:
        scheduler_result = runner(scheduler_command(args), repo)
        report["scheduler"] = scheduler_summary(scheduler_result, dry_run=False, cmd=scheduler_command(args))
        report["scheduler"]["exit_code"] = int(scheduler_result.get("exit_code", 0) or 0)
        if int(scheduler_result.get("exit_code", 0) or 0) != 0:
            report["status"] = "FAIL_SCHEDULER"
            report["scheduler"]["stderr_tail"] = "\n".join(str(scheduler_result.get("stderr", "")).splitlines()[-40:])
            report["finished_ts_utc"] = utc_now_iso()
            write_report(report_path, report)
            return 2, report, report_path
        new_pack_paths = new_pack_paths_from_batch_report(str(report["scheduler"].get("batch_report_path", "")))
        report["commands"]["phase6_v2"] = [v2_command(pack_path) for pack_path in new_pack_paths]
        report["phase6_v2"] = phase6_v2_auto_apply(new_pack_paths, repo=repo, runner=runner)
        if int(report["phase6_v2"].get("failed_count", 0) or 0) != 0:
            report["status"] = "FAIL_PHASE6_V2_AUTO_APPLY"
            tails = []
            for item in report["phase6_v2"].get("packs", []):
                if int(item.get("exit_code", 0) or 0) != 0:
                    tails.append(f"{item.get('pack_path','')}: {item.get('stderr_tail','')}")
            report["phase6_v2"]["stderr_tail"] = "\n".join(tails[-20:])
            report["finished_ts_utc"] = utc_now_iso()
            write_report(report_path, report)
            return 2, report, report_path

    export_result = runner(export_command(), repo)
    review_result = runner(review_command(), repo)
    report["candidate"] = candidate_summary(export_result, review_result, repo)
    report["candidate"]["export_exit_code"] = int(export_result.get("exit_code", 0) or 0)
    report["candidate"]["review_exit_code"] = int(review_result.get("exit_code", 0) or 0)
    if int(export_result.get("exit_code", 0) or 0) != 0:
        report["status"] = "FAIL_CANDIDATE_EXPORT"
        report["candidate"]["export_stderr_tail"] = "\n".join(str(export_result.get("stderr", "")).splitlines()[-40:])
        report["finished_ts_utc"] = utc_now_iso()
        write_report(report_path, report)
        return 2, report, report_path
    if int(review_result.get("exit_code", 0) or 0) != 0:
        report["status"] = "FAIL_CANDIDATE_REVIEW"
        report["candidate"]["review_stderr_tail"] = "\n".join(str(review_result.get("stderr", "")).splitlines()[-40:])
        report["finished_ts_utc"] = utc_now_iso()
        write_report(report_path, report)
        return 2, report, report_path

    report["status"] = "OK"
    report["finished_ts_utc"] = utc_now_iso()
    write_report(report_path, report)
    return 0, report, report_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    exit_code, report, report_path = run_orchestrator(args, repo=repo)
    print(f"status={report['status']}")
    print(f"planner_added_count={report['planner']['added_count']}")
    print(f"planner_skipped_existing_count={report['planner']['skipped_existing_count']}")
    print(f"planner_skipped_done_count={report['planner']['skipped_done_count']}")
    print(f"planner_windows_total={report['planner']['windows_total']}")
    if bool(report["scheduler"].get("dry_run", False)):
        print("scheduler_mode=DRY_RUN_SKIPPED")
        print(f"scheduler_command_preview={report['scheduler']['command_preview']}")
    else:
        print(f"scheduler_jobs_processed={report['scheduler']['jobs_processed']}")
        print(f"scheduler_done_count={report['scheduler']['done_count']}")
        print(f"scheduler_failed_count={report['scheduler']['failed_count']}")
        print(f"scheduler_promote_new_count={report['scheduler']['promote_new_count']}")
        print(f"scheduler_batch_report_path={report['scheduler']['batch_report_path']}")
        print(f"phase6_v2_pack_count={report['phase6_v2']['pack_count']}")
        print(f"phase6_v2_invoked_count={report['phase6_v2']['invoked_count']}")
        print(f"phase6_v2_record_appended_count={report['phase6_v2']['record_appended_count']}")
        print(f"phase6_v2_failed_count={report['phase6_v2']['failed_count']}")
    print(f"candidate_count_total={report['candidate']['candidate_count_total']}")
    print(f"strong_count={report['candidate']['strong_count']}")
    print(f"review_count={report['candidate']['review_count']}")
    print(f"top_pack_id={report['candidate']['top_pack_id']}")
    print(f"top_score={report['candidate']['top_score']}")
    print(f"report_path={report_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
