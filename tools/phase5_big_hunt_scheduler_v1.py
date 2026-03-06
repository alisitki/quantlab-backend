#!/usr/bin/env python3
"""Phase-5 Big Hunt v1 scheduler (batch-safe single-concurrency orchestrator)."""

from __future__ import annotations

import argparse
import errno
import json
import os
import random
import shlex
import subprocess
import time
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

try:
    from phase5_bighunt_state_v1 import (
        DEFAULT_STATE_DIR,
        append_queue_record,
        ensure_state_files,
        load_queue_records,
        rebuild_index,
        utc_now_iso,
        write_index,
    )
except ImportError:  # pragma: no cover
    from tools.phase5_bighunt_state_v1 import (
        DEFAULT_STATE_DIR,
        append_queue_record,
        ensure_state_files,
        load_queue_records,
        rebuild_index,
        utc_now_iso,
        write_index,
    )

try:
    from phase5_state_selection_v1 import build_object_keys_tsv, filter_rows, load_inventory
except ImportError:  # pragma: no cover
    from tools.phase5_state_selection_v1 import build_object_keys_tsv, filter_rows, load_inventory


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 Big Hunt v1 scheduler")
    p.add_argument("--max-jobs", type=int, default=1)
    p.add_argument("--max-tries", type=int, default=2)
    p.add_argument("--state-dir", default="")
    p.add_argument("--lock-file", default="")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--session-wall-budget-min", type=float, default=0.0)
    p.add_argument("--sleep-between-jobs-sec", type=float, default=30.0)
    p.add_argument("--sleep-jitter-sec", type=float, default=10.0)
    p.add_argument("--failure-backoff-sec", type=float, default=120.0)
    p.add_argument("--stop-after-failures", type=int, default=1)
    p.add_argument("--stale-running-min", type=float, default=180.0)
    p.add_argument("--active-window-start", default="21:00")
    p.add_argument("--active-window-end", default="08:00")
    p.add_argument("--active-window-tz", default="Europe/Istanbul")
    p.add_argument("--ignore-active-window", action="store_true")
    p.add_argument("--now-utc", default="", help="test-only now override (ISO8601 UTC)")
    p.add_argument("--inventory-state-json", default="/tmp/compacted__state.json")
    p.add_argument("--inventory-bucket", default="quantlab-compact")
    p.add_argument("--inventory-key", default="compacted/_state.json")
    p.add_argument("--inventory-s3-tool", default="/tmp/s3_compact_tool.py")
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
    return p.parse_args(argv)


def utc_stamp(dt: Optional[datetime] = None) -> str:
    base = dt or datetime.now(timezone.utc)
    return base.strftime("%Y%m%d_%H%M%S")


def parse_now_utc(raw: str) -> datetime:
    text = str(raw or "").strip()
    if not text:
        return datetime.now(timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_hhmm(raw: str) -> dt_time:
    text = str(raw or "").strip()
    parts = text.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid_hhmm:{text}")
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"invalid_hhmm:{text}") from exc
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"invalid_hhmm:{text}")
    return dt_time(hour=hh, minute=mm)


def is_in_active_window(
    *,
    now_utc: datetime,
    window_start: str,
    window_end: str,
    tz_name: str,
) -> Tuple[bool, str]:
    tz = ZoneInfo(str(tz_name).strip())
    local_now = now_utc.astimezone(tz)
    start = parse_hhmm(window_start)
    end = parse_hhmm(window_end)
    current_m = local_now.hour * 60 + local_now.minute
    start_m = start.hour * 60 + start.minute
    end_m = end.hour * 60 + end.minute

    if start_m == end_m:
        in_window = True
    elif start_m < end_m:
        in_window = start_m <= current_m < end_m
    else:
        # Cross-midnight range: active if >= start OR < end.
        in_window = current_m >= start_m or current_m < end_m

    local_hhmm = local_now.strftime("%H:%M")
    return in_window, local_hhmm


def parse_iso_utc(raw: str) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_kv_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in str(text).splitlines():
        line = raw.strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def trim_tail(text: str, max_lines: int) -> str:
    lines = str(text).splitlines()
    return "\n".join(lines[-max_lines:])


def sanitize_error(msg: str, max_chars: int = 5000) -> str:
    text = str(msg)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def build_run_id(plan: Dict[str, Any], next_tries: int, *, now_dt: datetime) -> str:
    plan_id = str(plan.get("plan_id", "")).strip()
    suffix = plan_id[:8] if plan_id else "unknown"
    return (
        "multi-hypothesis-phase5-bighunt-"
        f"{plan.get('exchange')}-{plan.get('stream')}-"
        f"{plan.get('start')}..{plan.get('end')}-"
        f"{utc_stamp(now_dt)}-{suffix}-t{next_tries}__FULLSCAN_MAJOR"
    )


def build_v0_command(plan: Dict[str, Any], run_id: str) -> List[str]:
    return [
        "python3",
        "tools/phase5_big_hunt_v0.py",
        "--exchange",
        str(plan.get("exchange", "")),
        "--stream",
        str(plan.get("stream", "")),
        "--start",
        str(plan.get("start", "")),
        "--end",
        str(plan.get("end", "")),
        "--objectKeysTsv",
        str(plan.get("object_keys_tsv", "")),
        "--max-symbols",
        str(int(plan.get("max_symbols", 20) or 20)),
        "--per-run-timeout-min",
        str(int(plan.get("per_run_timeout_min", 12) or 12)),
        "--max-wall-min",
        str(int(plan.get("max_wall_min", 120) or 120)),
        "--run-id",
        run_id,
    ]


def selection_reason(
    rec: Dict[str, Any],
    *,
    max_tries: int,
    stale_running_min: float,
    now_dt: datetime,
) -> Optional[str]:
    status = str(rec.get("status", "")).strip()
    tries = int(rec.get("tries", 0) or 0)
    if tries >= max_tries:
        return None
    if status == "PENDING":
        return "PENDING"
    if status == "FAILED":
        return "FAILED_RETRY"
    if status == "RUNNING":
        ts = parse_iso_utc(str(rec.get("updated_ts_utc", "")).strip())
        if ts is None:
            ts = parse_iso_utc(str(rec.get("created_ts_utc", "")).strip())
        if ts is None:
            return None
        age_min = (now_dt - ts).total_seconds() / 60.0
        if age_min > stale_running_min:
            return "RUNNING_STALE_RECLAIM"
    return None


def pick_next_plan(
    index_obj: Dict[str, Any],
    *,
    max_tries: int,
    stale_running_min: float,
    now_dt: datetime,
) -> Optional[Tuple[str, Dict[str, Any], str]]:
    latest = index_obj.get("plan_latest", {})
    order = index_obj.get("created_order_plan_ids", [])
    for plan_id in order:
        rec = latest.get(plan_id)
        if not isinstance(rec, dict):
            continue
        reason = selection_reason(
            rec,
            max_tries=max_tries,
            stale_running_min=stale_running_min,
            now_dt=now_dt,
        )
        if reason is not None:
            return str(plan_id), rec, reason
    return None


def resolve_campaign_report(repo: Path, run_id: str, kv: Dict[str, str]) -> Tuple[Optional[Path], Dict[str, Any]]:
    candidates: List[Path] = []
    archive_dir = kv.get("ARCHIVE_DIR", "")
    if archive_dir:
        candidates.append(Path(archive_dir) / "campaign_report.json")
    candidates.append(repo / "evidence" / run_id / "campaign_report.json")
    for p in candidates:
        if p.exists():
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return p, {}
            if isinstance(obj, dict):
                return p, obj
            return p, {}
    return None, {}


def estimate_plan_wall_min(plan: Dict[str, Any]) -> float:
    est = float(plan.get("max_wall_min", 0) or 0)
    if est > 0:
        return est
    return float(plan.get("per_run_timeout_min", 12) or 12)


def ensure_inventory_state(
    *,
    state_path: Path,
    bucket: str,
    key: str,
    s3_tool: str,
    repo: Path,
) -> Path:
    if state_path.exists():
        return state_path
    state_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["python3", s3_tool, "get", bucket, key, str(state_path)]
    proc = subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True)
    if proc.returncode != 0 or not state_path.exists():
        err = sanitize_error(
            f"inventory_fetch_failed exit={proc.returncode} bucket={bucket} key={key} "
            f"stderr={trim_tail(proc.stderr, 40)}"
        )
        raise RuntimeError(err)
    return state_path


def prepare_plan_object_keys_tsv(
    *,
    plan: Dict[str, Any],
    run_id: str,
    repo: Path,
    inventory_state_path: Path,
    inventory_bucket: str,
    require_quality_pass: bool,
) -> Tuple[Path, List[str], int]:
    rows = load_inventory(inventory_state_path, bucket=inventory_bucket)
    selected_rows, selected_symbols, _days = filter_rows(
        rows,
        exchange=str(plan.get("exchange", "")),
        stream=str(plan.get("stream", "")),
        start=str(plan.get("start", "")),
        end=str(plan.get("end", "")),
        require_status="success",
        require_quality_pass=require_quality_pass,
        max_symbols=int(plan.get("max_symbols", 20) or 20),
    )
    pack_dir = repo / "evidence" / run_id
    out_tsv = pack_dir / "state_selection" / "object_keys_selected.tsv"
    if not selected_rows or not selected_symbols:
        # Still write canonical header-only file for diagnosability.
        build_object_keys_tsv([], out_tsv)
        return out_tsv, [], 0
    build_object_keys_tsv(selected_rows, out_tsv)
    return out_tsv, selected_symbols, len(selected_rows)


def compute_default_budget_min(
    index_obj: Dict[str, Any],
    *,
    max_tries: int,
    stale_running_min: float,
    now_dt: datetime,
) -> float:
    total = 0.0
    latest = index_obj.get("plan_latest", {})
    for plan_id in index_obj.get("created_order_plan_ids", []):
        rec = latest.get(plan_id)
        if not isinstance(rec, dict):
            continue
        reason = selection_reason(
            rec,
            max_tries=max_tries,
            stale_running_min=stale_running_min,
            now_dt=now_dt,
        )
        if reason is None:
            continue
        total += estimate_plan_wall_min(rec)
    return total


def next_success_sleep_sec(base: float, jitter: float, rng: random.Random) -> float:
    b = max(0.0, float(base))
    j = max(0.0, float(jitter))
    if j <= 0:
        return b
    return b + rng.uniform(0.0, j)


def is_truthy(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def append_and_reindex(
    *,
    queue_path: Path,
    index_path: Path,
    records: List[Dict[str, Any]],
    record: Dict[str, Any],
    max_tries: int,
) -> Dict[str, Any]:
    append_queue_record(queue_path, record)
    records.append(record)
    new_index = rebuild_index(records, max_tries=max_tries)
    write_index(index_path, new_index)
    return new_index


class SchedulerLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd: Optional[int] = None

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            if exc.errno == errno.EPERM:
                return True
            return False

    def _existing_lock_is_live(self) -> bool:
        if not self.path.exists():
            return False
        try:
            text = self.path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return True
        for raw in text.splitlines():
            if raw.startswith("pid="):
                try:
                    pid = int(raw.split("=", 1)[1].strip())
                except ValueError:
                    return True
                return self._pid_alive(pid)
        return True

    def __enter__(self) -> "SchedulerLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        # Two-pass acquire to handle stale lock cleanup race.
        for _ in range(2):
            if self.path.exists() and not self._existing_lock_is_live():
                try:
                    self.path.unlink()
                except OSError:
                    pass
            try:
                self.fd = os.open(str(self.path), flags)
                payload = f"pid={os.getpid()}\ncreated_ts_utc={utc_now_iso()}\n"
                os.write(self.fd, payload.encode("utf-8"))
                os.fsync(self.fd)
                return self
            except FileExistsError:
                continue
        raise RuntimeError(f"lock_exists:{self.path}")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            if self.path.exists():
                self.path.unlink()
        except OSError:
            pass


def run_scheduler(
    args: argparse.Namespace,
    *,
    repo: Path,
    sleep_fn: Callable[[float], None] = time.sleep,
    rng: Optional[random.Random] = None,
    now_fn: Optional[Callable[[], datetime]] = None,
) -> int:
    if args.max_jobs <= 0:
        raise ValueError("max-jobs must be > 0")
    if args.max_tries <= 0:
        raise ValueError("max-tries must be > 0")
    if args.stop_after_failures <= 0:
        raise ValueError("stop-after-failures must be > 0")
    if args.sleep_between_jobs_sec < 0 or args.sleep_jitter_sec < 0 or args.failure_backoff_sec < 0:
        raise ValueError("sleep/backoff values must be >= 0")
    if args.stale_running_min < 0:
        raise ValueError("stale-running-min must be >= 0")

    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    lock_path = Path(args.lock_file).resolve() if args.lock_file else (state_dir / "bighunt_scheduler.lock")
    inventory_state_path = Path(args.inventory_state_json).resolve()
    queue_path, index_path = ensure_state_files(state_dir)
    records = load_queue_records(queue_path)
    index_obj = rebuild_index(records, max_tries=int(args.max_tries))
    write_index(index_path, index_obj)

    default_now_fn = lambda: parse_now_utc(args.now_utc) if str(args.now_utc).strip() else datetime.now(timezone.utc)
    now_provider = now_fn or default_now_fn
    rand = rng or random.Random()

    started_dt = now_provider()
    started_ts_utc = started_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if not bool(args.ignore_active_window):
        in_window, local_hhmm = is_in_active_window(
            now_utc=started_dt,
            window_start=str(args.active_window_start),
            window_end=str(args.active_window_end),
            tz_name=str(args.active_window_tz),
        )
        if not in_window:
            stop_reason = (
                "NOOP_OUTSIDE_ACTIVE_WINDOW "
                f"local_time={local_hhmm} tz={args.active_window_tz} "
                f"window={args.active_window_start}..{args.active_window_end}"
            )
            budget_min = float(args.session_wall_budget_min) if float(args.session_wall_budget_min) > 0 else 0.0
            report = {
                "started_ts_utc": started_ts_utc,
                "finished_ts_utc": started_ts_utc,
                "dry_run": bool(args.dry_run),
                "max_jobs": int(args.max_jobs),
                "max_tries": int(args.max_tries),
                "session_wall_budget_min": float(budget_min),
                "wall_used_min": 0.0,
                "sleep_config": {
                    "sleep_between_jobs_sec": float(args.sleep_between_jobs_sec),
                    "sleep_jitter_sec": float(args.sleep_jitter_sec),
                    "failure_backoff_sec": float(args.failure_backoff_sec),
                },
                "stop_policy": {
                    "stop_after_failures": int(args.stop_after_failures),
                    "stale_running_min": float(args.stale_running_min),
                },
                "active_window": {
                    "start": str(args.active_window_start),
                    "end": str(args.active_window_end),
                    "tz": str(args.active_window_tz),
                    "local_time": local_hhmm,
                    "ignored": bool(args.ignore_active_window),
                },
                "queue_path": str(queue_path),
                "index_path": str(index_path),
                "processed": [],
                "counts": {
                    "processed": 0,
                    "done": 0,
                    "failed": 0,
                    "skipped": int(args.max_jobs),
                    "reclaimed_running": 0,
                    "budget_stops": 0,
                },
                "failures": [],
                "promote_new_count": 0,
                "stop_reason": stop_reason,
            }
            report_name = f"bighunt_batch_report_{utc_stamp(started_dt)}.json"
            report_path = state_dir / report_name
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print("jobs_processed=0")
            print("done_count=0")
            print("failed_count=0")
            print("wall_used_min=0.000000")
            print(f"session_wall_budget_min={budget_min:.6f}")
            print("promote_new_count=0")
            print(f"batch_report_path={report_path}")
            print(f"stop_reason={stop_reason}")
            return 0

    inventory_state_path = ensure_inventory_state(
        state_path=inventory_state_path,
        bucket=str(args.inventory_bucket),
        key=str(args.inventory_key),
        s3_tool=str(args.inventory_s3_tool),
        repo=repo,
    )

    budget_min = (
        float(args.session_wall_budget_min)
        if float(args.session_wall_budget_min) > 0
        else compute_default_budget_min(
            index_obj,
            max_tries=int(args.max_tries),
            stale_running_min=float(args.stale_running_min),
            now_dt=started_dt,
        )
    )

    jobs_processed = 0
    failures = 0
    reclaimed_running = 0
    promote_new_count = 0
    wall_used_min = 0.0
    stop_reason = ""
    dry_run = bool(args.dry_run)
    processed_rows: List[Dict[str, Any]] = []
    failure_rows: List[Dict[str, str]] = []
    budget_stops = 0

    with SchedulerLock(lock_path):
        for _ in range(int(args.max_jobs)):
            now_dt = now_provider()
            picked = pick_next_plan(
                index_obj,
                max_tries=int(args.max_tries),
                stale_running_min=float(args.stale_running_min),
                now_dt=now_dt,
            )
            if picked is None:
                break
            plan_id, latest, reason = picked
            if reason == "RUNNING_STALE_RECLAIM":
                reclaimed_running += 1

            base = dict(latest)
            base.pop("_line_no", None)
            next_tries = int(base.get("tries", 0) or 0) + 1
            run_id = build_run_id(base, next_tries, now_dt=now_dt)
            resolved_tsv_path, selected_symbols, selected_row_count = prepare_plan_object_keys_tsv(
                plan=base,
                run_id=run_id,
                repo=repo,
                inventory_state_path=inventory_state_path,
                inventory_bucket=str(args.inventory_bucket),
                require_quality_pass=bool(args.inventory_require_quality_pass),
            )

            if len(selected_symbols) == 0:
                running_like = dict(base)
                running_like["updated_ts_utc"] = utc_now_iso()
                running_like["status"] = "FAILED"
                running_like["tries"] = next_tries
                running_like["last_run_id"] = run_id
                running_like["resolved_object_keys_tsv_path"] = str(resolved_tsv_path)
                running_like["available_symbols"] = []
                running_like["last_archive_dir"] = None
                running_like["last_decision"] = None
                running_like["last_error"] = "STOP_NO_COVERAGE"
                index_obj = append_and_reindex(
                    queue_path=queue_path,
                    index_path=index_path,
                    records=records,
                    record=running_like,
                    max_tries=int(args.max_tries),
                )
                failures += 1
                jobs_processed += 1
                processed_rows.append(
                    {
                        "plan_id": plan_id,
                        "selection_reason": reason,
                        "run_id": run_id,
                        "final_status": "FAILED",
                        "estimated_wall_min": round(estimate_plan_wall_min(base), 6),
                        "elapsed_min": 0.0,
                        "archive_dir": "",
                        "decision": "",
                        "record_appended": "",
                        "last_error": "STOP_NO_COVERAGE",
                    }
                )
                failure_rows.append({"plan_id": plan_id, "last_error": "STOP_NO_COVERAGE"})
                print(f"selected_plan_id={plan_id}")
                print(f"selection_reason={reason}")
                print(f"run_id={run_id}")
                print(f"resolved_object_keys_tsv_path={resolved_tsv_path}")
                print("selected_symbols_csv=")
                print("selected_symbol_count=0")
                print("status=FAILED")
                print("last_error=STOP_NO_COVERAGE")
                if failures >= int(args.stop_after_failures):
                    stop_reason = (
                        f"STOP_AFTER_FAILURES failures={failures} threshold={int(args.stop_after_failures)}"
                    )
                    break
                continue

            cmd = build_v0_command(base, run_id)
            # Replace plan input TSV with state-compatible window TSV built for this run.
            tsv_arg_idx = cmd.index("--objectKeysTsv") + 1
            cmd[tsv_arg_idx] = str(resolved_tsv_path)
            est_plan_min = estimate_plan_wall_min(base)

            # Quota gate (estimate-based pre-check).
            if budget_min > 0 and (wall_used_min + est_plan_min) > budget_min:
                budget_stops += 1
                stop_reason = (
                    f"SESSION_WALL_BUDGET_EXCEEDED wall_used_min={wall_used_min:.6f} "
                    f"next_est_min={est_plan_min:.6f} budget_min={budget_min:.6f}"
                )
                break

            print(f"selected_plan_id={plan_id}")
            print(f"selection_reason={reason}")
            print(f"run_id={run_id}")
            print(f"resolved_object_keys_tsv_path={resolved_tsv_path}")
            print("selected_symbols_csv=" + ",".join(selected_symbols))
            print(f"selected_symbol_count={len(selected_symbols)}")
            print(f"selected_row_count={selected_row_count}")
            print(f"estimated_wall_min={est_plan_min:.6f}")

            if dry_run:
                print("command=" + " ".join(shlex.quote(x) for x in cmd))
                jobs_processed += 1
                wall_used_min += est_plan_min
                processed_rows.append(
                    {
                        "plan_id": plan_id,
                        "selection_reason": reason,
                        "run_id": run_id,
                        "final_status": "DRY_RUN_PLANNED",
                        "estimated_wall_min": round(est_plan_min, 6),
                        "elapsed_min": 0.0,
                        "archive_dir": "",
                        "decision": "",
                        "record_appended": "",
                        "last_error": "",
                    }
                )
                # simulate progression without mutating queue/index
                tmp_latest = dict(index_obj.get("plan_latest", {}))
                tmp_rec = dict(base)
                tmp_rec["status"] = "DONE"
                tmp_latest[plan_id] = tmp_rec
                index_obj = dict(index_obj)
                index_obj["plan_latest"] = tmp_latest
                if jobs_processed < int(args.max_jobs):
                    planned_sleep = next_success_sleep_sec(
                        float(args.sleep_between_jobs_sec),
                        float(args.sleep_jitter_sec),
                        rand,
                    )
                    print(f"planned_sleep_sec={planned_sleep:.6f}")
                    wall_used_min += planned_sleep / 60.0
                continue

            running_record = dict(base)
            running_record["updated_ts_utc"] = utc_now_iso()
            running_record["status"] = "RUNNING"
            running_record["tries"] = next_tries
            running_record["last_run_id"] = run_id
            running_record["resolved_object_keys_tsv_path"] = str(resolved_tsv_path)
            running_record["available_symbols"] = list(selected_symbols)
            running_record["last_archive_dir"] = None
            running_record["last_decision"] = None
            running_record["last_error"] = None
            index_obj = append_and_reindex(
                queue_path=queue_path,
                index_path=index_path,
                records=records,
                record=running_record,
                max_tries=int(args.max_tries),
            )
            print("transition=PENDING_OR_FAILED_OR_STALE->RUNNING")

            started_mono = time.monotonic()
            proc = subprocess.run(
                cmd,
                cwd=str(repo),
                capture_output=True,
                text=True,
            )
            elapsed_min = max(0.0, (time.monotonic() - started_mono) / 60.0)
            wall_used_min += elapsed_min

            kv = parse_kv_lines(proc.stdout)
            report_path, report_obj = resolve_campaign_report(repo, run_id, kv)
            report_status = str(report_obj.get("status", "")).strip() if isinstance(report_obj, dict) else ""
            jobs_processed += 1

            if proc.returncode == 0 and report_status == "PASS":
                post_eval = report_obj.get("post_eval", {}) if isinstance(report_obj, dict) else {}
                decision = ""
                record_appended = ""
                if isinstance(post_eval, dict):
                    decision = str(post_eval.get("decision", "")).strip()
                    record_appended = str(post_eval.get("record_appended", "")).strip()
                if not decision:
                    decision = kv.get("decision", "")
                if not record_appended:
                    record_appended = kv.get("record_appended", "")
                archive_dir = str(report_obj.get("archive_dir", "")).strip() if isinstance(report_obj, dict) else ""
                if not archive_dir:
                    archive_dir = kv.get("ARCHIVE_DIR", "")

                done_record = dict(running_record)
                done_record["updated_ts_utc"] = utc_now_iso()
                done_record["status"] = "DONE"
                done_record["last_archive_dir"] = archive_dir or None
                done_record["last_decision"] = decision or None
                done_record["last_error"] = None
                index_obj = append_and_reindex(
                    queue_path=queue_path,
                    index_path=index_path,
                    records=records,
                    record=done_record,
                    max_tries=int(args.max_tries),
                )
                if is_truthy(record_appended):
                    promote_new_count += 1
                print(f"exit_code={proc.returncode}")
                print("status=DONE")
                print(f"archive_dir={archive_dir}")
                print(f"decision={decision}")
                print(f"record_appended={record_appended}")
                processed_rows.append(
                    {
                        "plan_id": plan_id,
                        "selection_reason": reason,
                        "run_id": run_id,
                        "final_status": "DONE",
                        "estimated_wall_min": round(est_plan_min, 6),
                        "elapsed_min": round(elapsed_min, 6),
                        "archive_dir": archive_dir,
                        "decision": decision,
                        "record_appended": record_appended,
                        "last_error": "",
                    }
                )
                state_diff = post_eval.get("state_diff", {}) if isinstance(post_eval, dict) else {}
                if isinstance(state_diff, dict):
                    if "records_nonempty" in state_diff:
                        print(f"state_diff_records_nonempty={state_diff['records_nonempty']}")
                    if "index_record_count" in state_diff:
                        print(f"state_diff_index_record_count={state_diff['index_record_count']}")

                # success backoff before next job
                if jobs_processed < int(args.max_jobs):
                    maybe_next = pick_next_plan(
                        index_obj,
                        max_tries=int(args.max_tries),
                        stale_running_min=float(args.stale_running_min),
                        now_dt=now_provider(),
                    )
                    if maybe_next is not None:
                        sleep_sec = next_success_sleep_sec(
                            float(args.sleep_between_jobs_sec),
                            float(args.sleep_jitter_sec),
                            rand,
                        )
                        print(f"sleep_between_jobs_sec={sleep_sec:.6f}")
                        sleep_fn(sleep_sec)
                        wall_used_min += sleep_sec / 60.0
                continue

            stop_reason_from_report = ""
            if isinstance(report_obj, dict):
                stop_reason_from_report = str(report_obj.get("stop_reason", "")).strip()
            stderr_tail = trim_tail(proc.stderr, 120)
            report_hint = str(report_path) if report_path else "campaign_report_missing"
            err = sanitize_error(
                f"exit_code={proc.returncode};report_status={report_status};"
                f"stop_reason={stop_reason_from_report};report={report_hint};stderr_tail={stderr_tail}"
            )

            failed_record = dict(running_record)
            failed_record["updated_ts_utc"] = utc_now_iso()
            failed_record["status"] = "FAILED"
            failed_record["last_archive_dir"] = (
                str(report_obj.get("archive_dir", "")).strip() if isinstance(report_obj, dict) else None
            ) or None
            failed_record["last_decision"] = None
            failed_record["last_error"] = err
            index_obj = append_and_reindex(
                queue_path=queue_path,
                index_path=index_path,
                records=records,
                record=failed_record,
                max_tries=int(args.max_tries),
            )
            failures += 1
            print(f"exit_code={proc.returncode}")
            print("status=FAILED")
            print(f"last_error={err}")
            processed_rows.append(
                {
                    "plan_id": plan_id,
                    "selection_reason": reason,
                    "run_id": run_id,
                    "final_status": "FAILED",
                    "estimated_wall_min": round(est_plan_min, 6),
                    "elapsed_min": round(elapsed_min, 6),
                    "archive_dir": failed_record["last_archive_dir"] or "",
                    "decision": "",
                    "record_appended": "",
                    "last_error": err,
                }
            )
            failure_rows.append({"plan_id": plan_id, "last_error": err})

            if failures >= int(args.stop_after_failures):
                stop_reason = (
                    f"STOP_AFTER_FAILURES failures={failures} threshold={int(args.stop_after_failures)}"
                )
                break

            if jobs_processed < int(args.max_jobs):
                backoff = float(args.failure_backoff_sec)
                if backoff > 0:
                    print(f"failure_backoff_sec={backoff:.6f}")
                    sleep_fn(backoff)
                    wall_used_min += backoff / 60.0

    finished_dt = now_provider()
    finished_ts_utc = finished_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    done_count = sum(1 for p in processed_rows if p.get("final_status") == "DONE")
    failed_count = sum(1 for p in processed_rows if p.get("final_status") == "FAILED")
    skipped_count = max(0, int(args.max_jobs) - jobs_processed)
    batch_report = {
        "started_ts_utc": started_ts_utc,
        "finished_ts_utc": finished_ts_utc,
        "dry_run": dry_run,
        "max_jobs": int(args.max_jobs),
        "max_tries": int(args.max_tries),
        "session_wall_budget_min": float(budget_min),
        "wall_used_min": round(float(wall_used_min), 6),
        "sleep_config": {
            "sleep_between_jobs_sec": float(args.sleep_between_jobs_sec),
            "sleep_jitter_sec": float(args.sleep_jitter_sec),
            "failure_backoff_sec": float(args.failure_backoff_sec),
        },
        "stop_policy": {
            "stop_after_failures": int(args.stop_after_failures),
            "stale_running_min": float(args.stale_running_min),
        },
        "queue_path": str(queue_path),
        "index_path": str(index_path),
        "processed": processed_rows,
        "counts": {
            "processed": jobs_processed,
            "done": done_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "reclaimed_running": reclaimed_running,
            "budget_stops": budget_stops,
        },
        "failures": failure_rows,
        "promote_new_count": promote_new_count,
        "stop_reason": stop_reason,
    }
    report_name = f"bighunt_batch_report_{utc_stamp(finished_dt)}.json"
    report_path = state_dir / report_name
    report_path.write_text(json.dumps(batch_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"jobs_processed={jobs_processed}")
    print(f"done_count={done_count}")
    print(f"failed_count={failed_count}")
    print(f"wall_used_min={wall_used_min:.6f}")
    print(f"session_wall_budget_min={budget_min:.6f}")
    print(f"promote_new_count={promote_new_count}")
    print(f"batch_report_path={report_path}")
    if stop_reason:
        print(f"stop_reason={stop_reason}")
        return 2
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    try:
        return run_scheduler(args, repo=repo)
    except RuntimeError as exc:
        if str(exc).startswith("lock_exists:"):
            print(f"STOP_LOCKED {exc}")
            return 2
        raise


if __name__ == "__main__":
    raise SystemExit(main())
