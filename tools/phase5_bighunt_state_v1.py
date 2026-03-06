#!/usr/bin/env python3
"""Shared state helpers for Phase-5 Big Hunt v1 queue/scheduler."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


QUEUE_FILENAME = "bighunt_queue.jsonl"
INDEX_FILENAME = "bighunt_index.json"
DEFAULT_STATE_DIR = Path(__file__).resolve().parent / "phase5_state"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_plan_id(
    *,
    exchange: str,
    stream: str,
    start: str,
    end: str,
    object_keys_tsv: str,
    max_symbols: int,
    per_run_timeout_min: int,
    max_wall_min: int,
    category: str,
) -> str:
    payload = {
        "exchange": str(exchange).strip().lower(),
        "stream": str(stream).strip().lower(),
        "start": str(start).strip(),
        "end": str(end).strip(),
        "object_keys_tsv_basename": Path(str(object_keys_tsv)).name,
        "max_symbols": int(max_symbols),
        "per_run_timeout_min": int(per_run_timeout_min),
        "max_wall_min": int(max_wall_min),
        "category": str(category).strip(),
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def default_index_object() -> Dict[str, Any]:
    return {
        "record_count": 0,
        "plan_latest": {},
        "by_status": {
            "PENDING": 0,
            "RUNNING": 0,
            "DONE": 0,
            "FAILED": 0,
        },
        "pending_plan_ids": [],
        "retryable_failed_plan_ids": [],
        "done_plan_ids": [],
        "created_order_plan_ids": [],
    }


def ensure_state_files(state_dir: Path) -> Tuple[Path, Path]:
    state_dir.mkdir(parents=True, exist_ok=True)
    queue_path = state_dir / QUEUE_FILENAME
    index_path = state_dir / INDEX_FILENAME
    if not queue_path.exists():
        queue_path.write_text("", encoding="utf-8")
    if not index_path.exists():
        write_index(index_path, default_index_object())
    return queue_path, index_path


def load_queue_records(queue_path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    text = queue_path.read_text(encoding="utf-8", errors="replace") if queue_path.exists() else ""
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"queue_jsonl_invalid line={lineno}: {exc}") from exc
        if not isinstance(obj, dict):
            raise RuntimeError(f"queue_jsonl_invalid_object line={lineno}")
        records.append(obj)
    return records


def append_queue_record(queue_path: Path, record: Dict[str, Any]) -> None:
    line = json.dumps(record, sort_keys=True, separators=(",", ":"))
    with queue_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def rebuild_index(records: List[Dict[str, Any]], *, max_tries: int = 2) -> Dict[str, Any]:
    latest: Dict[str, Dict[str, Any]] = {}
    first_seen_line: Dict[str, int] = {}

    for line_no, rec in enumerate(records, start=1):
        plan_id = str(rec.get("plan_id", "")).strip()
        if not plan_id:
            continue
        merged = dict(rec)
        merged["_line_no"] = line_no
        latest[plan_id] = merged
        if plan_id not in first_seen_line:
            first_seen_line[plan_id] = line_no

    created_order_plan_ids = [
        pid for pid, _ in sorted(first_seen_line.items(), key=lambda kv: (kv[1], kv[0]))
    ]

    by_status = {
        "PENDING": 0,
        "RUNNING": 0,
        "DONE": 0,
        "FAILED": 0,
    }
    pending_plan_ids: List[str] = []
    retryable_failed_plan_ids: List[str] = []
    done_plan_ids: List[str] = []

    for pid in created_order_plan_ids:
        rec = latest[pid]
        status = str(rec.get("status", "")).strip()
        tries = int(rec.get("tries", 0) or 0)
        if status in by_status:
            by_status[status] += 1
        if status == "PENDING":
            pending_plan_ids.append(pid)
        elif status == "FAILED" and tries < max_tries:
            retryable_failed_plan_ids.append(pid)
        elif status == "DONE":
            done_plan_ids.append(pid)

    ordered_latest = {pid: latest[pid] for pid in created_order_plan_ids}

    return {
        "record_count": len(records),
        "plan_latest": ordered_latest,
        "by_status": by_status,
        "pending_plan_ids": pending_plan_ids,
        "retryable_failed_plan_ids": retryable_failed_plan_ids,
        "done_plan_ids": done_plan_ids,
        "created_order_plan_ids": created_order_plan_ids,
    }


def write_index(index_path: Path, index_obj: Dict[str, Any]) -> None:
    index_path.write_text(json.dumps(index_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")

