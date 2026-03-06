#!/usr/bin/env python3
"""Phase-5 Big Hunt v1 plan generator (queue writer)."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

try:
    from phase5_bighunt_state_v1 import (
        DEFAULT_STATE_DIR,
        append_queue_record,
        canonical_plan_id,
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
        canonical_plan_id,
        ensure_state_files,
        load_queue_records,
        rebuild_index,
        utc_now_iso,
        write_index,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 Big Hunt v1 plan generator")
    p.add_argument("--objectKeysTsv", required=True)
    p.add_argument("--exchange", required=True)
    p.add_argument("--stream", required=True)
    p.add_argument("--windows", required=True, help="comma list: YYYYMMDD..YYYYMMDD,...")
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--per-run-timeout-min", type=int, default=12)
    p.add_argument("--max-wall-min", type=int, default=120)
    p.add_argument("--category", default="FULLSCAN_MAJOR")
    p.add_argument("--state-dir", default="")
    return p.parse_args()


def parse_window(raw: str) -> Tuple[str, str]:
    token = str(raw).strip()
    if ".." not in token:
        raise ValueError(f"invalid window token: {token}")
    start, end = token.split("..", 1)
    if len(start) != 8 or len(end) != 8 or not start.isdigit() or not end.isdigit():
        raise ValueError(f"invalid window format: {token}")
    if start > end:
        raise ValueError(f"window start>end: {token}")
    return start, end


def parse_windows(raw: str) -> List[Tuple[str, str]]:
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    if not parts:
        raise ValueError("windows list is empty")
    out: List[Tuple[str, str]] = []
    for part in parts:
        out.append(parse_window(part))
    return out


def build_plan_record(
    *,
    plan_id: str,
    exchange: str,
    stream: str,
    start: str,
    end: str,
    object_keys_tsv: str,
    max_symbols: int,
    per_run_timeout_min: int,
    max_wall_min: int,
    category: str,
) -> Dict[str, object]:
    ts = utc_now_iso()
    return {
        "plan_id": plan_id,
        "created_ts_utc": ts,
        "updated_ts_utc": ts,
        "exchange": exchange,
        "stream": stream,
        "start": start,
        "end": end,
        "object_keys_tsv": object_keys_tsv,
        "max_symbols": int(max_symbols),
        "per_run_timeout_min": int(per_run_timeout_min),
        "max_wall_min": int(max_wall_min),
        "category": category,
        "status": "PENDING",
        "tries": 0,
        "last_run_id": None,
        "last_archive_dir": None,
        "last_decision": None,
        "last_error": None,
    }


def generate_plans(
    *,
    object_keys_tsv: Path,
    exchange: str,
    stream: str,
    windows: Sequence[Tuple[str, str]],
    max_symbols: int,
    per_run_timeout_min: int,
    max_wall_min: int,
    category: str,
    state_dir: Path,
) -> Dict[str, object]:
    if max_symbols <= 0:
        raise ValueError("max-symbols must be > 0")
    if per_run_timeout_min <= 0:
        raise ValueError("per-run-timeout-min must be > 0")
    if max_wall_min <= 0:
        raise ValueError("max-wall-min must be > 0")
    if not object_keys_tsv.exists():
        raise FileNotFoundError(f"object_keys_tsv_missing:{object_keys_tsv}")

    queue_path, index_path = ensure_state_files(state_dir)
    records = load_queue_records(queue_path)
    idx = rebuild_index(records, max_tries=2)
    existing = set(idx.get("plan_latest", {}).keys())

    added_plan_ids: List[str] = []
    skipped_existing_count = 0

    for start, end in windows:
        plan_id = canonical_plan_id(
            exchange=exchange,
            stream=stream,
            start=start,
            end=end,
            object_keys_tsv=str(object_keys_tsv),
            max_symbols=max_symbols,
            per_run_timeout_min=per_run_timeout_min,
            max_wall_min=max_wall_min,
            category=category,
        )
        if plan_id in existing:
            skipped_existing_count += 1
            continue

        rec = build_plan_record(
            plan_id=plan_id,
            exchange=exchange,
            stream=stream,
            start=start,
            end=end,
            object_keys_tsv=str(object_keys_tsv),
            max_symbols=max_symbols,
            per_run_timeout_min=per_run_timeout_min,
            max_wall_min=max_wall_min,
            category=category,
        )
        append_queue_record(queue_path, rec)
        records.append(rec)
        existing.add(plan_id)
        added_plan_ids.append(plan_id)

    new_index = rebuild_index(records, max_tries=2)
    write_index(index_path, new_index)
    return {
        "added_count": len(added_plan_ids),
        "skipped_existing_count": skipped_existing_count,
        "added_plan_ids": added_plan_ids,
        "queue_path": queue_path,
        "index_path": index_path,
    }


def main() -> int:
    args = parse_args()
    windows = parse_windows(args.windows)
    exchange = str(args.exchange).strip().lower()
    stream = str(args.stream).strip().lower()
    category = str(args.category).strip() or "FULLSCAN_MAJOR"
    object_keys_tsv = Path(args.objectKeysTsv).resolve()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR

    out = generate_plans(
        object_keys_tsv=object_keys_tsv,
        exchange=exchange,
        stream=stream,
        windows=windows,
        max_symbols=int(args.max_symbols),
        per_run_timeout_min=int(args.per_run_timeout_min),
        max_wall_min=int(args.max_wall_min),
        category=category,
        state_dir=state_dir,
    )

    print(f"added_count={out['added_count']}")
    print(f"skipped_existing_count={out['skipped_existing_count']}")
    print(f"queue_path={out['queue_path']}")
    print(f"index_path={out['index_path']}")
    print(f"added_plan_ids={','.join(out['added_plan_ids'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

