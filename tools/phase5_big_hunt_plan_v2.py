#!/usr/bin/env python3
"""Phase-5 Big Hunt v2 plan generator (inventory-driven, multi pair)."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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

try:
    from phase5_state_selection_v1 import InventoryRow, load_inventory
except ImportError:  # pragma: no cover
    from tools.phase5_state_selection_v1 import InventoryRow, load_inventory


WindowWithDays = Tuple[str, str, Tuple[str, ...]]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 Big Hunt v2 plan generator")
    p.add_argument("--exchange", default="")
    p.add_argument("--stream", default="")
    p.add_argument("--exchanges", default="")
    p.add_argument("--streams", default="")
    p.add_argument("--window-days", type=int, default=1)
    p.add_argument("--lookback-days", type=int, default=30)
    p.add_argument("--all-dates", action="store_true")
    p.add_argument("--max-windows", type=int, default=0, help="legacy cap (0=unlimited)")
    p.add_argument("--max-windows-per-pair", type=int, default=0, help="0=unlimited")
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--per-run-timeout-min", type=int, default=12)
    p.add_argument("--max-wall-min", type=int, default=120)
    p.add_argument("--category", default="FULLSCAN_MAJOR")
    p.add_argument("--state-dir", default="")
    p.add_argument("--inventory-state-json", default="/tmp/compacted__state.json")
    p.add_argument("--inventory-bucket", default="quantlab-compact")
    p.add_argument("--inventory-key", default="compacted/_state.json")
    p.add_argument("--inventory-s3-tool", default="/tmp/s3_compact_tool.py")
    p.add_argument("--object-keys-tsv-ref", default="state_selection/object_keys_selected.tsv")
    p.add_argument(
        "--require-quality-pass",
        dest="require_quality_pass",
        action="store_true",
        default=True,
    )
    p.add_argument(
        "--allow-bad-quality",
        dest="require_quality_pass",
        action="store_false",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def parse_csv_lower(raw: str) -> List[str]:
    vals = [str(x).strip().lower() for x in str(raw).split(",") if str(x).strip()]
    return sorted(set(vals))


def resolve_pairs(args: argparse.Namespace) -> List[Tuple[str, str]]:
    exchanges = parse_csv_lower(args.exchanges)
    streams = parse_csv_lower(args.streams)

    if not exchanges and str(args.exchange).strip():
        exchanges = [str(args.exchange).strip().lower()]
    if not streams and str(args.stream).strip():
        streams = [str(args.stream).strip().lower()]
    if not exchanges or not streams:
        raise ValueError("missing_exchange_stream: provide --exchange/--stream or --exchanges/--streams")

    return sorted((ex, st) for ex in exchanges for st in streams)


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
        stderr = "\n".join(str(proc.stderr).splitlines()[-40:])
        raise RuntimeError(
            f"inventory_fetch_failed exit={proc.returncode} bucket={bucket} key={key} stderr={stderr}"
        )
    return state_path


def effective_window_cap(*, max_windows_per_pair: int, max_windows: int) -> int:
    if int(max_windows_per_pair) > 0:
        return int(max_windows_per_pair)
    if int(max_windows) > 0:
        return int(max_windows)
    return 0


def discover_windows_with_days(
    *,
    rows: Sequence[InventoryRow],
    exchange: str,
    stream: str,
    window_days: int,
    lookback_days: int,
    all_dates: bool,
    max_windows: int,
    max_windows_per_pair: int,
    require_quality_pass: bool,
) -> List[WindowWithDays]:
    if window_days <= 0:
        raise ValueError("window-days must be > 0")
    if not all_dates and lookback_days <= 0:
        raise ValueError("lookback-days must be > 0 when --all-dates is false")

    ex = str(exchange).strip().lower()
    st = str(stream).strip().lower()
    dates = sorted(
        {
            r.date
            for r in rows
            if r.exchange == ex
            and r.stream == st
            and r.status == "success"
            and (not require_quality_pass or r.day_quality_post != "BAD")
        }
    )
    if not dates:
        return []
    scoped_dates = dates if all_dates else dates[-int(lookback_days) :]
    if len(scoped_dates) < window_days:
        return []

    windows: List[WindowWithDays] = []
    for idx in range(0, len(scoped_dates) - window_days + 1):
        days = tuple(scoped_dates[idx : idx + window_days])
        windows.append((days[0], days[-1], days))

    cap = effective_window_cap(
        max_windows_per_pair=int(max_windows_per_pair),
        max_windows=int(max_windows),
    )
    if cap > 0 and len(windows) > cap:
        windows = windows[-cap:]
    return windows


def discover_windows(
    *,
    rows: Sequence[InventoryRow],
    exchange: str,
    stream: str,
    window_days: int,
    lookback_days: int,
    max_windows: int,
    require_quality_pass: bool,
) -> List[Tuple[str, str]]:
    out = discover_windows_with_days(
        rows=rows,
        exchange=exchange,
        stream=stream,
        window_days=window_days,
        lookback_days=lookback_days,
        all_dates=False,
        max_windows=max_windows,
        max_windows_per_pair=0,
        require_quality_pass=require_quality_pass,
    )
    return [(s, e) for s, e, _ in out]


def build_plan_record(
    *,
    plan_id: str,
    exchange: str,
    stream: str,
    start: str,
    end: str,
    object_keys_tsv_ref: str,
    max_symbols: int,
    per_run_timeout_min: int,
    max_wall_min: int,
    category: str,
    eligible_symbol_count: int = 0,
    selected_symbols_preview: str = "",
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
        "object_keys_tsv": object_keys_tsv_ref,
        "max_symbols": int(max_symbols),
        "per_run_timeout_min": int(per_run_timeout_min),
        "max_wall_min": int(max_wall_min),
        "category": category,
        "selection_rule": "full_window_coverage_sorted_first_N",
        "eligible_symbol_count": int(eligible_symbol_count),
        "selected_symbols_preview": str(selected_symbols_preview),
        "status": "PENDING",
        "tries": 0,
        "last_run_id": None,
        "last_archive_dir": None,
        "last_decision": None,
        "last_error": None,
    }


def enqueue_windows_from_inventory(
    *,
    state_dir: Path,
    object_keys_tsv_ref: str,
    exchange: str,
    stream: str,
    windows: Sequence[Tuple[str, str]],
    max_symbols: int,
    per_run_timeout_min: int,
    max_wall_min: int,
    category: str,
    dry_run: bool,
    window_debug_by_range: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    queue_path, index_path = ensure_state_files(state_dir)
    records = load_queue_records(queue_path)
    idx = rebuild_index(records, max_tries=2)
    latest = idx.get("plan_latest", {})

    added_plan_ids: List[str] = []
    would_add_plan_ids: List[str] = []
    skipped_done_count = 0
    skipped_existing_count = 0

    for start, end in windows:
        plan_id = canonical_plan_id(
            exchange=exchange,
            stream=stream,
            start=start,
            end=end,
            object_keys_tsv=object_keys_tsv_ref,
            max_symbols=max_symbols,
            per_run_timeout_min=per_run_timeout_min,
            max_wall_min=max_wall_min,
            category=category,
        )
        existing = latest.get(plan_id)
        if isinstance(existing, dict):
            if str(existing.get("status", "")).strip() == "DONE":
                skipped_done_count += 1
            else:
                skipped_existing_count += 1
            continue

        would_add_plan_ids.append(plan_id)
        if dry_run:
            continue

        dbg = (window_debug_by_range or {}).get((start, end), {})
        rec = build_plan_record(
            plan_id=plan_id,
            exchange=exchange,
            stream=stream,
            start=start,
            end=end,
            object_keys_tsv_ref=object_keys_tsv_ref,
            max_symbols=max_symbols,
            per_run_timeout_min=per_run_timeout_min,
            max_wall_min=max_wall_min,
            category=category,
            eligible_symbol_count=int(dbg.get("eligible_symbol_count", 0) or 0),
            selected_symbols_preview=str(dbg.get("selected_symbols_preview", "")),
        )
        append_queue_record(queue_path, rec)
        records.append(rec)
        latest[plan_id] = rec
        added_plan_ids.append(plan_id)

    new_idx = rebuild_index(records, max_tries=2)
    write_index(index_path, new_idx)
    return {
        "queue_path": queue_path,
        "index_path": index_path,
        "added_plan_ids": added_plan_ids,
        "would_add_plan_ids": would_add_plan_ids,
        "added_count": len(added_plan_ids),
        "would_add_count": len(would_add_plan_ids),
        "skipped_done_count": skipped_done_count,
        "skipped_existing_count": skipped_existing_count,
    }


def run_plan_generation(args: argparse.Namespace, *, repo: Path) -> Dict[str, Any]:
    pairs = resolve_pairs(args)
    category = str(args.category).strip() or "FULLSCAN_MAJOR"
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    object_keys_tsv_ref = str(args.object_keys_tsv_ref).strip() or "state_selection/object_keys_selected.tsv"

    inventory_state = ensure_inventory_state(
        state_path=Path(args.inventory_state_json).resolve(),
        bucket=str(args.inventory_bucket),
        key=str(args.inventory_key),
        s3_tool=str(args.inventory_s3_tool),
        repo=repo,
    )
    rows = load_inventory(inventory_state, bucket=str(args.inventory_bucket))

    all_windows_labels: List[str] = []
    added_plan_ids: List[str] = []
    would_add_plan_ids: List[str] = []
    added_count = 0
    would_add_count = 0
    skipped_done_count = 0
    skipped_existing_count = 0
    skipped_no_coverage = 0
    windows_total = 0
    queue_path: Optional[Path] = None
    index_path: Optional[Path] = None

    for exchange, stream in pairs:
        pair_rows = [
            r
            for r in rows
            if r.exchange == exchange
            and r.stream == stream
            and r.status == "success"
            and (not bool(args.require_quality_pass) or r.day_quality_post != "BAD")
        ]
        windows_with_days = discover_windows_with_days(
            rows=pair_rows,
            exchange=exchange,
            stream=stream,
            window_days=int(args.window_days),
            lookback_days=int(args.lookback_days),
            all_dates=bool(args.all_dates),
            max_windows=int(args.max_windows),
            max_windows_per_pair=int(args.max_windows_per_pair),
            require_quality_pass=bool(args.require_quality_pass),
        )
        windows_total += len(windows_with_days)
        all_windows_labels.extend([f"{exchange}/{stream}/{s}..{e}" for s, e, _ in windows_with_days])

        symbol_days: Dict[str, set[str]] = {}
        for r in pair_rows:
            symbol_days.setdefault(r.symbol, set()).add(r.date)

        eligible_windows: List[Tuple[str, str]] = []
        window_debug_by_range: Dict[Tuple[str, str], Dict[str, Any]] = {}
        max_symbols = int(args.max_symbols)
        if max_symbols <= 0:
            raise ValueError("max-symbols must be > 0")

        for start, end, needed_days in windows_with_days:
            needed = set(needed_days)
            eligible_symbols = sorted(
                sym for sym, covered_days in symbol_days.items() if needed.issubset(covered_days)
            )
            selected_symbols = eligible_symbols[:max_symbols]
            if not selected_symbols:
                skipped_no_coverage += 1
                continue
            eligible_windows.append((start, end))
            window_debug_by_range[(start, end)] = {
                "eligible_symbol_count": len(eligible_symbols),
                "selected_symbols_preview": ",".join(selected_symbols[:10]),
            }

        if not eligible_windows:
            continue

        stats = enqueue_windows_from_inventory(
            state_dir=state_dir,
            object_keys_tsv_ref=object_keys_tsv_ref,
            exchange=exchange,
            stream=stream,
            windows=eligible_windows,
            max_symbols=max_symbols,
            per_run_timeout_min=int(args.per_run_timeout_min),
            max_wall_min=int(args.max_wall_min),
            category=category,
            dry_run=bool(args.dry_run),
            window_debug_by_range=window_debug_by_range,
        )
        queue_path = stats["queue_path"]
        index_path = stats["index_path"]
        added_count += int(stats["added_count"])
        would_add_count += int(stats["would_add_count"])
        skipped_done_count += int(stats["skipped_done_count"])
        skipped_existing_count += int(stats["skipped_existing_count"])
        added_plan_ids.extend(stats["added_plan_ids"])
        would_add_plan_ids.extend(stats["would_add_plan_ids"])

    final_queue_path, final_index_path = ensure_state_files(state_dir)
    final_records = load_queue_records(final_queue_path)
    final_index = rebuild_index(final_records, max_tries=2)
    pending_total_after = len(final_index.get("pending_plan_ids", []) or [])

    if queue_path is None:
        queue_path = final_queue_path
    if index_path is None:
        index_path = final_index_path

    windows_plain = [w.split("/", 2)[-1] for w in all_windows_labels]
    return {
        "inventory_state_json": str(inventory_state),
        "exchange": str(args.exchange).strip().lower(),
        "stream": str(args.stream).strip().lower(),
        "exchanges": ",".join(sorted({ex for ex, _ in pairs})),
        "streams": ",".join(sorted({st for _, st in pairs})),
        "pairs_considered": len(pairs),
        "window_days": int(args.window_days),
        "lookback_days": int(args.lookback_days),
        "all_dates": bool(args.all_dates),
        "max_windows": int(args.max_windows),
        "max_windows_per_pair": int(args.max_windows_per_pair),
        "windows_considered": windows_total,  # backward-compat key
        "windows_total": windows_total,
        "windows_csv": ",".join(windows_plain),
        "windows_first3": ",".join(all_windows_labels[:3]),
        "windows_last3": ",".join(all_windows_labels[-3:]),
        "dry_run": bool(args.dry_run),
        "added_count": added_count,
        "would_add_count": would_add_count,
        "skipped_done_count": skipped_done_count,
        "skipped_existing_count": skipped_existing_count,
        "skipped_no_coverage": skipped_no_coverage,
        "pending_total_after": pending_total_after,
        "added_plan_ids": added_plan_ids,
        "would_add_plan_ids": would_add_plan_ids,
        "queue_path": queue_path,
        "index_path": index_path,
    }


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[1]
    out = run_plan_generation(args, repo=repo)
    print(f"inventory_state_json={out['inventory_state_json']}")
    print(f"exchange={out['exchange']}")
    print(f"stream={out['stream']}")
    print(f"exchanges={out['exchanges']}")
    print(f"streams={out['streams']}")
    print(f"pairs_considered={out['pairs_considered']}")
    print(f"window_days={out['window_days']}")
    print(f"lookback_days={out['lookback_days']}")
    print(f"all_dates={str(out['all_dates']).lower()}")
    print(f"max_windows={out['max_windows']}")
    print(f"max_windows_per_pair={out['max_windows_per_pair']}")
    print(f"windows_considered={out['windows_considered']}")
    print(f"windows_total={out['windows_total']}")
    print(f"windows_csv={out['windows_csv']}")
    print(f"windows_first3={out['windows_first3']}")
    print(f"windows_last3={out['windows_last3']}")
    print(f"dry_run={str(out['dry_run']).lower()}")
    print(f"added_count={out['added_count']}")
    print(f"would_add_count={out['would_add_count']}")
    print(f"skipped_done_count={out['skipped_done_count']}")
    print(f"skipped_existing_count={out['skipped_existing_count']}")
    print(f"skipped_no_coverage={out['skipped_no_coverage']}")
    print(f"pending_total_after={out['pending_total_after']}")
    print(f"queue_path={out['queue_path']}")
    print(f"index_path={out['index_path']}")
    print(f"added_plan_ids={','.join(out['added_plan_ids'])}")
    print(f"would_add_plan_ids={','.join(out['would_add_plan_ids'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
