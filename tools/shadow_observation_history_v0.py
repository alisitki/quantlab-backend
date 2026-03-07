#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY_JSON = ROOT / "tools" / "shadow_state" / "shadow_observation_summary_v0.json"
DEFAULT_HISTORY_JSONL = ROOT / "tools" / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_INDEX_JSON = ROOT / "tools" / "shadow_state" / "shadow_observation_index_v0.json"
SUMMARY_SCHEMA_VERSION = "shadow_observation_summary_v0"
HISTORY_SCHEMA_VERSION = "shadow_observation_history_v0"
INDEX_SCHEMA_VERSION = "shadow_observation_index_v0"


class HistoryError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise HistoryError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append/update deterministic shadow observation history and latest index."
    )
    parser.add_argument(
        "--summary-json",
        action="append",
        default=[],
        help="Summary JSON path. May be passed multiple times. Defaults to tools/shadow_state/shadow_observation_summary_v0.json",
    )
    parser.add_argument(
        "--summary-list",
        default="",
        help="Optional file containing newline-separated summary JSON paths.",
    )
    parser.add_argument("--history-jsonl", default=str(DEFAULT_HISTORY_JSONL))
    parser.add_argument("--index-json", default=str(DEFAULT_INDEX_JSON))
    return parser.parse_args(argv)


def load_json_file(path: Path, label: str) -> Any:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")


def load_summary_paths(args: argparse.Namespace) -> list[Path]:
    raw_paths: list[str] = []
    if args.summary_json:
        raw_paths.extend(str(value) for value in args.summary_json)
    if args.summary_list:
        summary_list_path = Path(args.summary_list).resolve()
        if not summary_list_path.exists():
            fail(f"summary_list_missing:{summary_list_path}")
        raw_paths.extend(
            line.strip()
            for line in summary_list_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if not raw_paths:
        raw_paths.append(str(DEFAULT_SUMMARY_JSON))
    return sorted(Path(value).resolve() for value in raw_paths)


def normalize_symbols(value: Any) -> list[str]:
    if not isinstance(value, list):
        fail("summary_missing_selected_symbols")
    normalized = [str(item or "").strip().upper() for item in value if str(item or "").strip()]
    if not normalized:
        fail("summary_missing_selected_symbols")
    return normalized


def normalize_execution_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {
            "snapshot_present": False,
            "positions_count": 0,
            "fills_count": 0,
            "total_realized_pnl": None,
            "total_unrealized_pnl": None,
            "equity": None,
            "max_position_value": None,
        }

    def parse_int(raw: Any) -> int:
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return 0
        return parsed if parsed >= 0 else 0

    def parse_float_or_none(raw: Any) -> float | None:
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            return None
        return parsed if parsed == parsed else None

    return {
        "snapshot_present": bool(value.get("snapshot_present")),
        "positions_count": parse_int(value.get("positions_count")),
        "fills_count": parse_int(value.get("fills_count")),
        "total_realized_pnl": parse_float_or_none(value.get("total_realized_pnl")),
        "total_unrealized_pnl": parse_float_or_none(value.get("total_unrealized_pnl")),
        "equity": parse_float_or_none(value.get("equity")),
        "max_position_value": parse_float_or_none(value.get("max_position_value")),
    }


def validate_summary(summary: dict[str, Any], path: Path) -> None:
    if summary.get("schema_version") != SUMMARY_SCHEMA_VERSION:
        fail(f"summary_schema_mismatch:{path}")
    required_nonempty = [
        "generated_ts_utc",
        "selected_pack_id",
        "selected_exchange",
        "selected_decision_tier",
        "selected_selection_slot",
        "live_run_id",
        "started_at",
        "finished_at",
        "stop_reason",
    ]
    for field in required_nonempty:
        if not str(summary.get(field, "")).strip():
            fail(f"summary_missing_field:{field}:{path}")
    if "selected_rank" not in summary:
        fail(f"summary_missing_field:selected_rank:{path}")
    normalize_symbols(summary.get("selected_symbols"))


def build_history_entry(summary: dict[str, Any]) -> dict[str, Any]:
    entry = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "observed_at": str(summary["generated_ts_utc"]).strip(),
        "selected_pack_id": str(summary["selected_pack_id"]).strip(),
        "selected_rank": int(summary["selected_rank"]),
        "selected_exchange": str(summary["selected_exchange"]).strip(),
        "selected_symbols": normalize_symbols(summary["selected_symbols"]),
        "selected_decision_tier": str(summary["selected_decision_tier"]).strip(),
        "selected_selection_slot": str(summary["selected_selection_slot"]).strip(),
        "live_run_id": str(summary["live_run_id"]).strip(),
        "started_at": str(summary["started_at"]).strip(),
        "finished_at": str(summary["finished_at"]).strip(),
        "stop_reason": str(summary["stop_reason"]).strip(),
        "run_duration_sec": summary.get("run_duration_sec", "unknown"),
        "verify_soft_live_pass": bool(summary.get("verify_soft_live_pass", False)),
        "processed_event_count": summary.get("processed_event_count", "unknown"),
        "heartbeat_seen": summary.get("heartbeat_seen", "unknown"),
        "execution_summary": normalize_execution_summary(summary.get("execution_summary")),
    }
    entry["observation_key"] = f"{entry['selected_pack_id']}|{entry['live_run_id']}"
    return entry


def load_summary_entry(path: Path) -> dict[str, Any]:
    obj = load_json_file(path, "summary_json")
    if not isinstance(obj, dict):
        fail(f"summary_not_object:{path}")
    validate_summary(obj, path)
    return build_history_entry(obj)


def load_existing_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(f"history_invalid_json:{path}:{lineno}:{exc}")
        if not isinstance(obj, dict):
            fail(f"history_not_object:{path}:{lineno}")
        if obj.get("schema_version") != HISTORY_SCHEMA_VERSION:
            fail(f"history_schema_mismatch:{path}:{lineno}")
        observation_key = str(obj.get("observation_key", "")).strip()
        if not observation_key:
            fail(f"history_missing_observation_key:{path}:{lineno}")
        entries.append(obj)
    return entries


def merge_history(existing_entries: list[dict[str, Any]], new_entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    by_key = {str(entry["observation_key"]).strip(): entry for entry in existing_entries}
    inserted_count = 0
    skipped_duplicate_count = 0
    for entry in new_entries:
        key = entry["observation_key"]
        if key in by_key:
            skipped_duplicate_count += 1
            continue
        by_key[key] = entry
        inserted_count += 1
    merged_entries = sorted(
        by_key.values(),
        key=lambda entry: (
            str(entry.get("observed_at", "")),
            str(entry.get("selected_pack_id", "")),
            str(entry.get("live_run_id", "")),
        ),
    )
    return merged_entries, inserted_count, skipped_duplicate_count


def build_index(entries: list[dict[str, Any]]) -> dict[str, Any]:
    latest_by_pack: dict[str, dict[str, Any]] = {}
    observation_count_by_pack: dict[str, int] = {}
    for entry in entries:
        pack_id = entry["selected_pack_id"]
        observation_count_by_pack[pack_id] = observation_count_by_pack.get(pack_id, 0) + 1
        candidate = {
            "selected_pack_id": pack_id,
            "last_observed_at": entry["observed_at"],
            "last_live_run_id": entry["live_run_id"],
            "last_verify_soft_live_pass": bool(entry["verify_soft_live_pass"]),
            "last_stop_reason": entry["stop_reason"],
            "last_processed_event_count": entry["processed_event_count"],
            "last_execution_summary": normalize_execution_summary(entry.get("execution_summary")),
            "observation_count": 0,
        }
        current = latest_by_pack.get(pack_id)
        if current is None or (candidate["last_observed_at"], candidate["last_live_run_id"]) > (
            current["last_observed_at"],
            current["last_live_run_id"],
        ):
            latest_by_pack[pack_id] = candidate
    for pack_id, count in observation_count_by_pack.items():
        latest_by_pack[pack_id]["observation_count"] = count
    return {
        "schema_version": INDEX_SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "record_count": len(entries),
        "pack_count": len(latest_by_pack),
        "observation_keys": [entry["observation_key"] for entry in entries],
        "pack_ids": sorted(latest_by_pack.keys()),
        "latest_by_pack_id": latest_by_pack,
    }


def write_history_jsonl(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries)
    path.write_text(payload, encoding="utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    summary_paths = load_summary_paths(args)
    history_path = Path(args.history_jsonl).resolve()
    index_path = Path(args.index_json).resolve()

    existing_entries = load_existing_history(history_path)
    seen_new_keys: set[str] = set()
    new_entries: list[dict[str, Any]] = []
    for summary_path in summary_paths:
        entry = load_summary_entry(summary_path)
        if entry["observation_key"] in seen_new_keys:
            continue
        seen_new_keys.add(entry["observation_key"])
        new_entries.append(entry)
    merged_entries, inserted_count, skipped_duplicate_count = merge_history(existing_entries, new_entries)
    index_payload = build_index(merged_entries)

    write_history_jsonl(history_path, merged_entries)
    write_json(index_path, index_payload)

    print(f"history_jsonl={history_path}")
    print(f"index_json={index_path}")
    print(f"input_summary_count={len(summary_paths)}")
    print(f"inserted_count={inserted_count}")
    print(f"skipped_duplicate_count={skipped_duplicate_count}")
    print(f"history_count={len(merged_entries)}")
    print(f"pack_count={index_payload['pack_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HistoryError as exc:
        print(f"SHADOW_OBSERVATION_HISTORY_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
