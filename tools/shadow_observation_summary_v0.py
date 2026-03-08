#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WATCHLIST = ROOT / "tools" / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_SUMMARY_JSON = Path("/tmp/quantlab-soft-live.json")
DEFAULT_AUDIT_SPOOL_DIR = Path("/tmp/quantlab-audit")
DEFAULT_OUT_JSON = ROOT / "tools" / "shadow_state" / "shadow_observation_summary_v0.json"
SCHEMA_VERSION = "shadow_observation_summary_v0"
HEARTBEAT_PATTERN = '"event":"soft_live_heartbeat"'
PROCESSED_PATTERN = re.compile(r"total_processed:\s*(\d+)")


class SummaryError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise SummaryError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize a completed shadow observe run for a selected watchlist item."
    )
    parser.add_argument("--watchlist", default=str(DEFAULT_WATCHLIST))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--audit-spool-dir", default=str(DEFAULT_AUDIT_SPOOL_DIR))
    parser.add_argument("--stdout-log", default="")
    parser.add_argument("--rank", type=int, default=None)
    parser.add_argument("--pack-id", default="")
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    args = parser.parse_args(argv)
    if args.rank is not None and args.rank <= 0:
        fail(f"invalid_rank:{args.rank}")
    args.pack_id = str(args.pack_id or "").strip()
    return args


def load_json_file(path: Path, label: str) -> Any:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    return None


def load_watchlist(path: Path) -> dict[str, Any]:
    obj = load_json_file(path, "watchlist")
    if not isinstance(obj, dict):
        fail(f"watchlist_not_object:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"watchlist_missing_items:{path}")
    if not items:
        fail(f"watchlist_empty:{path}")
    return obj


def resolve_by_rank(items: list[dict[str, Any]], rank: int | None) -> dict[str, Any] | None:
    if rank is None:
        return None
    for item in items:
        try:
            if int(item.get("rank", 0)) == rank:
                return item
        except (TypeError, ValueError):
            continue
    return None


def resolve_by_pack_id(items: list[dict[str, Any]], pack_id: str) -> dict[str, Any] | None:
    if not pack_id:
        return None
    for item in items:
        if str(item.get("pack_id", "")).strip() == pack_id:
            return item
    return None


def normalize_selected_item(item: dict[str, Any]) -> dict[str, Any]:
    exchange = str(item.get("exchange", "")).strip()
    symbols = item.get("symbols")
    if not exchange:
        fail("selected_item_missing_exchange")
    if not isinstance(symbols, list):
        fail("selected_item_missing_symbols")
    normalized_symbols = [str(value or "").strip().upper() for value in symbols if str(value or "").strip()]
    if not normalized_symbols:
        fail("selected_item_missing_symbols")
    return {
        "selected_rank": int(item.get("rank", 0)),
        "selected_pack_id": str(item.get("pack_id", "")).strip(),
        "selected_pack_path": str(item.get("pack_path", "")).strip(),
        "selected_exchange": exchange,
        "selected_symbols": normalized_symbols,
        "selected_decision_tier": str(item.get("decision_tier", "")).strip(),
        "selected_selection_slot": str(item.get("selection_slot", "")).strip(),
    }


def resolve_selected_item(items: list[dict[str, Any]], rank: int | None, pack_id: str) -> dict[str, Any]:
    rank_value = 1 if rank is None and not pack_id else rank
    by_rank = resolve_by_rank(items, rank_value)
    by_pack_id = resolve_by_pack_id(items, pack_id)

    if pack_id and by_pack_id is None:
        fail(f"pack_id_not_found:{pack_id}")
    if rank_value is not None and by_rank is None:
        fail(f"rank_not_found:{rank_value}")
    if pack_id and rank_value is not None:
        if str(by_rank.get("pack_id", "")).strip() != str(by_pack_id.get("pack_id", "")).strip():
            fail(f"selection_conflict:rank={rank_value}:pack_id={pack_id}")
        return by_pack_id
    if pack_id:
        return by_pack_id
    return by_rank


def load_run_summary(path: Path) -> dict[str, Any]:
    obj = load_json_file(path, "summary_json")
    if not isinstance(obj, dict):
        fail(f"summary_json_not_object:{path}")
    live_run_id = str(obj.get("live_run_id", "")).strip()
    if not live_run_id:
        fail(f"summary_json_missing_live_run_id:{path}")
    return obj


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_duration_seconds(started_at: str | None, finished_at: str | None) -> float | str:
    start_dt = parse_iso_utc(started_at)
    finish_dt = parse_iso_utc(finished_at)
    if start_dt is None or finish_dt is None:
        return "unknown"
    return round((finish_dt - start_dt).total_seconds(), 3)


def normalize_execution_summary(run_summary: dict[str, Any]) -> dict[str, Any]:
    raw = run_summary.get("execution_summary")
    if not isinstance(raw, dict):
        return {
            "snapshot_present": False,
            "positions_count": 0,
            "fills_count": 0,
            "total_realized_pnl": None,
            "total_unrealized_pnl": None,
            "equity": None,
            "max_position_value": None,
        }

    def parse_int(value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return parsed if parsed >= 0 else 0

    def parse_float_or_none(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed == parsed else None

    snapshot_present = bool(raw.get("snapshot_present"))
    return {
        "snapshot_present": snapshot_present,
        "positions_count": parse_int(raw.get("positions_count")),
        "fills_count": parse_int(raw.get("fills_count")),
        "total_realized_pnl": parse_float_or_none(raw.get("total_realized_pnl")),
        "total_unrealized_pnl": parse_float_or_none(raw.get("total_unrealized_pnl")),
        "equity": parse_float_or_none(raw.get("equity")),
        "max_position_value": parse_float_or_none(raw.get("max_position_value")),
    }


def normalize_execution_event(action: Any, metadata: Any, *, event_seq: int) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    event_type = str(action or "").strip().upper()
    symbol = str(metadata.get("symbol", "")).strip().upper()
    side = str(metadata.get("side", "")).strip().upper()
    ts_event = str(metadata.get("ts_event", "")).strip()
    reason = str(metadata.get("risk_reason") or metadata.get("reason") or "").strip()
    try:
        qty = float(metadata.get("qty"))
    except (TypeError, ValueError):
        qty = None
    try:
        fill_price = float(metadata.get("fill_price"))
    except (TypeError, ValueError):
        fill_price = None

    if event_type not in {"DECISION", "RISK_REJECT", "FILL"}:
        return None
    if not ts_event or not symbol or not side or qty is None or qty <= 0:
        return None
    if event_type == "FILL":
        if fill_price is None or fill_price <= 0:
            return None
    else:
        fill_price = None

    return {
        "event_seq": int(event_seq),
        "event_type": event_type,
        "ts_event": ts_event,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "fill_price": fill_price,
        "reason": reason,
    }


def scan_audit(audit_spool_dir: Path, live_run_id: str) -> dict[str, Any]:
    if not audit_spool_dir.exists():
        return {
            "audit_dir_exists": False,
            "audit_run_start_seen": False,
            "audit_run_stop_seen": False,
            "execution_events": [],
        }
    start_seen = False
    stop_seen = False
    execution_events: list[dict[str, Any]] = []
    for file_path in sorted(audit_spool_dir.rglob("*.jsonl")):
        try:
            lines = file_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            metadata = parsed.get("metadata") or {}
            if metadata.get("live_run_id") != live_run_id:
                continue
            action = parsed.get("action")
            if action == "RUN_START":
                start_seen = True
            if action == "RUN_STOP":
                stop_seen = True
            normalized_event = normalize_execution_event(action, metadata, event_seq=len(execution_events) + 1)
            if normalized_event is not None:
                execution_events.append(normalized_event)
    return {
        "audit_dir_exists": True,
        "audit_run_start_seen": start_seen,
        "audit_run_stop_seen": stop_seen,
        "execution_events": execution_events,
    }


def parse_stdout_log(stdout_log: Path | None) -> dict[str, Any]:
    if stdout_log is None:
        return {
            "stdout_log_exists": False,
            "heartbeat_seen": "unknown",
            "heartbeat_count": "unknown",
            "processed_event_count": "unknown",
        }
    if not stdout_log.exists():
        return {
            "stdout_log_exists": False,
            "heartbeat_seen": "unknown",
            "heartbeat_count": "unknown",
            "processed_event_count": "unknown",
        }
    try:
        lines = stdout_log.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {
            "stdout_log_exists": False,
            "heartbeat_seen": "unknown",
            "heartbeat_count": "unknown",
            "processed_event_count": "unknown",
        }
    heartbeat_count = sum(1 for line in lines if HEARTBEAT_PATTERN in line)
    processed_event_count: int | str = "unknown"
    for line in lines:
        match = PROCESSED_PATTERN.search(line)
        if match:
            processed_event_count = int(match.group(1))
    return {
        "stdout_log_exists": True,
        "heartbeat_seen": heartbeat_count > 0,
        "heartbeat_count": heartbeat_count,
        "processed_event_count": processed_event_count,
    }


def build_note(audit_info: dict[str, Any], log_info: dict[str, Any]) -> str:
    notes: list[str] = []
    if not audit_info["audit_dir_exists"]:
        notes.append("audit_spool_missing")
    if not log_info["stdout_log_exists"]:
        notes.append("stdout_log_missing_or_not_provided")
    if log_info["processed_event_count"] == "unknown":
        notes.append("processed_event_count_unknown_without_stdout_total_processed_line")
    if log_info["heartbeat_seen"] == "unknown":
        notes.append("heartbeat_seen_unknown_without_stdout_log")
    notes.append("verify_soft_live_pass_inferred_from_summary_json_and_audit")
    return ";".join(notes)


def build_summary(
    selected: dict[str, Any],
    run_summary: dict[str, Any],
    audit_info: dict[str, Any],
    log_info: dict[str, Any],
    *,
    generated_ts_utc: str | None = None,
) -> dict[str, Any]:
    started_at = run_summary.get("started_at")
    finished_at = run_summary.get("finished_at")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc or utc_now_iso(),
        **selected,
        "live_run_id": str(run_summary.get("live_run_id", "")).strip(),
        "started_at": started_at,
        "finished_at": finished_at,
        "stop_reason": run_summary.get("stop_reason"),
        "run_duration_sec": compute_duration_seconds(started_at, finished_at),
        "audit_run_start_seen": audit_info["audit_run_start_seen"],
        "audit_run_stop_seen": audit_info["audit_run_stop_seen"],
        "verify_soft_live_pass": bool(audit_info["audit_run_start_seen"] and audit_info["audit_run_stop_seen"]),
        "processed_event_count": log_info["processed_event_count"],
        "heartbeat_seen": log_info["heartbeat_seen"],
        "heartbeat_count": log_info["heartbeat_count"],
        "execution_summary": normalize_execution_summary(run_summary),
        "execution_events": list(audit_info["execution_events"]),
        "note": build_note(audit_info, log_info),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    watchlist_path = Path(args.watchlist).resolve()
    summary_path = Path(args.summary_json).resolve()
    audit_spool_dir = Path(args.audit_spool_dir).resolve()
    stdout_log = Path(args.stdout_log).resolve() if args.stdout_log else None
    out_json = Path(args.out_json).resolve()

    watchlist = load_watchlist(watchlist_path)
    selected_item = resolve_selected_item(watchlist["items"], args.rank, args.pack_id)
    selected = normalize_selected_item(selected_item)
    run_summary = load_run_summary(summary_path)
    audit_info = scan_audit(audit_spool_dir, selected_run_id := str(run_summary["live_run_id"]).strip())
    log_info = parse_stdout_log(stdout_log)
    summary = build_summary(selected, {"live_run_id": selected_run_id, **run_summary}, audit_info, log_info)
    write_json(out_json, summary)

    print(f"summary_json={out_json}")
    print(f"selected_pack_id={summary['selected_pack_id']}")
    print(f"live_run_id={summary['live_run_id']}")
    print(f"verify_soft_live_pass={int(summary['verify_soft_live_pass'])}")
    print(f"processed_event_count={summary['processed_event_count']}")
    print(f"heartbeat_seen={summary['heartbeat_seen']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SummaryError as exc:
        print(f"SHADOW_OBSERVATION_SUMMARY_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
