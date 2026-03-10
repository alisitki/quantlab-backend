#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_JSONL = ROOT / "tools" / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
HISTORY_SCHEMA_VERSION = "shadow_observation_history_v0"
EVENTS_SCHEMA_VERSION = "shadow_execution_events_v1"


class EventsError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise EventsError(message)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic shadow execution event stream from observation history."
    )
    parser.add_argument("--history-jsonl", default=str(DEFAULT_HISTORY_JSONL))
    parser.add_argument("--out-jsonl", default=str(DEFAULT_EVENTS_JSONL))
    return parser.parse_args(argv)


def load_history_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        fail(f"history_jsonl_missing:{path}")
    deduped: dict[str, dict[str, Any]] = {}
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
        deduped[observation_key] = obj
    return sorted(
        deduped.values(),
        key=lambda entry: (
            str(entry.get("observed_at", "")),
            str(entry.get("selected_pack_id", "")),
            str(entry.get("live_run_id", "")),
            str(entry.get("observation_key", "")),
        ),
    )


def normalize_execution_events(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    normalized: list[dict[str, Any]] = []
    for index, raw_event in enumerate(value, start=1):
        if not isinstance(raw_event, dict):
            continue
        event_type = str(raw_event.get("event_type", "")).strip().upper()
        ts_event = str(raw_event.get("ts_event", "")).strip()
        symbol = str(raw_event.get("symbol", "")).strip().upper()
        side = str(raw_event.get("side", "")).strip().upper()
        reason = str(raw_event.get("reason", "")).strip()
        try:
            event_seq = int(raw_event.get("event_seq", index))
        except (TypeError, ValueError):
            event_seq = index
        try:
            qty = float(raw_event.get("qty"))
        except (TypeError, ValueError):
            continue
        try:
            fill_price = float(raw_event.get("fill_price"))
        except (TypeError, ValueError):
            fill_price = None
        try:
            fill_fee = float(raw_event.get("fill_fee"))
        except (TypeError, ValueError):
            fill_fee = None
        try:
            fill_value = float(raw_event.get("fill_value"))
        except (TypeError, ValueError):
            fill_value = None
        if event_type not in {"DECISION", "RISK_REJECT", "FILL"}:
            continue
        if not ts_event or not symbol or not side or qty <= 0:
            continue
        if event_type == "FILL":
            if fill_price is None or fill_price <= 0:
                continue
        else:
            fill_price = None
        normalized.append(
            {
                "event_seq": event_seq if event_seq > 0 else index,
                "event_type": event_type,
                "ts_event": ts_event,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "fill_price": fill_price,
                "fill_fee": fill_fee if event_type == "FILL" and fill_fee is not None and fill_fee >= 0 else None,
                "fill_value": fill_value if event_type == "FILL" and fill_value is not None and fill_value > 0 else None,
                "reason": reason,
            }
        )
    return sorted(
        normalized,
        key=lambda event: (
            int(event.get("event_seq", 0)),
            str(event.get("event_type", "")),
            str(event.get("ts_event", "")),
        ),
    )


def build_event_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        observation_key = str(entry.get("observation_key", "")).strip()
        observed_at = str(entry.get("observed_at", "")).strip()
        selected_pack_id = str(entry.get("selected_pack_id", "")).strip()
        live_run_id = str(entry.get("live_run_id", "")).strip()
        execution_events = normalize_execution_events(entry.get("execution_events"))
        for event in execution_events:
            event_seq = int(event["event_seq"])
            event_id = f"{observation_key}|event|{event_seq}"
            rows.append(
                {
                    "schema_version": EVENTS_SCHEMA_VERSION,
                    "event_id": event_id,
                    "observation_key": observation_key,
                    "observed_at": observed_at,
                    "selected_pack_id": selected_pack_id,
                    "live_run_id": live_run_id,
                    "event_seq": event_seq,
                    "event_type": event["event_type"],
                    "ts_event": event["ts_event"],
                    "symbol": event["symbol"],
                    "side": event["side"],
                    "qty": event["qty"],
                    "fill_price": event["fill_price"],
                    "fill_fee": event.get("fill_fee"),
                    "fill_value": event.get("fill_value"),
                    "reason": event["reason"],
                }
            )
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    history_path = Path(args.history_jsonl).resolve()
    out_path = Path(args.out_jsonl).resolve()

    entries = load_history_entries(history_path)
    rows = build_event_rows(entries)
    write_jsonl(out_path, rows)

    print(f"history_jsonl={history_path}")
    print(f"events_jsonl={out_path}")
    print(f"history_count={len(entries)}")
    print(f"event_count={len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except EventsError as exc:
        print(f"SHADOW_EXECUTION_EVENTS_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
