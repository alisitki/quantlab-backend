#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXECUTION_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_ledger_v0.jsonl"
DEFAULT_EXECUTION_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
DEFAULT_TRADE_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_trade_ledger_v1.jsonl"
EXECUTION_LEDGER_SCHEMA_VERSION = "shadow_execution_ledger_v0"
EXECUTION_EVENTS_SCHEMA_VERSION = "shadow_execution_events_v1"
TRADE_LEDGER_SCHEMA_VERSION = "shadow_trade_ledger_v1"
REQUIRED_LEDGER_FIELDS = {
    "schema_version",
    "observation_key",
    "observed_at",
    "selected_pack_id",
    "live_run_id",
    "snapshot_present",
    "positions_count",
    "fills_count",
    "total_realized_pnl",
    "total_unrealized_pnl",
    "max_position_value",
}
REQUIRED_EVENT_FIELDS = {
    "schema_version",
    "event_id",
    "observation_key",
    "observed_at",
    "selected_pack_id",
    "live_run_id",
    "event_seq",
    "event_type",
    "ts_event",
    "symbol",
    "side",
    "qty",
    "fill_price",
    "reason",
}


class TradeLedgerError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise TradeLedgerError(message)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a minimal synthetic shadow trade ledger from the execution ledger."
    )
    parser.add_argument("--execution-ledger-jsonl", default=str(DEFAULT_EXECUTION_LEDGER_JSONL))
    parser.add_argument("--execution-events-jsonl", default=str(DEFAULT_EXECUTION_EVENTS_JSONL))
    parser.add_argument("--out-jsonl", default=str(DEFAULT_TRADE_LEDGER_JSONL))
    return parser.parse_args(argv)


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


def load_execution_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(f"execution_ledger_invalid_json:{path}:{lineno}:{exc}")
        if not isinstance(obj, dict):
            fail(f"execution_ledger_not_object:{path}:{lineno}")
        if obj.get("schema_version") != EXECUTION_LEDGER_SCHEMA_VERSION:
            fail(f"execution_ledger_schema_mismatch:{path}:{lineno}")
        missing = sorted(field for field in REQUIRED_LEDGER_FIELDS if field not in obj)
        if missing:
            fail(f"execution_ledger_missing_fields:{path}:{lineno}:{','.join(missing)}")
        rows.append(
            {
                "observation_key": str(obj.get("observation_key") or "").strip(),
                "observed_at": str(obj.get("observed_at") or "").strip(),
                "selected_pack_id": str(obj.get("selected_pack_id") or "").strip(),
                "live_run_id": str(obj.get("live_run_id") or "").strip(),
                "snapshot_present": bool(obj.get("snapshot_present")),
                "positions_count": parse_int(obj.get("positions_count")),
                "fills_count": parse_int(obj.get("fills_count")),
                "total_realized_pnl": parse_float_or_none(obj.get("total_realized_pnl")),
                "total_unrealized_pnl": parse_float_or_none(obj.get("total_unrealized_pnl")),
                "max_position_value": parse_float_or_none(obj.get("max_position_value")),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["selected_pack_id"],
            row["observed_at"],
            row["live_run_id"],
            row["observation_key"],
        ),
    )


def load_execution_events(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(f"execution_events_invalid_json:{path}:{lineno}:{exc}")
        if not isinstance(obj, dict):
            fail(f"execution_events_not_object:{path}:{lineno}")
        if obj.get("schema_version") != EXECUTION_EVENTS_SCHEMA_VERSION:
            fail(f"execution_events_schema_mismatch:{path}:{lineno}")
        missing = sorted(field for field in REQUIRED_EVENT_FIELDS if field not in obj)
        if missing:
            fail(f"execution_events_missing_fields:{path}:{lineno}:{','.join(missing)}")
        observation_key = str(obj.get("observation_key") or "").strip()
        selected_pack_id = str(obj.get("selected_pack_id") or "").strip()
        live_run_id = str(obj.get("live_run_id") or "").strip()
        event_type = str(obj.get("event_type") or "").strip().upper()
        ts_event = str(obj.get("ts_event") or "").strip()
        symbol = str(obj.get("symbol") or "").strip().upper()
        side = str(obj.get("side") or "").strip().upper()
        reason = str(obj.get("reason") or "").strip()
        try:
            event_seq = int(obj.get("event_seq"))
        except (TypeError, ValueError):
            fail(f"execution_events_invalid_event_seq:{path}:{lineno}")
        qty = parse_float_or_none(obj.get("qty"))
        fill_price = parse_float_or_none(obj.get("fill_price"))
        if not observation_key:
            fail(f"execution_events_missing_observation_key:{path}:{lineno}")
        if not selected_pack_id:
            fail(f"execution_events_missing_selected_pack_id:{path}:{lineno}")
        if not live_run_id:
            fail(f"execution_events_missing_live_run_id:{path}:{lineno}")
        if event_type not in {"DECISION", "RISK_REJECT", "FILL"}:
            fail(f"execution_events_invalid_event_type:{path}:{lineno}:{event_type}")
        if not ts_event:
            fail(f"execution_events_missing_ts_event:{path}:{lineno}")
        if not symbol:
            fail(f"execution_events_missing_symbol:{path}:{lineno}")
        if side not in {"BUY", "SELL"}:
            fail(f"execution_events_invalid_side:{path}:{lineno}:{side}")
        if event_seq <= 0:
            fail(f"execution_events_invalid_event_seq:{path}:{lineno}")
        if qty is None or qty <= 0:
            fail(f"execution_events_invalid_qty:{path}:{lineno}")
        if event_type == "FILL":
            if fill_price is None or fill_price <= 0:
                fail(f"execution_events_invalid_fill_price:{path}:{lineno}")
        else:
            fill_price = None
        grouped.setdefault(observation_key, []).append(
            {
                "event_id": str(obj.get("event_id") or "").strip(),
                "observation_key": observation_key,
                "observed_at": str(obj.get("observed_at") or "").strip(),
                "selected_pack_id": selected_pack_id,
                "live_run_id": live_run_id,
                "event_seq": event_seq,
                "event_type": event_type,
                "ts_event": ts_event,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "fill_price": fill_price,
                "reason": reason,
            }
        )
    for observation_key, events in grouped.items():
        grouped[observation_key] = sorted(
            events,
            key=lambda event: (
                int(event["event_seq"]),
                str(event["event_type"]),
                str(event["ts_event"]),
                str(event["event_id"]),
            ),
        )
    return grouped


def pnl_delta(current_total: float | None, anchor_total: float) -> float | None:
    if current_total is None:
        return None
    return current_total - anchor_total


def max_float(current: float | None, candidate: float | None) -> float | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def make_trade_id(pack_id: str, opened_at: str, open_live_run_id: str) -> str:
    return f"{pack_id}|trade|{opened_at}|{open_live_run_id}"


def start_position_trade(row: dict[str, Any], realized_anchor: float) -> dict[str, Any]:
    return {
        "schema_version": TRADE_LEDGER_SCHEMA_VERSION,
        "trade_id": make_trade_id(row["selected_pack_id"], row["observed_at"], row["live_run_id"]),
        "selected_pack_id": row["selected_pack_id"],
        "trade_mode": "POSITION_LIFECYCLE",
        "status": "OPEN",
        "opened_at": row["observed_at"],
        "last_observed_at": row["observed_at"],
        "closed_at": None,
        "open_live_run_id": row["live_run_id"],
        "last_live_run_id": row["live_run_id"],
        "observation_count": 1,
        "realized_pnl_delta": pnl_delta(row["total_realized_pnl"], realized_anchor),
        "latest_unrealized_pnl": row["total_unrealized_pnl"],
        "max_position_value_seen": row["max_position_value"],
        "side": None,
        "open_reason": "STATE_POSITION_OPENED",
        "close_reason": None,
        "entry_event_type": None,
        "exit_event_type": None,
        "entry_ts_event": None,
        "exit_ts_event": None,
        "entry_price": None,
        "exit_price": None,
        "_realized_anchor": realized_anchor,
        "_observation_keys": [row["observation_key"]],
        "_open_observation_key": row["observation_key"],
        "_close_observation_key": None,
    }


def update_position_trade(active_trade: dict[str, Any], row: dict[str, Any]) -> None:
    active_trade["last_observed_at"] = row["observed_at"]
    active_trade["last_live_run_id"] = row["live_run_id"]
    active_trade["observation_count"] = int(active_trade["observation_count"]) + 1
    active_trade["realized_pnl_delta"] = pnl_delta(
        row["total_realized_pnl"],
        float(active_trade["_realized_anchor"]),
    )
    active_trade["latest_unrealized_pnl"] = row["total_unrealized_pnl"]
    active_trade["max_position_value_seen"] = max_float(
        parse_float_or_none(active_trade.get("max_position_value_seen")),
        row["max_position_value"],
    )
    if row["observation_key"] not in active_trade["_observation_keys"]:
        active_trade["_observation_keys"].append(row["observation_key"])


def close_position_trade(active_trade: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    update_position_trade(active_trade, row)
    active_trade["status"] = "CLOSED"
    active_trade["closed_at"] = row["observed_at"]
    active_trade["_close_observation_key"] = row["observation_key"]
    return active_trade


def intrarun_trade(row: dict[str, Any], realized_anchor: float) -> dict[str, Any]:
    return {
        "schema_version": TRADE_LEDGER_SCHEMA_VERSION,
        "trade_id": make_trade_id(row["selected_pack_id"], row["observed_at"], row["live_run_id"]),
        "selected_pack_id": row["selected_pack_id"],
        "trade_mode": "INTRARUN_REALIZED",
        "status": "CLOSED",
        "opened_at": row["observed_at"],
        "last_observed_at": row["observed_at"],
        "closed_at": row["observed_at"],
        "open_live_run_id": row["live_run_id"],
        "last_live_run_id": row["live_run_id"],
        "observation_count": 1,
        "realized_pnl_delta": pnl_delta(row["total_realized_pnl"], realized_anchor),
        "latest_unrealized_pnl": row["total_unrealized_pnl"],
        "max_position_value_seen": row["max_position_value"],
        "side": None,
        "open_reason": "STATE_INTRARUN_REALIZED",
        "close_reason": "STATE_INTRARUN_REALIZED",
        "entry_event_type": None,
        "exit_event_type": None,
        "entry_ts_event": None,
        "exit_ts_event": None,
        "entry_price": None,
        "exit_price": None,
        "_observation_keys": [row["observation_key"]],
        "_open_observation_key": row["observation_key"],
        "_close_observation_key": row["observation_key"],
    }


def find_first_event(events: list[dict[str, Any]], *, side: str | None = None, event_type: str | None = None) -> dict[str, Any] | None:
    for event in events:
        if side is not None and str(event.get("side")) != side:
            continue
        if event_type is not None and str(event.get("event_type")) != event_type:
            continue
        return event
    return None


def find_last_event(events: list[dict[str, Any]], *, side: str | None = None, event_type: str | None = None) -> dict[str, Any] | None:
    for event in reversed(events):
        if side is not None and str(event.get("side")) != side:
            continue
        if event_type is not None and str(event.get("event_type")) != event_type:
            continue
        return event
    return None


def derive_trade_side(events: list[dict[str, Any]]) -> str | None:
    for event in events:
        if str(event.get("event_type")) not in {"DECISION", "FILL"}:
            continue
        side = str(event.get("side") or "").strip().upper()
        if side in {"BUY", "SELL"}:
            return side
    return None


def enrich_trade_with_events(trade: dict[str, Any], events_by_observation_key: dict[str, list[dict[str, Any]]]) -> None:
    open_observation_key = str(trade.get("_open_observation_key") or "").strip()
    close_observation_key = str(trade.get("_close_observation_key") or "").strip()
    open_events = [
        event
        for event in events_by_observation_key.get(open_observation_key, [])
        if str(event.get("selected_pack_id")) == str(trade.get("selected_pack_id"))
    ]
    if not open_events:
        return

    side = derive_trade_side(open_events)
    trade["side"] = side
    if side is None:
        return

    opposite_side = "SELL" if side == "BUY" else "BUY"
    first_same_side_event = find_first_event(open_events, side=side)
    first_same_side_fill = find_first_event(open_events, side=side, event_type="FILL")

    if first_same_side_event is not None:
        trade["entry_event_type"] = first_same_side_event["event_type"]
        trade["entry_ts_event"] = first_same_side_event["ts_event"]
        trade["open_reason"] = f"EVENT_{first_same_side_event['event_type']}"
    if first_same_side_fill is not None:
        trade["entry_price"] = first_same_side_fill["fill_price"]

    if str(trade.get("status")) != "CLOSED":
        return

    close_events = [
        event
        for event in events_by_observation_key.get(close_observation_key, [])
        if str(event.get("selected_pack_id")) == str(trade.get("selected_pack_id"))
    ]
    last_opposite_side_event = find_last_event(close_events, side=opposite_side)
    last_opposite_side_fill = find_last_event(close_events, side=opposite_side, event_type="FILL")
    if last_opposite_side_event is not None:
        trade["exit_event_type"] = last_opposite_side_event["event_type"]
        trade["exit_ts_event"] = last_opposite_side_event["ts_event"]
        trade["close_reason"] = f"EVENT_{last_opposite_side_event['event_type']}"
    elif str(trade.get("trade_mode")) == "INTRARUN_REALIZED":
        trade["close_reason"] = "STATE_INTRARUN_REALIZED"
    else:
        trade["close_reason"] = "STATE_POSITION_CLOSED"
    if last_opposite_side_fill is not None:
        trade["exit_price"] = last_opposite_side_fill["fill_price"]


def finalize_trade(trade: dict[str, Any], events_by_observation_key: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    enrich_trade_with_events(trade, events_by_observation_key)
    if str(trade.get("status")) == "CLOSED" and trade.get("close_reason") is None:
        if str(trade.get("trade_mode")) == "INTRARUN_REALIZED":
            trade["close_reason"] = "STATE_INTRARUN_REALIZED"
        else:
            trade["close_reason"] = "STATE_POSITION_CLOSED"
    return {key: value for key, value in trade.items() if not key.startswith("_")}


def build_trade_rows(rows: list[dict[str, Any]], events_by_observation_key: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        pack_id = row["selected_pack_id"]
        if not pack_id:
            fail(f"execution_ledger_missing_selected_pack_id:{row.get('observation_key', '')}")
        grouped.setdefault(pack_id, []).append(row)

    trades: list[dict[str, Any]] = []
    for pack_id in sorted(grouped):
        realized_anchor = 0.0
        active_trade: dict[str, Any] | None = None
        for row in grouped[pack_id]:
            snapshot_present = bool(row["snapshot_present"])
            positions_count = int(row["positions_count"])
            fills_count = int(row["fills_count"])
            current_realized = parse_float_or_none(row["total_realized_pnl"])

            if active_trade is not None:
                if snapshot_present and positions_count > 0:
                    update_position_trade(active_trade, row)
                    continue
                if snapshot_present and positions_count <= 0:
                    trades.append(close_position_trade(active_trade, row))
                    active_trade = None
                    if current_realized is not None:
                        realized_anchor = current_realized
                    continue
                continue

            if snapshot_present and positions_count > 0:
                active_trade = start_position_trade(row, realized_anchor)
                continue

            if snapshot_present and positions_count <= 0 and fills_count > 0:
                trades.append(intrarun_trade(row, realized_anchor))
                if current_realized is not None:
                    realized_anchor = current_realized
                continue

            if snapshot_present and current_realized is not None:
                realized_anchor = current_realized

        if active_trade is not None:
            trades.append(active_trade)

    return [finalize_trade(trade, events_by_observation_key) for trade in trades]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    execution_ledger_path = Path(args.execution_ledger_jsonl).resolve()
    execution_events_path = Path(args.execution_events_jsonl).resolve()
    out_path = Path(args.out_jsonl).resolve()

    rows = load_execution_rows(execution_ledger_path)
    events_by_observation_key = load_execution_events(execution_events_path)
    trade_rows = build_trade_rows(rows, events_by_observation_key)
    write_jsonl(out_path, trade_rows)

    print(f"execution_ledger_jsonl={execution_ledger_path}")
    print(f"execution_events_jsonl={execution_events_path}")
    print(f"trade_ledger_jsonl={out_path}")
    print(f"source_row_count={len(rows)}")
    print(f"trade_count={len(trade_rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TradeLedgerError as exc:
        print(f"SHADOW_TRADE_LEDGER_V1_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
