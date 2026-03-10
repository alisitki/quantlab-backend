#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_JSONL = ROOT / "tools" / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_EXECUTION_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
DEFAULT_BINDING_ARTIFACT = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_OUT_JSON = ROOT / "tools" / "shadow_state" / "shadow_futures_paper_ledger_v1.json"
HISTORY_SCHEMA_VERSION = "shadow_observation_history_v0"
EXECUTION_EVENTS_SCHEMA_VERSION = "shadow_execution_events_v1"
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
OUTPUT_SCHEMA_VERSION = "shadow_futures_paper_ledger_v1"
EPSILON = 1e-12

COST_ACCOUNTING_CAPABILITIES = {
    "fill_fee_accounting": "SUPPORTED_NOW_IF_FILL_FEE_PERSISTED",
    "turnover_accounting": "SUPPORTED_NOW_FROM_FILL_VALUE_OR_PRICE_QTY",
    "gross_realized_pnl": "SUPPORTED_NOW",
    "net_realized_pnl": "SUPPORTED_NOW_IF_FEE_BACKED",
    "gross_mark_to_market_pnl": "SUPPORTED_NOW_IF_MARK_PRICE_AVAILABLE",
    "net_mark_to_market_after_paid_fees": "SUPPORTED_NOW_IF_FEE_AND_MARK_BACKED",
    "exit_fee_estimate": "PARTIAL_MODEL_BASED",
    "funding_cost": "SUPPORTED_NOW_IF_FUNDING_AND_MARK_EVENTS_ALIGN_TO_CROSSED_BOUNDARY",
    "margin_accounting": "UNSUPPORTED",
    "exchange_leverage": "UNSUPPORTED",
    "exposure_ratio": "PARTIAL_SUMMARY_BACKED",
}


class FuturesPaperLedgerError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise FuturesPaperLedgerError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a futures-aware shadow paper ledger from persisted history and execution events."
    )
    parser.add_argument("--history-jsonl", default=str(DEFAULT_HISTORY_JSONL))
    parser.add_argument("--execution-events-jsonl", default=str(DEFAULT_EXECUTION_EVENTS_JSONL))
    parser.add_argument("--binding-artifact", default=str(DEFAULT_BINDING_ARTIFACT))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    return parser.parse_args(argv)


def parse_float_or_none(raw: Any) -> float | None:
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def parse_int(raw: Any) -> int:
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


def normalize_symbols(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized = [str(item or "").strip().upper() for item in value if str(item or "").strip()]
    return normalized


def normalize_execution_positions(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    positions: dict[str, dict[str, Any]] = {}
    for symbol, raw_position in value.items():
        if not isinstance(raw_position, dict):
            continue
        normalized_symbol = str(symbol or raw_position.get("symbol") or "").strip().upper()
        if not normalized_symbol:
            continue
        positions[normalized_symbol] = {
            "symbol": normalized_symbol,
            "size": parse_float_or_none(raw_position.get("size")),
            "avg_entry_price": parse_float_or_none(raw_position.get("avg_entry_price")),
            "realized_pnl": parse_float_or_none(raw_position.get("realized_pnl")),
            "unrealized_pnl": parse_float_or_none(raw_position.get("unrealized_pnl")),
            "current_price": parse_float_or_none(raw_position.get("current_price")),
        }
    return positions


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
            "positions": {},
        }
    return {
        "snapshot_present": bool(value.get("snapshot_present")),
        "positions_count": parse_int(value.get("positions_count")),
        "fills_count": parse_int(value.get("fills_count")),
        "total_realized_pnl": parse_float_or_none(value.get("total_realized_pnl")),
        "total_unrealized_pnl": parse_float_or_none(value.get("total_unrealized_pnl")),
        "equity": parse_float_or_none(value.get("equity")),
        "max_position_value": parse_float_or_none(value.get("max_position_value")),
        "positions": normalize_execution_positions(value.get("positions")),
    }


def normalize_execution_events(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, raw_event in enumerate(value, start=1):
        if not isinstance(raw_event, dict):
            continue
        event_type = str(raw_event.get("event_type", "")).strip().upper()
        if event_type not in {"DECISION", "RISK_REJECT", "FILL"}:
            continue
        ts_event = str(raw_event.get("ts_event", "")).strip()
        symbol = str(raw_event.get("symbol", "")).strip().upper()
        side = str(raw_event.get("side", "")).strip().upper()
        if not ts_event or not symbol or side not in {"BUY", "SELL"}:
            continue
        qty = parse_float_or_none(raw_event.get("qty"))
        if qty is None or qty <= 0:
            continue
        fill_price = parse_float_or_none(raw_event.get("fill_price"))
        fill_fee = parse_float_or_none(raw_event.get("fill_fee"))
        fill_value = parse_float_or_none(raw_event.get("fill_value"))
        if event_type == "FILL" and (fill_price is None or fill_price <= 0):
            continue
        if event_type != "FILL":
            fill_price = None
            fill_fee = None
            fill_value = None
        event_seq = parse_int(raw_event.get("event_seq", index))
        normalized.append(
            {
                "event_seq": event_seq if event_seq > 0 else index,
                "event_type": event_type,
                "ts_event": ts_event,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "fill_price": fill_price,
                "fill_fee": fill_fee if fill_fee is not None and fill_fee >= 0 else None,
                "fill_value": fill_value if fill_value is not None and fill_value > 0 else None,
                "reason": str(raw_event.get("reason", "")).strip(),
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


def normalize_funding_events(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, raw_event in enumerate(value, start=1):
        if not isinstance(raw_event, dict):
            continue
        ts_event = str(raw_event.get("ts_event", "")).strip()
        next_funding_ts = str(raw_event.get("next_funding_ts", "")).strip()
        exchange = str(raw_event.get("exchange", "")).strip().lower()
        symbol = str(raw_event.get("symbol", "")).strip().upper()
        funding_rate = parse_float_or_none(raw_event.get("funding_rate"))
        if not ts_event or not next_funding_ts or not exchange or not symbol or funding_rate is None:
            continue
        event_seq = parse_int(raw_event.get("event_seq", index))
        normalized.append(
            {
                "event_seq": event_seq if event_seq > 0 else index,
                "ts_event": ts_event,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate": funding_rate,
                "next_funding_ts": next_funding_ts,
            }
        )
    return sorted(
        normalized,
        key=lambda event: (
            int(event.get("event_seq", 0)),
            str(event.get("ts_event", "")),
            str(event.get("next_funding_ts", "")),
            str(event.get("symbol", "")),
        ),
    )


def normalize_mark_price_events(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, raw_event in enumerate(value, start=1):
        if not isinstance(raw_event, dict):
            continue
        ts_event = str(raw_event.get("ts_event", "")).strip()
        exchange = str(raw_event.get("exchange", "")).strip().lower()
        symbol = str(raw_event.get("symbol", "")).strip().upper()
        mark_price = parse_float_or_none(raw_event.get("mark_price"))
        index_price = parse_float_or_none(raw_event.get("index_price"))
        if not ts_event or not exchange or not symbol or mark_price is None or mark_price <= 0:
            continue
        event_seq = parse_int(raw_event.get("event_seq", index))
        normalized.append(
            {
                "event_seq": event_seq if event_seq > 0 else index,
                "ts_event": ts_event,
                "exchange": exchange,
                "symbol": symbol,
                "mark_price": mark_price,
                "index_price": index_price if index_price is not None and index_price > 0 else None,
            }
        )
    return sorted(
        normalized,
        key=lambda event: (
            int(event.get("event_seq", 0)),
            str(event.get("ts_event", "")),
            str(event.get("symbol", "")),
        ),
    )


def load_history_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        fail(f"history_jsonl_missing:{path}")
    entries: dict[str, dict[str, Any]] = {}
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
        entries[observation_key] = {
            "observation_key": observation_key,
            "observed_at": str(obj.get("observed_at", "")).strip(),
            "selected_pack_id": str(obj.get("selected_pack_id", "")).strip(),
            "selected_symbols": normalize_symbols(obj.get("selected_symbols")),
            "live_run_id": str(obj.get("live_run_id", "")).strip(),
            "started_at": str(obj.get("started_at", "")).strip(),
            "finished_at": str(obj.get("finished_at", "")).strip(),
            "execution_summary": normalize_execution_summary(obj.get("execution_summary")),
            "funding_events": normalize_funding_events(obj.get("funding_events")),
            "mark_price_events": normalize_mark_price_events(obj.get("mark_price_events")),
            "execution_events": normalize_execution_events(obj.get("execution_events")),
        }
    return sorted(
        entries.values(),
        key=lambda entry: (
            str(entry.get("observed_at", "")),
            str(entry.get("selected_pack_id", "")),
            str(entry.get("live_run_id", "")),
            str(entry.get("observation_key", "")),
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
        observation_key = str(obj.get("observation_key", "")).strip()
        if not observation_key:
            fail(f"execution_events_missing_observation_key:{path}:{lineno}")
        grouped.setdefault(observation_key, []).append(obj)
    return {
        observation_key: normalize_execution_events(events)
        for observation_key, events in grouped.items()
    }


def load_bindings(path: Path) -> tuple[dict[str, dict[str, Any]], bool]:
    if not path.exists():
        return {}, False
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"binding_artifact_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"binding_artifact_not_object:{path}")
    if obj.get("schema_version") != BINDING_SCHEMA_VERSION:
        fail(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"binding_artifact_missing_items:{path}")
    bindings: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            fail(f"binding_artifact_item_not_object:{path}:{index}")
        pack_id = str(item.get("pack_id", "")).strip()
        if not pack_id:
            continue
        bindings[pack_id] = item
    return bindings, True


def direction_from_size(size: float) -> str:
    if size > EPSILON:
        return "LONG"
    if size < -EPSILON:
        return "SHORT"
    return "FLAT"


def approx_equal(left: float | None, right: float | None, tol: float = 1e-9) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return abs(left - right) <= tol


def fill_turnover_quote(fill: dict[str, Any]) -> float | None:
    fill_value = parse_float_or_none(fill.get("fill_value"))
    if fill_value is not None and fill_value > 0:
        return fill_value
    qty = parse_float_or_none(fill.get("qty"))
    fill_price = parse_float_or_none(fill.get("fill_price"))
    if qty is None or qty <= 0 or fill_price is None or fill_price <= 0:
        return None
    return qty * fill_price


def compute_unrealized(size: float, avg_entry_price: float | None, current_price: float | None) -> float | None:
    if direction_from_size(size) == "FLAT":
        return 0.0
    if avg_entry_price is None or current_price is None:
        return None
    if size > 0:
        return abs(size) * (current_price - avg_entry_price)
    return abs(size) * (avg_entry_price - current_price)


def parse_epoch_ms(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def parse_iso_to_epoch_ms(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


def signed_position_size_at(fill_events: list[dict[str, Any]], symbol: str, boundary_ms: int) -> float:
    size = 0.0
    for event in fill_events:
        if str(event.get("symbol")) != symbol:
            continue
        ts_event = parse_epoch_ms(event.get("ts_event"))
        if ts_event is None or ts_event > boundary_ms:
            continue
        qty = parse_float_or_none(event.get("qty"))
        side = str(event.get("side", "")).strip().upper()
        if qty is None or qty <= 0 or side not in {"BUY", "SELL"}:
            continue
        size += qty if side == "BUY" else -qty
    return size


def latest_mark_price_before(mark_price_events: list[dict[str, Any]], symbol: str, boundary_ms: int) -> float | None:
    latest_ts: int | None = None
    latest_price: float | None = None
    for event in mark_price_events:
        if str(event.get("symbol")) != symbol:
            continue
        ts_event = parse_epoch_ms(event.get("ts_event"))
        mark_price = parse_float_or_none(event.get("mark_price"))
        if ts_event is None or mark_price is None or mark_price <= 0:
            continue
        if ts_event > boundary_ms:
            continue
        if latest_ts is None or ts_event >= latest_ts:
            latest_ts = ts_event
            latest_price = mark_price
    return latest_price


def compute_funding_costs(
    *,
    fill_events: list[dict[str, Any]],
    funding_events: list[dict[str, Any]],
    mark_price_events: list[dict[str, Any]],
    symbol: str | None,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    if symbol is None:
        return {
            "funding_cost_quote": None,
            "funding_support_status": "MULTI_SYMBOL_UNSUPPORTED",
            "funding_alignment_status": "MULTI_SYMBOL_UNSUPPORTED",
            "funding_rate_source": "UNAVAILABLE",
            "funding_events_count": 0,
            "funding_windows_crossed_count": 0,
            "funding_applied_count": 0,
            "funding_windows": [],
        }
    relevant_fills = [event for event in fill_events if str(event.get("symbol")) == symbol]
    if not relevant_fills:
        return {
            "funding_cost_quote": 0.0,
            "funding_support_status": "NO_FILL_ACTIVITY",
            "funding_alignment_status": "NO_FILL_ACTIVITY",
            "funding_rate_source": "UNAVAILABLE",
            "funding_events_count": 0,
            "funding_windows_crossed_count": 0,
            "funding_applied_count": 0,
            "funding_windows": [],
        }

    relevant_funding = [event for event in funding_events if str(event.get("symbol")) == symbol]
    if not relevant_funding:
        return {
            "funding_cost_quote": None,
            "funding_support_status": "NO_FUNDING_EVENTS_OBSERVED",
            "funding_alignment_status": "NO_FUNDING_EVENTS_OBSERVED",
            "funding_rate_source": "UNAVAILABLE",
            "funding_events_count": 0,
            "funding_windows_crossed_count": 0,
            "funding_applied_count": 0,
            "funding_windows": [],
        }

    started_ms = parse_iso_to_epoch_ms(started_at)
    finished_ms = parse_iso_to_epoch_ms(finished_at)
    if started_ms is None or finished_ms is None or finished_ms < started_ms:
        return {
            "funding_cost_quote": None,
            "funding_support_status": "RUN_WINDOW_UNAVAILABLE",
            "funding_alignment_status": "RUN_WINDOW_UNAVAILABLE",
            "funding_rate_source": "LIVE_STREAM_FUNDING",
            "funding_events_count": len(relevant_funding),
            "funding_windows_crossed_count": 0,
            "funding_applied_count": 0,
            "funding_windows": [],
        }

    grouped_by_boundary: dict[int, list[dict[str, Any]]] = {}
    for event in relevant_funding:
        boundary_ms = parse_epoch_ms(event.get("next_funding_ts"))
        ts_event_ms = parse_epoch_ms(event.get("ts_event"))
        if boundary_ms is None or ts_event_ms is None:
            continue
        if not (started_ms <= boundary_ms <= finished_ms):
            continue
        if ts_event_ms > boundary_ms:
            continue
        grouped_by_boundary.setdefault(boundary_ms, []).append(event)

    if not grouped_by_boundary:
        return {
            "funding_cost_quote": 0.0,
            "funding_support_status": "NO_FUNDING_WINDOW_CROSSED",
            "funding_alignment_status": "NO_FUNDING_WINDOW_CROSSED",
            "funding_rate_source": "LIVE_STREAM_FUNDING",
            "funding_events_count": len(relevant_funding),
            "funding_windows_crossed_count": 0,
            "funding_applied_count": 0,
            "funding_windows": [],
        }

    total_cost = 0.0
    applied_count = 0
    partial_missing_mark = False
    positionless_windows = 0
    windows: list[dict[str, Any]] = []
    for boundary_ms in sorted(grouped_by_boundary):
        chosen = sorted(
            grouped_by_boundary[boundary_ms],
            key=lambda event: (
                parse_epoch_ms(event.get("ts_event")) or 0,
                parse_int(event.get("event_seq")),
            ),
        )[-1]
        signed_qty = signed_position_size_at(relevant_fills, symbol, boundary_ms)
        position_direction = direction_from_size(signed_qty)
        funding_rate = parse_float_or_none(chosen.get("funding_rate"))
        mark_price = latest_mark_price_before(mark_price_events, symbol, boundary_ms)
        window = {
            "next_funding_ts": str(boundary_ms),
            "funding_rate": funding_rate,
            "position_direction": position_direction,
            "position_qty": abs(signed_qty),
            "mark_price": mark_price,
            "funding_cost_quote": None,
            "alignment_status": "NO_POSITION",
        }
        if position_direction == "FLAT":
            positionless_windows += 1
            windows.append(window)
            continue
        if funding_rate is None or mark_price is None:
            partial_missing_mark = True
            window["alignment_status"] = "MARK_PRICE_MISSING"
            windows.append(window)
            continue
        funding_cost = signed_qty * mark_price * funding_rate
        total_cost += funding_cost
        applied_count += 1
        window["funding_cost_quote"] = funding_cost
        window["alignment_status"] = "APPLIED"
        windows.append(window)

    if partial_missing_mark:
        support_status = "FUNDING_COST_PARTIAL_MARK_MISSING"
        alignment_status = "PARTIAL_MARK_PRICE_MISSING"
        funding_cost_quote = None
    elif applied_count > 0:
        support_status = "FUNDING_COST_BACKED"
        alignment_status = "ALL_APPLIED_WINDOWS_MARK_PRICE_BACKED"
        funding_cost_quote = total_cost
    elif positionless_windows == len(grouped_by_boundary):
        support_status = "NO_POSITION_AT_CROSSED_WINDOW"
        alignment_status = "ALL_CROSSED_WINDOWS_FLAT"
        funding_cost_quote = 0.0
    else:
        support_status = "FUNDING_COST_PARTIAL_MARK_MISSING"
        alignment_status = "PARTIAL_MARK_PRICE_MISSING"
        funding_cost_quote = None

    return {
        "funding_cost_quote": funding_cost_quote,
        "funding_support_status": support_status,
        "funding_alignment_status": alignment_status,
        "funding_rate_source": "LIVE_STREAM_FUNDING",
        "funding_events_count": len(relevant_funding),
        "funding_windows_crossed_count": len(grouped_by_boundary),
        "funding_applied_count": applied_count,
        "funding_windows": windows,
    }


def family_directionality_status(family_id: str) -> str:
    if family_id == "spread_reversion_v1":
        return "NON_DIRECTIONAL_SIGNAL_ONLY"
    if family_id == "momentum_v1":
        return "PRIMARY_DIRECTIONAL_LONG_SHORT"
    if family_id:
        return "UNKNOWN_FAMILY_DIRECTIONALITY"
    return "NO_FAMILY_CONTEXT"


def open_episode(
    observation_key: str,
    next_index: int,
    *,
    direction: str,
    action: str,
    ts_event: str,
    event_seq: int,
    fill_price: float,
    position_qty: float,
    fee_portion: float | None,
) -> dict[str, Any]:
    fee_complete = fee_portion is not None
    return {
        "episode_id": f"{observation_key}|episode|{next_index}",
        "direction": direction,
        "status": "OPEN",
        "open_action": action,
        "close_action": None,
        "opened_ts_event": ts_event,
        "closed_ts_event": None,
        "opened_event_seq": event_seq,
        "closed_event_seq": None,
        "entry_price": fill_price,
        "exit_price": None,
        "entry_avg_price": fill_price,
        "max_abs_position_qty": position_qty,
        "realized_pnl_quote_gross": 0.0,
        "realized_pnl_quote_net": None,
        "fee_quote": fee_portion if fee_complete else None,
        "_fee_complete": fee_complete,
    }


def add_fee(target: dict[str, Any], fee_portion: float | None) -> None:
    if not target:
        return
    if fee_portion is None:
        target["_fee_complete"] = False
        target["fee_quote"] = None
        return
    if target.get("_fee_complete") is False:
        target["fee_quote"] = None
        return
    target["fee_quote"] = (parse_float_or_none(target.get("fee_quote")) or 0.0) + fee_portion


def finalize_episode(episode: dict[str, Any]) -> dict[str, Any]:
    fee_complete = bool(episode.pop("_fee_complete", False))
    fee_quote = parse_float_or_none(episode.get("fee_quote"))
    if not fee_complete:
        episode["fee_quote"] = None
        episode["realized_pnl_quote_net"] = None
    else:
        episode["fee_quote"] = fee_quote or 0.0
        episode["realized_pnl_quote_net"] = (parse_float_or_none(episode.get("realized_pnl_quote_gross")) or 0.0) - (
            fee_quote or 0.0
        )
    return episode


def replay_fill_events(observation_key: str, fill_events: list[dict[str, Any]], symbol: str) -> dict[str, Any]:
    relevant_fills = [event for event in fill_events if str(event.get("symbol")) == symbol]
    size = 0.0
    avg_entry_price: float | None = None
    realized_gross = 0.0
    total_fee = 0.0
    fee_complete = True
    turnover_total = 0.0
    turnover_opening = 0.0
    turnover_closing = 0.0
    turnover_value_backed = True
    actions: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    active_episode: dict[str, Any] | None = None
    next_episode_index = 1

    for fill in relevant_fills:
        qty = float(fill["qty"])
        fill_price = float(fill["fill_price"])
        fill_fee = parse_float_or_none(fill.get("fill_fee"))
        fill_side = str(fill["side"])
        event_seq = int(fill["event_seq"])
        ts_event = str(fill["ts_event"])
        signed_qty = qty if fill_side == "BUY" else -qty
        old_size = size
        old_direction = direction_from_size(old_size)
        close_fraction = 0.0
        opening_fee_portion: float | None = None
        closing_fee_portion: float | None = None
        fill_value_backed = parse_float_or_none(fill.get("fill_value")) is not None
        fill_turnover = fill_turnover_quote(fill)
        if fill_turnover is None:
            fee_complete = False
            turnover_value_backed = False
            continue
        turnover_total += fill_turnover
        if not fill_value_backed:
            turnover_value_backed = False

        if fill_fee is None:
            fee_complete = False
        else:
            total_fee += fill_fee

        if old_direction == "FLAT":
            size = signed_qty
            avg_entry_price = fill_price
            new_direction = direction_from_size(size)
            turnover_opening += fill_turnover
            action = "LONG_OPEN" if new_direction == "LONG" else "SHORT_OPEN"
            actions.append(
                {
                    "event_seq": event_seq,
                    "ts_event": ts_event,
                    "action": action,
                    "side": fill_side,
                    "qty": qty,
                    "fill_price": fill_price,
                }
            )
            active_episode = open_episode(
                observation_key,
                next_episode_index,
                direction=new_direction,
                action=action,
                ts_event=ts_event,
                event_seq=event_seq,
                fill_price=fill_price,
                position_qty=abs(size),
                fee_portion=fill_fee,
            )
            next_episode_index += 1
            continue

        if (old_direction == "LONG" and fill_side == "BUY") or (old_direction == "SHORT" and fill_side == "SELL"):
            old_abs = abs(old_size)
            new_abs = old_abs + qty
            avg_entry_price = ((old_abs * float(avg_entry_price or 0.0)) + (qty * fill_price)) / new_abs
            size = old_size + signed_qty
            turnover_opening += fill_turnover
            action = "LONG_ADD" if old_direction == "LONG" else "SHORT_ADD"
            actions.append(
                {
                    "event_seq": event_seq,
                    "ts_event": ts_event,
                    "action": action,
                    "side": fill_side,
                    "qty": qty,
                    "fill_price": fill_price,
                }
            )
            if active_episode is not None:
                add_fee(active_episode, fill_fee)
                active_episode["entry_avg_price"] = avg_entry_price
                active_episode["max_abs_position_qty"] = max(
                    float(active_episode.get("max_abs_position_qty") or 0.0),
                    abs(size),
                )
            continue

        closing_qty = min(abs(old_size), qty)
        close_fraction = closing_qty / qty if qty > 0 else 0.0
        if fill_fee is not None:
            closing_fee_portion = fill_fee * close_fraction
            opening_fee_portion = fill_fee - closing_fee_portion
        closing_turnover = closing_qty * fill_price
        if old_direction == "LONG":
            realized_piece = closing_qty * (fill_price - float(avg_entry_price or 0.0))
        else:
            realized_piece = closing_qty * (float(avg_entry_price or 0.0) - fill_price)
        realized_gross += realized_piece
        if active_episode is not None:
            active_episode["realized_pnl_quote_gross"] = (
                float(active_episode.get("realized_pnl_quote_gross") or 0.0) + realized_piece
            )

        if qty < abs(old_size) - EPSILON:
            size = old_size + signed_qty
            turnover_closing += closing_turnover
            action = "LONG_REDUCE" if old_direction == "LONG" else "SHORT_REDUCE"
            if active_episode is not None:
                add_fee(active_episode, fill_fee)
                active_episode["max_abs_position_qty"] = max(
                    float(active_episode.get("max_abs_position_qty") or 0.0),
                    abs(size),
                )
            actions.append(
                {
                    "event_seq": event_seq,
                    "ts_event": ts_event,
                    "action": action,
                    "side": fill_side,
                    "qty": qty,
                    "fill_price": fill_price,
                }
            )
            continue

        if qty <= abs(old_size) + EPSILON:
            size = 0.0
            turnover_closing += fill_turnover
            action = "LONG_CLOSE" if old_direction == "LONG" else "SHORT_CLOSE"
            actions.append(
                {
                    "event_seq": event_seq,
                    "ts_event": ts_event,
                    "action": action,
                    "side": fill_side,
                    "qty": qty,
                    "fill_price": fill_price,
                }
            )
            if active_episode is not None:
                add_fee(active_episode, fill_fee)
                active_episode["status"] = "CLOSED"
                active_episode["close_action"] = action
                active_episode["closed_ts_event"] = ts_event
                active_episode["closed_event_seq"] = event_seq
                active_episode["exit_price"] = fill_price
                episodes.append(finalize_episode(active_episode))
                active_episode = None
            avg_entry_price = None
            continue

        residual_qty = qty - abs(old_size)
        size = residual_qty if fill_side == "BUY" else -residual_qty
        new_direction = direction_from_size(size)
        turnover_closing += closing_turnover
        turnover_opening += residual_qty * fill_price
        action = "SHORT_CLOSE_TO_LONG_REVERSAL" if old_direction == "SHORT" else "LONG_CLOSE_TO_SHORT_REVERSAL"
        actions.append(
            {
                "event_seq": event_seq,
                "ts_event": ts_event,
                "action": action,
                "side": fill_side,
                "qty": qty,
                "fill_price": fill_price,
            }
        )
        if active_episode is not None:
            add_fee(active_episode, closing_fee_portion)
            active_episode["status"] = "CLOSED"
            active_episode["close_action"] = action
            active_episode["closed_ts_event"] = ts_event
            active_episode["closed_event_seq"] = event_seq
            active_episode["exit_price"] = fill_price
            episodes.append(finalize_episode(active_episode))
            active_episode = None
        avg_entry_price = fill_price
        open_action = "LONG_OPEN" if new_direction == "LONG" else "SHORT_OPEN"
        active_episode = open_episode(
            observation_key,
            next_episode_index,
            direction=new_direction,
            action=open_action,
            ts_event=ts_event,
            event_seq=event_seq,
            fill_price=fill_price,
            position_qty=abs(size),
            fee_portion=opening_fee_portion,
        )
        next_episode_index += 1

    if active_episode is not None:
        episodes.append(finalize_episode(active_episode))

    return {
        "fill_events": relevant_fills,
        "action_sequence": actions,
        "episodes": episodes,
        "final_size": size,
        "final_avg_entry_price": avg_entry_price if direction_from_size(size) != "FLAT" else None,
        "turnover_quote": turnover_total,
        "opening_turnover_quote": turnover_opening,
        "closing_turnover_quote": turnover_closing,
        "turnover_support_status": (
            "NO_FILL_ACTIVITY"
            if not relevant_fills
            else "FILL_VALUE_BACKED"
            if turnover_value_backed
            else "PRICE_QTY_RECOMPUTED"
        ),
        "replayed_realized_pnl_quote_gross": realized_gross,
        "total_fee_quote": total_fee if fee_complete else None,
        "fee_support_status": (
            "NO_FILL_ACTIVITY"
            if not relevant_fills
            else "FILL_FEE_BACKED"
            if fee_complete
            else "FILL_FEE_PARTIAL"
        ),
        "replayed_realized_pnl_quote_net": (
            realized_gross - total_fee
            if relevant_fills and fee_complete
            else 0.0
            if not relevant_fills
            else None
        ),
        "effective_fee_rate": (
            (total_fee / turnover_total)
            if relevant_fills and fee_complete and turnover_total > 0
            else None
        ),
    }


def compute_position_notional(final_position_direction: str, final_position_qty: float | None, final_mark_price: float | None) -> float | None:
    if final_position_direction == "FLAT":
        return 0.0
    if final_position_qty is None or final_mark_price is None:
        return None
    return abs(final_position_qty) * final_mark_price


def classify_cost_accounting_status(
    *,
    paper_run_status: str,
    fee_support_status: str,
    funding_support_status: str,
    final_position_direction: str,
    mark_price_available: bool,
) -> str:
    if paper_run_status == "MULTI_SYMBOL_UNSUPPORTED":
        return "MULTI_SYMBOL_UNSUPPORTED"
    if fee_support_status == "NO_FILL_ACTIVITY":
        return "NO_FILL_ACTIVITY"
    if fee_support_status != "FILL_FEE_BACKED":
        return "GROSS_ONLY_FEE_PARTIAL"
    if funding_support_status in {"NO_FUNDING_EVENTS_OBSERVED", "RUN_WINDOW_UNAVAILABLE", "FUNDING_COST_PARTIAL_MARK_MISSING"}:
        return "NET_FEE_BACKED_FUNDING_PARTIAL"
    if final_position_direction == "FLAT":
        return "NET_FEE_BACKED_CLOSED_FUNDING_AWARE"
    if mark_price_available:
        return "NET_FEE_BACKED_MARK_TO_MARKET_FUNDING_AWARE"
    return "NET_FEE_BACKED_MARK_PRICE_UNAVAILABLE_FUNDING_AWARE"


def classify_profitability_status(
    *,
    paper_run_status: str,
    fee_support_status: str,
    funding_support_status: str,
    final_position_direction: str,
    estimated_exit_fee_quote: float | None,
    mark_to_market_net_paid_fees: float | None,
) -> str:
    if paper_run_status == "MULTI_SYMBOL_UNSUPPORTED":
        return "PROFITABILITY_UNSUPPORTED_MULTI_SYMBOL"
    if fee_support_status == "NO_FILL_ACTIVITY":
        return "NO_FILL_ACTIVITY"
    if fee_support_status != "FILL_FEE_BACKED":
        return "GROSS_ONLY_FEE_PARTIAL"
    if funding_support_status in {"NO_FUNDING_EVENTS_OBSERVED", "RUN_WINDOW_UNAVAILABLE", "FUNDING_COST_PARTIAL_MARK_MISSING"}:
        return "PROFITABILITY_PARTIAL_FUNDING_MISSING"
    if final_position_direction == "FLAT":
        return "NET_AFTER_FEES_AND_FUNDING"
    if mark_to_market_net_paid_fees is None:
        return "PROFITABILITY_PARTIAL_MARK_PRICE_UNAVAILABLE"
    if estimated_exit_fee_quote is not None:
        return "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE"
    return "NET_MARK_TO_MARKET_AFTER_FEES_AND_FUNDING"


def choose_summary_position(summary: dict[str, Any], symbol: str | None) -> tuple[dict[str, Any] | None, str]:
    positions = summary.get("positions") if isinstance(summary.get("positions"), dict) else {}
    if symbol and symbol in positions:
        return positions[symbol], "MATCHED_SYMBOL"
    if not positions:
        return None, "NO_POSITION_SNAPSHOT"
    if symbol and symbol not in positions:
        return None, "SYMBOL_NOT_PRESENT"
    if len(positions) == 1:
        return next(iter(positions.values())), "SINGLE_POSITION_FALLBACK"
    return None, "MULTI_POSITION_UNSUPPORTED"


def build_item(
    entry: dict[str, Any],
    events_by_observation_key: dict[str, list[dict[str, Any]]],
    bindings: dict[str, dict[str, Any]],
    *,
    binding_artifact_present: bool,
) -> dict[str, Any]:
    observation_key = str(entry.get("observation_key") or "").strip()
    selected_pack_id = str(entry.get("selected_pack_id") or "").strip()
    summary = normalize_execution_summary(entry.get("execution_summary"))
    history_events = normalize_execution_events(entry.get("execution_events"))
    funding_events = normalize_funding_events(entry.get("funding_events"))
    mark_price_events = normalize_mark_price_events(entry.get("mark_price_events"))
    execution_events = events_by_observation_key.get(observation_key, history_events)
    binding = bindings.get(selected_pack_id)
    runtime_strategy_config = binding.get("runtime_strategy_config") if isinstance(binding, dict) else None
    binding_mode = (
        str(runtime_strategy_config.get("binding_mode") or "").strip()
        if isinstance(runtime_strategy_config, dict)
        else ""
    )
    family_id = str(binding.get("family_id") or "").strip() if isinstance(binding, dict) else ""
    strategy_id = str(binding.get("strategy_id") or "").strip() if isinstance(binding, dict) else ""
    runtime_binding_status = (
        str(binding.get("runtime_binding_status") or "").strip()
        if isinstance(binding, dict)
        else "BINDING_ARTIFACT_MISSING" if not binding_artifact_present else "BINDING_NOT_FOUND"
    )
    selected_symbols = normalize_symbols(entry.get("selected_symbols"))
    symbol = selected_symbols[0] if len(selected_symbols) == 1 else None

    decision_event_count = sum(1 for event in execution_events if str(event.get("event_type")) == "DECISION")
    risk_reject_event_count = sum(1 for event in execution_events if str(event.get("event_type")) == "RISK_REJECT")
    fill_event_count = sum(1 for event in execution_events if str(event.get("event_type")) == "FILL")
    summary_equity = parse_float_or_none(summary.get("equity"))
    max_position_value_quote = parse_float_or_none(summary.get("max_position_value"))

    if len(selected_symbols) != 1:
        replay = {
            "fill_events": [],
            "action_sequence": [],
            "episodes": [],
            "final_size": 0.0,
            "final_avg_entry_price": None,
            "turnover_quote": 0.0,
            "opening_turnover_quote": 0.0,
            "closing_turnover_quote": 0.0,
            "turnover_support_status": "MULTI_SYMBOL_UNSUPPORTED",
            "replayed_realized_pnl_quote_gross": 0.0,
            "replayed_realized_pnl_quote_net": None,
            "replayed_unrealized_pnl_quote": None,
            "total_fee_quote": None,
            "fee_support_status": "MULTI_SYMBOL_UNSUPPORTED",
            "effective_fee_rate": None,
        }
        paper_run_status = "MULTI_SYMBOL_UNSUPPORTED"
        final_position_direction = "UNKNOWN"
        final_position_qty = None
        summary_position = None
        summary_position_status = "MULTI_SYMBOL_UNSUPPORTED"
        final_mark_price = None
        mark_price_source = "UNAVAILABLE"
        replayed_unrealized = None
        position_reconciliation_status = "MULTI_SYMBOL_UNSUPPORTED"
        pnl_reconciliation_status = "MULTI_SYMBOL_UNSUPPORTED"
    else:
        replay = replay_fill_events(
            observation_key,
            [event for event in execution_events if str(event.get("event_type")) == "FILL"],
            symbol,
        )
        final_position_direction = direction_from_size(float(replay["final_size"]))
        final_position_qty = abs(float(replay["final_size"])) if final_position_direction != "FLAT" else 0.0
        summary_position, summary_position_status = choose_summary_position(summary, symbol)
        final_mark_price = parse_float_or_none(summary_position.get("current_price")) if isinstance(summary_position, dict) else None
        if final_position_direction == "FLAT":
            mark_price_source = "FLAT_NO_MARK_REQUIRED"
        elif final_mark_price is not None:
            mark_price_source = "SUMMARY_POSITION_CURRENT_PRICE"
        else:
            mark_price_source = "UNAVAILABLE"
        replayed_unrealized = compute_unrealized(
            float(replay["final_size"]),
            parse_float_or_none(replay["final_avg_entry_price"]),
            final_mark_price,
        )
        if fill_event_count == 0:
            paper_run_status = "NO_FILL_ACTIVITY"
        elif final_position_direction == "FLAT":
            paper_run_status = "FILL_BACKED_FLAT"
        else:
            paper_run_status = "FILL_BACKED_POSITION_OPEN"

        summary_positions_count = parse_int(summary.get("positions_count"))
        summary_position_size = parse_float_or_none(summary_position.get("size")) if isinstance(summary_position, dict) else None
        summary_position_direction = direction_from_size(summary_position_size or 0.0)
        summary_avg_entry_price = (
            parse_float_or_none(summary_position.get("avg_entry_price")) if isinstance(summary_position, dict) else None
        )
        if final_position_direction == "FLAT":
            if summary_position is not None and summary_position_direction != "FLAT":
                position_reconciliation_status = "MISMATCHED_TO_SUMMARY"
            elif summary_position is None and summary_positions_count > 0 and summary_position_status != "NO_POSITION_SNAPSHOT":
                position_reconciliation_status = "SUMMARY_POSITION_UNAVAILABLE"
            else:
                position_reconciliation_status = "MATCHED_TO_SUMMARY"
        else:
            if summary_position is None:
                position_reconciliation_status = (
                    "SUMMARY_POSITION_UNAVAILABLE"
                    if summary_positions_count > 0 or summary_position_status != "NO_POSITION_SNAPSHOT"
                    else "MISMATCHED_TO_SUMMARY"
                )
            elif (
                summary_position_direction == final_position_direction
                and approx_equal(abs(summary_position_size or 0.0), final_position_qty)
                and approx_equal(summary_avg_entry_price, parse_float_or_none(replay["final_avg_entry_price"]))
            ):
                position_reconciliation_status = "MATCHED_TO_SUMMARY"
            else:
                position_reconciliation_status = "MISMATCHED_TO_SUMMARY"

        summary_realized = parse_float_or_none(summary.get("total_realized_pnl"))
        summary_unrealized = parse_float_or_none(summary.get("total_unrealized_pnl"))
        replayed_realized_net = parse_float_or_none(replay.get("replayed_realized_pnl_quote_net"))
        if summary_realized is None or summary_unrealized is None:
            pnl_reconciliation_status = "SUMMARY_PNL_UNAVAILABLE"
        elif fill_event_count == 0:
            pnl_reconciliation_status = "NO_FILL_ACTIVITY"
        elif replayed_realized_net is None:
            pnl_reconciliation_status = "NET_UNAVAILABLE_FEE_MISSING"
        elif replayed_unrealized is None:
            pnl_reconciliation_status = "MARK_PRICE_UNAVAILABLE"
        elif approx_equal(replayed_realized_net, summary_realized) and approx_equal(replayed_unrealized, summary_unrealized):
            pnl_reconciliation_status = "MATCHED_TO_SUMMARY"
        else:
            pnl_reconciliation_status = "MISMATCHED_TO_SUMMARY"

    final_position_notional_quote = compute_position_notional(
        final_position_direction,
        final_position_qty,
        final_mark_price,
    )
    if final_position_direction == "FLAT":
        position_notional_support_status = "NO_OPEN_POSITION"
    elif final_position_notional_quote is not None:
        position_notional_support_status = "SUMMARY_MARK_PRICE_BACKED"
    else:
        position_notional_support_status = "MARK_PRICE_UNAVAILABLE"

    if final_position_direction == "FLAT":
        exposure_ratio_status = "NO_OPEN_POSITION"
        exposure_to_equity_ratio = 0.0
    elif final_position_notional_quote is None:
        exposure_ratio_status = "MARK_PRICE_UNAVAILABLE"
        exposure_to_equity_ratio = None
    elif summary_equity is None or summary_equity <= 0:
        exposure_ratio_status = "EQUITY_UNAVAILABLE"
        exposure_to_equity_ratio = None
    else:
        exposure_ratio_status = "SUMMARY_EQUITY_BACKED"
        exposure_to_equity_ratio = final_position_notional_quote / summary_equity

    replayed_realized_gross = parse_float_or_none(replay.get("replayed_realized_pnl_quote_gross"))
    replayed_realized_net = parse_float_or_none(replay.get("replayed_realized_pnl_quote_net"))
    total_fee_quote = parse_float_or_none(replay.get("total_fee_quote"))
    effective_fee_rate = parse_float_or_none(replay.get("effective_fee_rate"))
    turnover_quote = parse_float_or_none(replay.get("turnover_quote"))
    opening_turnover_quote = parse_float_or_none(replay.get("opening_turnover_quote"))
    closing_turnover_quote = parse_float_or_none(replay.get("closing_turnover_quote"))
    mark_to_market_pnl_quote_gross = (
        (replayed_realized_gross or 0.0) + replayed_unrealized
        if replayed_unrealized is not None
        else None
    )
    mark_to_market_pnl_quote_net_paid_fees = (
        mark_to_market_pnl_quote_gross - total_fee_quote
        if mark_to_market_pnl_quote_gross is not None and total_fee_quote is not None
        else None
    )
    estimated_exit_fee_quote = (
        0.0
        if final_position_direction == "FLAT"
        else final_position_notional_quote * effective_fee_rate
        if final_position_notional_quote is not None and effective_fee_rate is not None
        else None
    )
    mark_to_market_pnl_quote_net_after_exit_estimate = (
        mark_to_market_pnl_quote_net_paid_fees - estimated_exit_fee_quote
        if mark_to_market_pnl_quote_net_paid_fees is not None and estimated_exit_fee_quote is not None
        else None
    )
    funding = compute_funding_costs(
        fill_events=list(replay.get("fill_events") or []),
        funding_events=funding_events,
        mark_price_events=mark_price_events,
        symbol=symbol,
        started_at=str(entry.get("started_at") or ""),
        finished_at=str(entry.get("finished_at") or ""),
    )
    funding_cost_quote = parse_float_or_none(funding.get("funding_cost_quote"))
    mark_to_market_pnl_quote_net_after_funding = (
        mark_to_market_pnl_quote_net_paid_fees - funding_cost_quote
        if mark_to_market_pnl_quote_net_paid_fees is not None and funding_cost_quote is not None
        else None
    )
    mark_to_market_pnl_quote_net_after_funding_and_exit_estimate = (
        mark_to_market_pnl_quote_net_after_exit_estimate - funding_cost_quote
        if mark_to_market_pnl_quote_net_after_exit_estimate is not None and funding_cost_quote is not None
        else None
    )
    cost_accounting_status = classify_cost_accounting_status(
        paper_run_status=paper_run_status,
        fee_support_status=str(replay.get("fee_support_status") or "NO_FILL_ACTIVITY"),
        funding_support_status=str(funding.get("funding_support_status") or "NO_FUNDING_EVENTS_OBSERVED"),
        final_position_direction=final_position_direction,
        mark_price_available=final_mark_price is not None,
    )
    profitability_status = classify_profitability_status(
        paper_run_status=paper_run_status,
        fee_support_status=str(replay.get("fee_support_status") or "NO_FILL_ACTIVITY"),
        funding_support_status=str(funding.get("funding_support_status") or "NO_FUNDING_EVENTS_OBSERVED"),
        final_position_direction=final_position_direction,
        estimated_exit_fee_quote=estimated_exit_fee_quote,
        mark_to_market_net_paid_fees=mark_to_market_pnl_quote_net_paid_fees,
    )

    return {
        "observation_key": observation_key,
        "observed_at": str(entry.get("observed_at") or "").strip(),
        "selected_pack_id": selected_pack_id,
        "live_run_id": str(entry.get("live_run_id") or "").strip(),
        "selected_symbols": selected_symbols,
        "symbol": symbol,
        "strategy_id": strategy_id,
        "family_id": family_id,
        "runtime_binding_status": runtime_binding_status,
        "binding_mode": binding_mode,
        "family_directionality_status": family_directionality_status(family_id),
        "paper_run_status": paper_run_status,
        "decision_event_count": decision_event_count,
        "risk_reject_event_count": risk_reject_event_count,
        "fill_event_count": fill_event_count,
        "turnover_quote": turnover_quote,
        "opening_turnover_quote": opening_turnover_quote,
        "closing_turnover_quote": closing_turnover_quote,
        "turnover_support_status": str(replay.get("turnover_support_status") or "NO_FILL_ACTIVITY"),
        "effective_fee_rate": effective_fee_rate,
        "final_position_direction": final_position_direction,
        "final_position_qty": final_position_qty,
        "final_avg_entry_price": parse_float_or_none(replay.get("final_avg_entry_price")),
        "final_mark_price": final_mark_price,
        "mark_price_source": mark_price_source,
        "final_position_notional_quote": final_position_notional_quote,
        "position_notional_support_status": position_notional_support_status,
        "summary_equity_quote": summary_equity,
        "max_position_value_quote": max_position_value_quote,
        "exposure_to_equity_ratio": exposure_to_equity_ratio,
        "exposure_ratio_status": exposure_ratio_status,
        "summary_realized_pnl_quote": parse_float_or_none(summary.get("total_realized_pnl")),
        "summary_unrealized_pnl_quote": parse_float_or_none(summary.get("total_unrealized_pnl")),
        "replayed_realized_pnl_quote_gross": parse_float_or_none(replay.get("replayed_realized_pnl_quote_gross")),
        "replayed_realized_pnl_quote_net": parse_float_or_none(replay.get("replayed_realized_pnl_quote_net")),
        "replayed_unrealized_pnl_quote": replayed_unrealized,
        "mark_to_market_pnl_quote_gross": mark_to_market_pnl_quote_gross,
        "mark_to_market_pnl_quote_net_paid_fees": mark_to_market_pnl_quote_net_paid_fees,
        "mark_to_market_pnl_quote_net_after_funding": mark_to_market_pnl_quote_net_after_funding,
        "estimated_exit_fee_quote": estimated_exit_fee_quote,
        "mark_to_market_pnl_quote_net_after_exit_estimate": mark_to_market_pnl_quote_net_after_exit_estimate,
        "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate": (
            mark_to_market_pnl_quote_net_after_funding_and_exit_estimate
        ),
        "total_fee_quote": parse_float_or_none(replay.get("total_fee_quote")),
        "fee_support_status": str(replay.get("fee_support_status") or "NO_FILL_ACTIVITY"),
        "funding_cost_quote": funding_cost_quote,
        "funding_support_status": str(funding.get("funding_support_status") or "NO_FUNDING_EVENTS_OBSERVED"),
        "funding_alignment_status": str(funding.get("funding_alignment_status") or "NO_FUNDING_EVENTS_OBSERVED"),
        "funding_rate_source": str(funding.get("funding_rate_source") or "UNAVAILABLE"),
        "funding_events_count": parse_int(funding.get("funding_events_count")),
        "funding_windows_crossed_count": parse_int(funding.get("funding_windows_crossed_count")),
        "funding_applied_count": parse_int(funding.get("funding_applied_count")),
        "funding_windows": list(funding.get("funding_windows") or []),
        "leverage_support_status": "UNSUPPORTED",
        "margin_support_status": "UNSUPPORTED",
        "reduce_only_support_status": "UNSUPPORTED",
        "cost_accounting_status": cost_accounting_status,
        "profitability_status": profitability_status,
        "position_reconciliation_status": position_reconciliation_status,
        "pnl_reconciliation_status": pnl_reconciliation_status,
        "action_sequence": list(replay.get("action_sequence") or []),
        "episodes": list(replay.get("episodes") or []),
    }


def build_payload(
    history_entries: list[dict[str, Any]],
    events_by_observation_key: dict[str, list[dict[str, Any]]],
    bindings: dict[str, dict[str, Any]],
    *,
    history_path: Path,
    events_path: Path,
    binding_path: Path,
    binding_artifact_present: bool,
) -> dict[str, Any]:
    items = [
        build_item(
            entry,
            events_by_observation_key,
            bindings,
            binding_artifact_present=binding_artifact_present,
        )
        for entry in history_entries
    ]
    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_history_jsonl": str(history_path),
        "source_execution_events_jsonl": str(events_path),
        "source_binding_artifact": str(binding_path),
        "cost_accounting_capabilities": dict(COST_ACCOUNTING_CAPABILITIES),
        "binding_artifact_present": binding_artifact_present,
        "run_count": len(items),
        "fill_backed_run_count": sum(1 for item in items if int(item.get("fill_event_count", 0)) > 0),
        "no_fill_activity_count": sum(1 for item in items if item.get("paper_run_status") == "NO_FILL_ACTIVITY"),
        "open_position_run_count": sum(1 for item in items if item.get("final_position_direction") in {"LONG", "SHORT"}),
        "net_fee_backed_run_count": sum(1 for item in items if str(item.get("fee_support_status")) == "FILL_FEE_BACKED"),
        "funding_cost_backed_run_count": sum(
            1 for item in items if str(item.get("funding_support_status")) == "FUNDING_COST_BACKED"
        ),
        "profitability_interpretable_run_count": sum(
            1
            for item in items
            if str(item.get("profitability_status"))
            in {
                "NET_AFTER_FEES_AND_FUNDING",
                "NET_MARK_TO_MARKET_AFTER_FEES_AND_FUNDING",
                "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
            }
        ),
        "items": items,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    history_path = Path(args.history_jsonl).resolve()
    events_path = Path(args.execution_events_jsonl).resolve()
    binding_path = Path(args.binding_artifact).resolve()
    out_path = Path(args.out_json).resolve()

    history_entries = load_history_entries(history_path)
    events_by_observation_key = load_execution_events(events_path)
    bindings, binding_artifact_present = load_bindings(binding_path)
    payload = build_payload(
        history_entries,
        events_by_observation_key,
        bindings,
        history_path=history_path,
        events_path=events_path,
        binding_path=binding_path,
        binding_artifact_present=binding_artifact_present,
    )
    write_json(out_path, payload)

    print(f"history_jsonl={history_path}")
    print(f"execution_events_jsonl={events_path}")
    print(f"binding_artifact={binding_path}")
    print(f"futures_paper_ledger_json={out_path}")
    print(f"run_count={payload['run_count']}")
    print(f"fill_backed_run_count={payload['fill_backed_run_count']}")
    print(f"net_fee_backed_run_count={payload['net_fee_backed_run_count']}")
    print(f"funding_cost_backed_run_count={payload['funding_cost_backed_run_count']}")
    print(f"profitability_interpretable_run_count={payload['profitability_interpretable_run_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FuturesPaperLedgerError as exc:
        print(f"SHADOW_FUTURES_PAPER_LEDGER_V1_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
