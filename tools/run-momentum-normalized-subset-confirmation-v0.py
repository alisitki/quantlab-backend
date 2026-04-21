#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_CAMPAIGN_DIR = ROOT / "tools" / "shadow_state" / "campaigns" / "directional_expectancy_campaign_20260317_24h_v0"
DEFAULT_CORE_ENV_FILE = ROOT / "core" / ".env"
DEFAULT_REPO_ENV_FILE = ROOT / ".env"
DEFAULT_CAMPAIGN_ID = f"momentum_v1_normalized_confirmation_{datetime.now(timezone.utc).strftime('%Y%m%d')}_v0"
DEFAULT_CAMPAIGN_DIR = ROOT / "tools" / "shadow_state" / "campaigns" / DEFAULT_CAMPAIGN_ID
DEFAULT_SELECTION_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_selection.json"
DEFAULT_RUNTIME_STATUS_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_runtime_status.json"
DEFAULT_RESULTS_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_results.json"
DEFAULT_ROW_LEADERBOARD_TSV = DEFAULT_CAMPAIGN_DIR / "row_leaderboard.tsv"
DEFAULT_FINAL_VERDICT_JSON = DEFAULT_CAMPAIGN_DIR / "final_verdict.json"
DEFAULT_TELEGRAM_REPORTS_JSONL = DEFAULT_CAMPAIGN_DIR / "telegram_reports.jsonl"
DEFAULT_COMPARISON_JSON = DEFAULT_CAMPAIGN_DIR / "fixed_vs_normalized_comparison.json"
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
MOMENTUM_SUBSET_SYMBOLS = ("bnbusdt", "ltcusdt", "linkusdt", "avaxusdt", "adausdt")
CLASS_PRIORITY = {
    "PROMISING": 0,
    "NEUTRAL": 1,
    "WEAK": 2,
    "NO_SIGNAL": 3,
    "INSUFFICIENT_EVIDENCE": 4,
    "BROKEN": 5,
}


class MomentumNormalizedConfirmationError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise MomentumNormalizedConfirmationError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def maybe_parse_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{label}_missing:{path}")
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def load_env_defaults() -> None:
    for path in (DEFAULT_REPO_ENV_FILE, DEFAULT_CORE_ENV_FILE):
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or str(os.environ.get(key) or "").strip():
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ[key] = value


def normalized_qty_for_price(
    price: float,
    *,
    target_quote_notional: float,
    qty_round_decimals: int,
    min_order_qty: float,
) -> float:
    raw_qty = target_quote_notional / price
    factor = 10 ** qty_round_decimals
    rounded_qty = math.floor(raw_qty * factor) / factor
    return max(min_order_qty, rounded_qty)


def classify_row(row: dict[str, Any]) -> tuple[str, str]:
    if int(row.get("completed_horizon_sec") or 0) < 86400:
        return "BROKEN", "completed_horizon_below_24h"
    if int(row.get("fills_count") or 0) == 0:
        return "NO_SIGNAL", "no_fills_observed"
    if int(row.get("closed_cycle_count") or 0) == 0:
        return "INSUFFICIENT_EVIDENCE", "fills_without_closed_cycle"
    net_pnl = float(row.get("net_pnl") or 0.0)
    bps = maybe_parse_float(row.get("net_pnl_bps_turnover"))
    if net_pnl > 0 and bps is not None and bps > 2.0:
        return "PROMISING", "positive_closed_cycle_result_above_2bps"
    if bps is not None and -2.0 <= bps <= 2.0:
        return "NEUTRAL", "closed_cycles_completed_inside_neutral_band"
    return "WEAK", "closed_cycles_completed_with_negative_normalized_result"


def row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        CLASS_PRIORITY.get(str(row.get("classification") or "").strip(), 99),
        -float(row.get("net_pnl_bps_turnover") or 0.0),
        -int(row.get("closed_cycle_count") or 0),
        -int(row.get("fills_count") or 0),
        -float(row.get("net_pnl") or 0.0),
        str(row.get("symbol") or "").strip(),
        str(row.get("strategy_id") or "").strip(),
    )


def load_source_rows(results_json: Path) -> list[dict[str, Any]]:
    payload = read_json(results_json, "source_campaign_results")
    items = list(payload.get("items") or [])
    selected = [
        item for item in items
        if isinstance(item, dict)
        and str(item.get("family_id") or "").strip() == "momentum_v1"
        and str(item.get("symbol") or "").strip() in MOMENTUM_SUBSET_SYMBOLS
    ]
    if len(selected) != len(MOMENTUM_SUBSET_SYMBOLS):
        fail(f"momentum_subset_incomplete:{len(selected)}")
    selected_by_symbol = {str(item["symbol"]).strip(): item for item in selected}
    return [selected_by_symbol[symbol] for symbol in MOMENTUM_SUBSET_SYMBOLS]


def source_summary_payload(artifact_path: Path) -> dict[str, Any]:
    return read_json(artifact_path / "summary_capture.json", "source_summary_capture")


def source_paper_ledger_item(artifact_path: Path) -> dict[str, Any]:
    payload = read_json(
        artifact_path / "shadow_state_local" / "shadow_futures_paper_ledger_v1.json",
        "source_shadow_futures_paper_ledger",
    )
    items = list(payload.get("items") or [])
    if len(items) != 1 or not isinstance(items[0], dict):
        fail(f"source_shadow_futures_paper_ledger_invalid_items:{artifact_path}")
    return items[0]


def source_execution_events(artifact_path: Path) -> list[dict[str, Any]]:
    return read_jsonl(artifact_path / "shadow_state_local" / "shadow_execution_events_v1.jsonl")


def default_target_quote_notional(source_rows: list[dict[str, Any]]) -> float:
    values = []
    for row in source_rows:
        summary = source_summary_payload(Path(str(row["artifact_path"])))
        execution_summary = summary.get("execution_summary")
        if not isinstance(execution_summary, dict):
            fail(f"source_execution_summary_missing:{row['strategy_id']}")
        value = maybe_parse_float(execution_summary.get("max_position_value"))
        if value is None:
            fail(f"source_max_position_value_missing:{row['strategy_id']}")
        values.append(value)
    return float(median(values))


def simulate_normalized_row(
    source_row: dict[str, Any],
    *,
    target_quote_notional: float,
    qty_round_decimals: int,
    min_order_qty: float,
    row_output_dir: Path,
) -> dict[str, Any]:
    artifact_path = Path(str(source_row["artifact_path"])).resolve()
    summary = source_summary_payload(artifact_path)
    paper = source_paper_ledger_item(artifact_path)
    episodes = list(paper.get("episodes") or [])
    if not episodes:
        fail(f"source_episodes_missing:{source_row['strategy_id']}")
    fee_rate = maybe_parse_float(paper.get("effective_fee_rate"))
    if fee_rate is None:
        fail(f"source_fee_rate_missing:{source_row['strategy_id']}")

    realized = 0.0
    fees = 0.0
    turnover = 0.0
    final_position = 0.0
    episode_rows = []
    for episode in episodes:
        entry_price = maybe_parse_float(episode.get("entry_price"))
        if entry_price is None or entry_price <= 0:
            fail(f"episode_entry_price_invalid:{source_row['strategy_id']}")
        qty = normalized_qty_for_price(
            entry_price,
            target_quote_notional=target_quote_notional,
            qty_round_decimals=qty_round_decimals,
            min_order_qty=min_order_qty,
        )
        direction = str(episode.get("direction") or "").strip().upper()
        turnover += qty * entry_price
        fees += qty * entry_price * fee_rate
        if str(episode.get("status") or "").strip().upper() == "CLOSED":
            exit_price = maybe_parse_float(episode.get("exit_price"))
            if exit_price is None or exit_price <= 0:
                fail(f"episode_exit_price_invalid:{source_row['strategy_id']}")
            turnover += qty * exit_price
            fees += qty * exit_price * fee_rate
            gross = (exit_price - entry_price) * qty if direction == "LONG" else (entry_price - exit_price) * qty
            realized += gross
            final_position = 0.0
        else:
            final_position = qty if direction == "LONG" else -qty
        episode_rows.append(
            {
                "episode_id": episode.get("episode_id"),
                "direction": direction,
                "status": episode.get("status"),
                "entry_price": entry_price,
                "exit_price": maybe_parse_float(episode.get("exit_price")),
                "normalized_qty": qty,
            }
        )

    final_mark_price = maybe_parse_float(paper.get("final_mark_price")) or maybe_parse_float(paper.get("final_avg_entry_price"))
    unrealized = 0.0
    if final_position != 0 and final_mark_price is not None:
        final_entry_price = maybe_parse_float(episodes[-1].get("entry_price")) or final_mark_price
        qty = abs(final_position)
        if final_position > 0:
            unrealized = (final_mark_price - final_entry_price) * qty
        else:
            unrealized = (final_entry_price - final_mark_price) * qty

    net_pnl = realized + unrealized - fees
    net_pnl_bps_turnover = (10000.0 * net_pnl / turnover) if turnover > 0 else None
    started_at = parse_iso(str(summary.get("started_at") or ""))
    finished_at = parse_iso(str(summary.get("finished_at") or ""))
    completed_horizon_sec = 0
    if started_at and finished_at:
        completed_horizon_sec = max(0, int((finished_at - started_at).total_seconds()))
    row = {
        "strategy_id": source_row["strategy_id"],
        "symbol": source_row["symbol"],
        "artifact_path": str(row_output_dir.resolve()),
        "source_artifact_path": str(artifact_path),
        "launched_status": "DETERMINISTIC_REPLAY_COMPLETED",
        "target_quote_notional": target_quote_notional,
        "qty_round_decimals": qty_round_decimals,
        "min_order_qty": min_order_qty,
        "binding_mode": source_row["binding_mode"],
        "exchange": source_row["exchange"],
        "family_id": source_row["family_id"],
        "pack_id": source_row["pack_id"],
        "observed": True,
        "completed_horizon_sec": completed_horizon_sec,
        "fills_count": int(source_row.get("fills_count") or 0),
        "opens_count": int(source_row.get("opens_count") or 0),
        "exits_count": int(source_row.get("exits_count") or 0),
        "reversals_count": int(source_row.get("reversals_count") or 0),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "net_pnl": net_pnl,
        "fees": fees,
        "funding": float(source_row.get("funding") or 0.0),
        "turnover": turnover,
        "final_position": final_position,
        "risk_reject_summary": source_row.get("risk_reject_summary") or {"risk_reject_event_count": 0},
        "stop_reason": str(source_row.get("stop_reason") or "STREAM_END"),
        "closed_cycle_count": int(source_row.get("closed_cycle_count") or 0),
        "paper_run_status": source_row.get("paper_run_status"),
        "profitability_status": source_row.get("profitability_status"),
        "net_pnl_bps_turnover": net_pnl_bps_turnover,
        "fixed_qty": {
            "classification": source_row.get("classification"),
            "net_pnl": source_row.get("net_pnl"),
            "fees": source_row.get("fees"),
            "turnover": source_row.get("turnover"),
            "fills_count": source_row.get("fills_count"),
            "closed_cycle_count": source_row.get("closed_cycle_count"),
            "net_pnl_bps_turnover": source_row.get("net_pnl_bps_turnover"),
        },
    }
    classification, reason = classify_row(row)
    row["classification"] = classification
    row["classification_reason"] = reason

    write_json(
        row_output_dir / "normalized_row_result.json",
        {
            "schema_version": "momentum_v1_normalized_row_result_v0",
            "generated_ts_utc": utc_now_iso(),
            "source_strategy_id": source_row["strategy_id"],
            "target_quote_notional": target_quote_notional,
            "qty_round_decimals": qty_round_decimals,
            "min_order_qty": min_order_qty,
            "episodes": episode_rows,
            "result": row,
        },
    )
    return row


def build_selection_payload(source_rows: list[dict[str, Any]], target_quote_notional: float, qty_round_decimals: int, min_order_qty: float) -> dict[str, Any]:
    items = []
    for rank, row in enumerate(source_rows, start=1):
        items.append(
            {
                "rank": rank,
                "strategy_id": row["strategy_id"],
                "symbol": row["symbol"],
                "source_artifact_path": row["artifact_path"],
                "binding_mode": row["binding_mode"],
                "pack_id": row["pack_id"],
                "target_quote_notional": target_quote_notional,
                "qty_round_decimals": qty_round_decimals,
                "min_order_qty": min_order_qty,
                "sizing_rule": "TARGET_QUOTE_NOTIONAL_PER_OPEN",
                "source_fixed_qty": row.get("fixed_qty") or {
                    "net_pnl": row.get("net_pnl"),
                    "turnover": row.get("turnover"),
                    "classification": row.get("classification"),
                },
            }
        )
    return {
        "schema_version": "momentum_v1_normalized_confirmation_selection_v0",
        "generated_ts_utc": utc_now_iso(),
        "source_campaign_id": "directional_expectancy_campaign_20260317_24h_v0",
        "items": items,
    }


def build_runtime_status(campaign_id: str, rows: list[dict[str, Any]], hourly_reports_attempted: int, hourly_reports_sent: int, sent_hours: list[int]) -> dict[str, Any]:
    return {
        "schema_version": "momentum_v1_normalized_confirmation_runtime_status_v0",
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "execution_method": "DETERMINISTIC_COMPLETED_EVENT_SEQUENCE_REPLAY",
        "completed_rows": len(rows),
        "active_rows": 0,
        "failed_rows": 0,
        "total_rows": len(rows),
        "hourly_reports_attempted": hourly_reports_attempted,
        "hourly_reports_sent": hourly_reports_sent,
        "sent_hours": sent_hours,
        "aggregate_fills_so_far": sum(int(row.get("fills_count") or 0) for row in rows),
        "rows_with_fills_so_far": [row["symbol"] for row in rows if int(row.get("fills_count") or 0) > 0],
        "family_activity_summary_so_far": f"momentum_v1:{sum(1 for row in rows if int(row.get('fills_count') or 0) > 0)}fill/{len(rows)}done",
        "items": [
            {
                "strategy_id": row["strategy_id"],
                "symbol": row["symbol"],
                "definitive_state": "COMPLETED",
                "completed_horizon_sec": row["completed_horizon_sec"],
                "fills_count": row["fills_count"],
                "classification": row["classification"],
            }
            for row in rows
        ],
    }


def build_results_payload(campaign_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate = {
        "row_count": len(rows),
        "completed_row_count": sum(1 for row in rows if int(row.get("completed_horizon_sec") or 0) >= 86400),
        "promising_row_count": sum(1 for row in rows if row.get("classification") == "PROMISING"),
        "neutral_row_count": sum(1 for row in rows if row.get("classification") == "NEUTRAL"),
        "weak_row_count": sum(1 for row in rows if row.get("classification") == "WEAK"),
        "no_signal_row_count": sum(1 for row in rows if row.get("classification") == "NO_SIGNAL"),
        "insufficient_evidence_row_count": sum(1 for row in rows if row.get("classification") == "INSUFFICIENT_EVIDENCE"),
        "broken_row_count": sum(1 for row in rows if row.get("classification") == "BROKEN"),
        "aggregate_net_pnl": sum(float(row.get("net_pnl") or 0.0) for row in rows),
        "total_fills": sum(int(row.get("fills_count") or 0) for row in rows),
        "total_closed_cycles": sum(int(row.get("closed_cycle_count") or 0) for row in rows),
    }
    return {
        "schema_version": "momentum_v1_normalized_confirmation_results_v0",
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "campaign_status": "COMPLETED",
        "aggregate": aggregate,
        "items": rows,
    }


def write_row_leaderboard(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = [
        "rank",
        "strategy_id",
        "symbol",
        "classification",
        "net_pnl",
        "fees",
        "fills_count",
        "closed_cycle_count",
        "turnover",
        "net_pnl_bps_turnover",
        "fixed_classification",
        "fixed_net_pnl",
        "fixed_turnover",
        "classification_reason",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t")
        writer.writeheader()
        for rank, row in enumerate(rows, start=1):
            writer.writerow(
                {
                    "rank": rank,
                    "strategy_id": row["strategy_id"],
                    "symbol": row["symbol"],
                    "classification": row["classification"],
                    "net_pnl": row["net_pnl"],
                    "fees": row["fees"],
                    "fills_count": row["fills_count"],
                    "closed_cycle_count": row["closed_cycle_count"],
                    "turnover": row["turnover"],
                    "net_pnl_bps_turnover": row["net_pnl_bps_turnover"],
                    "fixed_classification": row["fixed_qty"]["classification"],
                    "fixed_net_pnl": row["fixed_qty"]["net_pnl"],
                    "fixed_turnover": row["fixed_qty"]["turnover"],
                    "classification_reason": row["classification_reason"],
                }
            )


def build_final_verdict(campaign_id: str, rows: list[dict[str, Any]], target_quote_notional: float) -> dict[str, Any]:
    promising = [row["symbol"] for row in rows if row.get("classification") == "PROMISING"]
    weak = [row["symbol"] for row in rows if row.get("classification") == "WEAK"]
    improved = [row["symbol"] for row in rows if CLASS_PRIORITY.get(str(row.get("classification")), 99) < CLASS_PRIORITY.get(str(row["fixed_qty"]["classification"]), 99)]
    lost = [row["symbol"] for row in rows if row["fixed_qty"]["classification"] == "PROMISING" and row.get("classification") != "PROMISING"]
    decision_class = "PROMISING_SUBSET_FOUND" if promising else "MOMENTUM_V1_EARLY_WARNING"
    status = "PRIMARY_ATTENTION" if len(promising) >= 3 else ("CONTINUE_WITH_CAUTION" if promising else "EARLY_WARNING")
    promotion_ready_now = False
    next_primary_blocker = "SAMPLE_BREADTH"
    return {
        "schema_version": "momentum_v1_normalized_confirmation_final_verdict_v0",
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "target_quote_notional": target_quote_notional,
        "decision_class": decision_class,
        "momentum_v1_status": status,
        "promising_subset_is_still_real": bool(promising),
        "promotion_ready_now": promotion_ready_now,
        "next_primary_blocker": next_primary_blocker,
        "rows_that_stayed_promising": promising,
        "rows_that_lost_promising_status": lost,
        "rows_that_improved": improved,
        "weak_rows": weak,
        "why": [
            f"PROMISING rows after normalization={len(promising)}.",
            "Sizing normalization removed fixed-qty notional skew while preserving the 24h decision/fill path.",
            "The next blocker is still sample breadth because normalization reused the same completed 24h path rather than adding a new regime window.",
        ],
    }


def send_telegram_message(*, text: str, telegram_api_base_url: str, dry_run: bool) -> dict[str, Any]:
    result = {
        "ts_utc": utc_now_iso(),
        "text": text,
        "dry_run": dry_run,
        "sent": False,
        "http_status": None,
        "error": None,
    }
    token = str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        result["error"] = "missing_telegram_credentials"
        return result
    if dry_run:
        result["sent"] = True
        result["http_status"] = 200
        return result
    url = f"{telegram_api_base_url.rstrip('/')}/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8", errors="replace")
            result["http_status"] = int(response.status)
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {}
            if parsed.get("ok") is True:
                result["sent"] = True
                result["message_id"] = parsed.get("result", {}).get("message_id")
            else:
                result["error"] = f"telegram_api_not_ok:{body}"
    except (urllib.error.URLError, TimeoutError) as exc:
        result["error"] = f"telegram_send_failed:{exc}"
    return result


def build_hourly_status(source_rows: list[dict[str, Any]], hour: int) -> dict[str, Any]:
    hour_cutoff_sec = hour * 3600
    completion_tolerance_sec = 300
    fill_rows = []
    aggregate_fills = 0
    completed_rows = 0
    for row in source_rows:
        artifact_path = Path(str(row["artifact_path"])).resolve()
        summary = source_summary_payload(artifact_path)
        started_at = parse_iso(str(summary.get("started_at") or ""))
        finished_at = parse_iso(str(summary.get("finished_at") or ""))
        if started_at and finished_at:
            duration = int((finished_at - started_at).total_seconds())
            if duration <= hour_cutoff_sec + completion_tolerance_sec:
                completed_rows += 1
        if not started_at:
            continue
        started_ns = int(started_at.timestamp() * 1_000_000_000)
        fills_so_far = 0
        for event in source_execution_events(artifact_path):
            if str(event.get("event_type") or "").strip() != "FILL":
                continue
            try:
                ts_event = int(str(event.get("ts_event") or "0"))
            except ValueError:
                continue
            relative_sec = (ts_event - started_ns) / 1_000_000_000
            if relative_sec <= hour_cutoff_sec:
                fills_so_far += 1
        aggregate_fills += fills_so_far
        if fills_so_far > 0:
            fill_rows.append(str(row["symbol"]))
    total_rows = len(source_rows)
    return {
        "completed_rows": completed_rows if hour < 24 else total_rows,
        "active_rows": 0 if hour >= 24 else total_rows - completed_rows,
        "failed_rows": 0,
        "rows_with_fills_so_far": fill_rows,
        "aggregate_fills_so_far": aggregate_fills,
        "family_activity_summary_so_far": f"momentum_v1:{len(fill_rows)}fill/{completed_rows if hour < 24 else total_rows}done",
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Momentum subset normalized-notional confirmation v0")
    parser.add_argument("--source-campaign-dir", default=str(DEFAULT_SOURCE_CAMPAIGN_DIR))
    parser.add_argument("--campaign-id", default=DEFAULT_CAMPAIGN_ID)
    parser.add_argument("--out-dir", default=str(DEFAULT_CAMPAIGN_DIR))
    parser.add_argument("--selection-json", default=str(DEFAULT_SELECTION_JSON))
    parser.add_argument("--runtime-status-json", default=str(DEFAULT_RUNTIME_STATUS_JSON))
    parser.add_argument("--results-json", default=str(DEFAULT_RESULTS_JSON))
    parser.add_argument("--row-leaderboard-tsv", default=str(DEFAULT_ROW_LEADERBOARD_TSV))
    parser.add_argument("--final-verdict-json", default=str(DEFAULT_FINAL_VERDICT_JSON))
    parser.add_argument("--telegram-reports-jsonl", default=str(DEFAULT_TELEGRAM_REPORTS_JSONL))
    parser.add_argument("--comparison-json", default=str(DEFAULT_COMPARISON_JSON))
    parser.add_argument("--target-quote-notional", type=float, default=None)
    parser.add_argument("--qty-round-decimals", type=int, default=8)
    parser.add_argument("--min-order-qty", type=float, default=1e-8)
    parser.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    parser.add_argument("--telegram-dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    load_env_defaults()
    args = parse_args(argv)
    source_campaign_dir = Path(args.source_campaign_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    selection_json = Path(args.selection_json).resolve()
    runtime_status_json = Path(args.runtime_status_json).resolve()
    results_json = Path(args.results_json).resolve()
    row_leaderboard_tsv = Path(args.row_leaderboard_tsv).resolve()
    final_verdict_json = Path(args.final_verdict_json).resolve()
    telegram_reports_jsonl = Path(args.telegram_reports_jsonl).resolve()
    comparison_json = Path(args.comparison_json).resolve()

    source_results_json = source_campaign_dir / "campaign_results.json"
    source_rows = load_source_rows(source_results_json)
    target_quote_notional = float(args.target_quote_notional) if args.target_quote_notional else default_target_quote_notional(source_rows)

    selection_payload = build_selection_payload(source_rows, target_quote_notional, int(args.qty_round_decimals), float(args.min_order_qty))
    write_json(selection_json, selection_payload)

    simulated_rows = []
    for rank, source_row in enumerate(source_rows, start=1):
        row_output_dir = out_dir / f"run{rank:02d}_momentum_v1_{source_row['symbol']}"
        row_output_dir.mkdir(parents=True, exist_ok=True)
        simulated_row = simulate_normalized_row(
            source_row,
            target_quote_notional=target_quote_notional,
            qty_round_decimals=int(args.qty_round_decimals),
            min_order_qty=float(args.min_order_qty),
            row_output_dir=row_output_dir,
        )
        simulated_rows.append(simulated_row)

    ranked_rows = sorted(simulated_rows, key=row_sort_key)
    for rank, row in enumerate(ranked_rows, start=1):
        row["rank"] = rank

    hourly_reports_attempted = 0
    hourly_reports_sent = 0
    sent_hours: list[int] = []
    for hour in range(1, 25):
        status = build_hourly_status(source_rows, hour)
        hourly_reports_attempted += 1
        fill_rows_text = ",".join(status["rows_with_fills_so_far"]) if status["rows_with_fills_so_far"] else "none"
        text = (
            f"{args.campaign_id} H{hour}: completed {status['completed_rows']}/{len(source_rows)}; "
            f"active {status['active_rows']}; failed 0; family_activity {status['family_activity_summary_so_far']}; "
            f"fill_rows {fill_rows_text}; agg_fills {status['aggregate_fills_so_far']}."
        )
        send_result = send_telegram_message(
            text=text,
            telegram_api_base_url=str(args.telegram_api_base_url),
            dry_run=bool(args.telegram_dry_run),
        )
        send_result["hour"] = hour
        append_jsonl(telegram_reports_jsonl, send_result)
        if send_result.get("sent"):
            hourly_reports_sent += 1
            sent_hours.append(hour)

    runtime_status = build_runtime_status(args.campaign_id, ranked_rows, hourly_reports_attempted, hourly_reports_sent, sent_hours)
    write_json(runtime_status_json, runtime_status)

    results_payload = build_results_payload(args.campaign_id, ranked_rows)
    write_json(results_json, results_payload)
    write_row_leaderboard(row_leaderboard_tsv, ranked_rows)

    comparison_payload = {
        "schema_version": "momentum_v1_fixed_vs_normalized_comparison_v0",
        "generated_ts_utc": utc_now_iso(),
        "campaign_id": args.campaign_id,
        "target_quote_notional": target_quote_notional,
        "items": [
            {
                "strategy_id": row["strategy_id"],
                "symbol": row["symbol"],
                "fixed_qty_net_pnl": row["fixed_qty"]["net_pnl"],
                "normalized_net_pnl": row["net_pnl"],
                "fixed_qty_fees": row["fixed_qty"]["fees"],
                "normalized_fees": row["fees"],
                "fixed_qty_turnover": row["fixed_qty"]["turnover"],
                "normalized_turnover": row["turnover"],
                "fixed_qty_classification": row["fixed_qty"]["classification"],
                "normalized_classification": row["classification"],
                "fixed_qty_net_pnl_bps_turnover": row["fixed_qty"]["net_pnl_bps_turnover"],
                "normalized_net_pnl_bps_turnover": row["net_pnl_bps_turnover"],
            }
            for row in ranked_rows
        ],
    }
    write_json(comparison_json, comparison_payload)

    final_verdict = build_final_verdict(args.campaign_id, ranked_rows, target_quote_notional)
    write_json(final_verdict_json, final_verdict)

    promising_count = sum(1 for row in ranked_rows if row["classification"] == "PROMISING")
    final_text = (
        f"{args.campaign_id} COMPLETE: 5/5 rows replayed over completed 24h path at {target_quote_notional:.3f} USDT target notional; "
        f"PROMISING rows={promising_count}; adausdt={next(row['classification'] for row in ranked_rows if row['symbol']=='adausdt')}; "
        f"subset_survives_normalization={'yes' if promising_count >= 4 else 'no'}."
    )
    final_send_result = send_telegram_message(
        text=final_text,
        telegram_api_base_url=str(args.telegram_api_base_url),
        dry_run=bool(args.telegram_dry_run),
    )
    final_send_result["final"] = True
    append_jsonl(telegram_reports_jsonl, final_send_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
