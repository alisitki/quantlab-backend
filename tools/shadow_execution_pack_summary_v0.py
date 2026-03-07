#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_ledger_v0.jsonl"
DEFAULT_OUT_JSON = ROOT / "tools" / "shadow_state" / "shadow_execution_pack_summary_v0.json"
LEDGER_SCHEMA_VERSION = "shadow_execution_ledger_v0"
SUMMARY_SCHEMA_VERSION = "shadow_execution_pack_summary_v0"
RECENT_WINDOW_SIZE = 3


class PackSummaryError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise PackSummaryError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic latest-by-pack summary from the shadow execution ledger."
    )
    parser.add_argument("--ledger-jsonl", default=str(DEFAULT_LEDGER_JSONL))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    return parser.parse_args(argv)


def load_ledger_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    deduped: dict[str, dict[str, Any]] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(f"ledger_invalid_json:{path}:{lineno}:{exc}")
        if not isinstance(obj, dict):
            fail(f"ledger_not_object:{path}:{lineno}")
        if obj.get("schema_version") != LEDGER_SCHEMA_VERSION:
            fail(f"ledger_schema_mismatch:{path}:{lineno}")
        observation_key = str(obj.get("observation_key", "")).strip()
        if not observation_key:
            fail(f"ledger_missing_observation_key:{path}:{lineno}")
        deduped[observation_key] = obj
    return sorted(
        deduped.values(),
        key=lambda row: (
            str(row.get("observed_at", "")),
            str(row.get("selected_pack_id", "")),
            str(row.get("live_run_id", "")),
        ),
    )


def normalize_bool(value: Any) -> bool:
    return bool(value)


def normalize_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


def normalize_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def derive_sign(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value > 0:
        return "GAIN"
    if value < 0:
        return "LOSS"
    return "FLAT"


def derive_pnl_interpretation(latest: dict[str, Any]) -> str:
    if not latest["last_snapshot_present"]:
        return "NO_SNAPSHOT"
    last_pnl_state = latest["last_pnl_state"]
    if last_pnl_state == "ACTIVE_POSITION":
        unrealized_sign = latest["latest_unrealized_sign"]
        if unrealized_sign == "GAIN":
            return "ACTIVE_GAINING"
        if unrealized_sign == "LOSS":
            return "ACTIVE_LOSING"
        if unrealized_sign == "FLAT":
            return "ACTIVE_FLAT"
        return "ACTIVE_UNKNOWN"
    if last_pnl_state in {"REALIZED_GAIN", "REALIZED_LOSS", "REALIZED_FLAT", "FLAT_NO_FILLS"}:
        return last_pnl_state
    return "UNKNOWN"


def derive_attention_flag(latest: dict[str, Any]) -> bool:
    return latest["pnl_interpretation"] in {
        "NO_SNAPSHOT",
        "ACTIVE_LOSING",
        "ACTIVE_UNKNOWN",
        "REALIZED_LOSS",
        "UNKNOWN",
    }


def row_interpretation(row: dict[str, Any]) -> str:
    candidate = {
        "last_snapshot_present": row["snapshot_present"],
        "last_pnl_state": row["pnl_state"],
        "latest_realized_sign": derive_sign(row["total_realized_pnl"]),
        "latest_unrealized_sign": derive_sign(row["total_unrealized_pnl"]),
    }
    return derive_pnl_interpretation(candidate)


def update_recent_counts(target: dict[str, Any], interpretation: str) -> None:
    if interpretation in {"ACTIVE_GAINING", "REALIZED_GAIN"}:
        target["recent_gain_count"] += 1
    elif interpretation in {"ACTIVE_LOSING", "REALIZED_LOSS"}:
        target["recent_loss_count"] += 1
    elif interpretation in {"ACTIVE_FLAT", "REALIZED_FLAT", "FLAT_NO_FILLS"}:
        target["recent_flat_count"] += 1
    if interpretation in {"NO_SNAPSHOT", "ACTIVE_LOSING", "ACTIVE_UNKNOWN", "REALIZED_LOSS", "UNKNOWN"}:
        target["recent_attention_count"] += 1


def derive_recent_pnl_bias(target: dict[str, Any]) -> str:
    recent_run_count = int(target["recent_run_count"])
    if recent_run_count <= 0:
        return "NO_HISTORY"
    counts = {
        "GAIN_BIAS": int(target["recent_gain_count"]),
        "LOSS_BIAS": int(target["recent_loss_count"]),
        "FLAT_BIAS": int(target["recent_flat_count"]),
    }
    best_label, best_value = max(counts.items(), key=lambda item: (item[1], item[0]))
    winners = [label for label, value in counts.items() if value == best_value]
    if best_value <= 0:
        return "MIXED"
    if len(winners) > 1:
        return "MIXED"
    return best_label


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "observation_key": str(row.get("observation_key", "")).strip(),
        "observed_at": str(row.get("observed_at", "")).strip(),
        "selected_pack_id": str(row.get("selected_pack_id", "")).strip(),
        "live_run_id": str(row.get("live_run_id", "")).strip(),
        "stop_reason": str(row.get("stop_reason", "")).strip(),
        "snapshot_present": normalize_bool(row.get("snapshot_present")),
        "positions_count": normalize_int(row.get("positions_count")),
        "fills_count": normalize_int(row.get("fills_count")),
        "total_realized_pnl": normalize_float_or_none(row.get("total_realized_pnl")),
        "total_unrealized_pnl": normalize_float_or_none(row.get("total_unrealized_pnl")),
        "equity": normalize_float_or_none(row.get("equity")),
        "max_position_value": normalize_float_or_none(row.get("max_position_value")),
        "pnl_state": str(row.get("pnl_state", "")).strip() or "UNKNOWN",
    }


def build_pack_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest_by_pack: dict[str, dict[str, Any]] = {}
    run_count_by_pack: dict[str, int] = {}
    normalized_rows_by_pack: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = normalize_row(raw)
        pack_id = row["selected_pack_id"]
        if not pack_id:
            fail("ledger_missing_selected_pack_id")
        run_count_by_pack[pack_id] = run_count_by_pack.get(pack_id, 0) + 1
        normalized_rows_by_pack.setdefault(pack_id, []).append(row)
        candidate = {
            "selected_pack_id": pack_id,
            "last_observed_at": row["observed_at"],
            "last_live_run_id": row["live_run_id"],
            "last_stop_reason": row["stop_reason"],
            "last_snapshot_present": row["snapshot_present"],
            "last_positions_count": row["positions_count"],
            "last_fills_count": row["fills_count"],
            "last_total_realized_pnl": row["total_realized_pnl"],
            "last_total_unrealized_pnl": row["total_unrealized_pnl"],
            "last_equity": row["equity"],
            "last_max_position_value": row["max_position_value"],
            "last_pnl_state": row["pnl_state"],
            "latest_realized_sign": derive_sign(row["total_realized_pnl"]),
            "latest_unrealized_sign": derive_sign(row["total_unrealized_pnl"]),
            "pnl_interpretation": "UNKNOWN",
            "pnl_attention_flag": False,
            "run_count": 0,
            "recent_run_count": 0,
            "recent_gain_count": 0,
            "recent_loss_count": 0,
            "recent_flat_count": 0,
            "recent_attention_count": 0,
            "recent_pnl_bias": "NO_HISTORY",
        }
        current = latest_by_pack.get(pack_id)
        if current is None or (candidate["last_observed_at"], candidate["last_live_run_id"]) > (
            current["last_observed_at"],
            current["last_live_run_id"],
        ):
            latest_by_pack[pack_id] = candidate
    for pack_id, run_count in run_count_by_pack.items():
        latest = latest_by_pack[pack_id]
        latest["run_count"] = run_count
        latest["pnl_interpretation"] = derive_pnl_interpretation(latest)
        latest["pnl_attention_flag"] = derive_attention_flag(latest)
        recent_rows = sorted(
            normalized_rows_by_pack[pack_id],
            key=lambda row: (row["observed_at"], row["live_run_id"]),
            reverse=True,
        )[:RECENT_WINDOW_SIZE]
        latest["recent_run_count"] = len(recent_rows)
        for row in recent_rows:
            update_recent_counts(latest, row_interpretation(row))
        latest["recent_pnl_bias"] = derive_recent_pnl_bias(latest)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "record_count": len(rows),
        "pack_count": len(latest_by_pack),
        "latest_by_pack_id": latest_by_pack,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    ledger_path = Path(args.ledger_jsonl).resolve()
    out_path = Path(args.out_json).resolve()

    rows = load_ledger_rows(ledger_path)
    payload = build_pack_summary(rows)
    write_json(out_path, payload)

    print(f"ledger_jsonl={ledger_path}")
    print(f"summary_json={out_path}")
    print(f"record_count={payload['record_count']}")
    print(f"pack_count={payload['pack_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PackSummaryError as exc:
        print(f"SHADOW_EXECUTION_PACK_SUMMARY_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
