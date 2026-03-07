#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_JSONL = ROOT / "tools" / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_ledger_v0.jsonl"
HISTORY_SCHEMA_VERSION = "shadow_observation_history_v0"
LEDGER_SCHEMA_VERSION = "shadow_execution_ledger_v0"


class LedgerError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise LedgerError(message)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic per-run shadow execution ledger from observation history."
    )
    parser.add_argument("--history-jsonl", default=str(DEFAULT_HISTORY_JSONL))
    parser.add_argument("--out-jsonl", default=str(DEFAULT_LEDGER_JSONL))
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
        ),
    )


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


def derive_pnl_state(execution_summary: dict[str, Any]) -> str:
    if not execution_summary["snapshot_present"]:
        return "NO_SNAPSHOT"
    if execution_summary["positions_count"] > 0:
        return "ACTIVE_POSITION"
    if execution_summary["fills_count"] <= 0:
        return "FLAT_NO_FILLS"
    realized = execution_summary["total_realized_pnl"]
    if realized is None:
        return "REALIZED_FLAT"
    if realized > 0:
        return "REALIZED_GAIN"
    if realized < 0:
        return "REALIZED_LOSS"
    return "REALIZED_FLAT"


def build_ledger_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        execution_summary = normalize_execution_summary(entry.get("execution_summary"))
        rows.append(
            {
                "schema_version": LEDGER_SCHEMA_VERSION,
                "observation_key": str(entry["observation_key"]).strip(),
                "observed_at": str(entry.get("observed_at", "")).strip(),
                "selected_pack_id": str(entry.get("selected_pack_id", "")).strip(),
                "live_run_id": str(entry.get("live_run_id", "")).strip(),
                "stop_reason": str(entry.get("stop_reason", "")).strip(),
                "snapshot_present": execution_summary["snapshot_present"],
                "positions_count": execution_summary["positions_count"],
                "fills_count": execution_summary["fills_count"],
                "total_realized_pnl": execution_summary["total_realized_pnl"],
                "total_unrealized_pnl": execution_summary["total_unrealized_pnl"],
                "equity": execution_summary["equity"],
                "max_position_value": execution_summary["max_position_value"],
                "pnl_state": derive_pnl_state(execution_summary),
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
    rows = build_ledger_rows(entries)
    write_jsonl(out_path, rows)

    print(f"history_jsonl={history_path}")
    print(f"ledger_jsonl={out_path}")
    print(f"history_count={len(entries)}")
    print(f"ledger_count={len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LedgerError as exc:
        print(f"SHADOW_EXECUTION_LEDGER_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
