#!/usr/bin/env python3
"""Build a small operator-facing shadow execution trend/review queue from operator snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


SCHEMA_VERSION = "shadow_execution_review_queue_v0"
SOURCE_SCHEMA_VERSION = "shadow_operator_snapshot_v0"
DEFAULT_OPERATOR_SNAPSHOT_PATH = Path("tools") / "shadow_state" / "shadow_operator_snapshot_v0.json"
DEFAULT_OUT_PATH = Path("tools") / "shadow_state" / "shadow_execution_review_queue_v0.json"
REQUIRED_TOP_LEVEL_FIELDS = {"schema_version", "generated_ts_utc", "selected_count", "items"}
REQUIRED_ITEM_FIELDS = {
    "rank",
    "pack_id",
    "pnl_interpretation",
    "outcome_class",
    "latest_vs_recent_consistency",
    "outcome_attention_flag",
    "outcome_review_short",
}
PRIORITY_ORDER = {
    "HIGH": 0,
    "NORMAL": 1,
    "LOW": 2,
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a compact shadow execution review queue v0")
    parser.add_argument("--operator-snapshot", default=str(DEFAULT_OPERATOR_SNAPSHOT_PATH))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_PATH))
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_operator_snapshot(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"operator_snapshot_not_object:{path}")
    missing = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(obj))
    if missing:
        raise RuntimeError(f"operator_snapshot_missing_fields:{path}:{','.join(missing)}")
    if str(obj.get("schema_version") or "") != SOURCE_SCHEMA_VERSION:
        raise RuntimeError(f"operator_snapshot_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"operator_snapshot_items_invalid:{path}")
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise RuntimeError(f"operator_snapshot_item_not_object:{path}:{index}")
        item_missing = sorted(REQUIRED_ITEM_FIELDS - set(item))
        if item_missing:
            raise RuntimeError(f"operator_snapshot_item_missing_fields:{path}:{index}:{','.join(item_missing)}")
    return obj


def normalize_flag(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def normalize_rank(raw: Any, index: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"operator_snapshot_rank_invalid:{index}") from exc
    if value < 0:
        raise RuntimeError(f"operator_snapshot_rank_invalid:{index}")
    return value


def trend_direction(item: Dict[str, Any]) -> str:
    pnl_interpretation = str(item.get("pnl_interpretation") or "").strip().upper()
    if pnl_interpretation in {"ACTIVE_GAINING", "REALIZED_GAIN"}:
        return "GAINING"
    if pnl_interpretation in {"ACTIVE_LOSING", "REALIZED_LOSS"}:
        return "LOSING"
    if pnl_interpretation in {"ACTIVE_FLAT", "REALIZED_FLAT", "FLAT_NO_FILLS"}:
        return "FLAT"
    return "UNKNOWN"


def trend_class(item: Dict[str, Any]) -> str:
    outcome_class = str(item.get("outcome_class") or "").strip().upper()
    if outcome_class in {"STABLE_GAINING", "STABLE_LOSING", "STABLE_FLAT"}:
        return "STABLE"
    if outcome_class == "MIXED_RECENT":
        return "MIXED"
    if outcome_class == "ATTENTION_REQUIRED":
        return "ATTENTION"
    if outcome_class in {"NO_RECENT_HISTORY", "UNKNOWN", ""}:
        return "NO_HISTORY"
    return "ATTENTION"


def trend_attention_flag(item: Dict[str, Any]) -> str:
    return normalize_flag(item.get("outcome_attention_flag"))


def review_priority_bucket(trend_class_value: str, trend_attention_flag_value: str) -> str:
    if trend_class_value == "ATTENTION":
        return "HIGH"
    if trend_attention_flag_value == "true":
        return "HIGH"
    if trend_class_value == "MIXED":
        return "NORMAL"
    if trend_class_value in {"STABLE", "NO_HISTORY"}:
        return "LOW"
    return "HIGH"


def review_reason_short(
    trend_class_value: str,
    trend_direction_value: str,
    trend_attention_flag_value: str,
) -> str:
    if trend_class_value == "NO_HISTORY":
        return "no execution trend history"
    if trend_class_value == "ATTENTION":
        if trend_direction_value == "LOSING":
            return "losing or unclear latest execution outcome"
        return "latest execution outcome requires attention"
    if trend_class_value == "MIXED":
        if trend_attention_flag_value == "true":
            return "mixed recent execution outcome, attention active"
        return "mixed recent execution outcome"
    if trend_class_value == "STABLE":
        if trend_direction_value == "GAINING":
            if trend_attention_flag_value == "true":
                return "stable gaining execution trend, attention active"
            return "stable gaining execution trend"
        if trend_direction_value == "LOSING":
            if trend_attention_flag_value == "true":
                return "stable losing execution trend, attention active"
            return "stable losing execution trend"
        if trend_direction_value == "FLAT":
            if trend_attention_flag_value == "true":
                return "stable flat execution trend, attention active"
            return "stable flat execution trend"
        return "stable execution trend"
    return "execution trend requires review"


def queue_item(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    source_rank = normalize_rank(item.get("rank"), index)
    pack_id = str(item.get("pack_id") or "")
    trend_class_value = trend_class(item)
    trend_direction_value = trend_direction(item)
    trend_attention_flag_value = trend_attention_flag(item)
    return {
        "review_rank": 0,
        "source_rank": source_rank,
        "pack_id": pack_id,
        "trend_class": trend_class_value,
        "trend_direction": trend_direction_value,
        "trend_attention_flag": trend_attention_flag_value,
        "review_priority_bucket": review_priority_bucket(trend_class_value, trend_attention_flag_value),
        "review_reason_short": review_reason_short(
            trend_class_value,
            trend_direction_value,
            trend_attention_flag_value,
        ),
    }


def assign_review_ranks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(
        items,
        key=lambda item: (
            PRIORITY_ORDER.get(str(item.get("review_priority_bucket") or ""), 99),
            int(item.get("source_rank") or 0),
            str(item.get("pack_id") or ""),
        ),
    )
    return [
        {
            **item,
            "review_rank": index,
        }
        for index, item in enumerate(ordered, start=1)
    ]


def build_review_queue(operator_snapshot_path: Path, operator_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    items = [
        queue_item(item, index)
        for index, item in enumerate(list(operator_snapshot.get("items") or []))
    ]
    ranked_items = assign_review_ranks(items)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_operator_snapshot": str(operator_snapshot_path),
        "operator_snapshot_generated_ts_utc": str(operator_snapshot.get("generated_ts_utc") or ""),
        "selected_count": len(ranked_items),
        "items": ranked_items,
    }


def write_review_queue(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    operator_snapshot_path = Path(args.operator_snapshot)
    out_json_path = Path(args.out_json)
    operator_snapshot = load_operator_snapshot(operator_snapshot_path)
    payload = build_review_queue(operator_snapshot_path, operator_snapshot)
    write_review_queue(out_json_path, payload)
    print(f"selected_count={payload['selected_count']}")
    print(f"out_json={out_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
