#!/usr/bin/env python3
"""Build a compact operator-facing shadow execution outcome review from rollup snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


SCHEMA_VERSION = "shadow_execution_outcome_review_v0"
SOURCE_SCHEMA_VERSION = "shadow_execution_rollup_snapshot_v0"
DEFAULT_ROLLUP_SNAPSHOT_PATH = Path("tools") / "shadow_state" / "shadow_execution_rollup_snapshot_v0.json"
DEFAULT_OUT_PATH = Path("tools") / "shadow_state" / "shadow_execution_outcome_review_v0.json"
REQUIRED_TOP_LEVEL_FIELDS = {"schema_version", "generated_ts_utc", "selected_count", "items"}
REQUIRED_ITEM_FIELDS = {
    "selected_pack_id",
    "last_observed_at",
    "pnl_interpretation",
    "recent_pnl_bias",
    "recent_run_count",
    "recent_attention_count",
    "pnl_rollup_attention",
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a compact shadow execution outcome review v0")
    parser.add_argument("--rollup-snapshot", default=str(DEFAULT_ROLLUP_SNAPSHOT_PATH))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_PATH))
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_rollup_snapshot(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"rollup_snapshot_not_object:{path}")
    missing = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(obj))
    if missing:
        raise RuntimeError(f"rollup_snapshot_missing_fields:{path}:{','.join(missing)}")
    if str(obj.get("schema_version") or "") != SOURCE_SCHEMA_VERSION:
        raise RuntimeError(f"rollup_snapshot_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"rollup_snapshot_items_invalid:{path}")
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise RuntimeError(f"rollup_snapshot_item_not_object:{path}:{index}")
        item_missing = sorted(REQUIRED_ITEM_FIELDS - set(item))
        if item_missing:
            raise RuntimeError(f"rollup_snapshot_item_missing_fields:{path}:{index}:{','.join(item_missing)}")
    return obj


def normalize_flag(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def normalize_int(raw: Any) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return value if value >= 0 else 0


def latest_bucket(item: Dict[str, Any]) -> str:
    pnl_interpretation = str(item.get("pnl_interpretation") or "").strip().upper()
    if pnl_interpretation in {"ACTIVE_GAINING", "REALIZED_GAIN"}:
        return "GAIN"
    if pnl_interpretation in {"ACTIVE_LOSING", "REALIZED_LOSS"}:
        return "LOSS"
    if pnl_interpretation in {"ACTIVE_FLAT", "REALIZED_FLAT", "FLAT_NO_FILLS"}:
        return "FLAT"
    return "UNKNOWN"


def latest_vs_recent_consistency(item: Dict[str, Any], bucket: str) -> str:
    recent_run_count = normalize_int(item.get("recent_run_count"))
    recent_pnl_bias = str(item.get("recent_pnl_bias") or "").strip().upper() or "NO_HISTORY"
    if recent_run_count <= 0:
        return "UNKNOWN"
    if recent_pnl_bias == "NO_HISTORY":
        return "UNKNOWN"
    if bucket == "UNKNOWN":
        return "UNKNOWN"
    if bucket == "GAIN" and recent_pnl_bias == "GAIN_BIAS":
        return "CONSISTENT"
    if bucket == "LOSS" and recent_pnl_bias == "LOSS_BIAS":
        return "CONSISTENT"
    if bucket == "FLAT" and recent_pnl_bias == "FLAT_BIAS":
        return "CONSISTENT"
    return "DIVERGENT"


def outcome_class(item: Dict[str, Any], bucket: str) -> str:
    recent_run_count = normalize_int(item.get("recent_run_count"))
    recent_pnl_bias = str(item.get("recent_pnl_bias") or "").strip().upper() or "NO_HISTORY"
    if recent_run_count <= 0 or recent_pnl_bias == "NO_HISTORY":
        return "NO_RECENT_HISTORY"
    if bucket == "UNKNOWN":
        return "ATTENTION_REQUIRED"
    if bucket == "GAIN" and recent_pnl_bias == "GAIN_BIAS":
        return "STABLE_GAINING"
    if bucket == "LOSS" and recent_pnl_bias == "LOSS_BIAS":
        return "STABLE_LOSING"
    if bucket == "FLAT" and recent_pnl_bias == "FLAT_BIAS":
        return "STABLE_FLAT"
    return "MIXED_RECENT"


def outcome_attention_flag(item: Dict[str, Any], consistency: str) -> str:
    if normalize_flag(item.get("pnl_rollup_attention")) == "true":
        return "true"
    if consistency == "DIVERGENT":
        return "true"
    return "false"


def outcome_review_short(item: Dict[str, Any], cls: str, attention_flag: str) -> str:
    recent_pnl_bias = str(item.get("recent_pnl_bias") or "").strip().upper() or "NO_HISTORY"
    if cls == "NO_RECENT_HISTORY":
        return "no recent execution outcome history"
    if cls == "ATTENTION_REQUIRED":
        return "latest outcome is attention-class or unclear"
    if cls == "STABLE_GAINING":
        review = "latest and recent outcomes both lean gaining"
    elif cls == "STABLE_LOSING":
        review = "latest and recent outcomes both lean losing"
    elif cls == "STABLE_FLAT":
        review = "latest and recent outcomes both lean flat"
    elif recent_pnl_bias == "MIXED":
        review = "recent window is mixed against the latest outcome"
    else:
        review = "latest outcome diverges from recent bias"
    if attention_flag == "true" and cls in {
        "STABLE_GAINING",
        "STABLE_LOSING",
        "STABLE_FLAT",
        "MIXED_RECENT",
    }:
        review = f"{review}, but attention remains active"
    return review


def review_item(item: Dict[str, Any]) -> Dict[str, Any]:
    bucket = latest_bucket(item)
    consistency = latest_vs_recent_consistency(item, bucket)
    cls = outcome_class(item, bucket)
    attention_flag = outcome_attention_flag(item, consistency)
    return {
        "selected_pack_id": str(item.get("selected_pack_id") or ""),
        "last_observed_at": str(item.get("last_observed_at") or ""),
        "outcome_class": cls,
        "latest_vs_recent_consistency": consistency,
        "outcome_attention_flag": attention_flag,
        "outcome_review_short": outcome_review_short(item, cls, attention_flag),
    }


def build_review(rollup_snapshot_path: Path, rollup_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    items = [review_item(item) for item in list(rollup_snapshot.get("items") or [])]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_rollup_snapshot": str(rollup_snapshot_path),
        "rollup_snapshot_generated_ts_utc": str(rollup_snapshot.get("generated_ts_utc") or ""),
        "selected_count": len(items),
        "items": items,
    }


def write_review(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    rollup_snapshot_path = Path(args.rollup_snapshot)
    out_json_path = Path(args.out_json)
    rollup_snapshot = load_rollup_snapshot(rollup_snapshot_path)
    payload = build_review(rollup_snapshot_path, rollup_snapshot)
    write_review(out_json_path, payload)
    print(f"selected_count={payload['selected_count']}")
    print(f"out_json={out_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
