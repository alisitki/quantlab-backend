#!/usr/bin/env python3
"""Build a small operator-facing combined shadow snapshot from the watchlist."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


SCHEMA_VERSION = "shadow_operator_snapshot_v0"
DEFAULT_WATCHLIST_PATH = Path("tools") / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_OUTCOME_REVIEW_PATH = Path("tools") / "shadow_state" / "shadow_execution_outcome_review_v0.json"
DEFAULT_OUT_PATH = Path("tools") / "shadow_state" / "shadow_operator_snapshot_v0.json"
REQUIRED_ITEM_FIELDS = {
    "rank",
    "pack_id",
    "decision_tier",
    "score",
    "exchange",
    "stream",
    "symbols",
    "observation_status",
    "next_action_hint",
    "reobserve_status",
    "observation_last_outcome_short",
    "pnl_interpretation",
    "pnl_attention_flag",
    "latest_realized_sign",
    "latest_unrealized_sign",
    "recent_observation_trail",
}
OUTCOME_REVIEW_SCHEMA_VERSION = "shadow_execution_outcome_review_v0"
OUTCOME_REVIEW_REQUIRED_TOP_LEVEL_FIELDS = {"schema_version", "generated_ts_utc", "selected_count", "items"}
OUTCOME_REVIEW_REQUIRED_ITEM_FIELDS = {
    "selected_pack_id",
    "outcome_class",
    "latest_vs_recent_consistency",
    "outcome_attention_flag",
    "outcome_review_short",
}
OUTCOME_FALLBACK = {
    "outcome_class": "UNKNOWN",
    "latest_vs_recent_consistency": "UNKNOWN",
    "outcome_attention_flag": "false",
    "outcome_review_short": "",
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combined operator snapshot/export v0")
    parser.add_argument("--watchlist", default=str(DEFAULT_WATCHLIST_PATH))
    parser.add_argument("--outcome-review", default=str(DEFAULT_OUTCOME_REVIEW_PATH))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_PATH))
    parser.add_argument("--max-items", type=int, default=0)
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_watchlist(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"watchlist_not_object:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"watchlist_items_invalid:{path}")
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise RuntimeError(f"watchlist_item_not_object:{path}:{index}")
        missing = sorted(REQUIRED_ITEM_FIELDS - set(item))
        if missing:
            raise RuntimeError(f"watchlist_item_missing_fields:{path}:{index}:{','.join(missing)}")
    return obj


def load_outcome_review(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"outcome_review_not_object:{path}")
    missing = sorted(OUTCOME_REVIEW_REQUIRED_TOP_LEVEL_FIELDS - set(obj))
    if missing:
        raise RuntimeError(f"outcome_review_missing_fields:{path}:{','.join(missing)}")
    if str(obj.get("schema_version") or "") != OUTCOME_REVIEW_SCHEMA_VERSION:
        raise RuntimeError(f"outcome_review_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"outcome_review_items_invalid:{path}")
    outcome_by_pack_id: Dict[str, Dict[str, str]] = {}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise RuntimeError(f"outcome_review_item_not_object:{path}:{index}")
        item_missing = sorted(OUTCOME_REVIEW_REQUIRED_ITEM_FIELDS - set(item))
        if item_missing:
            raise RuntimeError(f"outcome_review_item_missing_fields:{path}:{index}:{','.join(item_missing)}")
        pack_id = str(item.get("selected_pack_id") or "")
        outcome_by_pack_id[pack_id] = {
            "outcome_class": str(item.get("outcome_class") or OUTCOME_FALLBACK["outcome_class"]),
            "latest_vs_recent_consistency": str(
                item.get("latest_vs_recent_consistency") or OUTCOME_FALLBACK["latest_vs_recent_consistency"]
            ),
            "outcome_attention_flag": normalize_flag(item.get("outcome_attention_flag")),
            "outcome_review_short": str(item.get("outcome_review_short") or OUTCOME_FALLBACK["outcome_review_short"]),
        }
    return outcome_by_pack_id


def normalize_flag(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def combined_status_short(item: Dict[str, Any]) -> str:
    observation_status = str(item.get("observation_status") or "").strip().upper() or "UNKNOWN"
    pnl_interpretation = str(item.get("pnl_interpretation") or "").strip().upper() or "UNKNOWN"
    return f"{observation_status}/{pnl_interpretation}"


def selected_items(items: List[Dict[str, Any]], max_items: int) -> List[Dict[str, Any]]:
    limit = len(items) if max_items <= 0 else max_items
    return [dict(item) for item in items[:limit]]


def snapshot_item(item: Dict[str, Any], outcome_by_pack_id: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    outcome_fields = outcome_by_pack_id.get(str(item.get("pack_id") or ""), OUTCOME_FALLBACK)
    return {
        "rank": int(item["rank"]),
        "pack_id": str(item["pack_id"]),
        "decision_tier": str(item["decision_tier"]),
        "score": str(item["score"]),
        "exchange": str(item["exchange"]),
        "stream": str(item["stream"]),
        "symbols": list(item["symbols"]),
        "observation_status": str(item["observation_status"]),
        "next_action_hint": str(item["next_action_hint"]),
        "reobserve_status": str(item["reobserve_status"]),
        "observation_last_outcome_short": str(item["observation_last_outcome_short"]),
        "pnl_interpretation": str(item["pnl_interpretation"]),
        "pnl_attention_flag": normalize_flag(item.get("pnl_attention_flag")),
        "latest_realized_sign": str(item["latest_realized_sign"]),
        "latest_unrealized_sign": str(item["latest_unrealized_sign"]),
        "outcome_class": str(outcome_fields["outcome_class"]),
        "latest_vs_recent_consistency": str(outcome_fields["latest_vs_recent_consistency"]),
        "outcome_attention_flag": normalize_flag(outcome_fields["outcome_attention_flag"]),
        "outcome_review_short": str(outcome_fields["outcome_review_short"]),
        "recent_observation_trail": str(item.get("recent_observation_trail") or ""),
        "combined_status_short": combined_status_short(item),
    }


def build_snapshot(
    watchlist_path: Path,
    max_items: int,
    watchlist: Dict[str, Any],
    outcome_by_pack_id: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    items = selected_items(list(watchlist.get("items") or []), max_items)
    snapshot_items = [snapshot_item(item, outcome_by_pack_id) for item in items]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_watchlist": str(watchlist_path),
        "watchlist_generated_ts_utc": str(watchlist.get("generated_ts_utc") or ""),
        "selected_count": len(snapshot_items),
        "items": snapshot_items,
    }


def write_snapshot(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    watchlist_path = Path(args.watchlist)
    outcome_review_path = Path(args.outcome_review)
    out_json_path = Path(args.out_json)
    watchlist = load_watchlist(watchlist_path)
    outcome_by_pack_id = load_outcome_review(outcome_review_path)
    payload = build_snapshot(watchlist_path, int(args.max_items), watchlist, outcome_by_pack_id)
    write_snapshot(out_json_path, payload)
    print(f"selected_count={payload['selected_count']}")
    print(f"out_json={out_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
