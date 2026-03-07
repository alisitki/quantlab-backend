#!/usr/bin/env python3
"""Proof-only shadow observation consumer for watchlist v0."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


DEFAULT_WATCHLIST = Path("tools") / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_OUT_LOG = Path("tools") / "shadow_state" / "shadow_observation_log_v0.jsonl"
PROMOTE_STRONG = "PROMOTE_STRONG"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mock shadow observer for watchlist v0")
    parser.add_argument("--watchlist", default=str(DEFAULT_WATCHLIST))
    parser.add_argument("--out-log", default=str(DEFAULT_OUT_LOG))
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_watchlist(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"watchlist_not_object:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"watchlist_missing_items:{path}")
    return obj


def build_events(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ts_utc = utc_now_iso()
    events: List[Dict[str, Any]] = []
    for item in items:
        pack_id = str(item.get("pack_id", "")).strip()
        exchange = str(item.get("exchange", "")).strip()
        stream = str(item.get("stream", "")).strip()
        base = {
            "ts_utc": ts_utc,
            "pack_id": pack_id,
            "exchange": exchange,
            "stream": stream,
        }
        events.append(
            {
                **base,
                "event_type": "watch_started",
                "reason": "watch_status=ACTIVE",
                "extra": {
                    "rank": item.get("rank"),
                    "selection_slot": item.get("selection_slot"),
                },
            }
        )
        events.append(
            {
                **base,
                "event_type": "signal_seen",
                "reason": "candidate_selected_for_shadow_watch",
                "extra": {
                    "decision_tier": item.get("decision_tier"),
                    "score": item.get("score"),
                    "context_flags": item.get("context_flags"),
                },
            }
        )
        decision_tier = str(item.get("decision_tier", "")).strip().upper()
        terminal_event = "would_trade" if decision_tier == PROMOTE_STRONG else "would_skip"
        events.append(
            {
                **base,
                "event_type": terminal_event,
                "reason": f"decision_tier={decision_tier}",
                "extra": {
                    "symbols": list(item.get("symbols") or []),
                },
            }
        )
    return events


def write_jsonl(path: Path, events: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    watchlist_path = Path(args.watchlist)
    out_log_path = Path(args.out_log)
    watchlist = load_watchlist(watchlist_path)
    items = [dict(item or {}) for item in watchlist.get("items", [])]
    events = build_events(items)
    write_jsonl(out_log_path, events)

    would_trade_count = sum(1 for event in events if event["event_type"] == "would_trade")
    would_skip_count = sum(1 for event in events if event["event_type"] == "would_skip")
    print(f"event_count={len(events)}")
    print(f"would_trade_count={would_trade_count}")
    print(f"would_skip_count={would_skip_count}")
    print(f"observation_log_path={out_log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
