#!/usr/bin/env python3
"""Build a compact operator-facing execution/PnL rollup snapshot from pack summary."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


SCHEMA_VERSION = "shadow_execution_rollup_snapshot_v0"
DEFAULT_PACK_SUMMARY_PATH = Path("tools") / "shadow_state" / "shadow_execution_pack_summary_v0.json"
DEFAULT_OUT_PATH = Path("tools") / "shadow_state" / "shadow_execution_rollup_snapshot_v0.json"
REQUIRED_TOP_LEVEL_FIELDS = {"schema_version", "generated_ts_utc", "record_count", "pack_count", "latest_by_pack_id"}
REQUIRED_PER_PACK_FIELDS = {
    "selected_pack_id",
    "last_observed_at",
    "last_pnl_state",
    "pnl_interpretation",
    "pnl_attention_flag",
    "recent_run_count",
    "recent_gain_count",
    "recent_loss_count",
    "recent_flat_count",
    "recent_attention_count",
    "recent_pnl_bias",
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a compact execution/PnL rollup snapshot v0")
    parser.add_argument("--pack-summary", default=str(DEFAULT_PACK_SUMMARY_PATH))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_PATH))
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_pack_summary(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"pack_summary_not_object:{path}")
    missing = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(obj))
    if missing:
        raise RuntimeError(f"pack_summary_missing_fields:{path}:{','.join(missing)}")
    latest_by_pack_id = obj.get("latest_by_pack_id")
    if not isinstance(latest_by_pack_id, dict):
        raise RuntimeError(f"pack_summary_latest_by_pack_id_invalid:{path}")
    for pack_id, item in latest_by_pack_id.items():
        if not isinstance(item, dict):
            raise RuntimeError(f"pack_summary_item_not_object:{path}:{pack_id}")
        item_missing = sorted(REQUIRED_PER_PACK_FIELDS - set(item))
        if item_missing:
            raise RuntimeError(f"pack_summary_item_missing_fields:{path}:{pack_id}:{','.join(item_missing)}")
    return obj


def normalize_bool(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def normalize_int(raw: Any) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return value if value >= 0 else 0


def combined_pnl_status_short(item: Dict[str, Any]) -> str:
    pnl_interpretation = str(item.get("pnl_interpretation") or "").strip().upper() or "UNKNOWN"
    recent_pnl_bias = str(item.get("recent_pnl_bias") or "").strip().upper() or "NO_HISTORY"
    return f"{pnl_interpretation}/{recent_pnl_bias}"


def recent_rollup_short(item: Dict[str, Any]) -> str:
    recent_run_count = normalize_int(item.get("recent_run_count"))
    recent_gain_count = normalize_int(item.get("recent_gain_count"))
    recent_loss_count = normalize_int(item.get("recent_loss_count"))
    recent_flat_count = normalize_int(item.get("recent_flat_count"))
    recent_attention_count = normalize_int(item.get("recent_attention_count"))
    return (
        f"r{recent_run_count}:"
        f"g{recent_gain_count}/"
        f"l{recent_loss_count}/"
        f"f{recent_flat_count}/"
        f"a{recent_attention_count}"
    )


def pnl_rollup_attention(item: Dict[str, Any]) -> str:
    if normalize_bool(item.get("pnl_attention_flag")) == "true":
        return "true"
    if normalize_int(item.get("recent_attention_count")) > 0:
        return "true"
    return "false"


def snapshot_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "selected_pack_id": str(item["selected_pack_id"]),
        "last_observed_at": str(item.get("last_observed_at") or ""),
        "last_pnl_state": str(item.get("last_pnl_state") or "UNKNOWN"),
        "pnl_interpretation": str(item.get("pnl_interpretation") or "UNKNOWN"),
        "recent_pnl_bias": str(item.get("recent_pnl_bias") or "NO_HISTORY"),
        "recent_run_count": normalize_int(item.get("recent_run_count")),
        "recent_attention_count": normalize_int(item.get("recent_attention_count")),
        "combined_pnl_status_short": combined_pnl_status_short(item),
        "recent_rollup_short": recent_rollup_short(item),
        "pnl_rollup_attention": pnl_rollup_attention(item),
    }


def build_snapshot(pack_summary_path: Path, pack_summary: Dict[str, Any]) -> Dict[str, Any]:
    latest_by_pack_id = pack_summary.get("latest_by_pack_id") or {}
    items = [
        snapshot_item(item)
        for _, item in sorted(latest_by_pack_id.items(), key=lambda kv: kv[0])
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_pack_summary": str(pack_summary_path),
        "pack_summary_generated_ts_utc": str(pack_summary.get("generated_ts_utc") or ""),
        "selected_count": len(items),
        "items": items,
    }


def write_snapshot(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    pack_summary_path = Path(args.pack_summary)
    out_json_path = Path(args.out_json)
    pack_summary = load_pack_summary(pack_summary_path)
    payload = build_snapshot(pack_summary_path, pack_summary)
    write_snapshot(out_json_path, payload)
    print(f"selected_count={payload['selected_count']}")
    print(f"out_json={out_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
