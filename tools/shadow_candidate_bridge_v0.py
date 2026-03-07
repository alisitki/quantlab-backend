#!/usr/bin/env python3
"""Minimal deterministic candidate-to-shadow watchlist bridge."""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROMOTE = "PROMOTE"
PROMOTE_STRONG = "PROMOTE_STRONG"
ELIGIBLE_TIERS = {PROMOTE, PROMOTE_STRONG}
SCHEMA_VERSION = "shadow_watchlist_v0"
DEFAULT_STATE_DIR = Path("tools") / "phase6_state"
DEFAULT_OUT_DIR = Path("tools") / "shadow_state"
WATCHLIST_JSON_FILENAME = "shadow_watchlist_v0.json"
WATCHLIST_TSV_FILENAME = "shadow_watchlist_v0.tsv"
REQUIRED_REVIEW_COLUMNS = {"pack_id", "pack_path", "decision_tier", "score", "context_flags"}
OBSERVATION_REVIEW_FIELDS = [
    "observed_before",
    "observation_count",
    "last_observed_at",
    "last_verify_soft_live_pass",
    "last_stop_reason",
    "last_processed_event_count",
    "last_observation_age_hours",
    "observation_recency_bucket",
    "observation_last_outcome_short",
    "observation_attention_flag",
    "observation_status",
    "next_action_hint",
    "reobserve_status",
    "recent_observation_trail",
    "last_pnl_state",
    "pnl_interpretation",
    "pnl_attention_flag",
    "latest_realized_sign",
    "latest_unrealized_sign",
]
TSV_COLUMNS = [
    "rank",
    "selection_slot",
    "pack_id",
    "pack_path",
    "decision_tier",
    "score",
    "exchange",
    "stream",
    "symbols_csv",
    "context_flags",
    "watch_status",
    "observed_before",
    "observation_count",
    "last_observed_at",
    "last_verify_soft_live_pass",
    "last_stop_reason",
    "last_processed_event_count",
    "last_observation_age_hours",
    "observation_recency_bucket",
    "observation_last_outcome_short",
    "observation_attention_flag",
    "observation_status",
    "next_action_hint",
    "reobserve_status",
    "recent_observation_trail",
    "last_pnl_state",
    "pnl_interpretation",
    "pnl_attention_flag",
    "latest_realized_sign",
    "latest_unrealized_sign",
    "notes",
]
LANE_PATTERN = re.compile(r"-(binance|bybit|okx)-(trade|bbo)-")
DIVERSITY_SLOTS: List[Tuple[str, Any]] = [
    ("bybit/bbo", lambda item: item["lane"] == "bybit/bbo"),
    ("binance/bbo", lambda item: item["lane"] == "binance/bbo"),
    ("*/trade", lambda item: item["stream"] == "trade"),
]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Candidate-to-shadow watchlist bridge v0")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--selection-source", default="candidate_review.tsv")
    parser.set_defaults(canonical_only=True)
    parser.add_argument("--canonical-only", dest="canonical_only", action="store_true")
    parser.add_argument("--include-noncanonical", dest="canonical_only", action="store_false")
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def tier_priority(value: str) -> int:
    return 0 if str(value or "").strip().upper() == PROMOTE_STRONG else 1


def format_score(value: float) -> str:
    return f"{value:.6f}"


def normalize_observed_before(raw: Any) -> bool:
    return str(raw or "").strip().lower() == "true"


def normalize_observation_count(raw: Any) -> int:
    value = str(raw or "").strip()
    try:
        return int(value or "0")
    except ValueError:
        return 0


def normalize_last_verify(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in {"true", "false"}:
        return value
    return "unknown"


def normalize_last_processed(raw: Any) -> str:
    value = str(raw or "").strip()
    return value or "unknown"


def normalize_last_observation_age_hours(raw: Any) -> str:
    value = str(raw or "").strip()
    return value or "unknown"


def normalize_observation_recency_bucket(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "NEVER_OBSERVED"


def normalize_observation_last_outcome_short(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "NO_HISTORY"


def normalize_observation_attention_flag(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def normalize_observation_status(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "NEW"


def normalize_next_action_hint(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "READY_TO_OBSERVE"


def normalize_reobserve_status(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "NOT_OBSERVED"


def normalize_recent_observation_trail(raw: Any) -> str:
    return str(raw or "").strip()


def normalize_execution_state(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "UNKNOWN"


def normalize_execution_attention(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def load_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"json_not_object:{path}")
    return obj


def load_candidate_index(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_json(path)
    latest = obj.get("latest_by_pack_id")
    if not isinstance(latest, dict):
        raise RuntimeError(f"candidate_index_invalid_latest_by_pack_id:{path}")
    return {str(k): dict(v or {}) for k, v in latest.items()}


def load_promotion_index(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_json(path)
    latest = obj.get("pack_latest")
    if not isinstance(latest, dict):
        raise RuntimeError(f"promotion_index_invalid_pack_latest:{path}")
    return {str(k): dict(v or {}) for k, v in latest.items()}


def load_candidate_review(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_REVIEW_COLUMNS - fieldnames)
        if missing:
            raise RuntimeError(f"candidate_review_missing_columns:{','.join(missing)}")
        rows: List[Dict[str, str]] = []
        for row in reader:
            rows.append({str(k): str(v or "") for k, v in row.items()})
        return rows


def parse_lane(text: str) -> Tuple[str, str]:
    match = LANE_PATTERN.search(str(text or ""))
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def canonical_lane(pack_path: str, pack_id: str) -> Tuple[str, str]:
    exchange, stream = parse_lane(pack_path)
    if exchange and stream:
        return exchange, stream
    return parse_lane(pack_id)


def context_flags_from_guards(record: Dict[str, Any]) -> str:
    guards = record.get("guards")
    if not isinstance(guards, dict):
        return "MARK=NA;FUNDING=NA;OI=NA"
    mapping = [
        ("MARK", "G4_MARK_CONTEXT"),
        ("FUNDING", "G5_FUNDING_CONTEXT"),
        ("OI", "G6_OI_CONTEXT"),
    ]
    parts: List[str] = []
    for label, guard_key in mapping:
        status = str(guards.get(guard_key, "")).strip().upper() or "NA"
        parts.append(f"{label}={status}")
    return ";".join(parts)


def observation_fields_from_review(row: Dict[str, str]) -> Dict[str, Any]:
    return {
        "observed_before": normalize_observed_before(row.get("observed_before")),
        "observation_count": normalize_observation_count(row.get("observation_count")),
        "last_observed_at": str(row.get("last_observed_at") or "").strip(),
        "last_verify_soft_live_pass": normalize_last_verify(row.get("last_verify_soft_live_pass")),
        "last_stop_reason": str(row.get("last_stop_reason") or "").strip(),
        "last_processed_event_count": normalize_last_processed(row.get("last_processed_event_count")),
        "last_observation_age_hours": normalize_last_observation_age_hours(row.get("last_observation_age_hours")),
        "observation_recency_bucket": normalize_observation_recency_bucket(row.get("observation_recency_bucket")),
        "observation_last_outcome_short": normalize_observation_last_outcome_short(
            row.get("observation_last_outcome_short")
        ),
        "observation_attention_flag": normalize_observation_attention_flag(row.get("observation_attention_flag")),
        "observation_status": normalize_observation_status(row.get("observation_status")),
        "next_action_hint": normalize_next_action_hint(row.get("next_action_hint")),
        "reobserve_status": normalize_reobserve_status(row.get("reobserve_status")),
        "recent_observation_trail": normalize_recent_observation_trail(row.get("recent_observation_trail")),
        "last_pnl_state": normalize_execution_state(row.get("last_pnl_state")),
        "pnl_interpretation": normalize_execution_state(row.get("pnl_interpretation")),
        "pnl_attention_flag": normalize_execution_attention(row.get("pnl_attention_flag")),
        "latest_realized_sign": normalize_execution_state(row.get("latest_realized_sign")),
        "latest_unrealized_sign": normalize_execution_state(row.get("latest_unrealized_sign")),
    }


def symbols_from_pack_path(pack_path: str) -> List[str]:
    runs_dir = Path(pack_path) / "runs"
    if not runs_dir.exists():
        return []
    return sorted(entry.name for entry in runs_dir.iterdir() if entry.is_dir())


def build_ranked_pool(
    review_rows: Iterable[Dict[str, str]],
    candidate_latest: Dict[str, Dict[str, Any]],
    promotion_latest: Dict[str, Dict[str, Any]],
    *,
    canonical_only: bool,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for row in review_rows:
        pack_id = str(row.get("pack_id", "")).strip()
        if not pack_id:
            continue
        candidate_record = dict(candidate_latest.get(pack_id) or {})
        promotion_record = dict(promotion_latest.get(pack_id) or {})
        if not candidate_record and not promotion_record:
            continue
        decision_tier = str(
            candidate_record.get("decision_tier")
            or promotion_record.get("decision_tier")
            or row.get("decision_tier")
            or ""
        ).strip().upper()
        if decision_tier not in ELIGIBLE_TIERS:
            continue
        pack_path = str(
            candidate_record.get("pack_path")
            or promotion_record.get("pack_path")
            or row.get("pack_path")
            or ""
        ).strip()
        exchange, stream = canonical_lane(pack_path, pack_id)
        if canonical_only and not (exchange and stream):
            continue
        score_value = float(str(row.get("score") or "0").strip() or "0")
        score = format_score(score_value)
        context_flags = str(row.get("context_flags") or "").strip()
        if not context_flags:
            context_flags = context_flags_from_guards(candidate_record or promotion_record)
        ranked.append(
            {
                "pack_id": pack_id,
                "pack_path": pack_path,
                "decision_tier": decision_tier,
                "score": score,
                "score_value": score_value,
                "exchange": exchange,
                "stream": stream,
                "lane": f"{exchange}/{stream}" if exchange and stream else "",
                "symbols": symbols_from_pack_path(pack_path),
                "context_flags": context_flags,
                "watch_status": "ACTIVE",
                "notes": "",
                **observation_fields_from_review(row),
            }
        )
    ranked.sort(
        key=lambda item: (
            tier_priority(item["decision_tier"]),
            -item["score_value"],
            item["pack_path"],
            item["pack_id"],
        )
    )
    return ranked


def select_watchlist_items(ranked_pool: List[Dict[str, Any]], top_n: int) -> List[Dict[str, Any]]:
    if top_n <= 0:
        return []
    selected: List[Dict[str, Any]] = []
    selected_ids: set[str] = set()
    for tier in (PROMOTE_STRONG, PROMOTE):
        tier_items = [item for item in ranked_pool if item["decision_tier"] == tier]
        for slot_name, predicate in DIVERSITY_SLOTS:
            if len(selected) >= top_n:
                break
            for item in tier_items:
                if item["pack_id"] in selected_ids:
                    continue
                if predicate(item):
                    chosen = dict(item)
                    chosen["selection_slot"] = slot_name
                    selected.append(chosen)
                    selected_ids.add(chosen["pack_id"])
                    break
        if len(selected) >= top_n:
            break
        for item in tier_items:
            if len(selected) >= top_n:
                break
            if item["pack_id"] in selected_ids:
                continue
            chosen = dict(item)
            chosen["selection_slot"] = "overall_fill"
            selected.append(chosen)
            selected_ids.add(chosen["pack_id"])
        if len(selected) >= top_n:
            break
    for idx, item in enumerate(selected, start=1):
        item["rank"] = idx
    return selected


def watchlist_payload(source: str, top_n: int, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload_items: List[Dict[str, Any]] = []
    for item in items:
        payload_items.append(
            {
                "rank": item["rank"],
                "selection_slot": item["selection_slot"],
                "pack_id": item["pack_id"],
                "pack_path": item["pack_path"],
                "decision_tier": item["decision_tier"],
                "score": item["score"],
                "exchange": item["exchange"],
                "stream": item["stream"],
                "symbols": list(item["symbols"]),
                "context_flags": item["context_flags"],
                "watch_status": item["watch_status"],
                "observed_before": bool(item["observed_before"]),
                "observation_count": int(item["observation_count"]),
                "last_observed_at": item["last_observed_at"],
                "last_verify_soft_live_pass": item["last_verify_soft_live_pass"],
                "last_stop_reason": item["last_stop_reason"],
                "last_processed_event_count": item["last_processed_event_count"],
                "last_observation_age_hours": item["last_observation_age_hours"],
                "observation_recency_bucket": item["observation_recency_bucket"],
                "observation_last_outcome_short": item["observation_last_outcome_short"],
                "observation_attention_flag": item["observation_attention_flag"],
                "observation_status": item["observation_status"],
                "next_action_hint": item["next_action_hint"],
                "reobserve_status": item["reobserve_status"],
                "recent_observation_trail": item["recent_observation_trail"],
                "last_pnl_state": item["last_pnl_state"],
                "pnl_interpretation": item["pnl_interpretation"],
                "pnl_attention_flag": item["pnl_attention_flag"],
                "latest_realized_sign": item["latest_realized_sign"],
                "latest_unrealized_sign": item["latest_unrealized_sign"],
                "notes": item["notes"],
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source": source,
        "selection_policy": {
            "top_n": top_n,
            "tier_priority": [PROMOTE_STRONG, PROMOTE],
            "sort": ["decision_tier_priority", "score_desc", "pack_path_asc"],
            "diversity_slots": [slot for slot, _ in DIVERSITY_SLOTS],
        },
        "items": payload_items,
    }


def write_watchlist_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_watchlist_tsv(path: Path, items: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TSV_COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for item in items:
            writer.writerow(
                {
                    "rank": str(item["rank"]),
                    "selection_slot": item["selection_slot"],
                    "pack_id": item["pack_id"],
                    "pack_path": item["pack_path"],
                    "decision_tier": item["decision_tier"],
                    "score": item["score"],
                    "exchange": item["exchange"],
                    "stream": item["stream"],
                    "symbols_csv": ",".join(item["symbols"]),
                    "context_flags": item["context_flags"],
                    "watch_status": item["watch_status"],
                    "observed_before": "true" if item["observed_before"] else "false",
                    "observation_count": str(item["observation_count"]),
                    "last_observed_at": item["last_observed_at"],
                    "last_verify_soft_live_pass": item["last_verify_soft_live_pass"],
                    "last_stop_reason": item["last_stop_reason"],
                    "last_processed_event_count": item["last_processed_event_count"],
                    "last_observation_age_hours": item["last_observation_age_hours"],
                    "observation_recency_bucket": item["observation_recency_bucket"],
                    "observation_last_outcome_short": item["observation_last_outcome_short"],
                    "observation_attention_flag": item["observation_attention_flag"],
                    "observation_status": item["observation_status"],
                    "next_action_hint": item["next_action_hint"],
                    "reobserve_status": item["reobserve_status"],
                    "recent_observation_trail": item["recent_observation_trail"],
                    "last_pnl_state": item["last_pnl_state"],
                    "pnl_interpretation": item["pnl_interpretation"],
                    "pnl_attention_flag": item["pnl_attention_flag"],
                    "latest_realized_sign": item["latest_realized_sign"],
                    "latest_unrealized_sign": item["latest_unrealized_sign"],
                    "notes": item["notes"],
                }
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    state_dir = Path(args.state_dir)
    out_dir = Path(args.out_dir)
    selection_source_path = Path(args.selection_source)
    if not selection_source_path.is_absolute():
        selection_source_path = state_dir / selection_source_path
    candidate_index_path = state_dir / "candidate_index.json"
    promotion_index_path = state_dir / "promotion_index.json"
    watchlist_json_path = out_dir / WATCHLIST_JSON_FILENAME
    watchlist_tsv_path = out_dir / WATCHLIST_TSV_FILENAME

    candidate_latest = load_candidate_index(candidate_index_path)
    promotion_latest = load_promotion_index(promotion_index_path)
    review_rows = load_candidate_review(selection_source_path)
    ranked_pool = build_ranked_pool(
        review_rows,
        candidate_latest,
        promotion_latest,
        canonical_only=bool(args.canonical_only),
    )
    selected = select_watchlist_items(ranked_pool, args.top_n)
    payload = watchlist_payload(str(selection_source_path), args.top_n, selected)
    write_watchlist_json(watchlist_json_path, payload)
    write_watchlist_tsv(watchlist_tsv_path, selected)

    print(f"selected_count={len(selected)}")
    print(f"selected_pack_ids_csv={','.join(item['pack_id'] for item in selected)}")
    print(f"watchlist_json={watchlist_json_path}")
    print(f"watchlist_tsv={watchlist_tsv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
