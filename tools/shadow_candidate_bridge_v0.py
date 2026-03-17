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
GOVERNANCE_REGISTRY_REF = "tools/system_state/canonical_truth_registry_v0.json"
DEFAULT_STATE_DIR = Path("tools") / "phase6_state"
DEFAULT_OUT_DIR = Path("tools") / "shadow_state"
DEFAULT_BINDING_ARTIFACT_FILENAME = "candidate_strategy_runtime_binding_v0.json"
WATCHLIST_JSON_FILENAME = "shadow_watchlist_v0.json"
WATCHLIST_TSV_FILENAME = "shadow_watchlist_v0.tsv"
REQUIRED_REVIEW_COLUMNS = {"pack_id", "pack_path", "decision_tier", "score", "context_flags"}
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
BINDING_BUCKET_BOUND_DIRECTIONAL = "BOUND_DIRECTIONAL_PRIORITY"
BINDING_BUCKET_BOUND_OBSERVE_ONLY = "BOUND_OBSERVE_ONLY"
BINDING_BUCKET_UNBOUND = "UNBOUND_CANDIDATE"
BINDING_PRIORITY_ORDER = [
    BINDING_BUCKET_BOUND_DIRECTIONAL,
    BINDING_BUCKET_BOUND_OBSERVE_ONLY,
    BINDING_BUCKET_UNBOUND,
]
V2_CLASS_PRIORITY = {
    "PROMISING": 0,
    "NEUTRAL": 1,
    "INSUFFICIENT_EVIDENCE": 2,
    "UNSEEN": 3,
    "NO_SIGNAL": 4,
    "WEAK": 5,
    "OBSERVE_ONLY": 6,
    "UNRUNNABLE": 7,
}
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
    "source_review_rank",
    "review_class",
    "class_priority",
    "exchange",
    "stream",
    "symbols_csv",
    "binding_priority_bucket",
    "binding_status",
    "binding_family_id",
    "binding_mode",
    "binding_strategy_id",
    "binding_reason",
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
    parser.add_argument("--binding-artifact", default="")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--selection-source", default="candidate_review_v2.tsv")
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


def parse_int(raw: Any, default: int) -> int:
    try:
        return int(str(raw or "").strip())
    except ValueError:
        return default


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


def load_binding_artifact(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"items": []}
    obj = load_json(path)
    schema_version = str(obj.get("schema_version") or "").strip()
    if schema_version and schema_version != BINDING_SCHEMA_VERSION:
        raise RuntimeError(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"binding_artifact_items_invalid:{path}")
    return obj


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


def binding_mode_from_row(item: Dict[str, Any]) -> str:
    direct = str(item.get("binding_mode") or "").strip()
    if direct:
        return direct
    runtime_config = item.get("runtime_strategy_config")
    if isinstance(runtime_config, dict):
        return str(runtime_config.get("binding_mode") or "").strip()
    return ""


def tradeability_class_from_binding_row(item: Dict[str, Any]) -> str:
    explicit = str(item.get("shadow_tradeability_class") or "").strip().upper()
    if explicit in {"DIRECTIONAL", "OBSERVE_ONLY", "UNBOUND"}:
        return explicit
    status = str(item.get("runtime_binding_status") or "").strip()
    if status != "BOUND_SHADOW_RUNNABLE":
        return "UNBOUND"
    return "OBSERVE_ONLY" if binding_mode_from_row(item) == "OBSERVE_ONLY" else "DIRECTIONAL"


def binding_pack_key(item: Dict[str, Any]) -> str:
    source_pack_id = str(item.get("source_pack_id") or "").strip()
    if source_pack_id:
        return source_pack_id
    runtime_config = item.get("runtime_strategy_config")
    if isinstance(runtime_config, dict):
        source_pack_id = str(runtime_config.get("source_pack_id") or "").strip()
        if source_pack_id:
            return source_pack_id
    return str(item.get("pack_id") or "").strip()


def binding_sort_key(item: Dict[str, Any]) -> Tuple[int, int, str, str, str]:
    status = str(item.get("runtime_binding_status") or "").strip()
    tradeability_class = tradeability_class_from_binding_row(item)
    if status == "BOUND_SHADOW_RUNNABLE" and tradeability_class == "DIRECTIONAL":
        bucket_rank = 0
        status_rank = 0
    elif status == "BOUND_SHADOW_RUNNABLE" and tradeability_class == "OBSERVE_ONLY":
        bucket_rank = 1
        status_rank = 0
    elif status == "UNBOUND_CONFIG_GAP":
        bucket_rank = 2
        status_rank = 0
    elif status == "UNBOUND_NO_RUNTIME_IMPL":
        bucket_rank = 2
        status_rank = 1
    else:
        bucket_rank = 2
        status_rank = 2
    return (
        bucket_rank,
        status_rank,
        str(item.get("family_id") or "").strip(),
        str(item.get("strategy_id") or "").strip(),
        str(item.get("binding_reason") or "").strip(),
    )


def default_binding_summary() -> Dict[str, Any]:
    return {
        "binding_priority_bucket": BINDING_BUCKET_UNBOUND,
        "binding_priority_rank": 2,
        "binding_status": "UNBOUND_TRANSLATION_REJECTED",
        "binding_family_id": "",
        "binding_mode": "",
        "binding_strategy_id": "",
        "binding_reason": "NO_BINDING_ROW_FOR_PACK",
        "bound_directional_row_count": 0,
        "bound_observe_only_row_count": 0,
        "bound_total_row_count": 0,
    }


def summarize_pack_bindings(binding_artifact: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for raw_item in list(binding_artifact.get("items") or []):
        if not isinstance(raw_item, dict):
            continue
        pack_id = binding_pack_key(raw_item)
        if not pack_id:
            continue
        grouped.setdefault(pack_id, []).append(dict(raw_item))

    summaries: Dict[str, Dict[str, Any]] = {}
    for pack_id, items in grouped.items():
        best_item = min(items, key=binding_sort_key)
        best_status = str(best_item.get("runtime_binding_status") or "").strip()
        best_tradeability = tradeability_class_from_binding_row(best_item)
        if best_status == "BOUND_SHADOW_RUNNABLE" and best_tradeability == "DIRECTIONAL":
            bucket = BINDING_BUCKET_BOUND_DIRECTIONAL
            bucket_rank = 0
        elif best_status == "BOUND_SHADOW_RUNNABLE" and best_tradeability == "OBSERVE_ONLY":
            bucket = BINDING_BUCKET_BOUND_OBSERVE_ONLY
            bucket_rank = 1
        else:
            bucket = BINDING_BUCKET_UNBOUND
            bucket_rank = 2
        summaries[pack_id] = {
            "binding_priority_bucket": bucket,
            "binding_priority_rank": bucket_rank,
            "binding_status": best_status,
            "binding_family_id": str(best_item.get("family_id") or "").strip(),
            "binding_mode": binding_mode_from_row(best_item),
            "binding_strategy_id": str(best_item.get("strategy_id") or "").strip(),
            "binding_reason": str(best_item.get("binding_reason") or "").strip(),
            "bound_directional_row_count": sum(
                1
                for item in items
                if str(item.get("runtime_binding_status") or "").strip() == "BOUND_SHADOW_RUNNABLE"
                and tradeability_class_from_binding_row(item) == "DIRECTIONAL"
            ),
            "bound_observe_only_row_count": sum(
                1
                for item in items
                if str(item.get("runtime_binding_status") or "").strip() == "BOUND_SHADOW_RUNNABLE"
                and tradeability_class_from_binding_row(item) == "OBSERVE_ONLY"
            ),
            "bound_total_row_count": sum(
                1
                for item in items
                if str(item.get("runtime_binding_status") or "").strip() == "BOUND_SHADOW_RUNNABLE"
            ),
        }
    return summaries


def build_ranked_pool(
    review_rows: Iterable[Dict[str, str]],
    candidate_latest: Dict[str, Dict[str, Any]],
    promotion_latest: Dict[str, Dict[str, Any]],
    pack_binding_summaries: Dict[str, Dict[str, Any]],
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
        review_class = str(row.get("review_class") or "").strip().upper()
        raw_class_priority = str(row.get("class_priority") or "").strip()
        class_priority_value = parse_int(raw_class_priority, V2_CLASS_PRIORITY.get(review_class, 99))
        source_review_rank = str(row.get("rank") or "").strip()
        source_review_rank_value = parse_int(source_review_rank, 10**9)
        context_flags = str(row.get("context_flags") or "").strip()
        if not context_flags:
            context_flags = context_flags_from_guards(candidate_record or promotion_record)
        binding_summary = dict(pack_binding_summaries.get(pack_id) or default_binding_summary())
        ranked.append(
            {
                "pack_id": pack_id,
                "pack_path": pack_path,
                "decision_tier": decision_tier,
                "score": score,
                "score_value": score_value,
                "source_review_rank": source_review_rank,
                "source_review_rank_value": source_review_rank_value,
                "review_class": review_class,
                "class_priority": raw_class_priority or (str(class_priority_value) if review_class else ""),
                "class_priority_value": class_priority_value,
                "exchange": exchange,
                "stream": stream,
                "lane": f"{exchange}/{stream}" if exchange and stream else "",
                "symbols": symbols_from_pack_path(pack_path),
                "binding_priority_bucket": binding_summary["binding_priority_bucket"],
                "binding_priority_rank": binding_summary["binding_priority_rank"],
                "binding_status": binding_summary["binding_status"],
                "binding_family_id": binding_summary["binding_family_id"],
                "binding_mode": binding_summary["binding_mode"],
                "binding_strategy_id": binding_summary["binding_strategy_id"],
                "binding_reason": binding_summary["binding_reason"],
                "context_flags": context_flags,
                "watch_status": "ACTIVE",
                "notes": "",
                **observation_fields_from_review(row),
            }
        )
    ranked.sort(
        key=lambda item: (
            int(item["class_priority_value"]),
            int(item["binding_priority_rank"]),
            tier_priority(item["decision_tier"]),
            -item["score_value"],
            int(item["source_review_rank_value"]),
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
    class_priorities = sorted({int(item["class_priority_value"]) for item in ranked_pool})
    for class_priority_value in class_priorities:
        class_items = [item for item in ranked_pool if int(item["class_priority_value"]) == class_priority_value]
        for bucket in BINDING_PRIORITY_ORDER:
            bucket_items = [item for item in class_items if item["binding_priority_bucket"] == bucket]
            for tier in (PROMOTE_STRONG, PROMOTE):
                tier_items = [item for item in bucket_items if item["decision_tier"] == tier]
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
        if len(selected) >= top_n:
            break
    for idx, item in enumerate(selected, start=1):
        item["rank"] = idx
    return selected


def watchlist_payload(
    source: str,
    binding_artifact_path: str,
    top_n: int,
    items: List[Dict[str, Any]],
    *,
    candidate_index_path: str,
    promotion_index_path: str,
) -> Dict[str, Any]:
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
                "source_review_rank": item["source_review_rank"],
                "review_class": item["review_class"],
                "class_priority": item["class_priority"],
                "exchange": item["exchange"],
                "stream": item["stream"],
                "symbols": list(item["symbols"]),
                "binding_priority_bucket": item["binding_priority_bucket"],
                "binding_status": item["binding_status"],
                "binding_family_id": item["binding_family_id"],
                "binding_mode": item["binding_mode"],
                "binding_strategy_id": item["binding_strategy_id"],
                "binding_reason": item["binding_reason"],
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
        "source_binding_artifact_json": binding_artifact_path,
        "source_candidate_index_json": candidate_index_path,
        "source_promotion_index_json": promotion_index_path,
        "governance": {
            "surface_role": "ACTIVE_SHADOW_SUBSET",
            "authoritative_scope": "Current global shadow observation/live-paper subset.",
            "authoritative_source_ref": GOVERNANCE_REGISTRY_REF,
            "produced_by": ["tools/shadow_candidate_bridge_v0.py"],
            "consumed_by": [
                "tools/run-shadow-observation-batch-v0.py",
                "tools/run-shadow-watchlist-v0.js",
                "tools/shadow_operator_snapshot_v0.py",
            ],
            "not_authoritative_for": [
                "candidate strategy runtime binding",
                "one-shot bound launch selection",
                "continuous session state",
            ],
            "notes": [
                "Ranks packs for observation/live-paper coverage.",
                "This surface may omit family_id; use runtime binding or registry for family/runtime truth.",
            ],
        },
        "selection_policy": {
            "top_n": top_n,
            "authoritative_review_fields": ["review_class", "class_priority"],
            "binding_priority_order": list(BINDING_PRIORITY_ORDER),
            "tier_priority": [PROMOTE_STRONG, PROMOTE],
            "sort": [
                "review_class_priority",
                "binding_priority",
                "decision_tier_priority",
                "score_desc",
                "source_review_rank_asc",
                "pack_path_asc",
            ],
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
                    "source_review_rank": item["source_review_rank"],
                    "review_class": item["review_class"],
                    "class_priority": item["class_priority"],
                    "exchange": item["exchange"],
                    "stream": item["stream"],
                    "symbols_csv": ",".join(item["symbols"]),
                    "binding_priority_bucket": item["binding_priority_bucket"],
                    "binding_status": item["binding_status"],
                    "binding_family_id": item["binding_family_id"],
                    "binding_mode": item["binding_mode"],
                    "binding_strategy_id": item["binding_strategy_id"],
                    "binding_reason": item["binding_reason"],
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
    binding_artifact_path = (
        Path(args.binding_artifact)
        if str(args.binding_artifact or "").strip()
        else state_dir / DEFAULT_BINDING_ARTIFACT_FILENAME
    )
    candidate_index_path = state_dir / "candidate_index.json"
    promotion_index_path = state_dir / "promotion_index.json"
    watchlist_json_path = out_dir / WATCHLIST_JSON_FILENAME
    watchlist_tsv_path = out_dir / WATCHLIST_TSV_FILENAME

    candidate_latest = load_candidate_index(candidate_index_path)
    promotion_latest = load_promotion_index(promotion_index_path)
    binding_artifact = load_binding_artifact(binding_artifact_path)
    pack_binding_summaries = summarize_pack_bindings(binding_artifact)
    review_rows = load_candidate_review(selection_source_path)
    ranked_pool = build_ranked_pool(
        review_rows,
        candidate_latest,
        promotion_latest,
        pack_binding_summaries,
        canonical_only=bool(args.canonical_only),
    )
    selected = select_watchlist_items(ranked_pool, args.top_n)
    payload = watchlist_payload(
        str(selection_source_path),
        str(binding_artifact_path),
        args.top_n,
        selected,
        candidate_index_path=str(candidate_index_path),
        promotion_index_path=str(promotion_index_path),
    )
    write_watchlist_json(watchlist_json_path, payload)
    write_watchlist_tsv(watchlist_tsv_path, selected)

    print(f"selected_count={len(selected)}")
    print(f"selected_pack_ids_csv={','.join(item['pack_id'] for item in selected)}")
    print(f"watchlist_json={watchlist_json_path}")
    print(f"watchlist_tsv={watchlist_tsv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
