#!/usr/bin/env python3
"""Phase-6 candidate review v2: tradeability-aware ranking surface."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records


CANDIDATE_QUEUE_FILENAME = "candidate_queue.jsonl"
CANDIDATE_INDEX_FILENAME = "candidate_index.json"
CANDIDATE_REPORT_FILENAME = "candidate_report.tsv"
CANDIDATE_REVIEW_V2_TSV_FILENAME = "candidate_review_v2.tsv"
CANDIDATE_REVIEW_V2_JSON_FILENAME = "candidate_review_v2.json"
DEFAULT_CANDIDATE_REVIEW_V0_TSV_PATH = Path("tools") / "phase6_state" / "candidate_review.tsv"
DEFAULT_STRATEGY_CONTRACT_JSON_PATH = Path("tools") / "phase6_state" / "candidate_strategy_contract_v0.json"
DEFAULT_RUNTIME_BINDING_JSON_PATH = Path("tools") / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_FAMILY_ROLE_JSON_PATH = Path("tools") / "phase6_state" / "hypothesis_family_role_classification_v0.json"
DEFAULT_WATCHLIST_JSON_PATH = Path("tools") / "shadow_state" / "shadow_watchlist_v0.json"
DEFAULT_PACK_SUMMARY_JSON_PATH = Path("tools") / "shadow_state" / "shadow_execution_pack_summary_v0.json"
DEFAULT_OUTCOME_REVIEW_JSON_PATH = Path("tools") / "shadow_state" / "shadow_execution_outcome_review_v0.json"
DEFAULT_TRADE_LEDGER_JSONL_PATH = Path("tools") / "shadow_state" / "shadow_trade_ledger_v1.jsonl"
DEFAULT_FUTURES_PAPER_LEDGER_JSON_PATH = Path("tools") / "shadow_state" / "shadow_futures_paper_ledger_v1.json"
DEFAULT_SPEC_PATH = Path("tools") / "phase6_candidate_review_v2_spec.json"

SCHEMA_VERSION = "candidate_review_v2"
PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
NA = "NA"
NEUTRAL = "NEUTRAL"
PROMOTE = "PROMOTE"
PROMOTE_STRONG = "PROMOTE_STRONG"
RUNNABLE_DIRECTIONAL = "RUNNABLE_DIRECTIONAL"
OBSERVE_ONLY = "OBSERVE_ONLY"
UNRUNNABLE = "UNRUNNABLE"
UNSEEN = "UNSEEN"
NO_SIGNAL = "NO_SIGNAL"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
WEAK = "WEAK"
PROMISING = "PROMISING"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
INTERPRETABLE_PROFITABILITY_STATUSES = {
    "NET_AFTER_FEES_AND_FUNDING",
    "NET_MARK_TO_MARKET_AFTER_FEES_AND_FUNDING",
    "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
}
NEGATIVE_PNL_INTERPRETATIONS = {"REALIZED_LOSS", "ACTIVE_LOSING"}
POSITIVE_PNL_INTERPRETATIONS = {"REALIZED_GAIN", "ACTIVE_GAINING"}
NEGATIVE_OUTCOME_CLASSES = {"STABLE_LOSING", "MIXED_RECENT"}
POSITIVE_OUTCOME_CLASSES = {"STABLE_GAINING"}
CLASS_PRIORITY = {
    PROMISING: 0,
    NEUTRAL: 1,
    INSUFFICIENT_EVIDENCE: 2,
    UNSEEN: 3,
    NO_SIGNAL: 4,
    WEAK: 5,
    OBSERVE_ONLY: 6,
    UNRUNNABLE: 7,
}
CONTEXT_GUARDS = {
    "G4_MARK_CONTEXT": "MARK",
    "G5_FUNDING_CONTEXT": "FUNDING",
    "G6_OI_CONTEXT": "OI",
}
TIER_SCORE = {
    PROMOTE_STRONG: 50.0,
    PROMOTE: 30.0,
}
SUFFICIENT_FILL_BACKED_RUN_COUNT = 3
SUFFICIENT_PROFITABILITY_RUN_COUNT = 3
SUFFICIENT_CLOSED_CYCLE_COUNT = 2
REQUIRED_INDEX_KEYS = {"record_count", "candidate_pack_ids", "latest_by_pack_id"}
REQUIRED_REPORT_COLUMNS = {
    "pack_id",
    "decision_tier",
    "pack_path",
    "det_pass",
    "det_supported",
    "det_skipped",
    "max_rss_kb",
    "max_elapsed_sec",
    "candidate_status",
}
TOP_CANDIDATES_LIMIT = 15
REVIEW_COLUMNS = [
    "rank",
    "class_rank",
    "review_class",
    "class_priority",
    "score",
    "research_score",
    "shadow_score",
    "v0_rank",
    "v0_score",
    "decision_tier",
    "pack_id",
    "pack_path",
    "selected_family_ids",
    "family_roles",
    "translation_statuses",
    "runtime_binding_statuses",
    "trade_surface_bucket",
    "shadow_tradeability_classes",
    "hard_guard_reason",
    "shadow_evidence_reason",
    "det_ratio",
    "det_pass",
    "det_supported",
    "det_skipped",
    "max_rss_kb",
    "max_elapsed_sec",
    "context_flags",
    "candidate_status",
    "shadow_run_count",
    "fill_backed_run_count",
    "profitability_interpretable_run_count",
    "no_fill_activity_count",
    "fill_backed_open_count",
    "fill_backed_flat_count",
    "closed_cycle_count",
    "realized_pnl_sum",
    "latest_trade_closed_at",
    "last_shadow_observed_at",
    "latest_pnl_interpretation",
    "recent_pnl_bias",
    "outcome_class",
    "outcome_attention_flag",
    "watch_status",
    "horizon_evidence_bucket",
    "score_explanation",
]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase-6 candidate review v2")
    parser.add_argument("--state-dir", default="", help="Default: tools/phase6_state")
    parser.add_argument("--candidate-review-v0-tsv", default=str(DEFAULT_CANDIDATE_REVIEW_V0_TSV_PATH))
    parser.add_argument("--strategy-contract-json", default=str(DEFAULT_STRATEGY_CONTRACT_JSON_PATH))
    parser.add_argument("--runtime-binding-json", default=str(DEFAULT_RUNTIME_BINDING_JSON_PATH))
    parser.add_argument("--family-role-json", default=str(DEFAULT_FAMILY_ROLE_JSON_PATH))
    parser.add_argument("--watchlist-json", default=str(DEFAULT_WATCHLIST_JSON_PATH))
    parser.add_argument("--execution-pack-summary-json", default=str(DEFAULT_PACK_SUMMARY_JSON_PATH))
    parser.add_argument("--execution-outcome-review-json", default=str(DEFAULT_OUTCOME_REVIEW_JSON_PATH))
    parser.add_argument("--trade-ledger-jsonl", default=str(DEFAULT_TRADE_LEDGER_JSONL_PATH))
    parser.add_argument("--futures-paper-ledger-json", default=str(DEFAULT_FUTURES_PAPER_LEDGER_JSON_PATH))
    parser.add_argument("--spec-json", default=str(DEFAULT_SPEC_PATH))
    parser.add_argument("--out-tsv", default="")
    parser.add_argument("--out-json", default="")
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc_iso(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def tier_priority(value: str) -> int:
    return 0 if str(value or "").strip().upper() == PROMOTE_STRONG else 1


def as_int(raw: Any) -> int:
    value = str(raw if raw is not None else "").strip()
    return int(value or "0")


def as_float(raw: Any) -> float:
    value = str(raw if raw is not None else "").strip()
    return float(value or "0")


def format_ratio(value: float) -> str:
    return f"{value:.6f}"


def format_score(value: float) -> str:
    return f"{value:.6f}"


def normalize_flag(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def sorted_csv(values: Iterable[str]) -> str:
    cleaned = sorted({str(value or "").strip() for value in values if str(value or "").strip()})
    return ",".join(cleaned)


def load_candidate_index(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"candidate_index_not_object:{path}")
    missing = sorted(REQUIRED_INDEX_KEYS - set(obj.keys()))
    if missing:
        raise RuntimeError(f"candidate_index_missing_keys:{','.join(missing)}")
    if not isinstance(obj.get("candidate_pack_ids"), list):
        raise RuntimeError(f"candidate_index_invalid_candidate_pack_ids:{path}")
    if not isinstance(obj.get("latest_by_pack_id"), dict):
        raise RuntimeError(f"candidate_index_invalid_latest_by_pack_id:{path}")
    return obj


def load_candidate_report(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_REPORT_COLUMNS - fieldnames)
        if missing:
            raise RuntimeError(f"candidate_report_missing_columns:{','.join(missing)}")
        return [{str(k): str(v or "") for k, v in row.items()} for row in reader]


def latest_by_pack_id(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        latest[pack_id] = dict(rec)
    return {pack_id: latest[pack_id] for pack_id in sorted(latest.keys())}


def load_optional_json_dict(path: Path, *, expected_list_key: str = "") -> Dict[str, Any]:
    if not path.exists():
        return {}
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"json_not_object:{path}")
    if expected_list_key:
        values = obj.get(expected_list_key)
        if values is not None and not isinstance(values, list):
            raise RuntimeError(f"json_invalid_list:{path}:{expected_list_key}")
    return obj


def load_jsonl_records_safe(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return read_jsonl_records(path)


def load_review_v0_map(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = [{str(k): str(v or "") for k, v in row.items()} for row in reader]
    return {str(row.get("pack_id", "")).strip(): row for row in rows if str(row.get("pack_id", "")).strip()}


def parse_decision_report_context_statuses(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    statuses: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        for guard_id in CONTEXT_GUARDS:
            prefix = f"{guard_id}="
            if line.startswith(prefix):
                statuses[guard_id] = line[len(prefix) :].split(" ", 1)[0].strip().upper()
    return statuses


def summarize_raw_statuses(values: List[str], *, is_oi: bool = False) -> str:
    cleaned = sorted({str(v or "").strip().upper() for v in values if str(v or "").strip()})
    if not cleaned:
        return NA
    if is_oi and cleaned == ["UNSUPPORTED_EXCHANGE"]:
        return NEUTRAL
    if len(cleaned) == 1:
        return cleaned[0]
    return "MIXED"


def parse_context_summary_flags(pack_path: Path) -> Dict[str, str]:
    paths = sorted(pack_path.glob("runs/*/artifacts/context/context_summary.tsv"))
    if not paths:
        return {}
    mark_values: List[str] = []
    funding_values: List[str] = []
    oi_values: List[str] = []
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                mark_values.append(str(row.get("ctx_mark_price_status", "")).strip())
                funding_values.append(str(row.get("ctx_funding_status", "")).strip())
                oi_values.append(str(row.get("ctx_oi_status", "")).strip())
    return {
        "MARK": summarize_raw_statuses(mark_values),
        "FUNDING": summarize_raw_statuses(funding_values),
        "OI": summarize_raw_statuses(oi_values, is_oi=True),
    }


def collect_context_flags(
    pack_path: Path,
    queue_record: Dict[str, Any],
    index_record: Dict[str, Any],
) -> Tuple[str, float]:
    guard_statuses: Dict[str, str] = {}
    for source in (queue_record.get("guards"), index_record.get("guards")):
        if not isinstance(source, dict):
            continue
        for guard_id in CONTEXT_GUARDS:
            status = str(source.get(guard_id, "")).strip().upper()
            if status:
                guard_statuses[guard_id] = status
    guard_report_statuses = parse_decision_report_context_statuses(pack_path / "guards" / "decision_report.txt")
    for guard_id, status in guard_report_statuses.items():
        guard_statuses.setdefault(guard_id, status)

    raw_flags = parse_context_summary_flags(pack_path) if pack_path.exists() else {}
    parts: List[str] = []
    for guard_id, label in CONTEXT_GUARDS.items():
        if guard_id in guard_statuses:
            value = guard_statuses[guard_id]
        else:
            value = raw_flags.get(label, NA)
        parts.append(f"{label}={value}")
    context_flags = ";".join(parts) if parts else NA

    if any(status == FAIL for status in guard_statuses.values()):
        context_bonus = -10.0
    else:
        context_bonus = 5.0 * sum(1 for status in guard_statuses.values() if status == PASS)
    return context_flags, context_bonus


def load_contract_by_pack(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    if not items:
        return {}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        pack_id = str(item.get("pack_id", "")).strip()
        if pack_id:
            grouped[pack_id].append(dict(item))
    return grouped


def load_binding_by_pack(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    if not items:
        return {}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        pack_id = str(item.get("pack_id", "")).strip()
        if pack_id:
            grouped[pack_id].append(dict(item))
    return grouped


def load_family_roles(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    roles: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        family_id = str(item.get("family_id", "")).strip()
        if family_id:
            roles[family_id] = dict(item)
    return roles


def load_watchlist_by_pack(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    return {
        str(item.get("pack_id", "")).strip(): dict(item)
        for item in items
        if isinstance(item, dict) and str(item.get("pack_id", "")).strip()
    }


def load_pack_summary_by_pack(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_optional_json_dict(path)
    latest = obj.get("latest_by_pack_id") if obj else {}
    if latest is None:
        return {}
    if not isinstance(latest, dict):
        raise RuntimeError(f"execution_pack_summary_invalid_latest_by_pack_id:{path}")
    return {str(k): dict(v or {}) for k, v in latest.items() if isinstance(v, dict)}


def load_outcome_by_pack(path: Path) -> Dict[str, Dict[str, Any]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    return {
        str(item.get("selected_pack_id", "")).strip(): dict(item)
        for item in items
        if isinstance(item, dict) and str(item.get("selected_pack_id", "")).strip()
    }


def load_futures_rows_by_pack(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    obj = load_optional_json_dict(path, expected_list_key="items")
    items = obj.get("items") if obj else []
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        pack_id = str(item.get("selected_pack_id", "")).strip()
        if pack_id:
            grouped[pack_id].append(dict(item))
    for pack_id, rows in grouped.items():
        rows.sort(
            key=lambda row: (
                str(row.get("observed_at", "")),
                str(row.get("live_run_id", "")),
            )
        )
    return grouped


def load_trade_rows_by_pack(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    if not path.exists():
        return {}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"trade_ledger_invalid_json:{path}:{lineno}:{exc}") from exc
        if not isinstance(item, dict):
            raise RuntimeError(f"trade_ledger_not_object:{path}:{lineno}")
        pack_id = str(item.get("selected_pack_id", "")).strip()
        if pack_id:
            grouped[pack_id].append(item)
    for pack_id, rows in grouped.items():
        rows.sort(key=lambda row: (str(row.get("closed_at", "")), str(row.get("trade_id", ""))))
    return grouped


def contract_summary(
    pack_id: str,
    rows: List[Dict[str, Any]],
    family_roles: Dict[str, Dict[str, Any]],
) -> Dict[str, str]:
    selected_family_ids = sorted_csv(row.get("selected_family_id") for row in rows)
    translation_statuses = sorted_csv(row.get("translation_status") for row in rows)
    reject_reasons = sorted_csv(row.get("reject_reason") for row in rows)
    family_role_values = sorted_csv(
        family_roles.get(str(row.get("selected_family_id") or "").strip(), {}).get("role", "")
        for row in rows
    )
    return {
        "selected_family_ids": selected_family_ids,
        "translation_statuses": translation_statuses,
        "reject_reasons": reject_reasons,
        "family_roles": family_role_values,
        "selected_family_count": str(len({fid for fid in selected_family_ids.split(",") if fid})),
        "pack_id": pack_id,
    }


def binding_summary(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    runtime_statuses = sorted_csv(row.get("runtime_binding_status") for row in rows)
    shadow_tradeability_classes = sorted_csv(row.get("shadow_tradeability_class") for row in rows)
    binding_reasons = sorted_csv(row.get("binding_reason") for row in rows)

    has_directional = any(
        str(row.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE
        and str(row.get("shadow_tradeability_class") or "").strip() == "DIRECTIONAL"
        for row in rows
    )
    has_observe_only = any(
        str(row.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE
        and str(row.get("shadow_tradeability_class") or "").strip() == OBSERVE_ONLY
        for row in rows
    )

    if has_directional:
        trade_surface_bucket = RUNNABLE_DIRECTIONAL
        hard_guard_reason = "BOUND_DIRECTIONAL_RUNTIME"
    elif has_observe_only:
        trade_surface_bucket = OBSERVE_ONLY
        hard_guard_reason = "BOUND_OBSERVE_ONLY_RUNTIME"
    else:
        trade_surface_bucket = UNRUNNABLE
        priority_reason = ""
        for status in ("UNBOUND_NO_RUNTIME_IMPL", "UNBOUND_CONFIG_GAP", "UNBOUND_TRANSLATION_REJECTED"):
            if status in runtime_statuses.split(","):
                priority_reason = status
                break
        hard_guard_reason = priority_reason or "NO_RUNTIME_BINDING_ROWS"

    return {
        "trade_surface_bucket": trade_surface_bucket,
        "runtime_binding_statuses": runtime_statuses,
        "shadow_tradeability_classes": shadow_tradeability_classes,
        "binding_reasons": binding_reasons,
        "hard_guard_reason": hard_guard_reason,
    }


def max_timestamp(*values: str) -> str:
    timestamps = [str(value or "").strip() for value in values if str(value or "").strip()]
    return max(timestamps) if timestamps else ""


def aggregate_shadow_evidence(
    futures_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    pack_summary_row: Dict[str, Any],
    outcome_row: Dict[str, Any],
    watchlist_row: Dict[str, Any],
) -> Dict[str, Any]:
    shadow_run_count = len(futures_rows)
    fill_backed_run_count = sum(1 for row in futures_rows if as_int(row.get("fill_event_count")) > 0)
    profitability_interpretable_run_count = sum(
        1
        for row in futures_rows
        if str(row.get("profitability_status") or "").strip() in INTERPRETABLE_PROFITABILITY_STATUSES
    )
    no_fill_activity_count = sum(
        1 for row in futures_rows if str(row.get("paper_run_status") or "").strip() == "NO_FILL_ACTIVITY"
    )
    fill_backed_open_count = sum(
        1 for row in futures_rows if str(row.get("paper_run_status") or "").strip() == "FILL_BACKED_POSITION_OPEN"
    )
    fill_backed_flat_count = sum(
        1 for row in futures_rows if str(row.get("paper_run_status") or "").strip() == "FILL_BACKED_FLAT"
    )
    closed_cycle_rows = [
        row for row in trade_rows if str(row.get("status") or "").strip().upper() == "CLOSED"
    ]
    closed_cycle_count = len(closed_cycle_rows)
    realized_pnl_sum = round(sum(float(row.get("realized_pnl_delta") or 0.0) for row in closed_cycle_rows), 6)
    latest_trade_closed_at = max_timestamp(*(row.get("closed_at", "") for row in closed_cycle_rows))
    last_shadow_observed_at = max_timestamp(
        *(row.get("observed_at", "") for row in futures_rows),
        pack_summary_row.get("last_observed_at", ""),
        outcome_row.get("last_observed_at", ""),
        watchlist_row.get("last_observed_at", ""),
    )
    latest_pnl_interpretation = str(
        pack_summary_row.get("pnl_interpretation")
        or watchlist_row.get("pnl_interpretation")
        or "UNKNOWN"
    ).strip().upper() or "UNKNOWN"
    recent_pnl_bias = str(pack_summary_row.get("recent_pnl_bias") or "NO_HISTORY").strip().upper() or "NO_HISTORY"
    outcome_class = str(outcome_row.get("outcome_class") or "").strip().upper() or "NONE"
    outcome_attention_flag = normalize_flag(
        outcome_row.get("outcome_attention_flag")
        or pack_summary_row.get("pnl_attention_flag")
        or watchlist_row.get("pnl_attention_flag")
    )
    watch_status = str(watchlist_row.get("watch_status") or "").strip().upper() or "INACTIVE"
    has_shadow_history = any(
        [
            shadow_run_count > 0,
            bool(pack_summary_row),
            bool(outcome_row),
            bool(watchlist_row),
            closed_cycle_count > 0,
        ]
    )

    if not has_shadow_history:
        horizon_evidence_bucket = "NO_SHADOW_HISTORY"
    elif closed_cycle_count > 0:
        horizon_evidence_bucket = "CLOSED_CYCLE_PRESENT"
    elif fill_backed_run_count > 0:
        horizon_evidence_bucket = "OPEN_CYCLE_ONLY"
    elif shadow_run_count > 0:
        horizon_evidence_bucket = "NO_FILL_ACTIVITY"
    else:
        horizon_evidence_bucket = "UNKNOWN"

    return {
        "shadow_run_count": shadow_run_count,
        "fill_backed_run_count": fill_backed_run_count,
        "profitability_interpretable_run_count": profitability_interpretable_run_count,
        "no_fill_activity_count": no_fill_activity_count,
        "fill_backed_open_count": fill_backed_open_count,
        "fill_backed_flat_count": fill_backed_flat_count,
        "closed_cycle_count": closed_cycle_count,
        "realized_pnl_sum": realized_pnl_sum,
        "latest_trade_closed_at": latest_trade_closed_at,
        "last_shadow_observed_at": last_shadow_observed_at,
        "latest_pnl_interpretation": latest_pnl_interpretation,
        "recent_pnl_bias": recent_pnl_bias,
        "outcome_class": outcome_class,
        "outcome_attention_flag": outcome_attention_flag,
        "watch_status": watch_status,
        "horizon_evidence_bucket": horizon_evidence_bucket,
        "has_shadow_history": has_shadow_history,
    }


def compute_research_score(
    decision_tier: str,
    det_ratio: float,
    max_rss_kb: float,
    max_elapsed_sec: float,
    context_bonus: float,
) -> float:
    value = (
        TIER_SCORE.get(decision_tier, 0.0)
        + 20.0 * det_ratio
        - min(max_rss_kb / 200000.0, 10.0)
        - min(max_elapsed_sec / 60.0, 10.0)
        + context_bonus
    )
    return round(value, 6)


def is_sufficient_shadow_sample(shadow: Dict[str, Any]) -> bool:
    return (
        int(shadow["fill_backed_run_count"]) >= SUFFICIENT_FILL_BACKED_RUN_COUNT
        or int(shadow["profitability_interpretable_run_count"]) >= SUFFICIENT_PROFITABILITY_RUN_COUNT
        or int(shadow["closed_cycle_count"]) >= SUFFICIENT_CLOSED_CYCLE_COUNT
    )


def classify_review_row(
    trade_surface_bucket: str,
    shadow: Dict[str, Any],
) -> Tuple[str, str]:
    if trade_surface_bucket == OBSERVE_ONLY:
        return OBSERVE_ONLY, "OBSERVE_ONLY_BINDING"
    if trade_surface_bucket != RUNNABLE_DIRECTIONAL:
        return UNRUNNABLE, "UNRUNNABLE_BINDING"

    if not bool(shadow["has_shadow_history"]):
        return UNSEEN, "NO_SHADOW_HISTORY"

    if (
        int(shadow["fill_backed_run_count"]) <= 0
        and int(shadow["profitability_interpretable_run_count"]) <= 0
        and int(shadow["closed_cycle_count"]) <= 0
    ):
        return NO_SIGNAL, "OBSERVED_NO_FILL_BACKED_SIGNAL"

    sufficient_sample = is_sufficient_shadow_sample(shadow)
    realized_pnl_sum = float(shadow["realized_pnl_sum"])
    latest_pnl_interpretation = str(shadow["latest_pnl_interpretation"])
    outcome_class = str(shadow["outcome_class"])

    negative_shadow = (
        (realized_pnl_sum < 0.0 and sufficient_sample)
        or (outcome_class in NEGATIVE_OUTCOME_CLASSES and sufficient_sample)
        or (latest_pnl_interpretation in NEGATIVE_PNL_INTERPRETATIONS and sufficient_sample)
    )
    if negative_shadow:
        if realized_pnl_sum < 0.0:
            return WEAK, "NEGATIVE_REALIZED_SHADOW"
        if outcome_class in NEGATIVE_OUTCOME_CLASSES:
            return WEAK, "NEGATIVE_OUTCOME_CLASS"
        return WEAK, "NEGATIVE_LATEST_PNL_STATE"

    if not sufficient_sample:
        return INSUFFICIENT_EVIDENCE, "SAMPLE_BELOW_MINIMUM"

    positive_shadow = (
        outcome_class in POSITIVE_OUTCOME_CLASSES
        or (realized_pnl_sum > 0.0 and int(shadow["closed_cycle_count"]) >= SUFFICIENT_CLOSED_CYCLE_COUNT)
        or (
            latest_pnl_interpretation in POSITIVE_PNL_INTERPRETATIONS
            and int(shadow["closed_cycle_count"]) >= SUFFICIENT_CLOSED_CYCLE_COUNT
        )
    )
    if positive_shadow:
        if outcome_class in POSITIVE_OUTCOME_CLASSES:
            return PROMISING, "STABLE_POSITIVE_OUTCOME"
        return PROMISING, "POSITIVE_REALIZED_SHADOW"

    return NEUTRAL, "SUFFICIENT_NONNEGATIVE_SHADOW"


def compute_shadow_score(review_class: str, shadow: Dict[str, Any]) -> float:
    fill_backed_run_count = int(shadow["fill_backed_run_count"])
    profitability_interpretable_run_count = int(shadow["profitability_interpretable_run_count"])
    closed_cycle_count = int(shadow["closed_cycle_count"])
    no_fill_activity_count = int(shadow["no_fill_activity_count"])
    shadow_run_count = int(shadow["shadow_run_count"])
    realized_pnl_sum = float(shadow["realized_pnl_sum"])
    outcome_class = str(shadow["outcome_class"])
    latest_pnl_interpretation = str(shadow["latest_pnl_interpretation"])
    attention_flag = str(shadow["outcome_attention_flag"])

    score = 0.0
    score += min(shadow_run_count, 20) * 0.05
    score += min(fill_backed_run_count, 20) * 0.25
    score += min(profitability_interpretable_run_count, 20) * 0.2
    score += min(closed_cycle_count, 5) * 1.0
    score -= min(no_fill_activity_count, 20) * 0.05

    if realized_pnl_sum > 0.0:
        score += min(realized_pnl_sum, 50.0) * 0.05 + 2.0
    elif realized_pnl_sum < 0.0:
        score -= min(abs(realized_pnl_sum), 50.0) * 0.05 + 2.0

    if outcome_class == "STABLE_GAINING":
        score += 4.0
    elif outcome_class == "STABLE_FLAT":
        score += 0.5
    elif outcome_class == "MIXED_RECENT":
        score -= 2.5
    elif outcome_class == "STABLE_LOSING":
        score -= 4.0
    elif outcome_class == "ATTENTION_REQUIRED":
        score -= 3.0

    if latest_pnl_interpretation in POSITIVE_PNL_INTERPRETATIONS:
        score += 1.0
    elif latest_pnl_interpretation in NEGATIVE_PNL_INTERPRETATIONS:
        score -= 1.0

    if attention_flag == "true":
        score -= 1.0

    if review_class == UNSEEN:
        return 0.0
    if review_class == NO_SIGNAL:
        return round(score - 1.0, 6)
    return round(score, 6)


def score_explanation(
    review_class: str,
    trade_surface_bucket: str,
    shadow_reason: str,
    research_score: float,
    shadow_score: float,
    shadow: Dict[str, Any],
) -> str:
    return (
        f"class={review_class};surface={trade_surface_bucket};reason={shadow_reason};"
        f"research={format_score(research_score)};shadow={format_score(shadow_score)};"
        f"runs={shadow['shadow_run_count']};fills={shadow['fill_backed_run_count']};"
        f"trades={shadow['closed_cycle_count']};pnl={format_score(float(shadow['realized_pnl_sum']))};"
        f"outcome={shadow['outcome_class']}"
    )


def normalize_row(
    report_row: Dict[str, str],
    queue_latest: Dict[str, Dict[str, Any]],
    index_latest: Dict[str, Dict[str, Any]],
    contract_by_pack: Dict[str, List[Dict[str, Any]]],
    binding_by_pack: Dict[str, List[Dict[str, Any]]],
    family_roles: Dict[str, Dict[str, Any]],
    watchlist_by_pack: Dict[str, Dict[str, Any]],
    pack_summary_by_pack: Dict[str, Dict[str, Any]],
    outcome_by_pack: Dict[str, Dict[str, Any]],
    futures_by_pack: Dict[str, List[Dict[str, Any]]],
    trade_by_pack: Dict[str, List[Dict[str, Any]]],
    review_v0_map: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    pack_id = str(report_row.get("pack_id", "")).strip()
    queue_record = dict(queue_latest.get(pack_id) or {})
    index_record = dict(index_latest.get(pack_id) or {})
    pack_path = str(report_row.get("pack_path") or queue_record.get("pack_path") or index_record.get("pack_path") or "").strip()
    decision_tier = str(
        report_row.get("decision_tier") or queue_record.get("decision_tier") or index_record.get("decision_tier") or PROMOTE
    ).strip().upper()
    det_pass = as_int(report_row.get("det_pass") or queue_record.get("det_pass") or index_record.get("det_pass"))
    det_supported = as_int(report_row.get("det_supported") or queue_record.get("det_supported") or index_record.get("det_supported"))
    det_skipped = as_int(report_row.get("det_skipped") or queue_record.get("det_skipped") or index_record.get("det_skipped"))
    max_rss_kb = as_float(report_row.get("max_rss_kb") or queue_record.get("max_rss_kb") or index_record.get("max_rss_kb"))
    max_elapsed_sec = as_float(
        report_row.get("max_elapsed_sec") or queue_record.get("max_elapsed_sec") or index_record.get("max_elapsed_sec")
    )
    candidate_status = str(
        report_row.get("candidate_status") or queue_record.get("candidate_status") or index_record.get("candidate_status") or ""
    ).strip()
    det_ratio = float(det_pass) / float(max(det_supported, 1))
    context_flags, context_bonus = collect_context_flags(Path(pack_path), queue_record, index_record)
    research_score = compute_research_score(decision_tier, det_ratio, max_rss_kb, max_elapsed_sec, context_bonus)

    contract_rows = contract_by_pack.get(pack_id, [])
    binding_rows = binding_by_pack.get(pack_id, [])
    contract_info = contract_summary(pack_id, contract_rows, family_roles)
    binding_info = binding_summary(binding_rows)
    shadow = aggregate_shadow_evidence(
        futures_by_pack.get(pack_id, []),
        trade_by_pack.get(pack_id, []),
        pack_summary_by_pack.get(pack_id, {}),
        outcome_by_pack.get(pack_id, {}),
        watchlist_by_pack.get(pack_id, {}),
    )
    review_class, shadow_reason = classify_review_row(binding_info["trade_surface_bucket"], shadow)
    shadow_score = compute_shadow_score(review_class, shadow)
    total_score = round(research_score + shadow_score, 6)
    v0_row = review_v0_map.get(pack_id, {})

    return {
        "rank": "0",
        "class_rank": "0",
        "review_class": review_class,
        "class_priority": str(CLASS_PRIORITY[review_class]),
        "score": format_score(total_score),
        "research_score": format_score(research_score),
        "shadow_score": format_score(shadow_score),
        "v0_rank": str(v0_row.get("rank", "")).strip(),
        "v0_score": str(v0_row.get("score", "")).strip(),
        "decision_tier": decision_tier,
        "pack_id": pack_id,
        "pack_path": pack_path,
        "selected_family_ids": contract_info["selected_family_ids"],
        "family_roles": contract_info["family_roles"],
        "translation_statuses": contract_info["translation_statuses"],
        "runtime_binding_statuses": binding_info["runtime_binding_statuses"],
        "trade_surface_bucket": binding_info["trade_surface_bucket"],
        "shadow_tradeability_classes": binding_info["shadow_tradeability_classes"],
        "hard_guard_reason": binding_info["hard_guard_reason"],
        "shadow_evidence_reason": shadow_reason,
        "det_ratio": format_ratio(det_ratio),
        "det_pass": str(det_pass),
        "det_supported": str(det_supported),
        "det_skipped": str(det_skipped),
        "max_rss_kb": str(max_rss_kb),
        "max_elapsed_sec": str(max_elapsed_sec),
        "context_flags": context_flags,
        "candidate_status": candidate_status,
        "shadow_run_count": str(shadow["shadow_run_count"]),
        "fill_backed_run_count": str(shadow["fill_backed_run_count"]),
        "profitability_interpretable_run_count": str(shadow["profitability_interpretable_run_count"]),
        "no_fill_activity_count": str(shadow["no_fill_activity_count"]),
        "fill_backed_open_count": str(shadow["fill_backed_open_count"]),
        "fill_backed_flat_count": str(shadow["fill_backed_flat_count"]),
        "closed_cycle_count": str(shadow["closed_cycle_count"]),
        "realized_pnl_sum": format_score(float(shadow["realized_pnl_sum"])),
        "latest_trade_closed_at": str(shadow["latest_trade_closed_at"]),
        "last_shadow_observed_at": str(shadow["last_shadow_observed_at"]),
        "latest_pnl_interpretation": str(shadow["latest_pnl_interpretation"]),
        "recent_pnl_bias": str(shadow["recent_pnl_bias"]),
        "outcome_class": str(shadow["outcome_class"]),
        "outcome_attention_flag": str(shadow["outcome_attention_flag"]),
        "watch_status": str(shadow["watch_status"]),
        "horizon_evidence_bucket": str(shadow["horizon_evidence_bucket"]),
        "score_explanation": score_explanation(
            review_class,
            binding_info["trade_surface_bucket"],
            shadow_reason,
            research_score,
            shadow_score,
            shadow,
        ),
        "_class_priority_value": CLASS_PRIORITY[review_class],
        "_score_value": total_score,
        "_research_score_value": research_score,
        "_shadow_score_value": shadow_score,
        "_det_ratio_value": det_ratio,
        "_rss_value": max_rss_kb,
        "_elapsed_value": max_elapsed_sec,
    }


def review_sort_key(row: Dict[str, Any]) -> Tuple[int, float, int, float, float, float, str, str]:
    return (
        int(row["_class_priority_value"]),
        -float(row["_score_value"]),
        tier_priority(row.get("decision_tier", "")),
        -float(row["_shadow_score_value"]),
        -float(row["_det_ratio_value"]),
        float(row["_rss_value"]),
        float(row["_elapsed_value"]),
        str(row.get("pack_path", "")),
        str(row.get("pack_id", "")),
    )


def write_review_tsv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(REVIEW_COLUMNS)
        for row in rows:
            writer.writerow([str(row.get(column, "")) for column in REVIEW_COLUMNS])


def write_review_json(
    path: Path,
    rows: List[Dict[str, Any]],
    *,
    generated_ts_utc: str,
    spec_path: Path,
) -> None:
    class_counts = Counter(str(row.get("review_class", "")) for row in rows)
    trade_surface_counts = Counter(str(row.get("trade_surface_bucket", "")) for row in rows)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "scoring_version": SCHEMA_VERSION,
        "spec_json": str(spec_path),
        "record_count": len(rows),
        "class_counts": dict(sorted(class_counts.items())),
        "trade_surface_counts": dict(sorted(trade_surface_counts.items())),
        "top_candidates": [{column: row[column] for column in REVIEW_COLUMNS} for row in rows[:TOP_CANDIDATES_LIMIT]],
        "rows": [{column: row[column] for column in REVIEW_COLUMNS} for row in rows],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    candidate_queue_path = state_dir / CANDIDATE_QUEUE_FILENAME
    candidate_index_path = state_dir / CANDIDATE_INDEX_FILENAME
    candidate_report_path = state_dir / CANDIDATE_REPORT_FILENAME
    out_tsv_path = Path(args.out_tsv).resolve() if args.out_tsv else (state_dir / CANDIDATE_REVIEW_V2_TSV_FILENAME)
    out_json_path = Path(args.out_json).resolve() if args.out_json else (state_dir / CANDIDATE_REVIEW_V2_JSON_FILENAME)
    review_v0_path = Path(args.candidate_review_v0_tsv).resolve()
    contract_json_path = Path(args.strategy_contract_json).resolve()
    runtime_binding_json_path = Path(args.runtime_binding_json).resolve()
    family_role_json_path = Path(args.family_role_json).resolve()
    watchlist_json_path = Path(args.watchlist_json).resolve()
    pack_summary_json_path = Path(args.execution_pack_summary_json).resolve()
    outcome_review_json_path = Path(args.execution_outcome_review_json).resolve()
    trade_ledger_jsonl_path = Path(args.trade_ledger_jsonl).resolve()
    futures_paper_ledger_json_path = Path(args.futures_paper_ledger_json).resolve()
    spec_path = Path(args.spec_json).resolve()

    candidate_queue = load_jsonl_records_safe(candidate_queue_path)
    candidate_index = load_candidate_index(candidate_index_path)
    candidate_report = load_candidate_report(candidate_report_path)
    queue_latest = latest_by_pack_id(candidate_queue)
    index_latest = {
        str(k): dict(v)
        for k, v in dict(candidate_index.get("latest_by_pack_id") or {}).items()
        if isinstance(v, dict)
    }
    review_v0_map = load_review_v0_map(review_v0_path)
    contract_by_pack = load_contract_by_pack(contract_json_path)
    binding_by_pack = load_binding_by_pack(runtime_binding_json_path)
    family_roles = load_family_roles(family_role_json_path)
    watchlist_by_pack = load_watchlist_by_pack(watchlist_json_path)
    pack_summary_by_pack = load_pack_summary_by_pack(pack_summary_json_path)
    outcome_by_pack = load_outcome_by_pack(outcome_review_json_path)
    futures_by_pack = load_futures_rows_by_pack(futures_paper_ledger_json_path)
    trade_by_pack = load_trade_rows_by_pack(trade_ledger_jsonl_path)

    ranked_rows = [
        normalize_row(
            report_row=row,
            queue_latest=queue_latest,
            index_latest=index_latest,
            contract_by_pack=contract_by_pack,
            binding_by_pack=binding_by_pack,
            family_roles=family_roles,
            watchlist_by_pack=watchlist_by_pack,
            pack_summary_by_pack=pack_summary_by_pack,
            outcome_by_pack=outcome_by_pack,
            futures_by_pack=futures_by_pack,
            trade_by_pack=trade_by_pack,
            review_v0_map=review_v0_map,
        )
        for row in candidate_report
    ]
    ranked_rows.sort(key=review_sort_key)

    class_counts: Dict[str, int] = defaultdict(int)
    for index, row in enumerate(ranked_rows, start=1):
        review_class = str(row["review_class"])
        class_counts[review_class] += 1
        row["rank"] = str(index)
        row["class_rank"] = str(class_counts[review_class])

    generated_ts_utc = utc_now_iso()
    write_review_tsv(out_tsv_path, ranked_rows)
    write_review_json(out_json_path, ranked_rows, generated_ts_utc=generated_ts_utc, spec_path=spec_path)

    top_pack_id = ranked_rows[0]["pack_id"] if ranked_rows else ""
    top_class = ranked_rows[0]["review_class"] if ranked_rows else ""
    print(f"review_count={len(ranked_rows)}")
    print(f"top_pack_id={top_pack_id}")
    print(f"top_class={top_class}")
    print(f"candidate_review_v2_tsv={out_tsv_path}")
    print(f"candidate_review_v2_json={out_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
