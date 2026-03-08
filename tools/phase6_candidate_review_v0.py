#!/usr/bin/env python3
"""Phase-6 candidate review v0: deterministic ranking for exported candidates."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records


CANDIDATE_QUEUE_FILENAME = "candidate_queue.jsonl"
CANDIDATE_INDEX_FILENAME = "candidate_index.json"
CANDIDATE_REPORT_FILENAME = "candidate_report.tsv"
CANDIDATE_REVIEW_TSV_FILENAME = "candidate_review.tsv"
CANDIDATE_REVIEW_JSON_FILENAME = "candidate_review.json"
DEFAULT_OBSERVATION_INDEX_PATH = Path("tools") / "shadow_state" / "shadow_observation_index_v0.json"
DEFAULT_OBSERVATION_HISTORY_PATH = Path("tools") / "shadow_state" / "shadow_observation_history_v0.jsonl"
DEFAULT_EXECUTION_PACK_SUMMARY_PATH = Path("tools") / "shadow_state" / "shadow_execution_pack_summary_v0.json"
DEFAULT_RECENT_OBSERVATION_HOURS = 24.0
DEFAULT_OBSERVATION_TRAIL_LENGTH = 3
SCORING_VERSION = "candidate_review_v0"
PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIPPED = "SKIPPED"
NA = "NA"
NEUTRAL = "NEUTRAL"
PROMOTE = "PROMOTE"
PROMOTE_STRONG = "PROMOTE_STRONG"
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
REVIEW_COLUMNS = [
    "rank",
    "score",
    "decision_tier",
    "pack_id",
    "pack_path",
    "det_ratio",
    "det_pass",
    "det_supported",
    "det_skipped",
    "max_rss_kb",
    "max_elapsed_sec",
    "context_flags",
    "candidate_status",
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
CONTEXT_GUARDS = {
    "G4_MARK_CONTEXT": "MARK",
    "G5_FUNDING_CONTEXT": "FUNDING",
    "G6_OI_CONTEXT": "OI",
}
TIER_SCORE = {
    PROMOTE_STRONG: 50.0,
    PROMOTE: 30.0,
}
TOP_CANDIDATES_LIMIT = 10


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 candidate review v0")
    p.add_argument("--state-dir", default="", help="Default: tools/phase6_state")
    p.add_argument(
        "--observation-index",
        default="",
        help="Default: tools/shadow_state/shadow_observation_index_v0.json",
    )
    p.add_argument(
        "--observation-history",
        default="",
        help="Default: tools/shadow_state/shadow_observation_history_v0.jsonl",
    )
    p.add_argument(
        "--execution-pack-summary",
        default="",
        help="Default: tools/shadow_state/shadow_execution_pack_summary_v0.json",
    )
    p.add_argument(
        "--recent-observation-hours",
        type=float,
        default=DEFAULT_RECENT_OBSERVATION_HOURS,
        help=f"Default: {DEFAULT_RECENT_OBSERVATION_HOURS}",
    )
    return p.parse_args(argv)


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
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_REPORT_COLUMNS - fieldnames)
        if missing:
            raise RuntimeError(f"candidate_report_missing_columns:{','.join(missing)}")
        return [{str(k): str(v or "") for k, v in row.items()} for row in reader]


def load_observation_index(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"observation_index_not_object:{path}")
    latest = obj.get("latest_by_pack_id")
    if not isinstance(latest, dict):
        raise RuntimeError(f"observation_index_invalid_latest_by_pack_id:{path}")
    return {str(k): dict(v or {}) for k, v in latest.items() if isinstance(v, dict)}


def load_observation_history(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    if not path.exists():
        return {}
    by_pack: Dict[str, List[Dict[str, Any]]] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"observation_history_invalid_json:{path}:{lineno}:{exc}") from exc
        if not isinstance(obj, dict):
            raise RuntimeError(f"observation_history_not_object:{path}:{lineno}")
        pack_id = str(obj.get("selected_pack_id", "")).strip()
        if not pack_id:
            raise RuntimeError(f"observation_history_missing_selected_pack_id:{path}:{lineno}")
        by_pack.setdefault(pack_id, []).append(dict(obj))
    for pack_id, entries in by_pack.items():
        entries.sort(
            key=lambda entry: (
                str(entry.get("observed_at", "")),
                str(entry.get("live_run_id", "")),
            ),
            reverse=True,
        )
    return by_pack


def load_execution_pack_summary(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"execution_pack_summary_not_object:{path}")
    latest = obj.get("latest_by_pack_id")
    if latest is None:
        return {}
    if not isinstance(latest, dict):
        return {}
    return {str(k): dict(v or {}) for k, v in latest.items() if isinstance(v, dict)}


def latest_by_pack_id(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        latest[pack_id] = dict(rec)
    return {pack_id: latest[pack_id] for pack_id in sorted(latest.keys())}


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
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
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


def format_observed_bool(value: bool) -> str:
    return "true" if value else "false"


def observation_enrichment(pack_id: str, observation_latest: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    record = dict(observation_latest.get(pack_id) or {})
    if not record:
        return {
            "observed_before": format_observed_bool(False),
            "observation_count": "0",
            "last_observed_at": "",
            "last_verify_soft_live_pass": "unknown",
            "last_stop_reason": "",
            "last_processed_event_count": "unknown",
        }
    verify_value = record.get("last_verify_soft_live_pass")
    if isinstance(verify_value, bool):
        last_verify = format_observed_bool(verify_value)
    else:
        last_verify = str(verify_value or "unknown").strip() or "unknown"
    processed_value = record.get("last_processed_event_count", "unknown")
    processed = str(processed_value).strip() if processed_value is not None else "unknown"
    return {
        "observed_before": format_observed_bool(True),
        "observation_count": str(record.get("observation_count", 0)),
        "last_observed_at": str(record.get("last_observed_at", "")).strip(),
        "last_verify_soft_live_pass": last_verify,
        "last_stop_reason": str(record.get("last_stop_reason", "")).strip(),
        "last_processed_event_count": processed or "unknown",
    }


def derive_observation_status(observation_fields: Dict[str, str]) -> str:
    observed_before = str(observation_fields.get("observed_before", "")).strip().lower() == "true"
    if not observed_before:
        return "NEW"

    verify_value = str(observation_fields.get("last_verify_soft_live_pass", "")).strip().lower()
    processed_raw = str(observation_fields.get("last_processed_event_count", "")).strip()
    processed_count: int | None
    try:
        processed_count = int(processed_raw)
    except (TypeError, ValueError):
        processed_count = None

    if verify_value == "true":
        if processed_count is not None and processed_count > 0:
            return "OBSERVED_PASS"
        return "OBSERVED_PASS_NO_EVENTS"
    if verify_value == "false":
        return "OBSERVED_FAIL"
    return "OBSERVED_UNKNOWN"


def derive_observation_last_outcome_short(observation_fields: Dict[str, str]) -> str:
    observed_before = str(observation_fields.get("observed_before", "")).strip().lower() == "true"
    if not observed_before:
        return "NO_HISTORY"

    verify_value = str(observation_fields.get("last_verify_soft_live_pass", "")).strip().lower()
    processed_raw = str(observation_fields.get("last_processed_event_count", "")).strip()
    try:
        processed_count = int(processed_raw)
    except (TypeError, ValueError):
        processed_count = None

    if verify_value == "true":
        if processed_count is not None and processed_count > 0:
            return f"PASS({processed_count})"
        return "PASS_NO_EVENTS"
    if verify_value == "false":
        return "FAIL"
    return "UNKNOWN"


def derive_next_action_hint(observation_status: str) -> str:
    normalized = str(observation_status or "").strip().upper()
    if normalized == "NEW":
        return "READY_TO_OBSERVE"
    if normalized == "OBSERVED_PASS":
        return "ALREADY_OBSERVED_GOOD"
    if normalized == "OBSERVED_PASS_NO_EVENTS":
        return "REOBSERVE_CANDIDATE"
    if normalized == "OBSERVED_FAIL":
        return "NEEDS_ATTENTION"
    return "REVIEW_OBSERVATION_STATE"


def derive_last_observation_age_hours(
    observation_fields: Dict[str, str],
    *,
    now_utc: datetime,
) -> str:
    observed_before = str(observation_fields.get("observed_before", "")).strip().lower() == "true"
    if not observed_before:
        return "unknown"

    last_observed_at = parse_utc_iso(str(observation_fields.get("last_observed_at", "")).strip())
    if last_observed_at is None:
        return "unknown"

    age_hours = max((now_utc - last_observed_at).total_seconds(), 0.0) / 3600.0
    return f"{age_hours:.3f}"


def derive_observation_recency_bucket(
    observation_fields: Dict[str, str],
    *,
    now_utc: datetime,
) -> str:
    observed_before = str(observation_fields.get("observed_before", "")).strip().lower() == "true"
    if not observed_before:
        return "NEVER_OBSERVED"

    last_observed_at = parse_utc_iso(str(observation_fields.get("last_observed_at", "")).strip())
    if last_observed_at is None:
        return "UNKNOWN"

    age_hours = max((now_utc - last_observed_at).total_seconds(), 0.0) / 3600.0
    if age_hours <= 24.0:
        return "WITHIN_24H"
    if age_hours <= 24.0 * 7.0:
        return "WITHIN_7D"
    return "OLDER_THAN_7D"


def derive_observation_attention_flag(last_outcome_short: str) -> str:
    normalized = str(last_outcome_short or "").strip().upper()
    if normalized in {"FAIL", "UNKNOWN"}:
        return "true"
    return "false"


def derive_reobserve_status(
    observation_fields: Dict[str, str],
    *,
    now_utc: datetime,
    recent_observation_hours: float,
) -> str:
    observed_before = str(observation_fields.get("observed_before", "")).strip().lower() == "true"
    if not observed_before:
        return "NOT_OBSERVED"

    last_observed_at = parse_utc_iso(str(observation_fields.get("last_observed_at", "")).strip())
    if last_observed_at is None:
        return "OBSERVATION_TIME_UNKNOWN"

    age_seconds = max((now_utc - last_observed_at).total_seconds(), 0.0)
    if age_seconds <= float(recent_observation_hours) * 3600.0:
        return "RECENTLY_OBSERVED"
    return "STALE_OBSERVATION"


def trail_status_token(entry: Dict[str, Any]) -> str:
    verify_value = entry.get("verify_soft_live_pass")
    if isinstance(verify_value, bool):
        verify_norm = "true" if verify_value else "false"
    else:
        verify_norm = str(verify_value or "").strip().lower()

    processed_raw = str(entry.get("processed_event_count", "")).strip()
    try:
        processed_count = int(processed_raw)
    except (TypeError, ValueError):
        processed_count = None

    if verify_norm == "true":
        if processed_count is not None and processed_count > 0:
            return f"PASS({processed_count})"
        return "PASS_NO_EVENTS"
    if verify_norm == "false":
        return "FAIL"
    return "UNKNOWN"


def derive_recent_observation_trail(
    pack_id: str,
    observation_history: Dict[str, List[Dict[str, Any]]],
    *,
    max_entries: int = DEFAULT_OBSERVATION_TRAIL_LENGTH,
) -> str:
    entries = list(observation_history.get(pack_id) or [])
    if not entries:
        return ""
    parts: List[str] = []
    for entry in entries[:max_entries]:
        observed_at = str(entry.get("observed_at", "")).strip() or "unknown"
        stop_reason = str(entry.get("stop_reason", "")).strip() or "NA"
        parts.append(f"{observed_at}/{trail_status_token(entry)}/{stop_reason}")
    return " | ".join(parts)


def normalize_execution_state(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value or "UNKNOWN"


def normalize_execution_attention(raw: Any) -> str:
    return "true" if str(raw or "").strip().lower() == "true" else "false"


def execution_enrichment(pack_id: str, execution_latest: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    record = dict(execution_latest.get(pack_id) or {})
    return {
        "last_pnl_state": normalize_execution_state(record.get("last_pnl_state")),
        "pnl_interpretation": normalize_execution_state(record.get("pnl_interpretation")),
        "pnl_attention_flag": normalize_execution_attention(record.get("pnl_attention_flag")),
        "latest_realized_sign": normalize_execution_state(record.get("latest_realized_sign")),
        "latest_unrealized_sign": normalize_execution_state(record.get("latest_unrealized_sign")),
    }


def normalize_row(
    report_row: Dict[str, str],
    queue_latest: Dict[str, Dict[str, Any]],
    index_latest: Dict[str, Dict[str, Any]],
    observation_latest: Dict[str, Dict[str, Any]],
    observation_history: Dict[str, List[Dict[str, Any]]],
    execution_latest: Dict[str, Dict[str, Any]],
    *,
    now_utc: datetime,
    recent_observation_hours: float,
) -> Dict[str, Any]:
    pack_id = str(report_row.get("pack_id", "")).strip()
    queue_record = dict(queue_latest.get(pack_id) or {})
    index_record = dict(index_latest.get(pack_id) or {})
    pack_path = str(report_row.get("pack_path") or queue_record.get("pack_path") or index_record.get("pack_path") or "").strip()
    decision_tier = str(
        report_row.get("decision_tier") or queue_record.get("decision_tier") or index_record.get("decision_tier") or PROMOTE
    ).strip().upper()
    det_pass = as_int(report_row.get("det_pass") or queue_record.get("det_pass") or index_record.get("det_pass"))
    det_supported = as_int(
        report_row.get("det_supported") or queue_record.get("det_supported") or index_record.get("det_supported")
    )
    det_skipped = as_int(report_row.get("det_skipped") or queue_record.get("det_skipped") or index_record.get("det_skipped"))
    max_rss_kb = as_float(report_row.get("max_rss_kb") or queue_record.get("max_rss_kb") or index_record.get("max_rss_kb"))
    max_elapsed_sec = as_float(
        report_row.get("max_elapsed_sec") or queue_record.get("max_elapsed_sec") or index_record.get("max_elapsed_sec")
    )
    candidate_status = str(
        report_row.get("candidate_status") or queue_record.get("candidate_status") or index_record.get("candidate_status") or ""
    ).strip()
    context_flags, context_bonus = collect_context_flags(Path(pack_path), queue_record, index_record)
    observation_fields = observation_enrichment(pack_id, observation_latest)
    last_observation_age_hours = derive_last_observation_age_hours(observation_fields, now_utc=now_utc)
    observation_recency_bucket = derive_observation_recency_bucket(observation_fields, now_utc=now_utc)
    observation_last_outcome_short = derive_observation_last_outcome_short(observation_fields)
    observation_attention_flag = derive_observation_attention_flag(observation_last_outcome_short)
    observation_status = derive_observation_status(observation_fields)
    next_action_hint = derive_next_action_hint(observation_status)
    reobserve_status = derive_reobserve_status(
        observation_fields,
        now_utc=now_utc,
        recent_observation_hours=recent_observation_hours,
    )
    recent_observation_trail = derive_recent_observation_trail(pack_id, observation_history)
    execution_fields = execution_enrichment(pack_id, execution_latest)
    det_ratio = float(det_pass) / float(max(det_supported, 1))
    score_value = (
        TIER_SCORE.get(decision_tier, 0.0)
        + 20.0 * det_ratio
        - min(max_rss_kb / 200000.0, 10.0)
        - min(max_elapsed_sec / 60.0, 10.0)
        + context_bonus
    )
    rounded_score = round(score_value, 6)
    return {
        "rank": 0,
        "score": format_score(rounded_score),
        "decision_tier": decision_tier,
        "pack_id": pack_id,
        "pack_path": pack_path,
        "det_ratio": format_ratio(det_ratio),
        "det_pass": str(det_pass),
        "det_supported": str(det_supported),
        "det_skipped": str(det_skipped),
        "max_rss_kb": str(max_rss_kb),
        "max_elapsed_sec": str(max_elapsed_sec),
        "context_flags": context_flags,
        "candidate_status": candidate_status,
        **observation_fields,
        "last_observation_age_hours": last_observation_age_hours,
        "observation_recency_bucket": observation_recency_bucket,
        "observation_last_outcome_short": observation_last_outcome_short,
        "observation_attention_flag": observation_attention_flag,
        "observation_status": observation_status,
        "next_action_hint": next_action_hint,
        "reobserve_status": reobserve_status,
        "recent_observation_trail": recent_observation_trail,
        **execution_fields,
        "_score_value": rounded_score,
        "_det_ratio_value": det_ratio,
        "_rss_value": max_rss_kb,
        "_elapsed_value": max_elapsed_sec,
    }


def review_sort_key(row: Dict[str, Any]) -> Tuple[float, int, float, float, float, str, str]:
    return (
        -float(row["_score_value"]),
        tier_priority(row.get("decision_tier", "")),
        -float(row["_det_ratio_value"]),
        float(row["_rss_value"]),
        float(row["_elapsed_value"]),
        str(row.get("pack_path", "")),
        str(row.get("pack_id", "")),
    )


def write_review_tsv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(REVIEW_COLUMNS)
        for row in rows:
            writer.writerow([str(row.get(col, "")) for col in REVIEW_COLUMNS])


def write_review_json(path: Path, rows: List[Dict[str, Any]], generated_ts_utc: str) -> None:
    top_candidates = []
    for row in rows[:TOP_CANDIDATES_LIMIT]:
        top_candidates.append({col: row[col] for col in REVIEW_COLUMNS})
    payload = {
        "generated_ts_utc": generated_ts_utc,
        "record_count": len(rows),
        "scoring_version": SCORING_VERSION,
        "top_candidates": top_candidates,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    observation_index_path = Path(args.observation_index).resolve() if args.observation_index else DEFAULT_OBSERVATION_INDEX_PATH.resolve()
    observation_history_path = (
        Path(args.observation_history).resolve()
        if args.observation_history
        else DEFAULT_OBSERVATION_HISTORY_PATH.resolve()
    )
    execution_pack_summary_path = (
        Path(args.execution_pack_summary).resolve()
        if args.execution_pack_summary
        else DEFAULT_EXECUTION_PACK_SUMMARY_PATH.resolve()
    )
    candidate_queue_path = state_dir / CANDIDATE_QUEUE_FILENAME
    candidate_index_path = state_dir / CANDIDATE_INDEX_FILENAME
    candidate_report_path = state_dir / CANDIDATE_REPORT_FILENAME
    candidate_review_tsv_path = state_dir / CANDIDATE_REVIEW_TSV_FILENAME
    candidate_review_json_path = state_dir / CANDIDATE_REVIEW_JSON_FILENAME

    candidate_queue = read_jsonl_records(candidate_queue_path)
    candidate_index = load_candidate_index(candidate_index_path)
    candidate_report = load_candidate_report(candidate_report_path)
    observation_latest = load_observation_index(observation_index_path)
    observation_history = load_observation_history(observation_history_path)
    execution_latest = load_execution_pack_summary(execution_pack_summary_path)
    queue_latest = latest_by_pack_id(candidate_queue)
    index_latest = {
        str(k): dict(v)
        for k, v in dict(candidate_index.get("latest_by_pack_id") or {}).items()
        if isinstance(v, dict)
    }

    run_now_utc = datetime.now(timezone.utc)
    ranked_rows = [
        normalize_row(
            row,
            queue_latest,
            index_latest,
            observation_latest,
            observation_history,
            execution_latest,
            now_utc=run_now_utc,
            recent_observation_hours=float(args.recent_observation_hours),
        )
        for row in candidate_report
    ]
    ranked_rows.sort(key=review_sort_key)
    for idx, row in enumerate(ranked_rows, start=1):
        row["rank"] = str(idx)

    generated_ts_utc = utc_now_iso()
    write_review_tsv(candidate_review_tsv_path, ranked_rows)
    write_review_json(candidate_review_json_path, ranked_rows, generated_ts_utc)

    top_pack_id = ranked_rows[0]["pack_id"] if ranked_rows else ""
    top_score = ranked_rows[0]["score"] if ranked_rows else ""
    print(f"review_count={len(ranked_rows)}")
    print(f"top_pack_id={top_pack_id}")
    print(f"top_score={top_score}")
    print(f"candidate_review_tsv={candidate_review_tsv_path}")
    print(f"candidate_review_json={candidate_review_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
