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
    return p.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def normalize_row(
    report_row: Dict[str, str],
    queue_latest: Dict[str, Dict[str, Any]],
    index_latest: Dict[str, Dict[str, Any]],
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
    candidate_queue_path = state_dir / CANDIDATE_QUEUE_FILENAME
    candidate_index_path = state_dir / CANDIDATE_INDEX_FILENAME
    candidate_report_path = state_dir / CANDIDATE_REPORT_FILENAME
    candidate_review_tsv_path = state_dir / CANDIDATE_REVIEW_TSV_FILENAME
    candidate_review_json_path = state_dir / CANDIDATE_REVIEW_JSON_FILENAME

    candidate_queue = read_jsonl_records(candidate_queue_path)
    candidate_index = load_candidate_index(candidate_index_path)
    candidate_report = load_candidate_report(candidate_report_path)
    queue_latest = latest_by_pack_id(candidate_queue)
    index_latest = {
        str(k): dict(v)
        for k, v in dict(candidate_index.get("latest_by_pack_id") or {}).items()
        if isinstance(v, dict)
    }

    ranked_rows = [normalize_row(row, queue_latest, index_latest) for row in candidate_report]
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
