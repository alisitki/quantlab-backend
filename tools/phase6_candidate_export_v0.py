#!/usr/bin/env python3
"""Phase-6 candidate export v0: surface PROMOTE and PROMOTE_STRONG packs."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v1 import DEFAULT_STATE_DIR, read_jsonl_records


CANDIDATE_QUEUE_FILENAME = "candidate_queue.jsonl"
CANDIDATE_INDEX_FILENAME = "candidate_index.json"
CANDIDATE_REPORT_FILENAME = "candidate_report.tsv"
ELIGIBLE_TIERS = {"PROMOTE", "PROMOTE_STRONG"}
REPORT_COLUMNS = [
    "pack_id",
    "decision_tier",
    "pack_path",
    "det_pass",
    "det_supported",
    "det_skipped",
    "max_rss_kb",
    "max_elapsed_sec",
    "candidate_status",
]


class PromotionIndexSchemaMismatch(RuntimeError):
    def __init__(self, expected: List[str], found: List[str], detail: str):
        super().__init__(detail)
        self.expected = expected
        self.found = found
        self.detail = detail


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 candidate export v0")
    p.add_argument("--state-dir", default="", help="Default: tools/phase6_state")
    return p.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_promotion_index(path: Path) -> Dict[str, Any]:
    expected = [
        "promotion_index.json is object",
        "keys include record_count, pack_latest, promote_packs",
        "pack_latest is object and promote_packs is list",
    ]
    if not path.exists():
        raise PromotionIndexSchemaMismatch(expected, [str(path)], "promotion_index_missing")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PromotionIndexSchemaMismatch(expected, [str(path)], f"promotion_index_invalid_json:{exc}") from exc
    if not isinstance(obj, dict):
        raise PromotionIndexSchemaMismatch(expected, [f"type={type(obj).__name__}"], "promotion_index_not_object")

    found_keys = sorted(obj.keys())
    for key in ["record_count", "pack_latest", "promote_packs"]:
        if key not in obj:
            raise PromotionIndexSchemaMismatch(expected, found_keys, f"promotion_index_missing_key:{key}")
    if not isinstance(obj.get("pack_latest"), dict):
        raise PromotionIndexSchemaMismatch(expected, found_keys, "promotion_index_invalid_pack_latest")
    if not isinstance(obj.get("promote_packs"), list):
        raise PromotionIndexSchemaMismatch(expected, found_keys, "promotion_index_invalid_promote_packs")
    if "promote_strong_packs" in obj and not isinstance(obj.get("promote_strong_packs"), list):
        raise PromotionIndexSchemaMismatch(expected, found_keys, "promotion_index_invalid_promote_strong_packs")
    return obj


def decision_tier_from_record(record: Dict[str, Any]) -> str:
    value = str(record.get("decision_tier") or record.get("decision") or "").strip().upper()
    return value if value in {"PROMOTE", "PROMOTE_STRONG", "HOLD"} else ""


def latest_records_by_pack_id(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        latest[pack_id] = dict(rec)
    return {pack_id: latest[pack_id] for pack_id in sorted(latest.keys())}


def build_candidate_record(source: Dict[str, Any], export_ts_utc: str) -> Dict[str, Any]:
    return {
        "export_ts_utc": export_ts_utc,
        "pack_id": str(source.get("pack_id", "")).strip(),
        "pack_path": str(source.get("pack_path", "")).strip(),
        "decision_tier": decision_tier_from_record(source),
        "source_decision": str(source.get("decision", "")).strip().upper(),
        "context_policy_hash": str(source.get("context_policy_hash", "")).strip(),
        "policy_hash": str(source.get("policy_hash", "")).strip(),
        "det_pass": int(source.get("det_pass", 0) or 0),
        "det_supported": int(source.get("det_supported", 0) or 0),
        "det_skipped": int(source.get("det_skipped", 0) or 0),
        "max_rss_kb": float(source.get("max_rss_kb", 0.0) or 0.0),
        "max_elapsed_sec": float(source.get("max_elapsed_sec", 0.0) or 0.0),
        "guards": dict(source.get("guards") or {}),
        "candidate_status": "NEW",
        "notes": "",
    }


def candidate_fingerprint(record: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "pack_id",
        "pack_path",
        "decision_tier",
        "source_decision",
        "context_policy_hash",
        "policy_hash",
        "det_pass",
        "det_supported",
        "det_skipped",
        "max_rss_kb",
        "max_elapsed_sec",
        "guards",
    ]
    return {key: record.get(key) for key in keys}


def load_candidate_queue(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return read_jsonl_records(path)


def latest_candidates_by_pack_id(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        latest[pack_id] = dict(rec)
    return {pack_id: latest[pack_id] for pack_id in sorted(latest.keys())}


def candidate_sort_key(record: Dict[str, Any]) -> Tuple[int, str, str]:
    tier = str(record.get("decision_tier", "")).strip().upper()
    tier_rank = 0 if tier == "PROMOTE_STRONG" else 1
    pack_path = str(record.get("pack_path", "")).strip()
    pack_id = str(record.get("pack_id", "")).strip()
    return (tier_rank, pack_path, pack_id)


def write_candidate_report(path: Path, current_candidates: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(REPORT_COLUMNS)
        for rec in sorted(current_candidates, key=candidate_sort_key):
            writer.writerow(
                [
                    str(rec.get("pack_id", "")),
                    str(rec.get("decision_tier", "")),
                    str(rec.get("pack_path", "")),
                    str(rec.get("det_pass", "")),
                    str(rec.get("det_supported", "")),
                    str(rec.get("det_skipped", "")),
                    str(rec.get("max_rss_kb", "")),
                    str(rec.get("max_elapsed_sec", "")),
                    str(rec.get("candidate_status", "")),
                ]
            )


def build_candidate_index(current_candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(current_candidates, key=candidate_sort_key)
    latest_by_pack_id = {str(rec["pack_id"]): dict(rec) for rec in ordered}
    latest_by_tier = {
        "PROMOTE_STRONG": [dict(rec) for rec in ordered if rec.get("decision_tier") == "PROMOTE_STRONG"],
        "PROMOTE": [dict(rec) for rec in ordered if rec.get("decision_tier") == "PROMOTE"],
    }
    export_timestamps = [str(rec.get("export_ts_utc", "")).strip() for rec in ordered if str(rec.get("export_ts_utc", "")).strip()]
    return {
        "record_count": len(ordered),
        "by_tier": {
            "PROMOTE": len(latest_by_tier["PROMOTE"]),
            "PROMOTE_STRONG": len(latest_by_tier["PROMOTE_STRONG"]),
        },
        "candidate_pack_ids": [str(rec["pack_id"]) for rec in ordered],
        "latest_by_pack_id": latest_by_pack_id,
        "latest_by_tier": latest_by_tier,
        "latest_export_ts_utc": max(export_timestamps) if export_timestamps else "",
    }


def write_candidate_index(path: Path, index_obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(index_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    promotion_records_path = state_dir / "promotion_records.jsonl"
    promotion_index_path = state_dir / "promotion_index.json"
    candidate_queue_path = state_dir / CANDIDATE_QUEUE_FILENAME
    candidate_index_path = state_dir / CANDIDATE_INDEX_FILENAME
    candidate_report_path = state_dir / CANDIDATE_REPORT_FILENAME

    try:
        _promotion_index = load_promotion_index(promotion_index_path)
    except PromotionIndexSchemaMismatch as exc:
        print("STOP: PROMOTION_INDEX_SCHEMA_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for item in exc.expected:
            print(f"  - {item}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for item in exc.found:
            print(f"  - {item}", file=sys.stderr)
        return 2

    promotion_records = read_jsonl_records(promotion_records_path)
    latest_promotion = latest_records_by_pack_id(promotion_records)
    eligible_latest = [
        rec
        for rec in latest_promotion.values()
        if decision_tier_from_record(rec) in ELIGIBLE_TIERS
    ]
    eligible_latest.sort(key=lambda rec: candidate_sort_key(build_candidate_record(rec, "")))

    candidate_queue = load_candidate_queue(candidate_queue_path)
    existing_latest = latest_candidates_by_pack_id(candidate_queue)
    export_ts_utc = utc_now_iso()
    exported_count = 0
    skipped_existing_count = 0

    candidate_queue_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_queue_path.touch(exist_ok=True)
    new_records: List[Dict[str, Any]] = []
    current_candidates: List[Dict[str, Any]] = []
    for promotion_rec in eligible_latest:
        candidate = build_candidate_record(promotion_rec, export_ts_utc)
        pack_id = candidate["pack_id"]
        existing = existing_latest.get(pack_id)
        if existing is not None and candidate_fingerprint(existing) == candidate_fingerprint(candidate):
            skipped_existing_count += 1
            current_candidates.append(existing)
            continue
        new_records.append(candidate)
        existing_latest[pack_id] = candidate
        current_candidates.append(candidate)
        exported_count += 1

    if new_records:
        with candidate_queue_path.open("a", encoding="utf-8") as f:
            for rec in new_records:
                f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")

    current_candidates.sort(key=candidate_sort_key)
    candidate_index = build_candidate_index(current_candidates)
    write_candidate_index(candidate_index_path, candidate_index)
    write_candidate_report(candidate_report_path, current_candidates)

    latest_three = sorted(
        current_candidates,
        key=lambda rec: (str(rec.get("export_ts_utc", "")), str(rec.get("pack_id", ""))),
        reverse=True,
    )[:3]

    print(f"exported_count={exported_count}")
    print(f"skipped_existing_count={skipped_existing_count}")
    print(f"candidate_count_total={candidate_index['record_count']}")
    print(f"strong_count={candidate_index['by_tier']['PROMOTE_STRONG']}")
    print(f"latest_export_ts_utc={candidate_index['latest_export_ts_utc']}")
    print("latest_3_candidate_pack_ids=" + "|".join(str(rec.get("pack_id", "")) for rec in latest_three))
    print(f"candidate_queue_path={candidate_queue_path}")
    print(f"candidate_index_path={candidate_index_path}")
    print(f"candidate_report_path={candidate_report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
