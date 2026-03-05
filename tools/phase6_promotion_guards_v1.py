#!/usr/bin/env python3
"""Phase-6 promotion guards v1: policy-driven + stateful promotion records."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from phase6_promotion_guards_v0 import (
        GuardResult,
        PackContractMismatch,
        discover_pack_contract,
        eval_g1_evidence,
        eval_g3_resource,
        write_reports,
    )
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v0 import (
        GuardResult,
        PackContractMismatch,
        discover_pack_contract,
        eval_g1_evidence,
        eval_g3_resource,
        write_reports,
    )


DEFAULT_STATE_DIR = Path(__file__).resolve().parent / "phase6_state"
DEFAULT_POLICY_FILENAME = "promotion_policy.json"
RECORDS_FILENAME = "promotion_records.jsonl"
INDEX_FILENAME = "promotion_index.json"

REQUIRED_POLICY_KEYS = {
    "pass_ratio",
    "max_rss_kb",
    "max_elapsed_sec",
    "exclude_statuses",
    "supported_statuses",
    "require_sha_ok_lines",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 promotion guards v1 (advisory only)")
    p.add_argument("--pack", required=True, help="Archive pack directory")
    p.add_argument("--policy", default="", help="Policy JSON path (default: tools/phase6_state/promotion_policy.json)")
    p.add_argument("--state-dir", default="", help="State dir (default: tools/phase6_state)")
    p.add_argument("--out-dir", default="", help="Default: <pack>/guards")
    return p.parse_args()


def canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_policy_hash(policy_obj: Dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(policy_obj)).hexdigest()


def load_policy(policy_path: Path) -> Tuple[Dict[str, Any], str]:
    if not policy_path.exists():
        raise RuntimeError(f"policy_missing:{policy_path}")
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"policy_invalid_json:{policy_path}:{exc}") from exc
    if not isinstance(policy, dict):
        raise RuntimeError(f"policy_not_object:{policy_path}")

    missing = sorted(REQUIRED_POLICY_KEYS - set(policy.keys()))
    if missing:
        raise RuntimeError(f"policy_missing_keys:{','.join(missing)}")

    if not isinstance(policy.get("exclude_statuses"), list):
        raise RuntimeError("policy_invalid:exclude_statuses must be list")
    if not isinstance(policy.get("supported_statuses"), list):
        raise RuntimeError("policy_invalid:supported_statuses must be list")

    # Normalize numeric fields deterministically.
    policy["pass_ratio"] = float(policy["pass_ratio"])
    policy["max_rss_kb"] = float(policy["max_rss_kb"])
    policy["max_elapsed_sec"] = float(policy["max_elapsed_sec"])
    policy["require_sha_ok_lines"] = int(policy["require_sha_ok_lines"])
    policy["exclude_statuses"] = [str(v) for v in policy["exclude_statuses"]]
    policy["supported_statuses"] = [str(v) for v in policy["supported_statuses"]]
    return policy, canonical_policy_hash(policy)


def eval_g2_determinism_policy(
    determinism_paths: List[Path],
    pass_ratio_threshold: float,
    exclude_statuses: List[str],
    supported_statuses: List[str],
) -> Tuple[GuardResult, Dict[str, Any]]:
    exclude = set(exclude_statuses)
    supported_status_set = set(supported_statuses)

    total = 0
    pass_count = 0
    skipped_count = 0
    status_counts: Dict[str, int] = {}
    unknown_status_count = 0
    malformed_rows = 0

    for path in sorted(determinism_paths, key=lambda p: str(p)):
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                total += 1
                status = (row.get("determinism_status") or "").strip()
                if not status:
                    malformed_rows += 1
                    status = "MISSING_STATUS"
                status_counts[status] = status_counts.get(status, 0) + 1
                if status not in supported_status_set:
                    unknown_status_count += 1
                if status in exclude:
                    skipped_count += 1
                    continue
                if status == "PASS":
                    pass_count += 1

    supported_count = total - skipped_count
    ratio = (pass_count / supported_count) if supported_count > 0 else 0.0
    passed = supported_count > 0 and ratio >= pass_ratio_threshold
    observed = (
        f"pass={pass_count};supported={supported_count};ratio={ratio:.6f};"
        f"skipped={skipped_count};unknown_status={unknown_status_count}"
    )
    threshold = f"ratio>={pass_ratio_threshold:.6f}"
    detail = "policy-driven determinism ratio with excluded statuses removed from denominator"
    result = GuardResult(
        guard_id="G2_DETERMINISM",
        status="PASS" if passed else "FAIL",
        observed=observed,
        threshold=threshold,
        detail=detail,
    )
    return result, {
        "total_rows": total,
        "pass_count": pass_count,
        "supported_count": supported_count,
        "skipped_unsupported_count": skipped_count,
        "ratio": ratio,
        "malformed_rows": malformed_rows,
        "unknown_status_count": unknown_status_count,
        "status_counts": dict(sorted(status_counts.items())),
        "source_paths": [str(p) for p in determinism_paths],
        "exclude_statuses": list(exclude_statuses),
        "supported_statuses": list(supported_statuses),
    }


def read_sha_ok_tar_path(sha_verify_path: Path) -> Tuple[bool, str, str]:
    text = sha_verify_path.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines():
        if ": OK" not in line:
            continue
        tar_path = line.split(": OK", 1)[0].strip()
        if tar_path:
            tar = Path(tar_path)
            if tar.exists() and tar.is_file():
                return True, str(tar.resolve()), sha256_file(tar)
    return False, "", ""


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def determine_pack_id(pack: Path, sha_verify_path: Path) -> Tuple[str, bool, str, str]:
    has_ok_tar, tar_path, tar_sha = read_sha_ok_tar_path(sha_verify_path)
    if has_ok_tar and tar_sha:
        return f"{pack.name}__{tar_sha}", True, tar_path, tar_sha
    return pack.name, False, "", ""


def ensure_state_files(state_dir: Path) -> Tuple[Path, Path]:
    state_dir.mkdir(parents=True, exist_ok=True)
    records_path = state_dir / RECORDS_FILENAME
    index_path = state_dir / INDEX_FILENAME
    if not records_path.exists():
        records_path.write_text("", encoding="utf-8")
    if not index_path.exists():
        index_path.write_text(
            json.dumps(
                {
                    "record_count": 0,
                    "pack_latest": {},
                    "promote_pack_ids": [],
                    "promote_packs": [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return records_path, index_path


def read_jsonl_records(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"records_jsonl_invalid line={lineno}: {exc}") from exc
        if not isinstance(obj, dict):
            raise RuntimeError(f"records_jsonl_invalid_object line={lineno}")
        records.append(obj)
    return records


def record_fingerprint(record: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "pack_path",
        "pack_id",
        "decision",
        "policy_hash",
        "sha_tar_ok",
        "max_rss_kb",
        "max_elapsed_sec",
        "det_pass",
        "det_supported",
        "det_skipped",
    ]
    return {k: record.get(k) for k in keys}


def rebuild_index(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest: Dict[str, Dict[str, Any]] = {}
    for i, rec in enumerate(records, start=1):
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        merged = dict(rec)
        merged["_line_no"] = i
        latest[pack_id] = merged

    ordered_pack_ids = sorted(latest.keys())
    pack_latest = {pid: latest[pid] for pid in ordered_pack_ids}
    promote_pack_ids = sorted(
        pid for pid in ordered_pack_ids if str(pack_latest[pid].get("decision")) == "PROMOTE"
    )
    promote_packs = sorted({str(pack_latest[pid].get("pack_path", "")) for pid in promote_pack_ids})

    return {
        "record_count": len(records),
        "pack_latest": pack_latest,
        "promote_pack_ids": promote_pack_ids,
        "promote_packs": promote_packs,
    }


def write_index(index_path: Path, index_obj: Dict[str, Any]) -> None:
    index_path.write_text(
        json.dumps(index_obj, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def append_record_if_needed(
    records_path: Path,
    index_path: Path,
    candidate: Dict[str, Any],
) -> Tuple[bool, int, Dict[str, Any]]:
    records = read_jsonl_records(records_path)
    current_index = rebuild_index(records)
    pack_id = candidate["pack_id"]
    latest = current_index.get("pack_latest", {}).get(pack_id)

    should_append = True
    if latest is not None and record_fingerprint(latest) == record_fingerprint(candidate):
        should_append = False

    if should_append:
        line = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
        with records_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        records.append(candidate)

    new_index = rebuild_index(records)
    write_index(index_path, new_index)
    return should_append, len(records), new_index


def main() -> int:
    args = parse_args()
    pack = Path(args.pack).resolve()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    policy_path = Path(args.policy).resolve() if args.policy else (state_dir / DEFAULT_POLICY_FILENAME)
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (pack / "guards")

    try:
        policy, policy_hash = load_policy(policy_path)
    except Exception as exc:  # noqa: BLE001
        print(f"STOP: POLICY_LOAD_FAIL detail={exc}", file=sys.stderr)
        return 2

    records_path, index_path = ensure_state_files(state_dir)

    try:
        contract = discover_pack_contract(pack)
    except PackContractMismatch as exc:
        print("STOP: PACK_CONTRACT_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for e in exc.expected:
            print(f"  - {e}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for f in exc.found:
            print(f"  - {f}", file=sys.stderr)
        print(
            "minimal_adapter_plan=add path resolver for discovered determinism/run_summary layout while keeping guard semantics unchanged",
            file=sys.stderr,
        )
        return 2

    g1, g1_detail = eval_g1_evidence(contract["sha_verify"])
    required_sha_ok = int(policy["require_sha_ok_lines"])
    if g1_detail.get("ok_line_count", 0) < required_sha_ok:
        g1 = GuardResult(
            guard_id="G1_EVIDENCE",
            status="FAIL",
            observed=f"ok_line_count={g1_detail.get('ok_line_count', 0)}",
            threshold=f">={required_sha_ok}",
            detail=f"path={contract['sha_verify']}",
        )

    g2, g2_detail = eval_g2_determinism_policy(
        contract["determinism_paths"],
        float(policy["pass_ratio"]),
        list(policy["exclude_statuses"]),
        list(policy["supported_statuses"]),
    )
    try:
        g3, g3_detail = eval_g3_resource(
            contract["run_summary"],
            float(policy["max_rss_kb"]),
            float(policy["max_elapsed_sec"]),
        )
    except PackContractMismatch as exc:
        print("STOP: PACK_CONTRACT_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for e in exc.expected:
            print(f"  - {e}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for f in exc.found:
            print(f"  - {f}", file=sys.stderr)
        print(
            "minimal_adapter_plan=align run_summary parser to actual column names while preserving G3 threshold semantics",
            file=sys.stderr,
        )
        return 2

    guards = [g1, g2, g3]
    final_decision = "PROMOTE" if all(g.status == "PASS" for g in guards) else "HOLD"

    pack_id, sha_tar_ok, tar_path, tar_sha = determine_pack_id(pack, contract["sha_verify"])
    ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    details = {
        "advisory_only": True,
        "decision": final_decision,
        "config": {
            "pack": str(pack),
            "out_dir": str(out_dir),
            "policy_path": str(policy_path),
            "policy_hash": policy_hash,
            "pass_ratio": float(policy["pass_ratio"]),
            "max_rss_kb": float(policy["max_rss_kb"]),
            "max_elapsed_sec": float(policy["max_elapsed_sec"]),
            "exclude_statuses": list(policy["exclude_statuses"]),
            "supported_statuses": list(policy["supported_statuses"]),
            "require_sha_ok_lines": int(policy["require_sha_ok_lines"]),
        },
        "resolved_paths": {
            "sha_verify": str(contract["sha_verify"]),
            "campaign_meta": str(contract["campaign_meta"]),
            "run_summary": str(contract["run_summary"]),
            "determinism_paths": [str(p) for p in contract["determinism_paths"]],
            "determinism_count": len(contract["determinism_paths"]),
        },
        "guards": {
            "G1_EVIDENCE": g1_detail,
            "G2_DETERMINISM": g2_detail,
            "G3_RESOURCE": g3_detail,
        },
        "promotion_record_preview": {
            "ts_utc": ts_utc,
            "pack_id": pack_id,
            "pack_path": str(pack),
            "decision": final_decision,
            "policy_hash": policy_hash,
            "sha_tar_ok": sha_tar_ok,
            "tar_path": tar_path,
            "tar_sha256": tar_sha,
        },
    }

    write_reports(out_dir, pack, final_decision, guards, details)

    candidate_record = {
        "ts_utc": ts_utc,
        "pack_path": str(pack),
        "pack_id": pack_id,
        "decision": final_decision,
        "policy_hash": policy_hash,
        "guards": {g.guard_id: g.status for g in guards},
        "sha_tar_ok": bool(sha_tar_ok),
        "max_rss_kb": float(g3_detail["max_rss_kb"]),
        "max_elapsed_sec": float(g3_detail["max_elapsed_sec"]),
        "det_pass": int(g2_detail["pass_count"]),
        "det_supported": int(g2_detail["supported_count"]),
        "det_skipped": int(g2_detail["skipped_unsupported_count"]),
    }

    appended, record_count, index_obj = append_record_if_needed(records_path, index_path, candidate_record)

    print(f"decision={final_decision}")
    print(f"report_txt={out_dir / 'decision_report.txt'}")
    print(f"report_tsv={out_dir / 'decision_report.tsv'}")
    print(f"report_json={out_dir / 'guard_details.json'}")
    print(f"state_dir={state_dir}")
    print(f"policy_path={policy_path}")
    print(f"records_path={records_path}")
    print(f"index_path={index_path}")
    print(f"record_appended={'true' if appended else 'false'}")
    print(f"record_count={record_count}")
    print(f"pack_id={pack_id}")
    print(f"promote_packs={len(index_obj.get('promote_packs', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
