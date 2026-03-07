#!/usr/bin/env python3
"""Phase-6 v2 backfill: apply context-aware guards to existing archive packs."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from phase6_promotion_guards_v0 import PackContractMismatch, discover_pack_contract
    from phase6_promotion_guards_v1 import (
        DEFAULT_POLICY_FILENAME,
        DEFAULT_STATE_DIR,
        DEFAULT_STATE_DIR as DEFAULT_PHASE6_STATE_DIR,
        determine_pack_id,
        ensure_state_files,
        read_jsonl_records,
    )
    from phase6_promotion_guards_v2 import (
        DEFAULT_CONTEXT_POLICY_FILENAME,
        decision_tier_from_record,
        discover_context_summary_paths,
        load_context_policy,
        rebuild_index_v2,
    )
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v0 import PackContractMismatch, discover_pack_contract
    from tools.phase6_promotion_guards_v1 import (
        DEFAULT_POLICY_FILENAME,
        DEFAULT_STATE_DIR,
        DEFAULT_STATE_DIR as DEFAULT_PHASE6_STATE_DIR,
        determine_pack_id,
        ensure_state_files,
        read_jsonl_records,
    )
    from tools.phase6_promotion_guards_v2 import (
        DEFAULT_CONTEXT_POLICY_FILENAME,
        decision_tier_from_record,
        discover_context_summary_paths,
        load_context_policy,
        rebuild_index_v2,
    )


DEFAULT_ARCHIVE_ROOT_GLOB = "/home/deploy/quantlab-evidence-archive/*_slim"
CANONICAL_EXCHANGES = {"binance", "bybit", "okx"}
CANONICAL_STREAMS = {"trade", "bbo"}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 v2 backfill for archive packs")
    p.add_argument("--archive-root-glob", default=DEFAULT_ARCHIVE_ROOT_GLOB)
    p.add_argument("--state-dir", default="", help="Default: tools/phase6_state")
    p.add_argument("--policy", default="", help="Default: <state-dir>/promotion_policy.json")
    p.add_argument("--context-policy", default="", help="Default: <state-dir>/context_policy_v2.json")
    p.add_argument("--pack", action="append", default=[], help="Optional explicit pack path(s)")
    return p.parse_args(argv)


def utc_now(dt: Optional[datetime] = None) -> datetime:
    return (dt or datetime.now(timezone.utc)).astimezone(timezone.utc)


def utc_now_iso(dt: Optional[datetime] = None) -> str:
    return utc_now(dt).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp(dt: Optional[datetime] = None) -> str:
    return utc_now(dt).strftime("%Y%m%d_%H%M%S")


def parse_kv_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in str(text).splitlines():
        line = raw.strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def is_canonical_lane_pack(pack: Path) -> bool:
    name = pack.name
    parts = name.split("-")
    if len(parts) < 6 or not name.endswith("__FULLSCAN_MAJOR"):
        return False
    for idx, part in enumerate(parts):
        if part in CANONICAL_EXCHANGES and idx + 1 < len(parts) and parts[idx + 1] in CANONICAL_STREAMS:
            return True
    return False


def iter_archive_packs(archive_root_glob: str, explicit_packs: Sequence[str]) -> List[Path]:
    discovered: List[Path] = []
    if explicit_packs:
        for raw in explicit_packs:
            pack = Path(str(raw)).resolve()
            if pack.is_dir():
                discovered.append(pack)
    else:
        for root in sorted(Path("/").glob(str(archive_root_glob).lstrip("/")) if str(archive_root_glob).startswith("/") else Path(".").glob(str(archive_root_glob))):
            if not root.is_dir():
                continue
            for pack in sorted(root.glob("*__FULLSCAN_MAJOR")):
                if pack.is_dir():
                    discovered.append(pack.resolve())
    unique = sorted({str(p): p for p in discovered if is_canonical_lane_pack(p)}.values(), key=lambda p: str(p))
    return unique


def latest_v2_records_by_pack_id(records_path: Path) -> Dict[str, Dict[str, Any]]:
    records = read_jsonl_records(records_path)
    rebuilt = rebuild_index_v2(records)
    latest = {}
    for pack_id, rec in dict(rebuilt.get("pack_latest") or {}).items():
        if not isinstance(rec, dict):
            continue
        latest[str(pack_id)] = dict(rec)
    return latest


def pack_has_v2_guard_report(pack: Path) -> bool:
    report = pack / "guards" / "decision_report.txt"
    if not report.exists():
        return False
    text = report.read_text(encoding="utf-8", errors="replace")
    return (
        "guard_count=6" in text
        and "G4_MARK_CONTEXT=" in text
        and "G5_FUNDING_CONTEXT=" in text
        and "G6_OI_CONTEXT=" in text
    )


def inspect_pack(
    pack: Path,
    *,
    state_latest_by_pack_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "pack_path": str(pack),
        "status": "",
        "pack_id": "",
        "reason": "",
    }
    try:
        contract = discover_pack_contract(pack)
    except PackContractMismatch as exc:
        info["status"] = "SKIP_INSUFFICIENT_CONTEXT"
        info["reason"] = f"pack_contract_mismatch:{exc.detail}"
        return info

    pack_id, _sha_ok, _tar_path, _tar_sha = determine_pack_id(pack, contract["sha_verify"])
    info["pack_id"] = str(pack_id)

    latest = state_latest_by_pack_id.get(str(pack_id))
    latest_has_v2 = latest is not None and bool(str(latest.get("context_policy_hash", "")).strip()) and bool(
        str(latest.get("decision_tier", "")).strip()
    )
    if latest_has_v2 or pack_has_v2_guard_report(pack):
        info["status"] = "SKIP_ALREADY_APPLIED"
        info["reason"] = "v2_present_in_state_or_pack"
        if latest is not None:
            info["latest_decision_tier"] = decision_tier_from_record(latest)
            info["context_policy_hash"] = str(latest.get("context_policy_hash", "")).strip()
        return info

    try:
        context_paths = discover_context_summary_paths(pack)
    except PackContractMismatch as exc:
        info["status"] = "SKIP_INSUFFICIENT_CONTEXT"
        info["reason"] = f"context_contract_mismatch:{exc.detail}"
        return info

    info["context_summary_count"] = len(context_paths)
    if not context_paths:
        info["status"] = "SKIP_INSUFFICIENT_CONTEXT"
        info["reason"] = "missing_context_summary"
        return info

    info["status"] = "ELIGIBLE"
    info["reason"] = "has_pack_contract_and_context_summary"
    return info


def run_v2_for_pack(
    pack: Path,
    *,
    repo: Path,
    state_dir: Path,
    policy_path: Path,
    context_policy_path: Path,
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((repo / "tools" / "phase6_promotion_guards_v2.py").resolve()),
        "--pack",
        str(pack),
        "--state-dir",
        str(state_dir),
        "--policy",
        str(policy_path),
        "--context-policy",
        str(context_policy_path),
    ]
    proc = subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True)
    kv = parse_kv_lines(proc.stdout)
    return {
        "cmd": cmd,
        "exit_code": int(proc.returncode),
        "stdout": str(proc.stdout),
        "stderr": str(proc.stderr),
        "kv": kv,
    }


def build_report_path(state_dir: Path, started: datetime) -> Path:
    return state_dir / f"phase6_v2_backfill_report_{utc_stamp(started)}.json"


def run_backfill(
    args: argparse.Namespace,
    *,
    repo: Path,
) -> Tuple[int, Dict[str, Any], Path]:
    started = utc_now()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else (repo / DEFAULT_PHASE6_STATE_DIR)
    policy_path = Path(args.policy).resolve() if args.policy else (state_dir / DEFAULT_POLICY_FILENAME)
    context_policy_path = Path(args.context_policy).resolve() if args.context_policy else (state_dir / DEFAULT_CONTEXT_POLICY_FILENAME)
    report_path = build_report_path(state_dir, started)

    # Validate policies up front so failures are deterministic before scanning.
    load_context_policy(context_policy_path)
    ensure_state_files(state_dir)
    records_path = state_dir / "promotion_records.jsonl"

    packs = iter_archive_packs(str(args.archive_root_glob), list(args.pack or []))
    state_latest = latest_v2_records_by_pack_id(records_path)

    report: Dict[str, Any] = {
        "started_ts_utc": utc_now_iso(started),
        "finished_ts_utc": "",
        "status": "OK",
        "archive_root_glob": str(args.archive_root_glob),
        "total_packs_scanned": len(packs),
        "eligible_count": 0,
        "applied_count": 0,
        "skipped_already_applied_count": 0,
        "skipped_insufficient_context_count": 0,
        "failed_count": 0,
        "packs": [],
        "policy_path": str(policy_path),
        "context_policy_path": str(context_policy_path),
    }

    for pack in packs:
        item = inspect_pack(pack, state_latest_by_pack_id=state_latest)
        status = item["status"]
        if status == "ELIGIBLE":
            report["eligible_count"] += 1
            result = run_v2_for_pack(
                pack,
                repo=repo,
                state_dir=state_dir,
                policy_path=policy_path,
                context_policy_path=context_policy_path,
            )
            item["exit_code"] = int(result["exit_code"])
            item["record_appended"] = str(result["kv"].get("record_appended", "")).strip()
            item["decision"] = str(result["kv"].get("decision", "")).strip()
            if int(result["exit_code"]) == 0:
                item["status"] = "APPLIED"
                report["applied_count"] += 1
            else:
                item["status"] = "FAILED"
                item["stderr_tail"] = "\n".join(str(result["stderr"]).splitlines()[-40:])
                report["failed_count"] += 1
        elif status == "SKIP_ALREADY_APPLIED":
            report["skipped_already_applied_count"] += 1
        else:
            report["skipped_insufficient_context_count"] += 1
        report["packs"].append(item)

    report["finished_ts_utc"] = utc_now_iso()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    exit_code = 0 if report["failed_count"] == 0 else 2
    return exit_code, report, report_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    try:
        exit_code, report, report_path = run_backfill(args, repo=repo)
    except Exception as exc:  # noqa: BLE001
        print(f"STOP: PHASE6_BACKFILL_FAIL detail={exc}", file=sys.stderr)
        return 2

    print(f"status={report['status']}")
    print(f"total_packs_scanned={report['total_packs_scanned']}")
    print(f"eligible_count={report['eligible_count']}")
    print(f"applied_count={report['applied_count']}")
    print(f"skipped_already_applied_count={report['skipped_already_applied_count']}")
    print(f"skipped_insufficient_context_count={report['skipped_insufficient_context_count']}")
    print(f"failed_count={report['failed_count']}")
    print(f"report_path={report_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
