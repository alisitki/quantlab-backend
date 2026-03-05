#!/usr/bin/env python3
"""Phase-6 advisory promotion guards for SLIM packs."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


REQUIRED_RUN_SUMMARY_COLUMNS = {
    "symbol",
    "exit_code",
    "elapsed_sec",
    "max_rss_kb",
    "determinism_statuses",
    "label",
}
SKIPPED_STATUS = "SKIPPED_UNSUPPORTED_STREAM"


@dataclass
class GuardResult:
    guard_id: str
    status: str
    observed: str
    threshold: str
    detail: str


class PackContractMismatch(RuntimeError):
    """Pack layout does not match expected contract."""

    def __init__(self, expected: List[str], found: List[str], detail: str):
        super().__init__(detail)
        self.expected = expected
        self.found = found
        self.detail = detail


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 promotion guards (advisory only)")
    p.add_argument("--pack", required=True, help="Archive pack directory")
    p.add_argument("--pass-ratio", "--pass_ratio", type=float, default=1.0)
    p.add_argument("--max-rss-kb", "--max_rss_kb", type=float, default=2500000.0)
    p.add_argument("--max-elapsed-sec", "--max_elapsed_sec", type=float, default=900.0)
    p.add_argument("--out-dir", default="", help="Default: <pack>/guards")
    return p.parse_args()


def discover_pack_contract(pack: Path) -> Dict[str, object]:
    if not pack.exists() or not pack.is_dir():
        raise PackContractMismatch(
            expected=["<pack_dir>"],
            found=[str(pack)],
            detail="--pack must be an existing directory",
        )

    expected_paths = [
        "sha_verify.txt",
        "campaign_meta.tsv",
        "run_summary.tsv",
        "runs/*/artifacts/multi_hypothesis/determinism_compare.tsv OR artifacts/multi_hypothesis/determinism_compare.tsv",
    ]

    sha_verify = pack / "sha_verify.txt"
    campaign_meta = pack / "campaign_meta.tsv"
    run_summary = pack / "run_summary.tsv"

    missing = []
    if not sha_verify.exists():
        missing.append(str(sha_verify))
    if not campaign_meta.exists():
        missing.append(str(campaign_meta))
    if not run_summary.exists():
        missing.append(str(run_summary))

    per_run = sorted(
        str(p)
        for p in pack.glob("runs/*/artifacts/multi_hypothesis/determinism_compare.tsv")
        if p.is_file()
    )
    fallback = pack / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv"
    if per_run:
        determinism_paths = [Path(p) for p in per_run]
    elif fallback.exists():
        determinism_paths = [fallback]
    else:
        determinism_paths = []

    if missing or not determinism_paths:
        found_candidates = sorted(
            str(p)
            for p in pack.glob("**/determinism_compare.tsv")
            if p.is_file()
        )
        detail_parts = []
        if missing:
            detail_parts.append(f"missing_required_files={','.join(missing)}")
        if not determinism_paths:
            detail_parts.append("missing_determinism_compare_paths")
        raise PackContractMismatch(
            expected=expected_paths,
            found=found_candidates[:50],
            detail="; ".join(detail_parts),
        )

    return {
        "pack": pack,
        "sha_verify": sha_verify,
        "campaign_meta": campaign_meta,
        "run_summary": run_summary,
        "determinism_paths": sorted(determinism_paths, key=lambda x: str(x)),
    }


def eval_g1_evidence(sha_verify_path: Path) -> Tuple[GuardResult, Dict[str, object]]:
    text = sha_verify_path.read_text(encoding="utf-8", errors="replace")
    ok_lines = [ln for ln in text.splitlines() if ": OK" in ln]
    passed = len(ok_lines) > 0
    result = GuardResult(
        guard_id="G1_EVIDENCE",
        status="PASS" if passed else "FAIL",
        observed=f"ok_line_count={len(ok_lines)}",
        threshold=">=1",
        detail=f"path={sha_verify_path}",
    )
    return result, {"ok_line_count": len(ok_lines), "ok_lines": ok_lines[:20]}


def eval_g2_determinism(determinism_paths: List[Path], pass_ratio_threshold: float) -> Tuple[GuardResult, Dict[str, object]]:
    total = 0
    pass_count = 0
    skipped_count = 0
    status_counts: Dict[str, int] = {}
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
                if status == SKIPPED_STATUS:
                    skipped_count += 1
                    continue
                if status == "PASS":
                    pass_count += 1

    supported_count = total - skipped_count
    ratio = (pass_count / supported_count) if supported_count > 0 else 0.0
    passed = supported_count > 0 and ratio >= pass_ratio_threshold

    observed = f"pass={pass_count};supported={supported_count};ratio={ratio:.6f};skipped={skipped_count}"
    threshold = f"ratio>={pass_ratio_threshold:.6f}"
    detail = "determinism_status=PASS for supported families; SKIPPED_UNSUPPORTED_STREAM excluded"

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
        "status_counts": dict(sorted(status_counts.items())),
        "source_paths": [str(p) for p in determinism_paths],
    }


def eval_g3_resource(run_summary_path: Path, max_rss_kb: float, max_elapsed_sec: float) -> Tuple[GuardResult, Dict[str, object]]:
    max_seen_rss = float("-inf")
    max_seen_elapsed = float("-inf")
    row_count = 0
    parse_errors: List[str] = []

    with run_summary_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        missing_cols = sorted(REQUIRED_RUN_SUMMARY_COLUMNS - fieldnames)
        if missing_cols:
            raise PackContractMismatch(
                expected=[f"run_summary columns include {','.join(sorted(REQUIRED_RUN_SUMMARY_COLUMNS))}"],
                found=[f"run_summary columns={','.join(sorted(fieldnames))}"],
                detail=f"run_summary_missing_columns={','.join(missing_cols)}",
            )

        for i, row in enumerate(reader, start=2):
            row_count += 1
            sym = (row.get("symbol") or "").strip()
            rss_raw = (row.get("max_rss_kb") or "").strip()
            elapsed_raw = (row.get("elapsed_sec") or "").strip()
            try:
                rss_val = float(rss_raw)
            except ValueError:
                parse_errors.append(f"line={i} symbol={sym} invalid_max_rss_kb={rss_raw}")
                continue
            try:
                elapsed_val = float(elapsed_raw)
            except ValueError:
                parse_errors.append(f"line={i} symbol={sym} invalid_elapsed_sec={elapsed_raw}")
                continue
            max_seen_rss = max(max_seen_rss, rss_val)
            max_seen_elapsed = max(max_seen_elapsed, elapsed_val)

    if row_count == 0:
        parse_errors.append("run_summary has no data rows")

    if max_seen_rss == float("-inf"):
        max_seen_rss = 0.0
    if max_seen_elapsed == float("-inf"):
        max_seen_elapsed = 0.0

    passed = (
        len(parse_errors) == 0
        and max_seen_rss <= max_rss_kb
        and max_seen_elapsed <= max_elapsed_sec
    )
    observed = f"max_rss_kb={max_seen_rss:.3f};max_elapsed_sec={max_seen_elapsed:.3f};parse_errors={len(parse_errors)}"
    threshold = f"max_rss_kb<={max_rss_kb:.3f};max_elapsed_sec<={max_elapsed_sec:.3f}"
    detail = parse_errors[0] if parse_errors else "resource limits satisfied"
    result = GuardResult(
        guard_id="G3_RESOURCE",
        status="PASS" if passed else "FAIL",
        observed=observed,
        threshold=threshold,
        detail=detail,
    )
    return result, {
        "row_count": row_count,
        "max_rss_kb": max_seen_rss,
        "max_elapsed_sec": max_seen_elapsed,
        "parse_errors": parse_errors[:50],
    }


def write_reports(
    out_dir: Path,
    pack: Path,
    final_decision: str,
    guard_results: List[GuardResult],
    details: Dict[str, object],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    txt_path = out_dir / "decision_report.txt"
    tsv_path = out_dir / "decision_report.tsv"
    json_path = out_dir / "guard_details.json"

    fail_reasons = [f"{g.guard_id}:{g.detail}" for g in guard_results if g.status != "PASS"]

    txt_lines = [
        f"decision={final_decision}",
        f"pack={pack}",
        f"out_dir={out_dir}",
        f"guard_count={len(guard_results)}",
    ]
    for g in guard_results:
        txt_lines.append(
            f"{g.guard_id}={g.status} observed=[{g.observed}] threshold=[{g.threshold}] detail=[{g.detail}]"
        )
    txt_lines.append("fail_reasons=" + ("|".join(fail_reasons) if fail_reasons else "NONE"))
    txt_lines.append(
        "resolved_paths="
        + "|".join(
            [
                f"sha_verify={details['resolved_paths']['sha_verify']}",
                f"campaign_meta={details['resolved_paths']['campaign_meta']}",
                f"run_summary={details['resolved_paths']['run_summary']}",
                f"determinism_count={details['resolved_paths']['determinism_count']}",
            ]
        )
    )
    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")

    with tsv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["guard_id", "status", "observed", "threshold", "detail"])
        for g in guard_results:
            w.writerow([g.guard_id, g.status, g.observed, g.threshold, g.detail])
        w.writerow(
            [
                "FINAL_DECISION",
                "PASS" if final_decision == "PROMOTE" else "FAIL",
                final_decision,
                "all_guards=PASS",
                "advisory_only",
            ]
        )

    json_path.write_text(
        json.dumps(details, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    pack = Path(args.pack).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (pack / "guards")

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
    g2, g2_detail = eval_g2_determinism(contract["determinism_paths"], float(args.pass_ratio))
    try:
        g3, g3_detail = eval_g3_resource(
            contract["run_summary"],
            float(args.max_rss_kb),
            float(args.max_elapsed_sec),
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

    details = {
        "advisory_only": True,
        "decision": final_decision,
        "config": {
            "pack": str(pack),
            "out_dir": str(out_dir),
            "pass_ratio": float(args.pass_ratio),
            "max_rss_kb": float(args.max_rss_kb),
            "max_elapsed_sec": float(args.max_elapsed_sec),
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
        "final": {
            "all_passed": all(g.status == "PASS" for g in guards),
            "guard_statuses": {g.guard_id: g.status for g in guards},
        },
    }

    write_reports(out_dir, pack, final_decision, guards, details)

    print(f"decision={final_decision}")
    print(f"report_txt={out_dir / 'decision_report.txt'}")
    print(f"report_tsv={out_dir / 'decision_report.tsv'}")
    print(f"report_json={out_dir / 'guard_details.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
