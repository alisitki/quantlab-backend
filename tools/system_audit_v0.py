#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path("/home/deploy/quantlab-backend").resolve()
OUTPUT_DIR = ROOT / "tools/system_audit_output"
AUDIT_REPORT_PATH = OUTPUT_DIR / "audit_report.json"
CLEANUP_PLAN_PATH = OUTPUT_DIR / "cleanup_plan.json"
COMPACT_STATE_SNAPSHOT_PATH = OUTPUT_DIR / "compacted_state_snapshot.json"

CANONICAL_REGISTRY_PATH = ROOT / "tools/system_state/canonical_truth_registry_v0.json"
PHASE5_STATE_DIR = ROOT / "tools/phase5_state"
PHASE6_STATE_DIR = ROOT / "tools/phase6_state"
SHADOW_STATE_DIR = ROOT / "tools/shadow_state"

EVIDENCE_ROOT = ROOT / "evidence"
EVIDENCE_ARCHIVE_ROOT = Path("/home/deploy/quantlab-evidence-archive").resolve()
SHADOW_CAMPAIGNS_ROOT = ROOT / "tools/shadow_state/campaigns"

DEFAULT_KEEP_N = int(os.environ.get("EVIDENCE_KEEP_N", "50"))
DEFAULT_S3_TOOL = Path("/tmp/s3_compact_tool.py")
DEFAULT_S3_BUCKET = "quantlab-compact"
DEFAULT_S3_KEY = "compacted/_state.json"
LOCAL_ARTIFACT_ROOTS = (
    EVIDENCE_ROOT,
    EVIDENCE_ARCHIVE_ROOT,
    SHADOW_CAMPAIGNS_ROOT,
)
STATE_SCAN_SUFFIXES = {".json", ".jsonl", ".tsv", ".txt", ".md"}
PATH_REF_RE = re.compile(
    r"/home/deploy/(?:quantlab-backend/evidence|quantlab-evidence-archive|quantlab-backend/tools/shadow_state/campaigns)[^\s\"'<>]+"
)


@dataclass
class InventoryUnit:
    path: str
    root: str
    kind: str
    bytes: int
    file_count: int
    mtime_utc: str | None
    complete_triple: bool | None = None
    parts: list[str] | None = None
    extra: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "path": self.path,
            "root": self.root,
            "kind": self.kind,
            "bytes": self.bytes,
            "file_count": self.file_count,
            "mtime_utc": self.mtime_utc,
        }
        if self.complete_triple is not None:
            payload["complete_triple"] = self.complete_triple
        if self.parts is not None:
            payload["parts"] = self.parts
        if self.extra:
            payload.update(self.extra)
        return payload


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_ts(raw: str | None) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_compact_date(raw: str | None) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.strptime(raw.strip(), "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def age_hours(raw: str | None, *, now: datetime) -> float | None:
    dt = parse_iso_ts(raw)
    if dt is None:
        return None
    return round((now - dt.astimezone(timezone.utc)).total_seconds() / 3600.0, 3)


def load_json(path: Path) -> tuple[Any | None, str | None]:
    if not path.exists():
        return None, f"missing:{path}"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as exc:  # pragma: no cover - defensive
        return None, f"json_error:{path}:{exc}"


def safe_stat(path: Path) -> os.stat_result | None:
    try:
        return path.stat()
    except FileNotFoundError:
        return None


def stat_iso(path: Path) -> str | None:
    st = safe_stat(path)
    if st is None:
        return None
    return iso_utc(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))


def run_command(cmd: list[str], *, timeout: int = 300) -> dict[str, Any]:
    started = utc_now()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return {
            "command": cmd,
            "started_ts_utc": iso_utc(started),
            "finished_ts_utc": iso_utc(utc_now()),
            "exit_code": int(result.returncode),
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": cmd,
            "started_ts_utc": iso_utc(started),
            "finished_ts_utc": iso_utc(utc_now()),
            "exit_code": 124,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or f"timeout:{timeout}s",
        }


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def find_latest(pattern: str) -> Path | None:
    matches = list(ROOT.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def trim_path_token(raw: str) -> str:
    return raw.rstrip(",.;:)]}")


def summarize_bighunt_index(path: Path) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    plan_latest = data.get("plan_latest") or {}
    latest_updated = None
    latest_created = None
    for item in plan_latest.values():
        updated = parse_iso_ts(item.get("updated_ts_utc"))
        created = parse_iso_ts(item.get("created_ts_utc"))
        if updated and (latest_updated is None or updated > latest_updated):
            latest_updated = updated
        if created and (latest_created is None or created > latest_created):
            latest_created = created
    return {
        "path": str(path),
        "record_count": int(data.get("record_count", 0) or 0),
        "status_counts": data.get("by_status") or {},
        "latest_updated_ts_utc": iso_utc(latest_updated),
        "latest_created_ts_utc": iso_utc(latest_created),
    }


def summarize_promotion_index(path: Path) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    return {
        "path": str(path),
        "record_count": int(data.get("record_count", 0) or 0),
        "promote_pack_count": len(data.get("promote_pack_ids") or []),
        "promote_strong_pack_count": len(data.get("promote_strong_pack_ids") or []),
        "mtime_utc": stat_iso(path),
    }


def summarize_candidate_review(path: Path, *, now: datetime) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    top = (data.get("top_candidates") or [{}])[0]
    return {
        "path": str(path),
        "generated_ts_utc": data.get("generated_ts_utc"),
        "age_hours": age_hours(data.get("generated_ts_utc"), now=now),
        "record_count": int(data.get("record_count", 0) or 0),
        "class_counts": data.get("class_counts") or {},
        "top_candidate": {
            "pack_id": top.get("pack_id"),
            "score": top.get("score"),
            "review_class": top.get("review_class"),
            "class_priority": top.get("class_priority"),
        },
    }


def summarize_shadow_refresh(path: Path, *, now: datetime) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    steps = data.get("steps") or []
    status_counts = Counter(str(step.get("status", "UNKNOWN")) for step in steps)
    return {
        "path": str(path),
        "generated_ts_utc": data.get("generated_ts_utc"),
        "age_hours": age_hours(data.get("generated_ts_utc"), now=now),
        "sync_ok": bool(data.get("sync_ok")),
        "skip_candidate_review": bool(data.get("skip_candidate_review")),
        "failed_step": data.get("failed_step") or "",
        "status_counts": dict(status_counts),
    }


def summarize_watchlist(path: Path, *, now: datetime) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    return {
        "path": str(path),
        "generated_ts_utc": data.get("generated_ts_utc"),
        "age_hours": age_hours(data.get("generated_ts_utc"), now=now),
        "items_count": len(data.get("items") or []),
        "source": data.get("source"),
        "source_binding_artifact_json": data.get("source_binding_artifact_json"),
    }


def summarize_runtime_binding(path: Path, *, now: datetime) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    return {
        "path": str(path),
        "generated_ts_utc": data.get("generated_ts_utc"),
        "age_hours": age_hours(data.get("generated_ts_utc"), now=now),
        "source_row_count": int(data.get("source_row_count", 0) or 0),
        "items_count": len(data.get("items") or []),
        "source_candidate_strategy_contract_json": data.get("source_candidate_strategy_contract_json"),
    }


def summarize_contract(path: Path, *, now: datetime) -> dict[str, Any]:
    data, error = load_json(path)
    if error:
        return {"path": str(path), "error": error}
    return {
        "path": str(path),
        "generated_ts_utc": data.get("generated_ts_utc"),
        "age_hours": age_hours(data.get("generated_ts_utc"), now=now),
        "source_row_count": int(data.get("source_row_count", 0) or 0),
        "items_count": len(data.get("items") or []),
        "source_candidate_review_tsv": data.get("source_candidate_review_tsv"),
    }


def summarize_latest_orchestrator(path: Path | None, *, now: datetime) -> dict[str, Any]:
    if path is None:
        return {"path": None, "status": "MISSING"}
    data, error = load_json(path)
    if error:
        return {"path": str(path), "status": "LOAD_ERROR", "error": error}
    return {
        "path": str(path),
        "status": data.get("status"),
        "started_ts_utc": data.get("started_ts_utc"),
        "finished_ts_utc": data.get("finished_ts_utc"),
        "age_hours": age_hours(data.get("finished_ts_utc"), now=now),
        "inventory_refresh": data.get("inventory_refresh") or {},
        "planner": data.get("planner") or {},
        "scheduler": data.get("scheduler") or {},
        "candidate": data.get("candidate") or {},
        "shadow_refresh": data.get("shadow_refresh") or {},
    }


def summarize_latest_bighunt_report(path: Path | None, *, now: datetime) -> dict[str, Any]:
    if path is None:
        return {"path": None, "status": "MISSING"}
    data, error = load_json(path)
    if error:
        return {"path": str(path), "status": "LOAD_ERROR", "error": error}
    processed = data.get("processed") or []
    final_status_counts = Counter(str(row.get("final_status", "UNKNOWN")) for row in processed)
    decision_counts = Counter(str(row.get("decision", "UNKNOWN")) for row in processed)
    return {
        "path": str(path),
        "started_ts_utc": data.get("started_ts_utc"),
        "finished_ts_utc": data.get("finished_ts_utc"),
        "age_hours": age_hours(data.get("finished_ts_utc"), now=now),
        "processed_count": len(processed),
        "final_status_counts": dict(final_status_counts),
        "decision_counts": dict(decision_counts),
        "sample_processed": processed[:5],
    }


def fetch_compacted_state(s3_tool: Path) -> tuple[dict[str, Any], Any | None]:
    if not s3_tool.exists():
        return {
            "status": "MISSING_S3_TOOL",
            "tool_path": str(s3_tool),
            "exit_code": 127,
        }, None
    result = run_command(
        [
            sys.executable,
            str(s3_tool),
            "get",
            DEFAULT_S3_BUCKET,
            DEFAULT_S3_KEY,
            str(COMPACT_STATE_SNAPSHOT_PATH),
        ],
        timeout=300,
    )
    if result["stdout"]:
        write_text(OUTPUT_DIR / "fetch_compacted_state.stdout.log", result["stdout"])
    if result["stderr"]:
        write_text(OUTPUT_DIR / "fetch_compacted_state.stderr.log", result["stderr"])
    if int(result["exit_code"]) != 0:
        return {
            "status": "FETCH_FAILED",
            "tool_path": str(s3_tool),
            "bucket": DEFAULT_S3_BUCKET,
            "key": DEFAULT_S3_KEY,
            "command": result["command"],
            "exit_code": result["exit_code"],
            "stdout_log": str((OUTPUT_DIR / "fetch_compacted_state.stdout.log").resolve()),
            "stderr_log": str((OUTPUT_DIR / "fetch_compacted_state.stderr.log").resolve()),
        }, None
    data, error = load_json(COMPACT_STATE_SNAPSHOT_PATH)
    if error:
        return {
            "status": "FETCH_INVALID_JSON",
            "tool_path": str(s3_tool),
            "bucket": DEFAULT_S3_BUCKET,
            "key": DEFAULT_S3_KEY,
            "command": result["command"],
            "exit_code": result["exit_code"],
            "error": error,
            "snapshot_path": str(COMPACT_STATE_SNAPSHOT_PATH),
        }, None
    return {
        "status": "OK",
        "tool_path": str(s3_tool),
        "bucket": DEFAULT_S3_BUCKET,
        "key": DEFAULT_S3_KEY,
        "command": result["command"],
        "exit_code": result["exit_code"],
        "snapshot_path": str(COMPACT_STATE_SNAPSHOT_PATH),
        "last_compacted_date": data.get("last_compacted_date"),
        "updated_at": data.get("updated_at"),
        "days_count": len(data.get("days") or {}),
        "partitions_count": len(data.get("partitions") or {}),
    }, data


def relative_root_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def dir_stats(path: Path) -> tuple[int, int]:
    total_bytes = 0
    file_count = 0
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            p = Path(dirpath) / filename
            try:
                st = p.stat()
            except FileNotFoundError:
                continue
            total_bytes += st.st_size
            file_count += 1
    return total_bytes, file_count


def inventory_repo_evidence() -> list[InventoryUnit]:
    items: list[InventoryUnit] = []
    if not EVIDENCE_ROOT.exists():
        return items
    triple_groups: dict[str, dict[str, Any]] = {}
    for entry in os.scandir(EVIDENCE_ROOT):
        path = Path(entry.path)
        st = safe_stat(path)
        if st is None:
            continue
        mtime = iso_utc(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))
        if entry.is_dir(follow_symlinks=False):
            bytes_total, file_count = dir_stats(path)
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(EVIDENCE_ROOT),
                    kind="unpacked_dir",
                    bytes=bytes_total,
                    file_count=file_count,
                    mtime_utc=mtime,
                )
            )
            continue
        name = path.name
        base_path: Path | None = None
        part_kind: str | None = None
        if name.endswith(".tar.gz"):
            base_path = EVIDENCE_ROOT / name[: -len(".tar.gz")]
            part_kind = "tar.gz"
        elif name.endswith(".tar.gz.sha256"):
            base_path = EVIDENCE_ROOT / name[: -len(".tar.gz.sha256")]
            part_kind = "tar.gz.sha256"
        elif name.endswith(".moved_to.txt"):
            base_path = EVIDENCE_ROOT / name[: -len(".moved_to.txt")]
            part_kind = "moved_to.txt"
        if base_path is None:
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(EVIDENCE_ROOT),
                    kind="file",
                    bytes=st.st_size,
                    file_count=1,
                    mtime_utc=mtime,
                )
            )
            continue
        rec = triple_groups.setdefault(
            str(base_path),
            {
                "path": str(base_path),
                "root": str(EVIDENCE_ROOT),
                "kind": "slim_triple",
                "bytes": 0,
                "file_count": 0,
                "mtime_utc": mtime,
                "parts": set(),
            },
        )
        rec["bytes"] += st.st_size
        rec["file_count"] += 1
        rec["parts"].add(part_kind)
        if mtime and (rec["mtime_utc"] is None or mtime > rec["mtime_utc"]):
            rec["mtime_utc"] = mtime
    for rec in triple_groups.values():
        parts = sorted(rec["parts"])
        items.append(
            InventoryUnit(
                path=rec["path"],
                root=rec["root"],
                kind=rec["kind"],
                bytes=rec["bytes"],
                file_count=rec["file_count"],
                mtime_utc=rec["mtime_utc"],
                complete_triple=set(parts) == {"moved_to.txt", "tar.gz", "tar.gz.sha256"},
                parts=parts,
            )
        )
    items.sort(key=lambda item: (item.root, item.path))
    return items


def inventory_archive_dates() -> list[InventoryUnit]:
    items: list[InventoryUnit] = []
    if not EVIDENCE_ARCHIVE_ROOT.exists():
        return items
    for entry in os.scandir(EVIDENCE_ARCHIVE_ROOT):
        path = Path(entry.path)
        st = safe_stat(path)
        if st is None:
            continue
        mtime = iso_utc(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))
        if entry.is_dir(follow_symlinks=False):
            bytes_total, file_count = dir_stats(path)
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(EVIDENCE_ARCHIVE_ROOT),
                    kind="archive_bucket",
                    bytes=bytes_total,
                    file_count=file_count,
                    mtime_utc=mtime,
                )
            )
        else:
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(EVIDENCE_ARCHIVE_ROOT),
                    kind="archive_file",
                    bytes=st.st_size,
                    file_count=1,
                    mtime_utc=mtime,
                )
            )
    items.sort(key=lambda item: item.path)
    return items


def inventory_shadow_campaigns() -> list[InventoryUnit]:
    items: list[InventoryUnit] = []
    if not SHADOW_CAMPAIGNS_ROOT.exists():
        return items
    for entry in os.scandir(SHADOW_CAMPAIGNS_ROOT):
        path = Path(entry.path)
        st = safe_stat(path)
        if st is None:
            continue
        mtime = iso_utc(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))
        if entry.is_dir(follow_symlinks=False):
            bytes_total, file_count = dir_stats(path)
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(SHADOW_CAMPAIGNS_ROOT),
                    kind="shadow_campaign",
                    bytes=bytes_total,
                    file_count=file_count,
                    mtime_utc=mtime,
                )
            )
        else:
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(SHADOW_CAMPAIGNS_ROOT),
                    kind="shadow_campaign_file",
                    bytes=st.st_size,
                    file_count=1,
                    mtime_utc=mtime,
                )
            )
    items.sort(key=lambda item: item.path)
    return items


def inventory_audit_output() -> list[InventoryUnit]:
    items: list[InventoryUnit] = []
    if not OUTPUT_DIR.exists():
        return items
    for entry in os.scandir(OUTPUT_DIR):
        path = Path(entry.path)
        if path in {AUDIT_REPORT_PATH, CLEANUP_PLAN_PATH}:
            continue
        st = safe_stat(path)
        if st is None:
            continue
        mtime = iso_utc(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))
        if entry.is_dir(follow_symlinks=False):
            bytes_total, file_count = dir_stats(path)
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(OUTPUT_DIR),
                    kind="audit_output_dir",
                    bytes=bytes_total,
                    file_count=file_count,
                    mtime_utc=mtime,
                )
            )
        else:
            items.append(
                InventoryUnit(
                    path=str(path),
                    root=str(OUTPUT_DIR),
                    kind="audit_output_file",
                    bytes=st.st_size,
                    file_count=1,
                    mtime_utc=mtime,
                )
            )
    items.sort(key=lambda item: item.path)
    return items


def scan_state_references() -> dict[str, Any]:
    refs_by_unit: dict[str, list[dict[str, str]]] = defaultdict(list)
    state_files_with_refs = 0
    scan_roots = [
        ROOT / "tools/system_state",
        ROOT / "tools/phase5_state",
        ROOT / "tools/phase6_state",
        ROOT / "tools/shadow_state",
    ]
    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(scan_root):
            current = Path(dirpath)
            if current == SHADOW_CAMPAIGNS_ROOT:
                dirnames[:] = []
                continue
            if current == OUTPUT_DIR:
                dirnames[:] = []
                continue
            dirnames[:] = [
                name
                for name in dirnames
                if (current / name) not in {SHADOW_CAMPAIGNS_ROOT, OUTPUT_DIR}
            ]
            for filename in filenames:
                path = current / filename
                if path.suffix.lower() not in STATE_SCAN_SUFFIXES:
                    continue
                try:
                    text = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                matches = []
                seen_raw = set()
                for match in PATH_REF_RE.finditer(text):
                    raw = trim_path_token(match.group(0))
                    if raw in seen_raw:
                        continue
                    seen_raw.add(raw)
                    unit = artifact_unit_for_reference(raw)
                    matches.append((raw, unit))
                if not matches:
                    continue
                state_files_with_refs += 1
                for raw, unit in matches:
                    if unit is None:
                        continue
                    refs_by_unit[unit].append(
                        {
                            "state_file": str(path),
                            "raw_path": raw,
                        }
                    )
    return {
        "refs_by_unit": refs_by_unit,
        "state_files_with_refs": state_files_with_refs,
    }


def artifact_unit_for_reference(raw: str) -> str | None:
    try:
        path = Path(raw)
    except Exception:
        return None
    for root in LOCAL_ARTIFACT_ROOTS:
        try:
            rel = path.relative_to(root)
        except ValueError:
            continue
        if root == EVIDENCE_ROOT:
            if not rel.parts:
                return str(root)
            first = rel.parts[0]
            return str(root / normalize_evidence_unit_name(first))
        if root == EVIDENCE_ARCHIVE_ROOT:
            if not rel.parts:
                return str(root)
            return str(root / rel.parts[0])
        if root == SHADOW_CAMPAIGNS_ROOT:
            if not rel.parts:
                return str(root)
            return str(root / rel.parts[0])
    return None


def normalize_evidence_unit_name(name: str) -> str:
    if name.endswith(".tar.gz"):
        return name[: -len(".tar.gz")]
    if name.endswith(".tar.gz.sha256"):
        return name[: -len(".tar.gz.sha256")]
    if name.endswith(".moved_to.txt"):
        return name[: -len(".moved_to.txt")]
    return name


def parse_keep_last_dry_run() -> dict[str, Any]:
    result = run_command([str(ROOT / "tools/prune_evidence_keep_last.sh"), "--dry-run"], timeout=600)
    stdout_path = OUTPUT_DIR / "prune_keep_last_dry_run.txt"
    stderr_path = OUTPUT_DIR / "prune_keep_last_dry_run.err"
    write_text(stdout_path, result["stdout"])
    write_text(stderr_path, result["stderr"])
    delete_prefixes: list[str] = []
    summary: dict[str, int] = {}
    for line in result["stdout"].splitlines():
        if line.startswith("DELETE_SLIM_PREFIX "):
            delete_prefixes.append(line.split(" ", 1)[1].strip())
        elif "=" in line:
            key, value = line.split("=", 1)
            if key in {"slim_triples_kept", "slim_triples_deleted", "skipped_incomplete"}:
                try:
                    summary[key] = int(value)
                except ValueError:
                    summary[key] = -1
    return {
        "command": result["command"],
        "exit_code": result["exit_code"],
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "delete_prefixes": delete_prefixes,
        "summary": summary,
    }


def parse_unpacked_dry_run() -> dict[str, Any]:
    result = run_command([str(ROOT / "tools/prune_evidence_unpacked.sh"), "--dry-run"], timeout=600)
    stdout_path = OUTPUT_DIR / "prune_unpacked_dry_run.txt"
    stderr_path = OUTPUT_DIR / "prune_unpacked_dry_run.err"
    write_text(stdout_path, result["stdout"])
    write_text(stderr_path, result["stderr"])
    delete_dirs: list[str] = []
    orphan_dirs: list[str] = []
    summary: dict[str, int] = {}
    for line in result["stdout"].splitlines():
        if line.startswith("DELETE_UNPACKED_CANDIDATE "):
            delete_dirs.append(line.split(" ", 1)[1].strip())
        elif line.startswith("ORPHAN_UNPACKED "):
            orphan_dirs.append(line.split(" ", 1)[1].strip())
        elif "=" in line:
            key, value = line.split("=", 1)
            if key in {"deleted_dirs", "orphan_dirs"}:
                try:
                    summary[key] = int(value)
                except ValueError:
                    summary[key] = -1
    return {
        "command": result["command"],
        "exit_code": result["exit_code"],
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "delete_dirs": delete_dirs,
        "orphan_dirs": orphan_dirs,
        "summary": summary,
    }


def classification_entry(unit: InventoryUnit, action: str, reason: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = unit.to_dict()
    payload["action"] = action
    payload["reason"] = reason
    if details:
        payload["details"] = details
    return payload


def classify_units(
    *,
    repo_units: list[InventoryUnit],
    archive_units: list[InventoryUnit],
    campaign_units: list[InventoryUnit],
    audit_units: list[InventoryUnit],
    refs_by_unit: dict[str, list[dict[str, str]]],
    keep_last: dict[str, Any],
    unpacked: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    plan: dict[str, list[dict[str, Any]]] = {
        "KEEP": [],
        "ARCHIVE": [],
        "DELETE": [],
        "UNKNOWN": [],
    }
    keep_last_delete = set(keep_last["delete_prefixes"])
    unpacked_delete = set(unpacked["delete_dirs"])
    orphan_dirs = set(unpacked["orphan_dirs"])
    unpacked_blocked = int(unpacked["exit_code"]) != 0 and bool(orphan_dirs)

    for unit in repo_units:
        ref_details = refs_by_unit.get(unit.path, [])
        if ref_details:
            plan["KEEP"].append(
                classification_entry(
                    unit,
                    "KEEP",
                    "state_referenced_local_evidence",
                    details={
                        "reference_count": len(ref_details),
                        "sample_references": ref_details[:5],
                    },
                )
            )
            continue
        if unit.kind == "slim_triple":
            if not unit.complete_triple:
                plan["UNKNOWN"].append(
                    classification_entry(unit, "UNKNOWN", "incomplete_slim_triple")
                )
            elif unit.path in keep_last_delete:
                plan["DELETE"].append(
                    classification_entry(unit, "DELETE", "repo_keep_last_dry_run_delete_safe")
                )
            else:
                plan["KEEP"].append(
                    classification_entry(unit, "KEEP", "repo_keep_last_window")
                )
            continue
        if unit.kind == "unpacked_dir":
            if unit.path in orphan_dirs:
                plan["ARCHIVE"].append(
                    classification_entry(unit, "ARCHIVE", "orphan_unpacked_needs_packaging")
                )
            elif unit.path in unpacked_delete and not unpacked_blocked:
                plan["DELETE"].append(
                    classification_entry(unit, "DELETE", "repo_unpacked_dry_run_delete_safe")
                )
            elif unpacked_blocked:
                plan["UNKNOWN"].append(
                    classification_entry(unit, "UNKNOWN", "unpacked_cleanup_blocked_by_orphans")
                )
            else:
                plan["KEEP"].append(
                    classification_entry(unit, "KEEP", "repo_unpacked_not_selected")
                )
            continue
        plan["KEEP"].append(classification_entry(unit, "KEEP", "repo_misc_artifact"))

    for unit in archive_units:
        ref_details = refs_by_unit.get(unit.path, [])
        if ref_details:
            plan["KEEP"].append(
                classification_entry(
                    unit,
                    "KEEP",
                    "archive_bucket_contains_state_references",
                    details={
                        "reference_count": len(ref_details),
                        "sample_references": ref_details[:5],
                    },
                )
            )
        else:
            plan["ARCHIVE"].append(
                classification_entry(unit, "ARCHIVE", "already_archived_bucket")
            )

    for unit in campaign_units:
        ref_details = refs_by_unit.get(unit.path, [])
        if ref_details:
            plan["KEEP"].append(
                classification_entry(
                    unit,
                    "KEEP",
                    "campaign_state_referenced",
                    details={
                        "reference_count": len(ref_details),
                        "sample_references": ref_details[:5],
                    },
                )
            )
        else:
            plan["ARCHIVE"].append(
                classification_entry(
                    unit,
                    "ARCHIVE",
                    "historical_shadow_campaign_not_in_active_path",
                )
            )

    for unit in audit_units:
        plan["KEEP"].append(classification_entry(unit, "KEEP", "current_audit_artifact"))

    for action in plan:
        plan[action].sort(key=lambda item: (item["root"], item["path"]))
    return plan


def summarize_classification(plan: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for action, items in plan.items():
        summary[action] = {
            "count": len(items),
            "bytes": sum(int(item["bytes"]) for item in items),
            "gib": round(sum(int(item["bytes"]) for item in items) / (1024**3), 3),
        }
    return summary


def truth_sources(now: datetime) -> dict[str, Any]:
    registry, registry_error = load_json(CANONICAL_REGISTRY_PATH)
    sources = [
        {
            "concept": "compacted_state",
            "authoritative": "s3://quantlab-compact/compacted/_state.json",
            "local_snapshot": str(COMPACT_STATE_SNAPSHOT_PATH),
            "confidence": "HIGH",
        },
        {
            "concept": "candidate_review",
            "authoritative": str((PHASE6_STATE_DIR / "candidate_review_v2.json").resolve()),
            "confidence": "HIGH",
        },
        {
            "concept": "promotion_index",
            "authoritative": str((PHASE6_STATE_DIR / "promotion_index.json").resolve()),
            "confidence": "MEDIUM",
        },
        {
            "concept": "active_shadow_subset",
            "authoritative": str((SHADOW_STATE_DIR / "shadow_watchlist_v0.json").resolve()),
            "confidence": "HIGH",
        },
        {
            "concept": "shadow_refresh_status",
            "authoritative": str((SHADOW_STATE_DIR / "shadow_derived_surface_refresh_v0.json").resolve()),
            "confidence": "HIGH",
        },
        {
            "concept": "runtime_binding",
            "authoritative": str((PHASE6_STATE_DIR / "candidate_strategy_runtime_binding_v0.json").resolve()),
            "confidence": "HIGH",
        },
    ]
    return {
        "canonical_registry_path": str(CANONICAL_REGISTRY_PATH),
        "canonical_registry_generated_ts_utc": registry.get("generated_ts_utc") if isinstance(registry, dict) else None,
        "canonical_registry_age_hours": age_hours(registry.get("generated_ts_utc") if isinstance(registry, dict) else None, now=now),
        "canonical_registry_error": registry_error,
        "active_paths": registry.get("active_paths") if isinstance(registry, dict) else None,
        "sources": sources,
    }


def freshness_warnings(
    *,
    candidate: dict[str, Any],
    contract: dict[str, Any],
    runtime_binding: dict[str, Any],
    watchlist: dict[str, Any],
    compact_state: dict[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    candidate_ts = parse_iso_ts(candidate.get("generated_ts_utc"))
    contract_ts = parse_iso_ts(contract.get("generated_ts_utc"))
    binding_ts = parse_iso_ts(runtime_binding.get("generated_ts_utc"))
    watchlist_ts = parse_iso_ts(watchlist.get("generated_ts_utc"))
    if candidate_ts and binding_ts and candidate_ts > binding_ts + timedelta(hours=24):
        warnings.append(
            {
                "code": "STALE_RUNTIME_BINDING",
                "severity": "WARN",
                "candidate_review_generated_ts_utc": candidate.get("generated_ts_utc"),
                "runtime_binding_generated_ts_utc": runtime_binding.get("generated_ts_utc"),
                "runtime_binding_age_hours": runtime_binding.get("age_hours"),
                "note": "Shadow watchlist refresh is newer than runtime binding; binding artifact may lag current candidate review.",
            }
        )
    if candidate_ts and contract_ts and candidate_ts > contract_ts + timedelta(hours=24):
        warnings.append(
            {
                "code": "STALE_CANDIDATE_CONTRACT",
                "severity": "WARN",
                "candidate_review_generated_ts_utc": candidate.get("generated_ts_utc"),
                "candidate_contract_generated_ts_utc": contract.get("generated_ts_utc"),
                "candidate_contract_age_hours": contract.get("age_hours"),
            }
        )
    if watchlist_ts and binding_ts and watchlist_ts > binding_ts + timedelta(hours=24):
        warnings.append(
            {
                "code": "WATCHLIST_BINDING_EPOCH_MISMATCH",
                "severity": "WARN",
                "watchlist_generated_ts_utc": watchlist.get("generated_ts_utc"),
                "runtime_binding_generated_ts_utc": runtime_binding.get("generated_ts_utc"),
            }
        )
    last_compacted = parse_compact_date(compact_state.get("last_compacted_date"))
    expected = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if last_compacted and last_compacted < expected:
        warnings.append(
            {
                "code": "COMPACTED_STATE_STALE",
                "severity": "FAIL",
                "last_compacted_date": compact_state.get("last_compacted_date"),
                "expected_at_least": expected.strftime("%Y%m%d"),
            }
        )
    return warnings


def pipeline_health(
    *,
    now: datetime,
    compact_state: dict[str, Any],
    orchestrator: dict[str, Any],
    bighunt_report: dict[str, Any],
    candidate: dict[str, Any],
    shadow_refresh: dict[str, Any],
    watchlist: dict[str, Any],
    runtime_binding: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")
    last_compacted = compact_state.get("last_compacted_date")
    compact_fresh = isinstance(last_compacted, str) and last_compacted >= yesterday
    return {
        "phase5_orchestrator": {
            "status": orchestrator.get("status"),
            "path": orchestrator.get("path"),
            "finished_ts_utc": orchestrator.get("finished_ts_utc"),
            "age_hours": orchestrator.get("age_hours"),
            "healthy": orchestrator.get("status") in {"OK", "NOOP_OUTSIDE_ACTIVE_WINDOW"},
        },
        "bighunt_scheduler": {
            "path": bighunt_report.get("path"),
            "finished_ts_utc": bighunt_report.get("finished_ts_utc"),
            "age_hours": bighunt_report.get("age_hours"),
            "processed_count": bighunt_report.get("processed_count"),
            "final_status_counts": bighunt_report.get("final_status_counts"),
            "healthy": bool(bighunt_report.get("processed_count", 0)),
        },
        "candidate_review": {
            "path": candidate.get("path"),
            "generated_ts_utc": candidate.get("generated_ts_utc"),
            "age_hours": candidate.get("age_hours"),
            "record_count": candidate.get("record_count"),
            "healthy": (candidate.get("age_hours") is not None and candidate.get("age_hours") <= 24.0),
        },
        "shadow_refresh": {
            "path": shadow_refresh.get("path"),
            "generated_ts_utc": shadow_refresh.get("generated_ts_utc"),
            "age_hours": shadow_refresh.get("age_hours"),
            "sync_ok": shadow_refresh.get("sync_ok"),
            "healthy": bool(shadow_refresh.get("sync_ok")),
        },
        "shadow_watchlist": {
            "path": watchlist.get("path"),
            "generated_ts_utc": watchlist.get("generated_ts_utc"),
            "age_hours": watchlist.get("age_hours"),
            "items_count": watchlist.get("items_count"),
            "healthy": (watchlist.get("age_hours") is not None and watchlist.get("age_hours") <= 24.0),
        },
        "runtime_binding": {
            "path": runtime_binding.get("path"),
            "generated_ts_utc": runtime_binding.get("generated_ts_utc"),
            "age_hours": runtime_binding.get("age_hours"),
            "source_row_count": runtime_binding.get("source_row_count"),
            "healthy": (runtime_binding.get("age_hours") is not None and runtime_binding.get("age_hours") <= 24.0),
        },
        "candidate_contract": {
            "path": contract.get("path"),
            "generated_ts_utc": contract.get("generated_ts_utc"),
            "age_hours": contract.get("age_hours"),
            "source_row_count": contract.get("source_row_count"),
            "healthy": (contract.get("age_hours") is not None and contract.get("age_hours") <= 24.0),
        },
        "compacted_state": {
            "path": compact_state.get("snapshot_path"),
            "last_compacted_date": last_compacted,
            "updated_at": compact_state.get("updated_at"),
            "expected_yesterday": yesterday,
            "healthy": compact_fresh,
        },
    }


def root_summary(items: list[InventoryUnit]) -> dict[str, Any]:
    total_bytes = sum(item.bytes for item in items)
    return {
        "count": len(items),
        "total_bytes": total_bytes,
        "total_gib": round(total_bytes / (1024**3), 3),
    }


def inventory_summary(
    repo_units: list[InventoryUnit],
    archive_units: list[InventoryUnit],
    campaign_units: list[InventoryUnit],
    audit_units: list[InventoryUnit],
) -> dict[str, Any]:
    return {
        "repo_evidence": root_summary(repo_units),
        "archive_root": root_summary(archive_units),
        "shadow_campaigns": root_summary(campaign_units),
        "audit_outputs": root_summary(audit_units),
    }


def delete_paths_for_unit(item: dict[str, Any]) -> list[str]:
    path = item["path"]
    if item["kind"] == "slim_triple":
        return [
            f"{path}.tar.gz",
            f"{path}.tar.gz.sha256",
            f"{path}.moved_to.txt",
        ]
    return [path]


def apply_delete_plan(delete_items: list[dict[str, Any]]) -> dict[str, Any]:
    removed: list[str] = []
    missing: list[str] = []
    errors: list[dict[str, str]] = []
    for item in delete_items:
        for target in delete_paths_for_unit(item):
            p = Path(target)
            if not p.exists():
                missing.append(target)
                continue
            try:
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()
                removed.append(target)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append({"path": target, "error": str(exc)})
    return {
        "removed_count": len(removed),
        "missing_count": len(missing),
        "error_count": len(errors),
        "removed": removed,
        "missing": missing,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="QuantLab ops audit and cleanup planner")
    parser.add_argument("--apply-delete", action="store_true", help="Delete only items classified as DELETE-safe")
    parser.add_argument("--keep-n", type=int, default=DEFAULT_KEEP_N, help="Expected slim-triple keep count for repo evidence")
    parser.add_argument("--s3-tool", default=str(DEFAULT_S3_TOOL), help="Path to compact S3 helper")
    args = parser.parse_args()

    ensure_output_dir()
    now = utc_now()

    truth = truth_sources(now)
    compact_state_summary, compact_state_data = fetch_compacted_state(Path(args.s3_tool))
    stop_reasons: list[str] = []
    if compact_state_data is None:
        stop_reasons.append("STATE_LOAD_FAILED")

    latest_orchestrator = summarize_latest_orchestrator(
        find_latest("tools/phase5_state/nightly_orchestrator_report_*.json"),
        now=now,
    )
    latest_bighunt = summarize_latest_bighunt_report(
        find_latest("tools/phase5_state/bighunt_batch_report_*.json"),
        now=now,
    )
    bighunt_index = summarize_bighunt_index(PHASE5_STATE_DIR / "bighunt_index.json")
    promotion_index = summarize_promotion_index(PHASE6_STATE_DIR / "promotion_index.json")
    candidate_review = summarize_candidate_review(PHASE6_STATE_DIR / "candidate_review_v2.json", now=now)
    candidate_contract = summarize_contract(PHASE6_STATE_DIR / "candidate_strategy_contract_v0.json", now=now)
    runtime_binding = summarize_runtime_binding(PHASE6_STATE_DIR / "candidate_strategy_runtime_binding_v0.json", now=now)
    shadow_refresh = summarize_shadow_refresh(SHADOW_STATE_DIR / "shadow_derived_surface_refresh_v0.json", now=now)
    shadow_watchlist = summarize_watchlist(SHADOW_STATE_DIR / "shadow_watchlist_v0.json", now=now)

    keep_last = parse_keep_last_dry_run()
    unpacked = parse_unpacked_dry_run()
    refs_scan = scan_state_references()
    refs_by_unit = refs_scan["refs_by_unit"]

    repo_units = inventory_repo_evidence()
    archive_units = inventory_archive_dates()
    campaign_units = inventory_shadow_campaigns()
    audit_units = inventory_audit_output()

    plan = classify_units(
        repo_units=repo_units,
        archive_units=archive_units,
        campaign_units=campaign_units,
        audit_units=audit_units,
        refs_by_unit=refs_by_unit,
        keep_last=keep_last,
        unpacked=unpacked,
    )
    classification_summary = summarize_classification(plan)

    warnings = freshness_warnings(
        candidate=candidate_review,
        contract=candidate_contract,
        runtime_binding=runtime_binding,
        watchlist=shadow_watchlist,
        compact_state=compact_state_summary,
        now=now,
    )
    if any(w["severity"] == "FAIL" for w in warnings):
        stop_reasons.append("CRITICAL_FRESHNESS_FAILURE")

    audit_report = {
        "generated_at": iso_utc(now),
        "status": "BLOCKED" if stop_reasons else "OK",
        "stop_reasons": stop_reasons,
        "truth_sources": truth,
        "pipeline_health": pipeline_health(
            now=now,
            compact_state=compact_state_summary,
            orchestrator=latest_orchestrator,
            bighunt_report=latest_bighunt,
            candidate=candidate_review,
            shadow_refresh=shadow_refresh,
            watchlist=shadow_watchlist,
            runtime_binding=runtime_binding,
            contract=candidate_contract,
        ),
        "state_surfaces": {
            "compacted_state": compact_state_summary,
            "bighunt_index": bighunt_index,
            "promotion_index": promotion_index,
            "candidate_review_v2": candidate_review,
            "candidate_strategy_contract_v0": candidate_contract,
            "candidate_strategy_runtime_binding_v0": runtime_binding,
            "shadow_refresh": shadow_refresh,
            "shadow_watchlist": shadow_watchlist,
        },
        "latest_reports": {
            "nightly_orchestrator": latest_orchestrator,
            "bighunt_batch": latest_bighunt,
        },
        "retention_dry_run": {
            "keep_last": {
                "exit_code": keep_last["exit_code"],
                "summary": keep_last["summary"],
                "stdout_path": keep_last["stdout_path"],
                "stderr_path": keep_last["stderr_path"],
                "delete_candidate_count": len(keep_last["delete_prefixes"]),
                "sample_delete_prefixes": keep_last["delete_prefixes"][:20],
            },
            "unpacked": {
                "exit_code": unpacked["exit_code"],
                "summary": unpacked["summary"],
                "stdout_path": unpacked["stdout_path"],
                "stderr_path": unpacked["stderr_path"],
                "delete_candidate_count": len(unpacked["delete_dirs"]),
                "orphan_count": len(unpacked["orphan_dirs"]),
                "sample_delete_dirs": unpacked["delete_dirs"][:20],
                "sample_orphan_dirs": unpacked["orphan_dirs"][:20],
            },
        },
        "state_reference_scan": {
            "state_files_with_refs": refs_scan["state_files_with_refs"],
            "referenced_unit_count": len(refs_by_unit),
            "sample_referenced_units": [
                {
                    "unit_path": path,
                    "reference_count": len(details),
                    "sample_references": details[:5],
                }
                for path, details in sorted(refs_by_unit.items())[:20]
            ],
        },
        "inventory_summary": inventory_summary(repo_units, archive_units, campaign_units, audit_units),
        "classification_summary": classification_summary,
        "warnings": warnings,
    }

    delete_items = plan["DELETE"]
    cleanup_plan = {
        "generated_at": iso_utc(now),
        "status": "BLOCKED" if stop_reasons else "DRY_RUN_READY",
        "apply_delete_requested": bool(args.apply_delete),
        "stop_reasons": stop_reasons,
        "inventory_granularity": {
            "repo_evidence": "top_level evidence units; slim triples grouped by prefix; unpacked dirs preserved as units",
            "archive_root": "top_level archive date buckets",
            "shadow_campaigns": "top_level campaign directories",
            "audit_outputs": "top_level current audit artifacts",
        },
        "retention_inputs": {
            "expected_keep_n": int(args.keep_n),
            "keep_last_summary": keep_last["summary"],
            "unpacked_summary": unpacked["summary"],
        },
        "summary": classification_summary,
        "KEEP": plan["KEEP"],
        "ARCHIVE": plan["ARCHIVE"],
        "DELETE": delete_items,
        "UNKNOWN": plan["UNKNOWN"],
    }

    if args.apply_delete and not stop_reasons:
        cleanup_plan["apply_result"] = apply_delete_plan(delete_items)
        cleanup_plan["status"] = "APPLIED_DELETE"
    elif args.apply_delete and stop_reasons:
        cleanup_plan["apply_result"] = {
            "removed_count": 0,
            "error_count": 0,
            "missing_count": 0,
            "removed": [],
            "missing": [],
            "errors": [],
            "note": "apply_delete_requested_but_blocked",
        }

    write_text(AUDIT_REPORT_PATH, json.dumps(audit_report, indent=2, sort_keys=True) + "\n")
    write_text(CLEANUP_PLAN_PATH, json.dumps(cleanup_plan, indent=2, sort_keys=True) + "\n")

    print(f"status={audit_report['status']}")
    print(f"audit_report={AUDIT_REPORT_PATH}")
    print(f"cleanup_plan={CLEANUP_PLAN_PATH}")
    print(f"delete_count={len(delete_items)}")
    print(f"archive_count={len(plan['ARCHIVE'])}")
    print(f"keep_count={len(plan['KEEP'])}")
    print(f"unknown_count={len(plan['UNKNOWN'])}")
    if stop_reasons:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
