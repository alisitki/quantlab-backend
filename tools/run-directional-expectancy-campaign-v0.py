#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDING_JSON = ROOT / "tools" / "phase6_state" / "directional_expectancy_runtime_binding_v0.json"
DEFAULT_BASELINE_RESULTS_JSON = (
    ROOT / "tools" / "shadow_state" / "campaigns" / "directional_expectancy_campaign_20260317_same_slice_8h_v2" / "campaign_results.json"
)
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_BATCH_TOOL = ROOT / "tools" / "run-shadow-observation-batch-v0.py"
DEFAULT_CAMPAIGN_ID = f"directional_expectancy_campaign_{datetime.now(timezone.utc).strftime('%Y%m%d')}_24h_v0"
DEFAULT_CAMPAIGN_DIR = ROOT / "tools" / "shadow_state" / "campaigns" / DEFAULT_CAMPAIGN_ID
DEFAULT_SELECTION_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_selection.json"
DEFAULT_LAUNCH_MANIFEST_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_launch_manifest.json"
DEFAULT_RUNTIME_STATUS_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_runtime_status.json"
DEFAULT_RESULTS_JSON = DEFAULT_CAMPAIGN_DIR / "campaign_results.json"
DEFAULT_FINAL_VERDICT_JSON = DEFAULT_CAMPAIGN_DIR / "final_verdict.json"
DEFAULT_ROW_LEADERBOARD_TSV = DEFAULT_CAMPAIGN_DIR / "row_leaderboard.tsv"
DEFAULT_FAMILY_LEADERBOARD_TSV = DEFAULT_CAMPAIGN_DIR / "family_leaderboard.tsv"
DEFAULT_TELEGRAM_REPORTS_JSONL = DEFAULT_CAMPAIGN_DIR / "telegram_reports.jsonl"
DEFAULT_DURATION_SEC = 24 * 60 * 60
DEFAULT_TIMEOUT_SEC = DEFAULT_DURATION_SEC + 300
DEFAULT_HEARTBEAT_MS = 5000
DEFAULT_ROW_COUNT_PER_FAMILY = 5
DEFAULT_POLL_INTERVAL_SEC = 300
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
SCHEMA_SELECTION = "directional_expectancy_campaign_selection_v0"
SCHEMA_MANIFEST = "directional_expectancy_campaign_launch_manifest_v0"
SCHEMA_STATUS = "directional_expectancy_campaign_runtime_status_v0"
SCHEMA_RESULTS = "directional_expectancy_campaign_results_v0"
SCHEMA_FINAL_VERDICT = "directional_expectancy_campaign_final_verdict_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"

CLASS_PRIORITY = {
    "PROMISING": 0,
    "NEUTRAL": 1,
    "WEAK": 2,
    "NO_SIGNAL": 3,
    "INSUFFICIENT_EVIDENCE": 4,
    "BROKEN": 5,
}

DECISION_TIER_PRIORITY = {
    "PROMOTE_STRONG": 0,
    "PROMOTE": 1,
}

FINAL_DECISION_PRIORITY = (
    "PROMISING_SUBSET_FOUND",
    "SHIFT_ATTENTION_TO_BACKUP_FAMILY",
    "MOMENTUM_V1_EARLY_WARNING",
    "NEED_MORE_LONG_RUN_BREADTH",
    "BLOCKED_BY_EXECUTION_EVIDENCE_PATH",
)


class DirectionalExpectancyCampaignError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise DirectionalExpectancyCampaignError(message)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{label}_missing:{path}")
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def resolve_env_file() -> Path:
    override = str(os.environ.get("QUANTLAB_ENV_FILE") or "").strip()
    if override:
        return Path(override).resolve()
    return DEFAULT_ENV_FILE.resolve()


def load_env_defaults_from_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or str(os.environ.get(key) or "").strip():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key in os.environ:
            continue
        os.environ[key] = value


def maybe_parse_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def maybe_parse_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def short_id(value: str, *, tail: int = 10) -> str:
    raw = str(value or "").strip()
    if len(raw) <= tail:
        return raw or "-"
    return f"...{raw[-tail:]}"


def sign_of_position(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def decision_tier_priority(value: str) -> int:
    return DECISION_TIER_PRIORITY.get(str(value or "").strip(), 9)


def family_binding_candidates(binding_payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in list(binding_payload.get("items") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
            continue
        if str(item.get("shadow_tradeability_class") or "").strip() != "DIRECTIONAL":
            continue
        if str(item.get("binding_mode") or "").strip() != "PAPER_DIRECTIONAL_V1":
            continue
        if str(item.get("exchange") or "").strip().lower() != "binance":
            continue
        if str(item.get("stream") or "").strip().lower() != "trade":
            continue
        family_id = str(item.get("family_id") or "").strip()
        if not family_id:
            continue
        grouped[family_id].append(item)
    return grouped


def item_selected_cell(item: dict[str, Any]) -> dict[str, Any]:
    runtime_config = item.get("runtime_strategy_config")
    if not isinstance(runtime_config, dict):
        return {}
    selected_cell = runtime_config.get("selected_cell")
    return selected_cell if isinstance(selected_cell, dict) else {}


def item_metric(item: dict[str, Any], key: str, default: float = 0.0) -> float:
    selected_cell = item_selected_cell(item)
    value = maybe_parse_float(selected_cell.get(key))
    return value if value is not None else default


def item_source_review_priority(item: dict[str, Any]) -> int:
    raw = item.get("source_review_class_priority")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 99


def item_source_review_score(item: dict[str, Any]) -> float:
    value = maybe_parse_float(item.get("source_review_score"))
    return value if value is not None else 0.0


def item_decision_tier(item: dict[str, Any]) -> str:
    return str(item.get("decision_tier") or "").strip()


def threshold_distance(item: dict[str, Any]) -> float:
    family_id = str(item.get("family_id") or "").strip()
    selected_cell = item_selected_cell(item)
    if family_id == "momentum_v1":
        return abs(item_metric(item, "t_stat") - 2.0)
    if family_id == "return_reversal_v1":
        return abs(abs(item_metric(item, "t_stat")) - 2.0)
    if family_id == "jump_reversion_v1":
        return abs(item_metric(item, "t_stat") - 2.0)
    if family_id == "family_b_simple_momentum":
        return abs(abs(item_metric(item, "t_stat")) - 2.0)
    return 0.0


def family_strength_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    family_id = str(item.get("family_id") or "").strip()
    pack_id = str(item.get("pack_id") or "").strip()
    symbol = str(item.get("selected_symbol") or "").strip()
    common = (
        item_source_review_priority(item),
        decision_tier_priority(item_decision_tier(item)),
    )
    if family_id == "momentum_v1":
        return common + (
            -item_metric(item, "mean_product"),
            -item_metric(item, "t_stat"),
            -item_metric(item, "event_count"),
            pack_id,
            symbol,
        )
    if family_id == "return_reversal_v1":
        return common + (
            item_metric(item, "mean_product"),
            item_metric(item, "t_stat"),
            -item_metric(item, "event_count"),
            pack_id,
            symbol,
        )
    if family_id == "jump_reversion_v1":
        return common + (
            -item_metric(item, "mean_signed_reversal"),
            -item_metric(item, "t_stat"),
            -item_metric(item, "jump_count"),
            pack_id,
            symbol,
        )
    if family_id == "family_b_simple_momentum":
        return common + (
            -item_metric(item, "mean_forward_return"),
            -abs(item_metric(item, "t_stat")),
            -item_metric(item, "signal_support"),
            pack_id,
            symbol,
        )
    return common + (-item_source_review_score(item), pack_id, symbol)


def normalized_net_bps(item: dict[str, Any]) -> float:
    net_pnl = maybe_parse_float(item.get("net_pnl"))
    turnover = maybe_parse_float(item.get("turnover"))
    if net_pnl is None or turnover is None or turnover <= 0:
        return float("-inf")
    return 10_000.0 * net_pnl / turnover


def choose_next(
    candidates: list[dict[str, Any]],
    selected_keys: set[tuple[str, str, str]],
    *,
    require_new_symbol: bool = False,
    used_symbols: set[str] | None = None,
) -> dict[str, Any] | None:
    used_symbols = used_symbols or set()
    for item in candidates:
        key = (
            str(item.get("pack_id") or "").strip(),
            str(item.get("family_id") or "").strip(),
            str(item.get("selected_symbol") or "").strip(),
        )
        symbol = str(item.get("selected_symbol") or "").strip()
        if key in selected_keys:
            continue
        if require_new_symbol and symbol in used_symbols:
            continue
        return item
    if require_new_symbol:
        return choose_next(candidates, selected_keys, require_new_symbol=False, used_symbols=used_symbols)
    return None


def add_selected(target: list[dict[str, Any]], item: dict[str, Any], selected_keys: set[tuple[str, str, str]], used_symbols: set[str]) -> None:
    key = (
        str(item.get("pack_id") or "").strip(),
        str(item.get("family_id") or "").strip(),
        str(item.get("selected_symbol") or "").strip(),
    )
    if key in selected_keys:
        return
    target.append(item)
    selected_keys.add(key)
    used_symbols.add(str(item.get("selected_symbol") or "").strip())


def select_momentum_rows(candidates: list[dict[str, Any]], baseline_items: list[dict[str, Any]], row_count: int) -> list[dict[str, Any]]:
    sorted_candidates = sorted(candidates, key=family_strength_sort_key)
    by_strategy = {str(item.get("strategy_id") or "").strip(): item for item in candidates}
    baseline_sorted = sorted(baseline_items, key=normalized_net_bps)
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()
    used_symbols: set[str] = set()
    baseline_strategy_ids = {str(item.get("strategy_id") or "").strip() for item in baseline_items}

    if baseline_sorted:
        worst = baseline_sorted[0]
        if str(worst.get("strategy_id") or "").strip() in by_strategy:
            add_selected(selected, by_strategy[str(worst.get("strategy_id") or "").strip()], selected_keys, used_symbols)
        best = max(baseline_items, key=normalized_net_bps)
        if str(best.get("strategy_id") or "").strip() in by_strategy:
            add_selected(selected, by_strategy[str(best.get("strategy_id") or "").strip()], selected_keys, used_symbols)
        delayed = [
            item for item in baseline_items
            if str(item.get("prior_2h_classification") or "").strip() == "NO_SIGNAL"
            and int(item.get("fills_count") or 0) > 0
        ]
        if delayed:
            best_delayed = max(delayed, key=normalized_net_bps)
            if str(best_delayed.get("strategy_id") or "").strip() in by_strategy:
                add_selected(selected, by_strategy[str(best_delayed.get("strategy_id") or "").strip()], selected_keys, used_symbols)

    unseen_candidates = [
        item for item in sorted_candidates
        if str(item.get("strategy_id") or "").strip() not in baseline_strategy_ids
    ]
    while len(selected) < row_count:
        require_new_symbol = len(used_symbols) < row_count
        chosen = choose_next(unseen_candidates, selected_keys, require_new_symbol=require_new_symbol, used_symbols=used_symbols)
        if chosen is None:
            chosen = choose_next(sorted_candidates, selected_keys, require_new_symbol=require_new_symbol, used_symbols=used_symbols)
        if chosen is None:
            break
        add_selected(selected, chosen, selected_keys, used_symbols)
    return selected[:row_count]


def select_family_rows(family_id: str, candidates: list[dict[str, Any]], row_count: int) -> list[dict[str, Any]]:
    sorted_candidates = sorted(candidates, key=family_strength_sort_key)
    if not sorted_candidates:
        return []
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()
    used_symbols: set[str] = set()

    strongest = sorted_candidates[0]
    add_selected(selected, strongest, selected_keys, used_symbols)

    second_distinct = choose_next(sorted_candidates[1:], selected_keys, require_new_symbol=True, used_symbols=used_symbols)
    if second_distinct is not None:
        add_selected(selected, second_distinct, selected_keys, used_symbols)

    median_item = sorted_candidates[len(sorted_candidates) // 2]
    add_selected(selected, median_item, selected_keys, used_symbols)

    threshold_sorted = sorted(sorted_candidates, key=lambda item: (threshold_distance(item), family_strength_sort_key(item)))
    threshold_item = choose_next(threshold_sorted, selected_keys, require_new_symbol=False, used_symbols=used_symbols)
    if threshold_item is not None:
        add_selected(selected, threshold_item, selected_keys, used_symbols)

    remaining_best_distinct = choose_next(sorted_candidates, selected_keys, require_new_symbol=True, used_symbols=used_symbols)
    if remaining_best_distinct is not None:
        add_selected(selected, remaining_best_distinct, selected_keys, used_symbols)

    while len(selected) < row_count:
        chosen = choose_next(sorted_candidates, selected_keys, require_new_symbol=True, used_symbols=used_symbols)
        if chosen is None:
            break
        add_selected(selected, chosen, selected_keys, used_symbols)
    return selected[:row_count]


def prepare_selection(binding_payload: dict[str, Any], baseline_results: dict[str, Any], row_count_per_family: int) -> list[dict[str, Any]]:
    grouped = family_binding_candidates(binding_payload)
    baseline_items = list(baseline_results.get("items") or [])
    selected_rows: list[dict[str, Any]] = []
    ordered_family_ids = ["momentum_v1", "return_reversal_v1", "jump_reversion_v1", "family_b_simple_momentum"]
    for family_id in ordered_family_ids:
        candidates = grouped.get(family_id, [])
        if family_id == "momentum_v1":
            family_rows = select_momentum_rows(candidates, baseline_items, row_count_per_family)
        else:
            family_rows = select_family_rows(family_id, candidates, row_count_per_family)
        selected_rows.extend(family_rows)
    return selected_rows


def row_label(index: int, item: dict[str, Any]) -> str:
    family_id = str(item.get("family_id") or "").strip()
    symbol = str(item.get("selected_symbol") or "").strip()
    return f"run{index:02d}_{family_id}_{symbol}"


def build_selection_payload(campaign_id: str, binding_json: Path, baseline_json: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    per_family_counts = Counter(str(item.get("family_id") or "").strip() for item in rows)
    return {
        "schema_version": SCHEMA_SELECTION,
        "campaign_id": campaign_id,
        "campaign_shape": "BINANCE_TRADE_STRATIFIED_MULTI_FAMILY",
        "comparability_mode": "COMMON_DIRECTIONAL_EXPECTANCY_CONTRACT",
        "duration_rule": "24H_COMPLETED_PER_ROW",
        "generated_ts_utc": utc_now_iso(),
        "source_binding_json": str(binding_json),
        "baseline_campaign_results_json": str(baseline_json),
        "total_row_count": len(rows),
        "per_family_row_counts": dict(sorted(per_family_counts.items())),
        "selected_rows": [
            {
                "rank": index,
                "run_label": row_label(index, item),
                "strategy_id": str(item.get("strategy_id") or "").strip(),
                "pack_id": str(item.get("pack_id") or "").strip(),
                "family_id": str(item.get("family_id") or "").strip(),
                "symbol": str(item.get("selected_symbol") or "").strip(),
                "exchange": str(item.get("exchange") or "").strip(),
                "stream": str(item.get("stream") or "").strip(),
                "binding_mode": str(item.get("binding_mode") or "").strip(),
                "decision_tier": item_decision_tier(item),
                "review_class": str(item.get("source_review_class") or "").strip(),
                "review_class_priority": item.get("source_review_class_priority"),
                "review_score": item.get("source_review_score"),
                "pack_path": str(item.get("pack_path") or "").strip(),
                "runtime_strategy_file": str(item.get("runtime_strategy_file") or "").strip(),
                "runtime_strategy_config": item.get("runtime_strategy_config"),
                "family_metric_snapshot": item_selected_cell(item),
            }
            for index, item in enumerate(rows, start=1)
        ],
    }


def build_launch_manifest(campaign_id: str, campaign_dir: Path, selection_payload: dict[str, Any]) -> dict[str, Any]:
    items = []
    for row in list(selection_payload.get("selected_rows") or []):
        run_label = str(row.get("run_label") or "").strip()
        artifact_path = campaign_dir / run_label
        items.append(
            {
                "run_label": run_label,
                "strategy_id": str(row.get("strategy_id") or "").strip(),
                "pack_id": str(row.get("pack_id") or "").strip(),
                "family_id": str(row.get("family_id") or "").strip(),
                "symbol": str(row.get("symbol") or "").strip(),
                "exchange": str(row.get("exchange") or "").strip(),
                "binding_mode": str(row.get("binding_mode") or "").strip(),
                "artifact_path": str(artifact_path),
                "watchlist_path": str(artifact_path / "watchlist.json"),
                "summary_json_path": str(artifact_path / "summary_capture.json"),
                "history_jsonl_path": str(artifact_path / "history.jsonl"),
                "index_json_path": str(artifact_path / "index.json"),
                "refresh_result_json_path": str(artifact_path / "refresh_result.json"),
                "batch_result_json_path": str(artifact_path / "batch_result.json"),
                "execution_ledger_jsonl_path": str(artifact_path / "execution_ledger.jsonl"),
                "execution_pack_summary_json_path": str(artifact_path / "execution_pack_summary.json"),
                "shadow_state_dir": str(artifact_path / "shadow_state_local"),
                "stdout_log_path": str(artifact_path / "batch_stdout.log"),
                "stderr_log_path": str(artifact_path / "batch_stderr.log"),
                "launcher_log_path": str(artifact_path / "launcher.log"),
                "audit_base_dir": str(artifact_path / "audit"),
                "launched_status": "NOT_LAUNCHED",
            }
        )
    return {
        "schema_version": SCHEMA_MANIFEST,
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "items": items,
    }


def build_single_row_watchlist(selection_row: dict[str, Any], *, source_binding_json: Path) -> dict[str, Any]:
    return {
        "schema_version": "directional_expectancy_campaign_watchlist_v0",
        "generated_ts_utc": utc_now_iso(),
        "source_binding_artifact_json": str(source_binding_json),
        "selection_policy": {
            "surface_role": "MULTI_FAMILY_DIRECTIONAL_EXPECTANCY_CAMPAIGN",
            "authoritative_source": "directional_expectancy_runtime_binding_v0.json",
        },
        "items": [
            {
                "rank": int(selection_row.get("rank") or 0),
                "pack_id": str(selection_row.get("pack_id") or "").strip(),
                "pack_path": str(selection_row.get("pack_path") or "").strip(),
                "exchange": str(selection_row.get("exchange") or "").strip(),
                "stream": str(selection_row.get("stream") or "").strip(),
                "symbols": [str(selection_row.get("symbol") or "").strip().upper()],
                "decision_tier": str(selection_row.get("decision_tier") or "").strip(),
                "selection_slot": f"{selection_row.get('exchange')}/{selection_row.get('stream')}",
                "binding_mode": str(selection_row.get("binding_mode") or "").strip(),
                "binding_family_id": str(selection_row.get("family_id") or "").strip(),
                "binding_strategy_id": str(selection_row.get("strategy_id") or "").strip(),
                "review_class": str(selection_row.get("review_class") or "").strip(),
                "class_priority": selection_row.get("review_class_priority"),
                "score": selection_row.get("review_score"),
            }
        ],
    }


def build_batch_command(
    batch_tool: Path,
    manifest_item: dict[str, Any],
    selection_row: dict[str, Any],
    duration_sec: int,
    timeout_sec: int,
    heartbeat_ms: int,
) -> list[str]:
    runtime_strategy_config = selection_row.get("runtime_strategy_config")
    if not isinstance(runtime_strategy_config, dict):
        fail(f"selection_row_runtime_strategy_config_invalid:{selection_row.get('strategy_id')}")
    strategy_file = str(selection_row.get("runtime_strategy_file") or "").strip()
    if not strategy_file:
        fail(f"selection_row_runtime_strategy_file_missing:{selection_row.get('strategy_id')}")
    return [
        sys.executable,
        str(batch_tool.resolve()),
        "--watchlist",
        str(Path(manifest_item["watchlist_path"]).resolve()),
        "--max-items",
        "1",
        "--strategy",
        strategy_file,
        "--strategy-config-json",
        json.dumps(runtime_strategy_config, sort_keys=True),
        "--summary-json-path",
        str(Path(manifest_item["summary_json_path"]).resolve()),
        "--history-jsonl",
        str(Path(manifest_item["history_jsonl_path"]).resolve()),
        "--index-json",
        str(Path(manifest_item["index_json_path"]).resolve()),
        "--shadow-state-dir",
        str(Path(manifest_item["shadow_state_dir"]).resolve()),
        "--refresh-result-json",
        str(Path(manifest_item["refresh_result_json_path"]).resolve()),
        "--execution-ledger-jsonl",
        str(Path(manifest_item["execution_ledger_jsonl_path"]).resolve()),
        "--execution-pack-summary-json",
        str(Path(manifest_item["execution_pack_summary_json_path"]).resolve()),
        "--audit-base-dir",
        str(Path(manifest_item["audit_base_dir"]).resolve()),
        "--out-dir",
        str((Path(manifest_item["artifact_path"]).resolve() / "out")),
        "--result-json",
        str(Path(manifest_item["batch_result_json_path"]).resolve()),
        "--per-run-timeout-sec",
        str(int(timeout_sec)),
        "--run-max-duration-sec",
        str(int(duration_sec)),
        "--heartbeat-ms",
        str(int(heartbeat_ms)),
    ]


def process_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def launch_rows(
    manifest_payload: dict[str, Any],
    selection_payload: dict[str, Any],
    *,
    batch_tool: Path,
    binding_json: Path,
    duration_sec: int,
    timeout_sec: int,
    heartbeat_ms: int,
    dry_run: bool,
) -> dict[str, Any]:
    selection_by_strategy = {
        str(row.get("strategy_id") or "").strip(): row
        for row in list(selection_payload.get("selected_rows") or [])
    }
    for item in list(manifest_payload.get("items") or []):
        strategy_id = str(item.get("strategy_id") or "").strip()
        selection_row = selection_by_strategy.get(strategy_id)
        if selection_row is None:
            item["launched_status"] = "FAILED_SELECTION_MISSING"
            continue
        artifact_path = Path(item["artifact_path"]).resolve()
        artifact_path.mkdir(parents=True, exist_ok=True)
        write_json(Path(item["watchlist_path"]).resolve(), build_single_row_watchlist(selection_row, source_binding_json=binding_json))
        cmd = build_batch_command(batch_tool, item, selection_row, duration_sec, timeout_sec, heartbeat_ms)
        item["launch_command"] = " ".join(cmd)
        if dry_run:
            item["launched_status"] = "DRY_RUN_ONLY"
            item["pid"] = None
            continue
        stdout_handle = Path(item["launcher_log_path"]).resolve().open("a", encoding="utf-8")
        stderr_handle = Path(item["stderr_log_path"]).resolve().open("a", encoding="utf-8")
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            start_new_session=True,
        )
        stdout_handle.close()
        stderr_handle.close()
        item["pid"] = int(proc.pid)
        item["launched_status"] = "BACKGROUND_LAUNCHED"
        item["launch_started_ts_utc"] = utc_now_iso()
    manifest_payload["generated_ts_utc"] = utc_now_iso()
    return manifest_payload


def summarize_row_progress(item: dict[str, Any]) -> dict[str, Any]:
    pid = maybe_parse_int(item.get("pid"))
    batch_result = read_json_if_exists(Path(str(item.get("batch_result_json_path") or "")).resolve())
    refresh_result = read_json_if_exists(Path(str(item.get("refresh_result_json_path") or "")).resolve())
    summary = read_json_if_exists(Path(str(item.get("summary_json_path") or "")).resolve())
    execution_summary = summary.get("execution_summary") if isinstance(summary, dict) and isinstance(summary.get("execution_summary"), dict) else {}
    fills_count = int(execution_summary.get("fills_count") or 0)
    process_is_alive = process_alive(pid)
    if batch_result and refresh_result and summary and not process_is_alive:
        definitive_state = "COMPLETED"
    elif process_is_alive:
        definitive_state = "ACTIVE"
    elif str(item.get("launched_status") or "").strip() == "DRY_RUN_ONLY":
        definitive_state = "DRY_RUN"
    else:
        definitive_state = "FAILED"
    return {
        "strategy_id": str(item.get("strategy_id") or "").strip(),
        "family_id": str(item.get("family_id") or "").strip(),
        "symbol": str(item.get("symbol") or "").strip(),
        "pid": pid,
        "definitive_state": definitive_state,
        "fills_count": fills_count,
        "summary_present": summary is not None,
        "batch_present": batch_result is not None,
        "refresh_present": refresh_result is not None,
    }


def family_activity_summary(rows: list[dict[str, Any]]) -> str:
    parts = []
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_family[str(row.get("family_id") or "").strip()].append(row)
    for family_id in sorted(by_family.keys()):
        family_rows = by_family[family_id]
        fill_rows = sum(1 for row in family_rows if int(row.get("fills_count") or 0) > 0)
        completed = sum(1 for row in family_rows if str(row.get("definitive_state") or "") == "COMPLETED")
        parts.append(f"{family_id}:{fill_rows}fill/{completed}done")
    return ",".join(parts)


def send_telegram_message(
    *,
    message_text: str,
    telegram_api_base_url: str,
    dry_run: bool,
) -> dict[str, Any]:
    load_env_defaults_from_file(resolve_env_file())
    token = str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    result = {
        "sent_ts_utc": utc_now_iso(),
        "message_text": message_text,
        "sent": False,
        "http_status": None,
        "error": "",
        "dry_run": dry_run,
    }
    if dry_run:
        result["sent"] = True
        return result
    if not token or not chat_id:
        result["error"] = "missing_telegram_credentials"
        return result
    url = f"{telegram_api_base_url.rstrip('/')}/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message_text}).encode("utf-8")
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            body = json.loads(response_body)
            result["sent"] = bool(body.get("ok"))
            result["http_status"] = int(response.status)
            if isinstance(body.get("result"), dict):
                result["message_id"] = body["result"].get("message_id")
            if not result["sent"]:
                result["error"] = f"telegram_api_not_ok:{body}"
    except urllib.error.HTTPError as exc:
        result["http_status"] = int(exc.code)
        result["error"] = f"http_error:{exc.code}"
    except Exception as exc:  # pragma: no cover - network failures are environmental
        result["error"] = f"telegram_send_failed:{exc}"
    return result


def maybe_send_hourly_report(
    status_payload: dict[str, Any],
    *,
    campaign_id: str,
    telegram_reports_jsonl: Path,
    telegram_api_base_url: str,
    telegram_dry_run: bool,
) -> dict[str, Any] | None:
    elapsed_sec = int(status_payload.get("elapsed_sec") or 0)
    elapsed_hour = elapsed_sec // 3600
    sent_hours = {int(value) for value in list(status_payload.get("sent_hours") or [])}
    if elapsed_hour <= 0 or elapsed_hour in sent_hours:
        return None
    fill_rows = list(status_payload.get("rows_with_fills_so_far") or [])
    message_text = (
        f"{campaign_id} H{elapsed_hour}: completed {status_payload.get('completed_rows')}/{status_payload.get('total_rows')}; "
        f"active {status_payload.get('active_rows')}; failed {status_payload.get('failed_rows')}; "
        f"family_activity {status_payload.get('family_activity_summary_so_far')}; "
        f"fill_rows {','.join(fill_rows) if fill_rows else 'none'}; agg_fills {status_payload.get('aggregate_fills_so_far')}."
    )
    status_payload["hourly_reports_attempted"] = int(status_payload.get("hourly_reports_attempted") or 0) + 1
    result = send_telegram_message(
        message_text=message_text,
        telegram_api_base_url=telegram_api_base_url,
        dry_run=telegram_dry_run,
    )
    result["hour"] = elapsed_hour
    append_jsonl(telegram_reports_jsonl, result)
    if result.get("sent"):
        sent_hours.add(elapsed_hour)
        status_payload["hourly_reports_sent"] = int(status_payload.get("hourly_reports_sent") or 0) + 1
        status_payload["sent_hours"] = sorted(sent_hours)
    return result


def monitor_status(
    manifest_payload: dict[str, Any],
    *,
    campaign_id: str,
    started_ts_utc: str,
    runtime_status_json: Path,
    telegram_reports_jsonl: Path,
    telegram_api_base_url: str,
    telegram_dry_run: bool,
) -> dict[str, Any]:
    rows = [summarize_row_progress(item) for item in list(manifest_payload.get("items") or [])]
    completed_rows = sum(1 for row in rows if row["definitive_state"] == "COMPLETED")
    active_rows = sum(1 for row in rows if row["definitive_state"] == "ACTIVE")
    failed_rows = sum(1 for row in rows if row["definitive_state"] == "FAILED")
    started_dt = parse_iso(started_ts_utc) or utc_now()
    elapsed_sec = int((utc_now() - started_dt).total_seconds())
    rows_with_fills = [str(row["symbol"]) for row in rows if int(row.get("fills_count") or 0) > 0]
    payload = read_json_if_exists(runtime_status_json) or {
        "schema_version": SCHEMA_STATUS,
        "campaign_id": campaign_id,
        "started_ts_utc": started_ts_utc,
        "hourly_reports_attempted": 0,
        "hourly_reports_sent": 0,
        "sent_hours": [],
    }
    payload.update(
        {
            "schema_version": SCHEMA_STATUS,
            "campaign_id": campaign_id,
            "started_ts_utc": started_ts_utc,
            "updated_ts_utc": utc_now_iso(),
            "elapsed_sec": elapsed_sec,
            "completed_rows": completed_rows,
            "active_rows": active_rows,
            "failed_rows": failed_rows,
            "total_rows": len(rows),
            "aggregate_fills_so_far": sum(int(row.get("fills_count") or 0) for row in rows),
            "rows_with_fills_so_far": rows_with_fills,
            "family_activity_summary_so_far": family_activity_summary(rows),
            "items": rows,
        }
    )
    maybe_send_hourly_report(
        payload,
        campaign_id=campaign_id,
        telegram_reports_jsonl=telegram_reports_jsonl,
        telegram_api_base_url=telegram_api_base_url,
        telegram_dry_run=telegram_dry_run,
    )
    write_json(runtime_status_json, payload)
    return payload


def summarize_local_execution_events(events_path: Path) -> tuple[int, int, int, int, int, float, float]:
    events = read_jsonl(events_path)
    fills = [row for row in events if str(row.get("event_type") or "").strip() == "FILL"]
    fills_count = len(fills)
    fees = sum(float(row.get("fill_fee") or 0.0) for row in fills)
    turnover = sum(abs(float(row.get("fill_value") or 0.0)) for row in fills)
    opens_count = 0
    exits_count = 0
    reversals_count = 0
    closed_cycle_count = 0
    current_position = 0.0
    last_open_side = 0
    for row in fills:
        qty = float(row.get("qty") or 0.0)
        side = str(row.get("side") or "").strip().upper()
        delta = qty if side == "BUY" else -qty
        next_position = current_position + delta
        if current_position == 0 and next_position != 0:
            opens_count += 1
            next_side = sign_of_position(next_position)
            if last_open_side != 0 and next_side != last_open_side:
                reversals_count += 1
            last_open_side = next_side
        elif current_position != 0 and next_position == 0:
            exits_count += 1
            closed_cycle_count += 1
        elif current_position != 0 and sign_of_position(next_position) != sign_of_position(current_position):
            reversals_count += 1
            closed_cycle_count += 1
            opens_count += 1
            last_open_side = sign_of_position(next_position)
        current_position = next_position
    return fills_count, opens_count, exits_count, reversals_count, closed_cycle_count, fees, turnover


def classify_row(metrics: dict[str, Any]) -> tuple[str, str]:
    completed_horizon_sec = int(metrics.get("completed_horizon_sec") or 0)
    fills_count = int(metrics.get("fills_count") or 0)
    closed_cycle_count = int(metrics.get("closed_cycle_count") or 0)
    net_pnl = float(metrics.get("net_pnl") or 0.0)
    net_pnl_bps_turnover = metrics.get("net_pnl_bps_turnover")
    if completed_horizon_sec < DEFAULT_DURATION_SEC:
        return "BROKEN", "completed_horizon_below_24h"
    if fills_count == 0:
        return "NO_SIGNAL", "no_fills_observed"
    if closed_cycle_count == 0:
        return "INSUFFICIENT_EVIDENCE", "fills_without_closed_cycles"
    if net_pnl_bps_turnover is None:
        return "INSUFFICIENT_EVIDENCE", "turnover_missing_for_normalized_pnl"
    if net_pnl > 0 and net_pnl_bps_turnover > 2.0:
        return "PROMISING", "positive_closed_cycle_result_above_2bps"
    if -2.0 <= net_pnl_bps_turnover <= 2.0:
        return "NEUTRAL", "closed_cycles_completed_with_near_flat_net_result"
    return "WEAK", "closed_cycles_completed_with_negative_normalized_result"


def build_row_result(selection_row: dict[str, Any], manifest_item: dict[str, Any]) -> dict[str, Any]:
    artifact_path = Path(manifest_item["artifact_path"]).resolve()
    summary = read_json_if_exists(Path(manifest_item["summary_json_path"]).resolve())
    batch_result = read_json_if_exists(Path(manifest_item["batch_result_json_path"]).resolve())
    execution_summary = summary.get("execution_summary") if isinstance(summary, dict) and isinstance(summary.get("execution_summary"), dict) else {}
    started_dt = parse_iso(str(summary.get("started_at") or "")) if isinstance(summary, dict) else None
    finished_dt = parse_iso(str(summary.get("finished_at") or "")) if isinstance(summary, dict) else None
    if started_dt is None:
        started_dt = parse_iso(str(manifest_item.get("launch_started_ts_utc") or ""))
    if finished_dt is None and isinstance(batch_result, dict):
        finished_dt = parse_iso(str(batch_result.get("generated_ts_utc") or ""))
    completed_horizon_sec = int((finished_dt - started_dt).total_seconds()) if started_dt and finished_dt else 0
    execution_events_path = Path(manifest_item["shadow_state_dir"]).resolve() / "shadow_execution_events_v1.jsonl"
    fills_count, opens_count, exits_count, reversals_count, derived_closed_cycles, fees, turnover = summarize_local_execution_events(execution_events_path)
    realized_pnl = float(execution_summary.get("total_realized_pnl") or 0.0)
    unrealized_pnl = float(execution_summary.get("total_unrealized_pnl") or 0.0)
    net_pnl = realized_pnl + unrealized_pnl
    final_position = 0.0
    positions = execution_summary.get("positions") if isinstance(execution_summary.get("positions"), dict) else {}
    for position in positions.values():
        size = maybe_parse_float(position.get("size") if isinstance(position, dict) else None)
        if size is not None:
            final_position = size
            break
    risk_reject_event_count = sum(
        1
        for row in read_jsonl(execution_events_path)
        if "RISK_REJECT" in str(row.get("reason") or "").upper()
    )
    net_pnl_bps_turnover = None if turnover <= 0 else 10_000.0 * net_pnl / turnover
    row = {
        "run_label": str(selection_row.get("run_label") or "").strip(),
        "strategy_id": str(selection_row.get("strategy_id") or "").strip(),
        "pack_id": str(selection_row.get("pack_id") or "").strip(),
        "family_id": str(selection_row.get("family_id") or "").strip(),
        "symbol": str(selection_row.get("symbol") or "").strip(),
        "exchange": str(selection_row.get("exchange") or "").strip(),
        "binding_mode": str(selection_row.get("binding_mode") or "").strip(),
        "artifact_path": str(artifact_path),
        "launched_status": str(manifest_item.get("launched_status") or "").strip(),
        "observed": summary is not None,
        "completed_horizon_sec": completed_horizon_sec,
        "fills_count": fills_count,
        "opens_count": opens_count,
        "exits_count": exits_count,
        "reversals_count": reversals_count,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "net_pnl": net_pnl,
        "fees": fees,
        "funding": 0.0,
        "turnover": turnover,
        "final_position": final_position,
        "risk_reject_summary": {"risk_reject_event_count": risk_reject_event_count},
        "stop_reason": str(summary.get("stop_reason") or "") if isinstance(summary, dict) else "",
        "closed_cycle_count": derived_closed_cycles,
        "net_pnl_bps_turnover": net_pnl_bps_turnover,
        "paper_run_status": "ACTIVE_POSITION" if final_position != 0 else ("FILL_BACKED_FLAT" if fills_count > 0 else "NO_ACTIVITY"),
        "profitability_status": "NET_AFTER_FEES_AND_FUNDING" if fills_count > 0 else "NO_ACTIVITY",
    }
    row["classification"], row["classification_reason"] = classify_row(row)
    return row


def row_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    metric = item.get("net_pnl_bps_turnover")
    metric_value = float(metric) if metric is not None else float("-inf")
    return (
        CLASS_PRIORITY[str(item.get("classification") or "BROKEN")],
        -metric_value,
        -int(item.get("closed_cycle_count") or 0),
        -int(item.get("fills_count") or 0),
        -float(item.get("net_pnl") or 0.0),
        str(item.get("family_id") or ""),
        str(item.get("strategy_id") or ""),
    )


def family_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(item.get("promising_row_count") or 0),
        -int(item.get("neutral_row_count") or 0),
        -(float(item.get("median_row_net_pnl_bps_turnover") or float("-inf"))),
        -int(item.get("fill_backed_row_count") or 0),
        -int(item.get("total_closed_cycles") or 0),
        int(item.get("broken_row_count") or 0),
        str(item.get("family_id") or ""),
    )


def family_takeaway(item: dict[str, Any]) -> str:
    family_id = str(item.get("family_id") or "").strip()
    promising = int(item.get("promising_row_count") or 0)
    neutral = int(item.get("neutral_row_count") or 0)
    weak = int(item.get("weak_row_count") or 0)
    no_signal = int(item.get("no_signal_row_count") or 0)
    if promising > 0:
        return f"{family_id} has real positive closed-cycle evidence."
    if neutral > 0 and weak == 0:
        return f"{family_id} is active but only neutral so far."
    if weak > 0 and neutral == 0:
        return f"{family_id} is active and weak."
    if no_signal == int(item.get("row_count") or 0):
        return f"{family_id} produced no fill-backed activity."
    return f"{family_id} is mixed without a promising row."


def build_family_leaderboard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family_id") or "").strip()].append(row)
    families = []
    for family_id, items in grouped.items():
        net_values = [float(item.get("net_pnl") or 0.0) for item in items]
        normalized_values = [float(item["net_pnl_bps_turnover"]) for item in items if item.get("net_pnl_bps_turnover") is not None]
        payload = {
            "family_id": family_id,
            "row_count": len(items),
            "completed_row_count": sum(1 for item in items if int(item.get("completed_horizon_sec") or 0) >= DEFAULT_DURATION_SEC),
            "promising_row_count": sum(1 for item in items if item.get("classification") == "PROMISING"),
            "neutral_row_count": sum(1 for item in items if item.get("classification") == "NEUTRAL"),
            "weak_row_count": sum(1 for item in items if item.get("classification") == "WEAK"),
            "no_signal_row_count": sum(1 for item in items if item.get("classification") == "NO_SIGNAL"),
            "insufficient_evidence_row_count": sum(1 for item in items if item.get("classification") == "INSUFFICIENT_EVIDENCE"),
            "broken_row_count": sum(1 for item in items if item.get("classification") == "BROKEN"),
            "active_row_count": sum(1 for item in items if int(item.get("fills_count") or 0) > 0),
            "fill_backed_row_count": sum(1 for item in items if int(item.get("fills_count") or 0) > 0),
            "closed_cycle_row_count": sum(1 for item in items if int(item.get("closed_cycle_count") or 0) > 0),
            "aggregate_net_pnl": sum(net_values),
            "median_row_net_pnl": median(net_values) if net_values else 0.0,
            "median_row_net_pnl_bps_turnover": median(normalized_values) if normalized_values else None,
            "total_fills": sum(int(item.get("fills_count") or 0) for item in items),
            "total_closed_cycles": sum(int(item.get("closed_cycle_count") or 0) for item in items),
        }
        payload["concise_family_takeaway"] = family_takeaway(payload)
        families.append(payload)
    return sorted(families, key=family_sort_key)


def write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def final_decision(rows: list[dict[str, Any]], families: list[dict[str, Any]]) -> tuple[str, str, str, list[str], list[str]]:
    top_family = families[0]["family_id"] if families else ""
    promising_families = [item["family_id"] for item in families if int(item.get("promising_row_count") or 0) > 0]
    momentum_family = next((item for item in families if item["family_id"] == "momentum_v1"), None)
    if promising_families:
        decision = "PROMISING_SUBSET_FOUND"
    elif top_family and top_family != "momentum_v1":
        decision = "SHIFT_ATTENTION_TO_BACKUP_FAMILY"
    else:
        decision = "MOMENTUM_V1_EARLY_WARNING"
    if any(item.get("classification") == "PROMISING" for item in rows):
        blocker = "MIXED_BUT_PRIMARY_ONE_REQUIRED"
    elif any(int(item.get("fills_count") or 0) > 0 for item in rows):
        blocker = "ENTRY_QUALITY"
    else:
        blocker = "NO_EDGE_VISIBLE"
    momentum_status = "DEPRIORITIZE"
    if momentum_family is not None:
        if int(momentum_family.get("promising_row_count") or 0) > 0:
            momentum_status = "PRIMARY_ATTENTION"
        elif int(momentum_family.get("neutral_row_count") or 0) > 0:
            momentum_status = "CONTINUE_WITH_CAUTION"
    primary_attention = promising_families[0] if promising_families else top_family
    continue_with_caution = [item["family_id"] for item in families if int(item.get("neutral_row_count") or 0) > 0 and item["family_id"] != primary_attention]
    early_warning = [item["family_id"] for item in families if item["family_id"] not in {primary_attention, *continue_with_caution}]
    return decision, blocker, momentum_status, continue_with_caution, early_warning


def build_results_payload(campaign_id: str, selection_payload: dict[str, Any], rows: list[dict[str, Any]], families: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], str]:
    decision, blocker, momentum_status, continue_with_caution, early_warning = final_decision(rows, families)
    top_family = families[0]["family_id"] if families else ""
    final_message = (
        f"{campaign_id} COMPLETE: {sum(1 for row in rows if int(row.get('completed_horizon_sec') or 0) >= DEFAULT_DURATION_SEC)}/{len(rows)} rows reached 24h; "
        f"top family {top_family or '-'}; PROMISING families={sum(1 for item in families if int(item.get('promising_row_count') or 0) > 0)}; "
        f"PROMISING rows={sum(1 for row in rows if row.get('classification') == 'PROMISING')}; momentum_v1={momentum_status}."
    )
    results_payload = {
        "schema_version": SCHEMA_RESULTS,
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "campaign_status": "COMPLETED",
        "aggregate": {
            "row_count": len(rows),
            "completed_row_count": sum(1 for row in rows if int(row.get("completed_horizon_sec") or 0) >= DEFAULT_DURATION_SEC),
            "promising_row_count": sum(1 for row in rows if row.get("classification") == "PROMISING"),
            "neutral_row_count": sum(1 for row in rows if row.get("classification") == "NEUTRAL"),
            "weak_row_count": sum(1 for row in rows if row.get("classification") == "WEAK"),
            "no_signal_row_count": sum(1 for row in rows if row.get("classification") == "NO_SIGNAL"),
            "insufficient_evidence_row_count": sum(1 for row in rows if row.get("classification") == "INSUFFICIENT_EVIDENCE"),
            "broken_row_count": sum(1 for row in rows if row.get("classification") == "BROKEN"),
            "aggregate_net_pnl": sum(float(row.get("net_pnl") or 0.0) for row in rows),
            "total_fills": sum(int(row.get("fills_count") or 0) for row in rows),
            "total_closed_cycles": sum(int(row.get("closed_cycle_count") or 0) for row in rows),
        },
        "items": rows,
    }
    final_verdict = {
        "schema_version": SCHEMA_FINAL_VERDICT,
        "campaign_id": campaign_id,
        "generated_ts_utc": utc_now_iso(),
        "decision_class": decision,
        "new_primary_blocker": blocker,
        "momentum_v1_status": momentum_status,
        "backup_family_attention_needed": top_family != "momentum_v1",
        "primary_attention_family": top_family,
        "continue_with_caution": continue_with_caution,
        "early_warning_or_deprioritize": early_warning,
        "why": [
            f"Top family by 24h leaderboard is {top_family or 'UNKNOWN'}.",
            f"PROMISING families={sum(1 for item in families if int(item.get('promising_row_count') or 0) > 0)} and PROMISING rows={sum(1 for row in rows if row.get('classification') == 'PROMISING')}.",
            f"Momentum status is {momentum_status}.",
        ],
    }
    return results_payload, final_verdict, final_message


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Directional expectancy campaign v0")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    prepare.add_argument("--baseline-results-json", default=str(DEFAULT_BASELINE_RESULTS_JSON))
    prepare.add_argument("--campaign-id", default=DEFAULT_CAMPAIGN_ID)
    prepare.add_argument("--campaign-dir", default=str(DEFAULT_CAMPAIGN_DIR))
    prepare.add_argument("--selection-json", default=str(DEFAULT_SELECTION_JSON))
    prepare.add_argument("--launch-manifest-json", default=str(DEFAULT_LAUNCH_MANIFEST_JSON))
    prepare.add_argument("--row-count-per-family", type=int, default=DEFAULT_ROW_COUNT_PER_FAMILY)

    launch = subparsers.add_parser("launch")
    launch.add_argument("--selection-json", default=str(DEFAULT_SELECTION_JSON))
    launch.add_argument("--launch-manifest-json", default=str(DEFAULT_LAUNCH_MANIFEST_JSON))
    launch.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    launch.add_argument("--batch-tool", default=str(DEFAULT_BATCH_TOOL))
    launch.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_DURATION_SEC)
    launch.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_TIMEOUT_SEC)
    launch.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    launch.add_argument("--dry-run", action="store_true")

    monitor = subparsers.add_parser("monitor")
    monitor.add_argument("--launch-manifest-json", default=str(DEFAULT_LAUNCH_MANIFEST_JSON))
    monitor.add_argument("--runtime-status-json", default=str(DEFAULT_RUNTIME_STATUS_JSON))
    monitor.add_argument("--telegram-reports-jsonl", default=str(DEFAULT_TELEGRAM_REPORTS_JSONL))
    monitor.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    monitor.add_argument("--telegram-dry-run", action="store_true")
    monitor.add_argument("--poll-interval-sec", type=int, default=DEFAULT_POLL_INTERVAL_SEC)
    monitor.add_argument("--once", action="store_true")

    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--selection-json", default=str(DEFAULT_SELECTION_JSON))
    finalize.add_argument("--launch-manifest-json", default=str(DEFAULT_LAUNCH_MANIFEST_JSON))
    finalize.add_argument("--results-json", default=str(DEFAULT_RESULTS_JSON))
    finalize.add_argument("--final-verdict-json", default=str(DEFAULT_FINAL_VERDICT_JSON))
    finalize.add_argument("--row-leaderboard-tsv", default=str(DEFAULT_ROW_LEADERBOARD_TSV))
    finalize.add_argument("--family-leaderboard-tsv", default=str(DEFAULT_FAMILY_LEADERBOARD_TSV))
    finalize.add_argument("--telegram-reports-jsonl", default=str(DEFAULT_TELEGRAM_REPORTS_JSONL))
    finalize.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    finalize.add_argument("--telegram-dry-run", action="store_true")

    return parser.parse_args(argv)


def cmd_prepare(args: argparse.Namespace) -> int:
    binding_json = Path(args.binding_json).resolve()
    baseline_results_json = Path(args.baseline_results_json).resolve()
    campaign_dir = Path(args.campaign_dir).resolve()
    selection_json = Path(args.selection_json).resolve()
    launch_manifest_json = Path(args.launch_manifest_json).resolve()
    binding_payload = read_json(binding_json, "binding_json")
    baseline_results = read_json(baseline_results_json, "baseline_results_json")
    rows = prepare_selection(binding_payload, baseline_results, int(args.row_count_per_family))
    selection_payload = build_selection_payload(str(args.campaign_id), binding_json, baseline_results_json, rows)
    manifest_payload = build_launch_manifest(str(args.campaign_id), campaign_dir, selection_payload)
    write_json(selection_json, selection_payload)
    write_json(launch_manifest_json, manifest_payload)
    print(f"campaign_selection_json={selection_json}")
    print(f"campaign_launch_manifest_json={launch_manifest_json}")
    print(f"selected_row_count={len(rows)}")
    return 0


def cmd_launch(args: argparse.Namespace) -> int:
    selection_json = Path(args.selection_json).resolve()
    launch_manifest_json = Path(args.launch_manifest_json).resolve()
    binding_json = Path(args.binding_json).resolve()
    batch_tool = Path(args.batch_tool).resolve()
    selection_payload = read_json(selection_json, "selection_json")
    manifest_payload = read_json(launch_manifest_json, "launch_manifest_json")
    updated = launch_rows(
        manifest_payload,
        selection_payload,
        batch_tool=batch_tool,
        binding_json=binding_json,
        duration_sec=int(args.run_max_duration_sec),
        timeout_sec=int(args.per_run_timeout_sec),
        heartbeat_ms=int(args.heartbeat_ms),
        dry_run=bool(args.dry_run),
    )
    write_json(launch_manifest_json, updated)
    launched = sum(1 for item in updated.get("items", []) if str(item.get("launched_status") or "").startswith("BACKGROUND"))
    print(f"campaign_launch_manifest_json={launch_manifest_json}")
    print(f"launched_row_count={launched}")
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    launch_manifest_json = Path(args.launch_manifest_json).resolve()
    runtime_status_json = Path(args.runtime_status_json).resolve()
    telegram_reports_jsonl = Path(args.telegram_reports_jsonl).resolve()
    manifest_payload = read_json(launch_manifest_json, "launch_manifest_json")
    campaign_id = str(manifest_payload.get("campaign_id") or "").strip() or DEFAULT_CAMPAIGN_ID
    started_ts_utc = str((read_json_if_exists(runtime_status_json) or {}).get("started_ts_utc") or utc_now_iso())
    while True:
        manifest_payload = read_json(launch_manifest_json, "launch_manifest_json")
        status_payload = monitor_status(
            manifest_payload,
            campaign_id=campaign_id,
            started_ts_utc=started_ts_utc,
            runtime_status_json=runtime_status_json,
            telegram_reports_jsonl=telegram_reports_jsonl,
            telegram_api_base_url=str(args.telegram_api_base_url),
            telegram_dry_run=bool(args.telegram_dry_run),
        )
        done = int(status_payload.get("completed_rows") or 0) + int(status_payload.get("failed_rows") or 0) >= int(status_payload.get("total_rows") or 0)
        if bool(args.once) or done:
            print(f"campaign_runtime_status_json={runtime_status_json}")
            print(f"completed_rows={status_payload.get('completed_rows')}")
            print(f"active_rows={status_payload.get('active_rows')}")
            print(f"failed_rows={status_payload.get('failed_rows')}")
            return 0
        time.sleep(int(args.poll_interval_sec))


def cmd_finalize(args: argparse.Namespace) -> int:
    selection_json = Path(args.selection_json).resolve()
    launch_manifest_json = Path(args.launch_manifest_json).resolve()
    results_json = Path(args.results_json).resolve()
    final_verdict_json = Path(args.final_verdict_json).resolve()
    row_leaderboard_tsv = Path(args.row_leaderboard_tsv).resolve()
    family_leaderboard_tsv = Path(args.family_leaderboard_tsv).resolve()
    telegram_reports_jsonl = Path(args.telegram_reports_jsonl).resolve()
    selection_payload = read_json(selection_json, "selection_json")
    manifest_payload = read_json(launch_manifest_json, "launch_manifest_json")
    selection_by_strategy = {
        str(item.get("strategy_id") or "").strip(): item
        for item in list(selection_payload.get("selected_rows") or [])
    }
    rows = [
        build_row_result(selection_by_strategy[str(item.get("strategy_id") or "").strip()], item)
        for item in list(manifest_payload.get("items") or [])
        if str(item.get("strategy_id") or "").strip() in selection_by_strategy
    ]
    rows = sorted(rows, key=row_sort_key)
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    families = build_family_leaderboard(rows)
    for index, item in enumerate(families, start=1):
        item["rank"] = index
    results_payload, final_verdict, final_message = build_results_payload(
        str(selection_payload.get("campaign_id") or DEFAULT_CAMPAIGN_ID),
        selection_payload,
        rows,
        families,
    )
    write_json(results_json, results_payload)
    write_json(final_verdict_json, final_verdict)
    write_tsv(
        row_leaderboard_tsv,
        ["rank", "strategy_id", "family_id", "symbol", "classification", "net_pnl", "fees", "fills_count", "closed_cycle_count", "classification_reason"],
        rows,
    )
    write_tsv(
        family_leaderboard_tsv,
        [
            "rank",
            "family_id",
            "row_count",
            "completed_row_count",
            "promising_row_count",
            "neutral_row_count",
            "weak_row_count",
            "no_signal_row_count",
            "insufficient_evidence_row_count",
            "broken_row_count",
            "active_row_count",
            "fill_backed_row_count",
            "closed_cycle_row_count",
            "aggregate_net_pnl",
            "median_row_net_pnl",
            "median_row_net_pnl_bps_turnover",
            "total_fills",
            "total_closed_cycles",
            "concise_family_takeaway",
        ],
        families,
    )
    final_result = send_telegram_message(
        message_text=final_message,
        telegram_api_base_url=str(args.telegram_api_base_url),
        dry_run=bool(args.telegram_dry_run),
    )
    final_result["message_kind"] = "FINAL_COMPLETION"
    append_jsonl(telegram_reports_jsonl, final_result)
    print(f"campaign_results_json={results_json}")
    print(f"final_verdict_json={final_verdict_json}")
    print(f"row_leaderboard_tsv={row_leaderboard_tsv}")
    print(f"family_leaderboard_tsv={family_leaderboard_tsv}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.command == "prepare":
        return cmd_prepare(args)
    if args.command == "launch":
        return cmd_launch(args)
    if args.command == "monitor":
        return cmd_monitor(args)
    if args.command == "finalize":
        return cmd_finalize(args)
    fail(f"unsupported_command:{args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DirectionalExpectancyCampaignError as exc:
        print(f"DIRECTIONAL_EXPECTANCY_CAMPAIGN_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
