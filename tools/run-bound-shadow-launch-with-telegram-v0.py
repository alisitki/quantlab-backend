#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_BOUND_LAUNCH_TOOL = ROOT / "tools" / "run-bound-shadow-launch-v0.py"
DEFAULT_BINDING_ARTIFACT = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_BOUND_LAUNCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_launch_v0.json"
DEFAULT_TELEGRAM_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_launch_telegram_v0.json"
DEFAULT_GENERATED_WATCHLIST_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_launch_watchlist_v0.json"
DEFAULT_CHILD_LAUNCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_launch_v0.json"
DEFAULT_CHILD_BATCH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_result_v0.json"
DEFAULT_REFRESH_RESULT_JSON = ROOT / "tools" / "shadow_state" / "shadow_derived_surface_refresh_v0.json"
DEFAULT_OPERATOR_SNAPSHOT_JSON = ROOT / "tools" / "shadow_state" / "shadow_operator_snapshot_v0.json"
DEFAULT_EXECUTION_REVIEW_QUEUE_JSON = ROOT / "tools" / "shadow_state" / "shadow_execution_review_queue_v0.json"
DEFAULT_EXECUTION_EVENTS_JSONL = ROOT / "tools" / "shadow_state" / "shadow_execution_events_v1.jsonl"
DEFAULT_TRADE_LEDGER_JSONL = ROOT / "tools" / "shadow_state" / "shadow_trade_ledger_v1.jsonl"
DEFAULT_BATCH_STDOUT_LOG = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_stdout_v0.log"
DEFAULT_BATCH_STDERR_LOG = ROOT / "tools" / "shadow_state" / "shadow_bound_long_shadow_batch_stderr_v0.log"
DEFAULT_AUDIT_BASE_DIR = Path("/tmp/quantlab-bound-shadow-telegram-audit-v0")
DEFAULT_OUT_DIR = Path("/tmp/quantlab-bound-shadow-telegram-out-v0")
DEFAULT_PER_RUN_TIMEOUT_SEC = 90
DEFAULT_RUN_MAX_DURATION_SEC = 60
DEFAULT_HEARTBEAT_MS = 5000
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
SCHEMA_VERSION = "shadow_bound_launch_telegram_v0"
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"


class BoundShadowTelegramError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise BoundShadowTelegramError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_env_file() -> Path:
    override = str(os.environ.get("QUANTLAB_ENV_FILE") or "").strip()
    if override:
        return Path(override).resolve()
    return DEFAULT_ENV_FILE


def load_env_defaults_from_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if str(os.environ.get(key) or "").strip():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        # First occurrence wins inside .env so accidental duplicates do not clobber earlier values.
        if key in os.environ:
            continue
        os.environ[key] = value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bound shadow launch + Telegram notifier v0")
    parser.add_argument("--bound-launch-tool", default=str(DEFAULT_BOUND_LAUNCH_TOOL))
    parser.add_argument("--binding-artifact", default=str(DEFAULT_BINDING_ARTIFACT))
    parser.add_argument("--pack-id", default="")
    parser.add_argument("--strategy-id", default="")
    parser.add_argument("--bound-launch-result-json", default=str(DEFAULT_BOUND_LAUNCH_RESULT_JSON))
    parser.add_argument("--telegram-result-json", default=str(DEFAULT_TELEGRAM_RESULT_JSON))
    parser.add_argument("--generated-watchlist-json", default=str(DEFAULT_GENERATED_WATCHLIST_JSON))
    parser.add_argument("--child-launch-result-json", default=str(DEFAULT_CHILD_LAUNCH_RESULT_JSON))
    parser.add_argument("--child-batch-result-json", default=str(DEFAULT_CHILD_BATCH_RESULT_JSON))
    parser.add_argument("--refresh-result-json", default=str(DEFAULT_REFRESH_RESULT_JSON))
    parser.add_argument("--operator-snapshot-json", default=str(DEFAULT_OPERATOR_SNAPSHOT_JSON))
    parser.add_argument("--execution-review-queue-json", default=str(DEFAULT_EXECUTION_REVIEW_QUEUE_JSON))
    parser.add_argument("--execution-events-jsonl", default=str(DEFAULT_EXECUTION_EVENTS_JSONL))
    parser.add_argument("--trade-ledger-jsonl", default=str(DEFAULT_TRADE_LEDGER_JSONL))
    parser.add_argument("--batch-stdout-log", default=str(DEFAULT_BATCH_STDOUT_LOG))
    parser.add_argument("--batch-stderr-log", default=str(DEFAULT_BATCH_STDERR_LOG))
    parser.add_argument("--audit-base-dir", default=str(DEFAULT_AUDIT_BASE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    parser.add_argument("--telegram-message-style", choices=["compact", "verbose"], default="compact")
    parser.add_argument("--telegram-dry-run", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    args.pack_id = str(args.pack_id or "").strip()
    args.strategy_id = str(args.strategy_id or "").strip()
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    return args


def mask_secret(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= 8:
        return "*" * len(raw)
    return f"{raw[:4]}...{raw[-4:]}"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def load_jsonl(path: Path) -> list[dict[str, Any]]:
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


def selection_mode(args: argparse.Namespace) -> str:
    if args.strategy_id and args.pack_id:
        return "STRATEGY_ID_AND_PACK_ID"
    if args.strategy_id:
        return "STRATEGY_ID"
    if args.pack_id:
        return "PACK_ID"
    return "FIRST_BOUND"


def row_pack_id(item: dict[str, Any]) -> str:
    return str(item.get("pack_id") or "").strip()


def row_strategy_id(item: dict[str, Any]) -> str:
    return str(item.get("strategy_id") or "").strip()


def short_id(value: str, *, tail: int = 8) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    if len(raw) <= tail:
        return raw
    return f"...{raw[-tail:]}"


def primary_symbol(item: dict[str, Any]) -> str:
    values = [str(value).upper() for value in list(item.get("symbols") or []) if str(value).strip()]
    if not values:
        return "?"
    return values[0]


def compact_identity(item: dict[str, Any]) -> str:
    family = str(item.get("family_id") or "").strip() or "unknown_family"
    symbol = primary_symbol(item)
    exchange = str(item.get("exchange") or "").strip() or "unknown_exchange"
    return f"{family} | {symbol} | {exchange}"


def result_symbol(launch_result: dict[str, Any]) -> str:
    symbols = launch_result.get("selected_symbols")
    if isinstance(symbols, list) and symbols:
        return str(symbols[0]).upper()
    value = str(symbols or "").strip()
    if value:
        return value.upper()
    return "?"


def load_binding_artifact(path: Path) -> dict[str, Any]:
    obj = load_json(path, "binding_artifact")
    if str(obj.get("schema_version") or "").strip() != BINDING_SCHEMA_VERSION:
        fail(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"binding_artifact_items_invalid:{path}")
    return obj


def select_bound_item(items: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    by_strategy = None
    by_pack = None
    if args.strategy_id:
        by_strategy = next((item for item in items if row_strategy_id(item) == args.strategy_id), None)
        if by_strategy is None:
            fail(f"strategy_id_not_found:{args.strategy_id}")
    if args.pack_id:
        by_pack = next((item for item in items if row_pack_id(item) == args.pack_id), None)
        if by_pack is None:
            fail(f"pack_id_not_found:{args.pack_id}")
    if by_strategy is not None and by_pack is not None:
        if row_strategy_id(by_strategy) != row_strategy_id(by_pack):
            fail(f"selector_conflict:strategy_id={args.strategy_id}:pack_id={args.pack_id}")
        selected = by_strategy
    elif by_strategy is not None:
        selected = by_strategy
    elif by_pack is not None:
        selected = by_pack
    else:
        selected = next(
            (item for item in items if str(item.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE),
            None,
        )
        if selected is None:
            fail("no_bound_shadow_runnable_rows")
    if str(selected.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
        fail(f"selected_row_not_bound:{str(selected.get('runtime_binding_status') or '').strip()}")
    return selected


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def telegram_config(args: argparse.Namespace) -> dict[str, Any]:
    token = str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if args.telegram_dry_run:
        return {
            "enabled": True,
            "send_mode": "DRY_RUN",
            "token": token,
            "chat_id": chat_id,
            "token_masked": mask_secret(token),
            "chat_id_masked": mask_secret(chat_id),
            "api_base_url": str(args.telegram_api_base_url).rstrip("/"),
        }
    if not token or not chat_id:
        fail("missing_telegram_env")
    return {
        "enabled": True,
        "send_mode": "LIVE",
        "token": token,
        "chat_id": chat_id,
        "token_masked": mask_secret(token),
        "chat_id_masked": mask_secret(chat_id),
        "api_base_url": str(args.telegram_api_base_url).rstrip("/"),
    }


def send_telegram_message(config: dict[str, Any], *, text: str) -> tuple[bool, str]:
    if config["send_mode"] == "DRY_RUN":
        return True, "DRY_RUN"
    url = f"{config['api_base_url']}/bot{config['token']}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": config["chat_id"],
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status = getattr(response, "status", 200)
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return False, f"TELEGRAM_HTTP_{exc.code}"
    except urllib.error.URLError:
        return False, "TELEGRAM_FETCH_ERROR"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "TELEGRAM_INVALID_JSON"
    if status == 200 and parsed.get("ok") is True:
        return True, "OK"
    description = str(parsed.get("description") or "").strip()
    if description:
        return False, f"TELEGRAM_API_{description.replace(' ', '_').upper()[:40]}"
    return False, f"TELEGRAM_HTTP_{status}"


def count_execution_events(launch_result: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, int]:
    selected_pack_id = str(launch_result.get("selected_pack_id") or "").strip()
    selected_live_run_id = str(launch_result.get("selected_live_run_id") or "").strip()
    counts = {"DECISION": 0, "RISK_REJECT": 0, "FILL": 0}
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != selected_pack_id:
            continue
        if str(row.get("live_run_id") or "").strip() != selected_live_run_id:
            continue
        event_type = str(row.get("event_type") or "").strip().upper()
        if event_type in counts:
            counts[event_type] += 1
    return counts


def count_trade_statuses(launch_result: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, int]:
    selected_pack_id = str(launch_result.get("selected_pack_id") or "").strip()
    selected_live_run_id = str(launch_result.get("selected_live_run_id") or "").strip()
    counts = {"OPEN": 0, "CLOSED": 0}
    for row in rows:
        if str(row.get("selected_pack_id") or "").strip() != selected_pack_id:
            continue
        live_run_match = selected_live_run_id in {
            str(row.get("open_live_run_id") or "").strip(),
            str(row.get("last_live_run_id") or "").strip(),
        }
        if not live_run_match:
            continue
        status = str(row.get("status") or "").strip().upper()
        if status in counts:
            counts[status] += 1
    return counts


def render_start_message(selected: dict[str, Any], *, style: str) -> str:
    config = selected.get("runtime_strategy_config") if isinstance(selected.get("runtime_strategy_config"), dict) else {}
    if style == "verbose":
        symbols = ", ".join(str(value).upper() for value in list(selected.get("symbols") or []))
        return "\n".join(
            [
                "QuantLab Bound Shadow Launch START",
                "Mode: shadow/paper synthetic",
                f"Family: {str(selected.get('family_id') or '').strip()}",
                f"Strategy ID: {row_strategy_id(selected)}",
                f"Pack: {row_pack_id(selected)}",
                f"Exchange/Stream: {str(selected.get('exchange') or '').strip()}/{str(selected.get('stream') or '').strip()}",
                f"Symbols: {symbols}",
                f"Decision Tier: {str(config.get('source_decision_tier') or '').strip()}",
            ]
        )
    return "\n".join(
        [
            "🟢 Bound shadow started",
            f"{compact_identity(selected)}",
            f"Tier {str(config.get('source_decision_tier') or '-').strip()} | pack {short_id(row_pack_id(selected))}",
            "Mode: shadow/paper",
            "Caveat: paper flow only, not live execution.",
        ]
    )


def render_finish_message(
    launch_result: dict[str, Any],
    event_counts: dict[str, int],
    trade_counts: dict[str, int],
    *,
    style: str,
    selected: dict[str, Any] | None = None,
) -> str:
    processed_event_count = launch_result.get("summary_processed_event_count", "unknown")
    if style == "verbose":
        lines = [
            "QuantLab Bound Shadow Launch FINISH",
            f"Status: {str(launch_result.get('launch_status') or 'UNKNOWN')}",
            "Mode: shadow/paper synthetic",
            f"Strategy ID: {str(launch_result.get('selected_strategy_id') or '').strip()}",
            f"Pack: {str(launch_result.get('selected_pack_id') or '').strip()}",
            f"Processed Events: {processed_event_count}",
            f"Persisted decision events: {event_counts['DECISION']}",
            f"Persisted risk rejects: {event_counts['RISK_REJECT']}",
            f"Persisted fill events: {event_counts['FILL']}",
            f"Synthetic trades open: {trade_counts['OPEN']}",
            f"Synthetic trades closed: {trade_counts['CLOSED']}",
        ]
        if event_counts["FILL"] > 0:
            lines.append("Note: 'fill' means persisted shadow FILL evidence, not exchange-confirmed live execution.")
        elif sum(event_counts.values()) == 0 and sum(trade_counts.values()) == 0:
            lines.append("Note: no persisted execution-event or synthetic trade activity observed in this bounded run.")
        return "\n".join(lines)
    family = str(launch_result.get("selected_family_id") or "").strip()
    exchange = str(launch_result.get("selected_exchange") or "").strip()
    symbol = result_symbol(launch_result)
    if selected:
        if not family:
            family = str(selected.get("family_id") or "").strip()
        if exchange == "unknown_exchange" or not exchange:
            exchange = str(selected.get("exchange") or "").strip()
        if symbol == "?":
            symbol = primary_symbol(selected)
    family = family or "bound_family"
    exchange = exchange or "unknown_exchange"
    status = str(launch_result.get("launch_status") or "UNKNOWN").strip()
    emoji = "📈" if "WITH_EXECUTION_ACTIVITY" in status else ("⚠️" if status == "INVALID" else "📋")
    lines = [
        f"{emoji} Bound shadow finished",
        f"{family} | {str(symbol).upper()} | {exchange}",
        f"Status: {status}",
        f"Events {processed_event_count} | fill {event_counts['FILL']} | trades {trade_counts['OPEN'] + trade_counts['CLOSED']}",
    ]
    if event_counts["FILL"] > 0:
        lines.append("Paper fills observed; not exchange-confirmed.")
    elif sum(event_counts.values()) == 0 and sum(trade_counts.values()) == 0:
        lines.append("No persisted execution or trade activity.")
    else:
        lines.append(f"Decisions {event_counts['DECISION']} | rejects {event_counts['RISK_REJECT']}")
    lines.append(f"Pack {short_id(str(launch_result.get('selected_pack_id') or '').strip())}")
    return "\n".join(lines)


def render_invalid_message(launch_result: dict[str, Any], *, style: str) -> str:
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Bound Shadow Launch INVALID",
                "Mode: shadow/paper synthetic",
                f"Strategy ID: {str(launch_result.get('selected_strategy_id') or '').strip()}",
                f"Pack: {str(launch_result.get('selected_pack_id') or '').strip()}",
                f"Reason: {str(launch_result.get('invalid_reason') or 'unknown').strip()}",
            ]
        )
    return "\n".join(
        [
            "⚠️ Bound shadow invalid",
            f"Status reason: {str(launch_result.get('invalid_reason') or 'unknown').strip()}",
            f"Pack {short_id(str(launch_result.get('selected_pack_id') or '').strip())}",
            "Mode: shadow/paper",
        ]
    )


def render_failed_message(selected: dict[str, Any] | None, reason: str, *, style: str) -> str:
    strategy_id = row_strategy_id(selected or {}) if isinstance(selected, dict) else ""
    pack_id = row_pack_id(selected or {}) if isinstance(selected, dict) else ""
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Bound Shadow Launch FAILED",
                "Mode: shadow/paper synthetic",
                f"Strategy ID: {strategy_id}",
                f"Pack: {pack_id}",
                f"Reason: {reason}",
            ]
        )
    return "\n".join(
        [
            "🔴 Bound shadow failed",
            f"{compact_identity(selected or {})}",
            f"Reason: {reason}",
            f"Pack {short_id(pack_id)} | Mode: shadow/paper",
        ]
    )


def resolve_processed_event_count(bound_launch_result: dict[str, Any] | None, child_launch_result_path: Path) -> Any:
    if isinstance(bound_launch_result, dict):
        value = bound_launch_result.get("summary_processed_event_count")
        if value not in (None, "", "unknown"):
            return value
    if child_launch_result_path.exists():
        child = load_json(child_launch_result_path, "child_launch_result_json")
        value = child.get("summary_processed_event_count")
        if value not in (None, "", "unknown"):
            return value
    return "unknown"


def build_bound_launch_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(Path(args.bound_launch_tool).resolve()),
        "--binding-artifact",
        str(Path(args.binding_artifact).resolve()),
        "--result-json",
        str(Path(args.bound_launch_result_json).resolve()),
        "--generated-watchlist-json",
        str(Path(args.generated_watchlist_json).resolve()),
        "--child-launch-result-json",
        str(Path(args.child_launch_result_json).resolve()),
        "--child-batch-result-json",
        str(Path(args.child_batch_result_json).resolve()),
        "--refresh-result-json",
        str(Path(args.refresh_result_json).resolve()),
        "--operator-snapshot-json",
        str(Path(args.operator_snapshot_json).resolve()),
        "--execution-review-queue-json",
        str(Path(args.execution_review_queue_json).resolve()),
        "--execution-events-jsonl",
        str(Path(args.execution_events_jsonl).resolve()),
        "--trade-ledger-jsonl",
        str(Path(args.trade_ledger_jsonl).resolve()),
        "--batch-stdout-log",
        str(Path(args.batch_stdout_log).resolve()),
        "--batch-stderr-log",
        str(Path(args.batch_stderr_log).resolve()),
        "--audit-base-dir",
        str(Path(args.audit_base_dir).resolve()),
        "--out-dir",
        str(Path(args.out_dir).resolve()),
        "--per-run-timeout-sec",
        str(int(args.per_run_timeout_sec)),
        "--run-max-duration-sec",
        str(int(args.run_max_duration_sec)),
        "--heartbeat-ms",
        str(int(args.heartbeat_ms)),
    ] + (["--pack-id", args.pack_id] if args.pack_id else []) + (["--strategy-id", args.strategy_id] if args.strategy_id else []) + (["--dry-run"] if args.dry_run else [])


def build_result_payload(
    *,
    args: argparse.Namespace,
    telegram: dict[str, Any],
    selected: dict[str, Any] | None,
    bound_launch_command: list[str],
    bound_launch_exit_code: int | str,
    bound_launch_result: dict[str, Any] | None,
    messages: list[dict[str, Any]],
    event_counts: dict[str, int],
    trade_counts: dict[str, int],
    resolved_processed_event_count: Any,
    final_error: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "bound_launch_tool": str(Path(args.bound_launch_tool).resolve()),
        "binding_artifact_path": str(Path(args.binding_artifact).resolve()),
        "bound_launch_result_json": str(Path(args.bound_launch_result_json).resolve()),
        "generated_watchlist_json": str(Path(args.generated_watchlist_json).resolve()),
        "child_launch_result_json": str(Path(args.child_launch_result_json).resolve()),
        "child_batch_result_json": str(Path(args.child_batch_result_json).resolve()),
        "refresh_result_json": str(Path(args.refresh_result_json).resolve()),
        "operator_snapshot_json": str(Path(args.operator_snapshot_json).resolve()),
        "execution_review_queue_json": str(Path(args.execution_review_queue_json).resolve()),
        "execution_events_jsonl": str(Path(args.execution_events_jsonl).resolve()),
        "trade_ledger_jsonl": str(Path(args.trade_ledger_jsonl).resolve()),
        "batch_stdout_log": str(Path(args.batch_stdout_log).resolve()),
        "batch_stderr_log": str(Path(args.batch_stderr_log).resolve()),
        "telegram_send_mode": telegram["send_mode"],
        "telegram_api_base_url": telegram["api_base_url"],
        "telegram_bot_token_masked": telegram["token_masked"],
        "telegram_chat_id_masked": telegram["chat_id_masked"],
        "selection_mode": selection_mode(args),
        "selected_pack_id": row_pack_id(selected or {}) if isinstance(selected, dict) else "",
        "selected_strategy_id": row_strategy_id(selected or {}) if isinstance(selected, dict) else "",
        "selected_family_id": str((selected or {}).get("family_id") or "").strip() if isinstance(selected, dict) else "",
        "bound_launch_command": command_preview(bound_launch_command),
        "bound_launch_exit_code": bound_launch_exit_code,
        "launch_status": str((bound_launch_result or {}).get("launch_status") or "INVALID"),
        "valid_run": bool((bound_launch_result or {}).get("valid_run")),
        "invalid_reason": str((bound_launch_result or {}).get("invalid_reason") or final_error),
        "required_artifacts_ok": bool((bound_launch_result or {}).get("required_artifacts_ok")),
        "matched_execution_event_count": (bound_launch_result or {}).get("matched_execution_event_count"),
        "matched_trade_count": (bound_launch_result or {}).get("matched_trade_count"),
        "summary_processed_event_count": resolved_processed_event_count,
        "messages_attempted": len(messages),
        "messages_sent": sum(1 for item in messages if item["sent"]),
        "error_count": sum(1 for item in messages if not item["sent"]),
        "event_types_attempted": [item["event_type"] for item in messages],
        "event_types_sent": [item["event_type"] for item in messages if item["sent"]],
        "message_results": messages,
        "execution_event_counts": event_counts,
        "synthetic_trade_status_counts": trade_counts,
        "final_error": final_error,
    }


def run_command(cmd: list[str], *, cwd: Path) -> int:
    completed = subprocess.run(cmd, cwd=str(cwd), text=True)
    return int(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    load_env_defaults_from_file(resolve_env_file())
    args = parse_args(argv or sys.argv[1:])
    telegram = {
        "enabled": True,
        "send_mode": "CONFIG_ERROR",
        "token": "",
        "chat_id": "",
        "token_masked": "",
        "chat_id_masked": "",
        "api_base_url": str(args.telegram_api_base_url).rstrip("/"),
    }
    telegram_result_json = Path(args.telegram_result_json).resolve()
    selected: dict[str, Any] | None = None
    bound_launch_result: dict[str, Any] | None = None
    messages: list[dict[str, Any]] = []
    event_counts = {"DECISION": 0, "RISK_REJECT": 0, "FILL": 0}
    trade_counts = {"OPEN": 0, "CLOSED": 0}
    resolved_processed_event_count: Any = "unknown"
    final_error = ""
    bound_launch_exit_code: int | str = "not_run"
    bound_launch_command: list[str] = []

    try:
        telegram = telegram_config(args)
        binding_artifact = load_binding_artifact(Path(args.binding_artifact).resolve())
        selected = select_bound_item(list(binding_artifact.get("items") or []), args)
        start_message = render_start_message(selected, style=args.telegram_message_style)
        start_sent, start_reason = send_telegram_message(telegram, text=start_message)
        messages.append({"event_type": "launch_started", "sent": bool(start_sent), "reason": start_reason})

        bound_launch_command = build_bound_launch_command(args)
        bound_launch_exit_code = run_command(bound_launch_command, cwd=ROOT)
        bound_launch_result = load_json(Path(args.bound_launch_result_json).resolve(), "bound_launch_result_json")
        resolved_processed_event_count = resolve_processed_event_count(
            bound_launch_result,
            Path(args.child_launch_result_json).resolve(),
        )
        bound_launch_result["summary_processed_event_count"] = resolved_processed_event_count

        if bound_launch_result.get("launch_status") == "INVALID":
            finish_text = render_invalid_message(bound_launch_result, style=args.telegram_message_style)
            event_type = "launch_invalid"
        else:
            event_counts = count_execution_events(bound_launch_result, load_jsonl(Path(args.execution_events_jsonl).resolve()))
            trade_counts = count_trade_statuses(bound_launch_result, load_jsonl(Path(args.trade_ledger_jsonl).resolve()))
            finish_text = render_finish_message(
                bound_launch_result,
                event_counts,
                trade_counts,
                style=args.telegram_message_style,
                selected=selected,
            )
            status = str(bound_launch_result.get("launch_status") or "").strip()
            event_type = "launch_finished_valid_with_execution_activity" if status == "VALID_WITH_EXECUTION_ACTIVITY" else "launch_finished_valid_no_execution_activity"
        finish_sent, finish_reason = send_telegram_message(telegram, text=finish_text)
        messages.append({"event_type": event_type, "sent": bool(finish_sent), "reason": finish_reason})
    except BoundShadowTelegramError as exc:
        final_error = str(exc)
        failed_text = render_failed_message(selected, final_error, style=args.telegram_message_style)
        if telegram.get("send_mode") in {"DRY_RUN", "LIVE"}:
            failed_sent, failed_reason = send_telegram_message(telegram, text=failed_text)
        else:
            failed_sent, failed_reason = False, "TELEGRAM_CONFIG_ERROR"
        messages.append({"event_type": "launch_failed", "sent": bool(failed_sent), "reason": failed_reason})

    payload = build_result_payload(
        args=args,
        telegram=telegram,
        selected=selected,
        bound_launch_command=bound_launch_command,
        bound_launch_exit_code=bound_launch_exit_code,
        bound_launch_result=bound_launch_result,
        messages=messages,
        event_counts=event_counts,
        trade_counts=trade_counts,
        resolved_processed_event_count=resolved_processed_event_count,
        final_error=final_error,
    )
    write_json(telegram_result_json, payload)

    print(f"telegram_result_json={telegram_result_json}")
    print(f"launch_status={payload['launch_status']}")
    print(f"valid_run={1 if payload['valid_run'] else 0}")
    print(f"messages_attempted={payload['messages_attempted']}")
    print(f"messages_sent={payload['messages_sent']}")
    print(f"error_count={payload['error_count']}")
    print(f"telegram_send_mode={payload['telegram_send_mode']}")
    if payload["launch_status"] == "INVALID" or payload["final_error"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
