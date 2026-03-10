#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_BINDING_ARTIFACT = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_CHILD_TOOL = ROOT / "tools" / "run-soft-live.js"
DEFAULT_SESSION_JSON = ROOT / "tools" / "shadow_state" / "momentum_continuous_shadow_session_v1.json"
DEFAULT_SESSION_ARTIFACTS_DIR = ROOT / "tools" / "shadow_state" / "momentum_continuous_shadow_session_artifacts_v1"
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
DEFAULT_RUN_MAX_DURATION_SEC = 21600
DEFAULT_HEARTBEAT_MS = 5000
DEFAULT_HEARTBEAT_TIMEOUT_SEC = 20
DEFAULT_POLL_INTERVAL_SEC = 1.0
DEFAULT_MAX_RUNTIME_SEC = 0
SCHEMA_VERSION = "momentum_continuous_shadow_session_v1"
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
TARGET_FAMILY_ID = "momentum_v1"
EPSILON = 1e-12

STOP_REQUESTED = False
STOP_REASON = ""


class MomentumContinuousShadowError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise MomentumContinuousShadowError(message)


def request_stop(reason: str) -> None:
    global STOP_REQUESTED, STOP_REASON
    STOP_REQUESTED = True
    STOP_REASON = str(reason or "manual_stop")


def _signal_handler(signum: int, _frame: Any) -> None:
    if signum == signal.SIGINT:
        request_stop("signal:SIGINT")
        return
    if signum == signal.SIGTERM:
        request_stop("signal:SIGTERM")
        return
    request_stop(f"signal:{signum}")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_monotonic() -> float:
    return time.monotonic()


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
        if key in os.environ:
            continue
        os.environ[key] = value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a continuous momentum_v1 shadow session with compact Telegram alerts.")
    parser.add_argument("--binding-artifact", default=str(DEFAULT_BINDING_ARTIFACT))
    parser.add_argument("--child-tool", default=str(DEFAULT_CHILD_TOOL))
    parser.add_argument("--strategy-id", default="")
    parser.add_argument("--pack-id", default="")
    parser.add_argument("--session-json", default=str(DEFAULT_SESSION_JSON))
    parser.add_argument("--session-artifacts-dir", default=str(DEFAULT_SESSION_ARTIFACTS_DIR))
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--heartbeat-timeout-sec", type=int, default=DEFAULT_HEARTBEAT_TIMEOUT_SEC)
    parser.add_argument("--poll-interval-sec", type=float, default=DEFAULT_POLL_INTERVAL_SEC)
    parser.add_argument("--session-max-runtime-sec", type=int, default=DEFAULT_MAX_RUNTIME_SEC)
    parser.add_argument("--stop-file", default="")
    parser.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    parser.add_argument("--telegram-message-style", choices=["compact", "verbose"], default="compact")
    parser.add_argument("--telegram-dry-run", action="store_true")
    args = parser.parse_args(argv)
    args.strategy_id = str(args.strategy_id or "").strip()
    args.pack_id = str(args.pack_id or "").strip()
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    if args.heartbeat_timeout_sec <= 0:
        fail(f"invalid_heartbeat_timeout_sec:{args.heartbeat_timeout_sec}")
    if args.poll_interval_sec <= 0:
        fail(f"invalid_poll_interval_sec:{args.poll_interval_sec}")
    if args.session_max_runtime_sec < 0:
        fail(f"invalid_session_max_runtime_sec:{args.session_max_runtime_sec}")
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


def parse_int(raw: Any) -> int:
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return 0
    return parsed


def parse_float(raw: Any) -> float | None:
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def parse_ts_ns(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def row_pack_id(item: dict[str, Any]) -> str:
    return str(item.get("pack_id") or "").strip()


def row_strategy_id(item: dict[str, Any]) -> str:
    return str(item.get("strategy_id") or "").strip()


def primary_symbol(item: dict[str, Any]) -> str:
    values = [str(value).upper() for value in list(item.get("symbols") or []) if str(value).strip()]
    if not values:
        return "?"
    return values[0]


def short_id(value: str, *, tail: int = 8) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    if len(raw) <= tail:
        return raw
    return f"...{raw[-tail:]}"


def short_session_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    prefix = "momentum_continuous_session_"
    if raw.startswith(prefix):
        raw = raw[len(prefix):]
    raw = raw.replace("_pid", "/p")
    return raw


def compact_identity(item: dict[str, Any]) -> str:
    family = str(item.get("family_id") or "").strip() or TARGET_FAMILY_ID
    symbol = primary_symbol(item)
    exchange = str(item.get("exchange") or "").strip() or "unknown_exchange"
    return f"{family} | {symbol} | {exchange}"


def selection_mode(args: argparse.Namespace) -> str:
    if args.strategy_id and args.pack_id:
        return "STRATEGY_ID_AND_PACK_ID"
    if args.strategy_id:
        return "STRATEGY_ID"
    if args.pack_id:
        return "PACK_ID"
    return "FIRST_BOUND_MOMENTUM"


def load_binding_artifact(path: Path) -> dict[str, Any]:
    obj = load_json(path, "binding_artifact")
    if str(obj.get("schema_version") or "").strip() != BINDING_SCHEMA_VERSION:
        fail(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"binding_artifact_items_invalid:{path}")
    return obj


def is_momentum_bound(item: dict[str, Any]) -> bool:
    return (
        str(item.get("family_id") or "").strip() == TARGET_FAMILY_ID
        and str(item.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE
    )


def select_momentum_item(items: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
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
        selected = next((item for item in items if is_momentum_bound(item)), None)
        if selected is None:
            fail("no_bound_shadow_runnable_momentum_rows")
    if not is_momentum_bound(selected):
        fail(f"selected_row_not_launchable:{str(selected.get('family_id') or '').strip()}:{str(selected.get('runtime_binding_status') or '').strip()}")
    return selected


def create_session_id() -> str:
    return datetime.now(timezone.utc).strftime("momentum_continuous_session_%Y%m%dT%H%M%SZ") + f"_pid{os.getpid()}"


def build_session_paths(*, session_artifacts_dir: Path, session_id: str) -> dict[str, Path]:
    session_dir = session_artifacts_dir / session_id
    return {
        "session_dir": session_dir,
        "session_binding_json": session_dir / "session_binding_artifact.json",
        "summary_json": session_dir / "soft_live_summary.json",
        "stdout_log": session_dir / "soft_live_stdout.log",
        "stderr_log": session_dir / "soft_live_stderr.log",
        "audit_spool_dir": session_dir / "audit_spool",
    }


def build_session_binding_artifact(source: dict[str, Any], selected: dict[str, Any], *, generated_ts_utc: str) -> dict[str, Any]:
    row = json.loads(json.dumps(selected))
    return {
        "schema_version": BINDING_SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "source_candidate_strategy_contract_json": str(source.get("source_candidate_strategy_contract_json") or ""),
        "source_binding_map_json": str(source.get("source_binding_map_json") or ""),
        "source_row_count": 1,
        "translated_spec_count": 1 if str(row.get("translation_status") or "").strip() == "TRANSLATABLE" else 0,
        "bound_shadow_runnable_count": 1,
        "unbound_no_runtime_impl_count": 0,
        "unbound_config_gap_count": 0,
        "unbound_translation_rejected_count": 0,
        "bindable_family_ids": [TARGET_FAMILY_ID],
        "items": [row],
    }


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


def append_message_result(state: dict[str, Any], *, event_type: str, sent: bool, reason: str) -> None:
    state["telegram_messages_attempted"] += 1
    state["telegram_event_types_attempted"].append(event_type)
    if sent:
        state["telegram_messages_sent"] += 1
        state["telegram_event_types_sent"].append(event_type)
    else:
        state["telegram_error_count"] += 1
    state["latest_message_results"].append({"event_type": event_type, "sent": bool(sent), "reason": str(reason or "")})
    state["latest_message_results"] = state["latest_message_results"][-20:]


def send_session_message(state: dict[str, Any], telegram: dict[str, Any], *, event_type: str, text: str) -> None:
    sent, reason = send_telegram_message(telegram, text=text)
    append_message_result(state, event_type=event_type, sent=sent, reason=reason)


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def build_child_env(selected: dict[str, Any], paths: dict[str, Path], args: argparse.Namespace) -> dict[str, str]:
    config = selected.get("runtime_strategy_config")
    if not isinstance(config, dict):
        fail("selected_runtime_strategy_config_missing")
    binding_mode = str(config.get("binding_mode") or "").strip()
    if binding_mode != "PAPER_DIRECTIONAL_V1":
        fail(f"selected_binding_mode_not_supported:{binding_mode}")
    env = os.environ.copy()
    env["GO_LIVE_EXCHANGE"] = str(selected.get("exchange") or "").strip()
    env["GO_LIVE_SYMBOLS"] = ",".join(str(value).upper() for value in list(selected.get("symbols") or []) if str(value).strip())
    env["GO_LIVE_STRATEGY"] = str(selected.get("runtime_strategy_file") or "").strip()
    env["GO_LIVE_STRATEGY_CONFIG"] = json.dumps(config, sort_keys=True)
    env["STRATEGY_MODE"] = binding_mode
    env["POSITION_SIZE_MODE"] = "FIXED"
    env["CORE_LIVE_WS_ENABLED"] = str(env.get("CORE_LIVE_WS_ENABLED") or "1")
    env["GO_LIVE_DATASET_PARQUET"] = str(env.get("GO_LIVE_DATASET_PARQUET") or "live")
    env["GO_LIVE_DATASET_META"] = str(env.get("GO_LIVE_DATASET_META") or "live")
    env["AUDIT_ENABLED"] = "1"
    env["AUDIT_SPOOL_DIR"] = str(paths["audit_spool_dir"].resolve())
    env["SOFT_LIVE_SUMMARY_JSON"] = str(paths["summary_json"].resolve())
    env["SHADOW_BATCH_SUMMARY_JSON"] = str(paths["summary_json"].resolve())
    env["RUN_MAX_DURATION_SEC"] = str(int(args.run_max_duration_sec))
    env["SOFT_LIVE_HEARTBEAT_MS"] = str(int(args.heartbeat_ms))
    return env


def build_child_command(args: argparse.Namespace) -> list[str]:
    child_path = Path(args.child_tool).resolve()
    if child_path.suffix == ".py":
        return [sys.executable, str(child_path)]
    return ["node", str(child_path)]


def start_child_process(cmd: list[str], *, env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        start_new_session=True,
    )


def terminate_child_process(proc: subprocess.Popen[str], *, force_after_sec: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + force_after_sec
    while proc.poll() is None and time.time() < deadline:
        time.sleep(0.2)
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return


def stop_file_requested(stop_file: Path | None) -> bool:
    if stop_file is None:
        return False
    return stop_file.exists()


class AuditCursor:
    def __init__(self) -> None:
        self._line_counts: dict[str, int] = {}

    def read_new_rows(self, root: Path) -> list[dict[str, Any]]:
        if not root.exists():
            return []
        rows: list[dict[str, Any]] = []
        for path in sorted(root.rglob("*.jsonl")):
            text = path.read_text(encoding="utf-8")
            lines = [line for line in text.splitlines() if line.strip()]
            key = str(path.resolve())
            start = self._line_counts.get(key, 0)
            if start < 0:
                start = 0
            for line in lines[start:]:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
            self._line_counts[key] = len(lines)
        return rows


def direction_from_size(size: float) -> str:
    if size > EPSILON:
        return "LONG"
    if size < -EPSILON:
        return "SHORT"
    return "FLAT"


def compute_unrealized(size: float, avg_entry_price: float | None, current_price: float | None) -> float | None:
    if direction_from_size(size) == "FLAT":
        return 0.0
    if avg_entry_price is None or current_price is None:
        return None
    if size > 0:
        return abs(size) * (current_price - avg_entry_price)
    return abs(size) * (avg_entry_price - current_price)


def fill_turnover_quote(fill: dict[str, Any]) -> float | None:
    fill_value = parse_float(fill.get("fill_value"))
    if fill_value is not None and fill_value > 0:
        return fill_value
    qty = parse_float(fill.get("qty"))
    fill_price = parse_float(fill.get("fill_price"))
    if qty is None or qty <= 0 or fill_price is None or fill_price <= 0:
        return None
    return qty * fill_price


def latest_mark_price_before(mark_events: list[dict[str, Any]], symbol: str, boundary_ns: int) -> float | None:
    latest_ts: int | None = None
    latest_price: float | None = None
    for event in mark_events:
        if str(event.get("symbol") or "").strip().upper() != symbol:
            continue
        ts_ns = parse_ts_ns(event.get("ts_event"))
        price = parse_float(event.get("mark_price"))
        if ts_ns is None or price is None or price <= 0:
            continue
        if ts_ns > boundary_ns:
            continue
        if latest_ts is None or ts_ns >= latest_ts:
            latest_ts = ts_ns
            latest_price = price
    return latest_price


def signed_position_size_at(fill_events: list[dict[str, Any]], symbol: str, boundary_ns: int) -> float:
    size = 0.0
    for event in fill_events:
        if str(event.get("symbol") or "").strip().upper() != symbol:
            continue
        ts_ns = parse_ts_ns(event.get("ts_event"))
        qty = parse_float(event.get("qty"))
        side = str(event.get("side") or "").strip().upper()
        if ts_ns is None or qty is None or qty <= 0 or side not in {"BUY", "SELL"}:
            continue
        if ts_ns > boundary_ns:
            continue
        size += qty if side == "BUY" else -qty
    return size


def usd_text(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f} USD"


def pnl_source_label(source_field: str) -> str:
    mapping = {
        "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate": "net+funding+exit",
        "mark_to_market_pnl_quote_net_after_funding": "net+funding",
        "realized_pnl_quote_net_after_funding": "realized net+funding",
    }
    return mapping.get(source_field, source_field or "unknown")


class LivePaperState:
    def __init__(self, *, selected: dict[str, Any]) -> None:
        self.selected = selected
        self.symbol = primary_symbol(selected)
        self.exchange = str(selected.get("exchange") or "").strip().lower()
        self.family_id = str(selected.get("family_id") or TARGET_FAMILY_ID).strip()
        self.live_run_id = ""
        self.decision_event_count = 0
        self.risk_reject_event_count = 0
        self.fill_event_count = 0
        self.mark_price_event_count = 0
        self.funding_events_count = 0
        self.position_size = 0.0
        self.avg_entry_price: float | None = None
        self.realized_pnl_quote_gross = 0.0
        self.total_fee_quote = 0.0
        self.turnover_quote = 0.0
        self.opening_turnover_quote = 0.0
        self.closing_turnover_quote = 0.0
        self.latest_mark_price: float | None = None
        self.latest_mark_ts_event = ""
        self.fill_events: list[dict[str, Any]] = []
        self.mark_events: list[dict[str, Any]] = []
        self.funding_by_boundary: dict[int, dict[str, Any]] = {}
        self.applied_funding_boundaries: set[int] = set()
        self.funding_cost_quote_total = 0.0
        self.funding_support_status = "NO_FILL_ACTIVITY"
        self.funding_alignment_status = "NO_FILL_ACTIVITY"
        self.funding_rate_source = "UNAVAILABLE"
        self.funding_windows_crossed_count = 0
        self.funding_applied_count = 0
        self.funding_windows: list[dict[str, Any]] = []
        self.trade_open_count = 0
        self.trade_exit_count = 0
        self.trade_reversal_count = 0
        self.action_sequence: list[dict[str, Any]] = []
        self.last_trade_message_key = ""
        self.last_profitability_key = ""
        self.current_pnl_usd: float | None = None
        self.current_pnl_source_field = ""
        self.profitability_status = "NO_FILL_ACTIVITY"
        self.cost_accounting_status = "NO_FILL_ACTIVITY"
        self.last_action = "STAY_FLAT"
        self.last_action_ts_event = ""

    def _record_action(self, *, action: str, fill: dict[str, Any]) -> None:
        payload = {
            "action": action,
            "ts_event": str(fill.get("ts_event") or ""),
            "side": str(fill.get("side") or "").strip().upper(),
            "qty": parse_float(fill.get("qty")),
            "fill_price": parse_float(fill.get("fill_price")),
        }
        self.action_sequence.append(payload)
        self.action_sequence = self.action_sequence[-50:]
        self.last_action = action
        self.last_action_ts_event = payload["ts_event"]

    def _apply_fill(self, fill: dict[str, Any]) -> str:
        qty = float(fill["qty"])
        fill_price = float(fill["fill_price"])
        fill_fee = parse_float(fill.get("fill_fee")) or 0.0
        fill_side = str(fill["side"]).upper()
        old_size = self.position_size
        old_direction = direction_from_size(old_size)
        signed_qty = qty if fill_side == "BUY" else -qty
        fill_turnover = fill_turnover_quote(fill) or 0.0
        self.turnover_quote += fill_turnover
        self.total_fee_quote += fill_fee
        self.fill_event_count += 1
        self.fill_events.append(fill)

        if old_direction == "FLAT":
            self.position_size = signed_qty
            self.avg_entry_price = fill_price
            self.opening_turnover_quote += fill_turnover
            action = "LONG_OPEN" if self.position_size > 0 else "SHORT_OPEN"
            self.trade_open_count += 1
            self._record_action(action=action, fill=fill)
            return action

        if (old_direction == "LONG" and fill_side == "BUY") or (old_direction == "SHORT" and fill_side == "SELL"):
            old_abs = abs(old_size)
            new_abs = old_abs + qty
            self.avg_entry_price = ((old_abs * float(self.avg_entry_price or 0.0)) + (qty * fill_price)) / new_abs
            self.position_size = old_size + signed_qty
            self.opening_turnover_quote += fill_turnover
            action = "LONG_ADD" if old_direction == "LONG" else "SHORT_ADD"
            self._record_action(action=action, fill=fill)
            return action

        closing_qty = min(abs(old_size), qty)
        opening_qty = max(0.0, qty - closing_qty)
        self.closing_turnover_quote += closing_qty * fill_price
        if opening_qty > 0:
            self.opening_turnover_quote += opening_qty * fill_price
        entry_price = float(self.avg_entry_price or 0.0)
        if old_direction == "LONG":
            self.realized_pnl_quote_gross += closing_qty * (fill_price - entry_price)
        else:
            self.realized_pnl_quote_gross += closing_qty * (entry_price - fill_price)

        new_size = old_size + signed_qty
        if abs(new_size) <= EPSILON:
            self.position_size = 0.0
            self.avg_entry_price = None
            action = "LONG_EXIT" if old_direction == "LONG" else "SHORT_EXIT"
            self.trade_exit_count += 1
            self._record_action(action=action, fill=fill)
            return action

        new_direction = direction_from_size(new_size)
        if new_direction != old_direction:
            self.position_size = new_size
            self.avg_entry_price = fill_price
            action = "LONG_TO_SHORT_REVERSAL" if old_direction == "LONG" else "SHORT_TO_LONG_REVERSAL"
            self.trade_reversal_count += 1
            self._record_action(action=action, fill=fill)
            return action

        self.position_size = new_size
        action = "LONG_REDUCE" if old_direction == "LONG" else "SHORT_REDUCE"
        self._record_action(action=action, fill=fill)
        return action

    def _apply_funding_up_to(self, current_ts_ns: int) -> None:
        pending = sorted(boundary for boundary in self.funding_by_boundary if boundary <= current_ts_ns and boundary not in self.applied_funding_boundaries)
        if not pending:
            return
        for boundary_ns in pending:
            self.funding_windows_crossed_count += 1
            funding = self.funding_by_boundary[boundary_ns]
            rate = parse_float(funding.get("funding_rate"))
            mark_price = latest_mark_price_before(self.mark_events, self.symbol, boundary_ns)
            signed_qty = signed_position_size_at(self.fill_events, self.symbol, boundary_ns)
            position_direction = direction_from_size(signed_qty)
            window = {
                "next_funding_ts": str(boundary_ns),
                "funding_rate": rate,
                "position_direction": position_direction,
                "position_qty": abs(signed_qty),
                "mark_price": mark_price,
                "funding_cost_quote": None,
                "alignment_status": "NO_POSITION",
            }
            if position_direction == "FLAT":
                self.applied_funding_boundaries.add(boundary_ns)
                self.funding_windows.append(window)
                continue
            if rate is None or mark_price is None:
                self.applied_funding_boundaries.add(boundary_ns)
                window["alignment_status"] = "MARK_PRICE_MISSING"
                self.funding_windows.append(window)
                continue
            funding_cost = signed_qty * mark_price * rate
            self.applied_funding_boundaries.add(boundary_ns)
            self.funding_applied_count += 1
            self.funding_cost_quote_total += funding_cost
            window["funding_cost_quote"] = funding_cost
            window["alignment_status"] = "APPLIED"
            self.funding_windows.append(window)
        self.funding_windows = self.funding_windows[-20:]

    def process_audit_row(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        action = str(row.get("action") or "").strip().upper()
        meta = row.get("metadata")
        if not isinstance(meta, dict):
            return []
        live_run_id = str(meta.get("live_run_id") or "").strip()
        if live_run_id and not self.live_run_id:
            self.live_run_id = live_run_id

        ts_ns = parse_ts_ns(meta.get("ts_event"))
        if ts_ns is not None:
            self._apply_funding_up_to(ts_ns)

        notifications: list[dict[str, Any]] = []
        if action == "DECISION":
            self.decision_event_count += 1
            return notifications
        if action == "RISK_REJECT":
            self.risk_reject_event_count += 1
            return notifications
        if action == "MARK_PRICE":
            symbol = str(meta.get("symbol") or "").strip().upper()
            mark_price = parse_float(meta.get("mark_price"))
            if symbol == self.symbol and ts_ns is not None and mark_price is not None and mark_price > 0:
                self.mark_price_event_count += 1
                self.latest_mark_price = mark_price
                self.latest_mark_ts_event = str(meta.get("ts_event") or "")
                self.mark_events.append(
                    {
                        "symbol": symbol,
                        "ts_event": str(meta.get("ts_event") or ""),
                        "mark_price": mark_price,
                    }
                )
            return notifications
        if action == "FUNDING":
            symbol = str(meta.get("symbol") or "").strip().upper()
            boundary_ns = parse_ts_ns(meta.get("next_funding_ts"))
            funding_rate = parse_float(meta.get("funding_rate"))
            if symbol == self.symbol and boundary_ns is not None and funding_rate is not None:
                self.funding_events_count += 1
                self.funding_rate_source = "LIVE_STREAM_FUNDING"
                existing = self.funding_by_boundary.get(boundary_ns)
                current = {
                    "symbol": symbol,
                    "ts_event": str(meta.get("ts_event") or ""),
                    "next_funding_ts": str(meta.get("next_funding_ts") or ""),
                    "funding_rate": funding_rate,
                }
                if existing is None or (parse_ts_ns(existing.get("ts_event")) or 0) <= (ts_ns or 0):
                    self.funding_by_boundary[boundary_ns] = current
                if ts_ns is not None:
                    self._apply_funding_up_to(ts_ns)
            return notifications
        if action != "FILL":
            return notifications

        symbol = str(meta.get("symbol") or "").strip().upper()
        side = str(meta.get("side") or "").strip().upper()
        qty = parse_float(meta.get("qty"))
        fill_price = parse_float(meta.get("fill_price"))
        if symbol != self.symbol or side not in {"BUY", "SELL"} or qty is None or qty <= 0 or fill_price is None or fill_price <= 0:
            return notifications

        fill = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "fill_price": fill_price,
            "fill_fee": parse_float(meta.get("fill_fee")),
            "fill_value": parse_float(meta.get("fill_value")),
            "ts_event": str(meta.get("ts_event") or ""),
        }
        action_label = self._apply_fill(fill)
        notifications.append({"type": "trade_action", "action": action_label, "fill": fill})
        return notifications

    def current_metrics(self) -> dict[str, Any]:
        final_direction = direction_from_size(self.position_size)
        unrealized = compute_unrealized(self.position_size, self.avg_entry_price, self.latest_mark_price)
        realized_net_after_funding = self.realized_pnl_quote_gross - self.total_fee_quote - self.funding_cost_quote_total
        effective_fee_rate = (self.total_fee_quote / self.turnover_quote) if self.turnover_quote > 0 else None
        final_position_notional = abs(self.position_size) * self.latest_mark_price if final_direction != "FLAT" and self.latest_mark_price is not None else 0.0 if final_direction == "FLAT" else None
        estimated_exit_fee = (
            final_position_notional * effective_fee_rate
            if final_direction != "FLAT" and final_position_notional is not None and effective_fee_rate is not None
            else 0.0 if final_direction == "FLAT"
            else None
        )
        mark_to_market_net_after_funding = (
            realized_net_after_funding + unrealized
            if unrealized is not None
            else None
        )
        mark_to_market_net_after_funding_and_exit_estimate = (
            mark_to_market_net_after_funding - estimated_exit_fee
            if mark_to_market_net_after_funding is not None and estimated_exit_fee is not None
            else None
        )
        if self.fill_event_count == 0:
            funding_support_status = "NO_FILL_ACTIVITY"
            funding_alignment_status = "NO_FILL_ACTIVITY"
        elif any(window.get("alignment_status") == "MARK_PRICE_MISSING" for window in self.funding_windows):
            funding_support_status = "FUNDING_COST_PARTIAL_MARK_MISSING"
            funding_alignment_status = "PARTIAL_MARK_PRICE_MISSING"
        elif self.funding_events_count == 0:
            funding_support_status = "NO_FUNDING_EVENTS_OBSERVED"
            funding_alignment_status = "NO_FUNDING_EVENTS_OBSERVED"
        elif self.funding_windows_crossed_count == 0:
            funding_support_status = "NO_FUNDING_WINDOW_CROSSED"
            funding_alignment_status = "NO_FUNDING_WINDOW_CROSSED"
        elif self.funding_applied_count == 0:
            funding_support_status = "NO_POSITION_AT_CROSSED_WINDOW"
            funding_alignment_status = "ALL_CROSSED_WINDOWS_FLAT"
        else:
            funding_support_status = "FUNDING_COST_BACKED"
            funding_alignment_status = "ALL_APPLIED_WINDOWS_MARK_PRICE_BACKED"

        if self.fill_event_count == 0:
            cost_accounting_status = "NO_FILL_ACTIVITY"
            profitability_status = "NO_FILL_ACTIVITY"
            pnl_source_field = ""
            current_pnl_usd = None
        elif funding_support_status in {"NO_FUNDING_EVENTS_OBSERVED", "FUNDING_COST_PARTIAL_MARK_MISSING"}:
            cost_accounting_status = "NET_FEE_BACKED_FUNDING_PARTIAL"
            profitability_status = "PROFITABILITY_PARTIAL_FUNDING_MISSING"
            pnl_source_field = "mark_to_market_pnl_quote_net_after_funding" if mark_to_market_net_after_funding is not None else "realized_pnl_quote_net_after_funding"
            current_pnl_usd = mark_to_market_net_after_funding if mark_to_market_net_after_funding is not None else realized_net_after_funding
        elif final_direction == "FLAT":
            cost_accounting_status = "NET_FEE_BACKED_CLOSED_FUNDING_AWARE"
            profitability_status = "NET_AFTER_FEES_AND_FUNDING"
            pnl_source_field = "realized_pnl_quote_net_after_funding"
            current_pnl_usd = realized_net_after_funding
        elif mark_to_market_net_after_funding_and_exit_estimate is not None:
            cost_accounting_status = "NET_FEE_BACKED_MARK_TO_MARKET_FUNDING_AWARE"
            profitability_status = "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE"
            pnl_source_field = "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate"
            current_pnl_usd = mark_to_market_net_after_funding_and_exit_estimate
        elif mark_to_market_net_after_funding is not None:
            cost_accounting_status = "NET_FEE_BACKED_MARK_TO_MARKET_FUNDING_AWARE"
            profitability_status = "NET_MARK_TO_MARKET_AFTER_FEES_AND_FUNDING"
            pnl_source_field = "mark_to_market_pnl_quote_net_after_funding"
            current_pnl_usd = mark_to_market_net_after_funding
        else:
            cost_accounting_status = "NET_FEE_BACKED_MARK_PRICE_UNAVAILABLE_FUNDING_AWARE"
            profitability_status = "NET_AFTER_FEES_AND_FUNDING"
            pnl_source_field = "realized_pnl_quote_net_after_funding"
            current_pnl_usd = realized_net_after_funding

        self.funding_support_status = funding_support_status
        self.funding_alignment_status = funding_alignment_status
        self.cost_accounting_status = cost_accounting_status
        self.profitability_status = profitability_status
        self.current_pnl_usd = current_pnl_usd
        self.current_pnl_source_field = pnl_source_field
        return {
            "paper_run_status": "NO_FILL_ACTIVITY" if self.fill_event_count == 0 else ("FILL_BACKED_POSITION_OPEN" if final_direction != "FLAT" else "FILL_BACKED_FLAT"),
            "final_position_direction": final_direction,
            "final_position_qty": abs(self.position_size),
            "final_avg_entry_price": self.avg_entry_price,
            "final_mark_price": self.latest_mark_price,
            "turnover_quote": self.turnover_quote,
            "opening_turnover_quote": self.opening_turnover_quote,
            "closing_turnover_quote": self.closing_turnover_quote,
            "effective_fee_rate": effective_fee_rate,
            "realized_pnl_quote_gross": self.realized_pnl_quote_gross,
            "total_fee_quote": self.total_fee_quote,
            "realized_pnl_quote_net_after_funding": realized_net_after_funding,
            "replayed_unrealized_pnl_quote": unrealized,
            "mark_to_market_pnl_quote_net_after_funding": mark_to_market_net_after_funding,
            "estimated_exit_fee_quote": estimated_exit_fee,
            "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate": mark_to_market_net_after_funding_and_exit_estimate,
            "funding_cost_quote": self.funding_cost_quote_total,
            "funding_support_status": funding_support_status,
            "funding_alignment_status": funding_alignment_status,
            "funding_rate_source": self.funding_rate_source,
            "funding_events_count": self.funding_events_count,
            "funding_windows_crossed_count": self.funding_windows_crossed_count,
            "funding_applied_count": self.funding_applied_count,
            "funding_windows": list(self.funding_windows),
            "cost_accounting_status": cost_accounting_status,
            "profitability_status": profitability_status,
            "operator_pnl_usd": current_pnl_usd,
            "operator_pnl_source_field": pnl_source_field,
            "action_sequence": list(self.action_sequence),
        }


def render_session_started_message(selected: dict[str, Any], *, session_id: str, args: argparse.Namespace) -> str:
    if args.telegram_message_style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Continuous Session START",
                "Mode: shadow/paper",
                f"Session ID: {session_id}",
                f"Strategy ID: {row_strategy_id(selected)}",
                f"Pack: {row_pack_id(selected)}",
                f"Identity: {compact_identity(selected)}",
                f"Run cap: {int(args.run_max_duration_sec)}s | heartbeat {int(args.heartbeat_ms)}ms",
                "Note: paper fills are deterministic paper execution, not live exchange execution.",
            ]
        )
    return "\n".join(
        [
            "🟢 Momentum session started",
            compact_identity(selected),
            f"Session {short_session_id(session_id)}",
            f"Run cap {int(args.run_max_duration_sec)}s | hb {int(args.heartbeat_ms)}ms",
            "Mode: shadow/paper",
            "Caveat: paper fills only, not live execution.",
        ]
    )


def render_trade_message(event_type: str, selected: dict[str, Any], metrics: dict[str, Any], *, action: str, fill: dict[str, Any], style: str) -> str:
    symbol = primary_symbol(selected)
    qty = parse_float(fill.get("qty")) or 0.0
    fill_price = parse_float(fill.get("fill_price"))
    pnl_text = usd_text(parse_float(metrics.get("operator_pnl_usd")))
    if style == "verbose":
        return "\n".join(
            [
                f"QuantLab Momentum {event_type.upper()}",
                "Mode: shadow/paper",
                f"Identity: {compact_identity(selected)}",
                f"Action: {action}",
                f"Qty: {qty}",
                f"Fill Price: {fill_price}",
                f"PnL: {pnl_text}",
                "Note: paper fill-backed actions are deterministic paper fills, not live executed trades.",
            ]
        )
    headline = {
        "trade_opened_long": "📈 LONG open",
        "trade_opened_short": "📈 SHORT open",
        "trade_exited": "📉 Exit",
        "trade_reversed": "🔄 Reversal",
    }.get(event_type, "📈 Trade update")
    lines = [
        f"{headline} | {symbol}",
        f"{action} | qty {qty:.4g} @ {fill_price:.2f}" if fill_price is not None else f"{action} | qty {qty:.4g}",
        f"PnL: {pnl_text}",
        "Paper fill-backed, not live executed.",
    ]
    return "\n".join(lines)


def render_profitability_message(selected: dict[str, Any], metrics: dict[str, Any], *, style: str) -> str:
    pnl_text = usd_text(parse_float(metrics.get("operator_pnl_usd")))
    profitability_status = str(metrics.get("profitability_status") or "UNKNOWN")
    funding_support = str(metrics.get("funding_support_status") or "UNKNOWN")
    source_label = pnl_source_label(str(metrics.get("operator_pnl_source_field") or ""))
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Profitability Update",
                "Mode: shadow/paper",
                f"Identity: {compact_identity(selected)}",
                f"Profitability Status: {profitability_status}",
                f"PnL: {pnl_text}",
                f"Source: {source_label}",
                f"Funding: {funding_support}",
                "Funding-aware only; leverage/margin/liquidation still unsupported.",
            ]
        )
    return "\n".join(
        [
            "💰 Profitability update",
            f"PnL: {pnl_text}",
            f"Status: {profitability_status}",
            f"Funding: {funding_support} | basis {source_label}",
        ]
    )


def render_funding_message(selected: dict[str, Any], metrics: dict[str, Any], *, style: str) -> str:
    funding_cost = parse_float(metrics.get("funding_cost_quote"))
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Funding Update",
                "Mode: shadow/paper",
                f"Identity: {compact_identity(selected)}",
                f"Funding Cost: {usd_text(funding_cost)}",
                f"Funding Support: {str(metrics.get('funding_support_status') or 'UNKNOWN')}",
                f"Applied Windows: {int(metrics.get('funding_applied_count') or 0)}",
            ]
        )
    return "\n".join(
        [
            "💸 Funding applied",
            f"Funding: {usd_text(funding_cost)}",
            f"Support: {str(metrics.get('funding_support_status') or 'UNKNOWN')}",
        ]
    )


def render_warning_message(selected: dict[str, Any], *, reason: str, style: str) -> str:
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session WARNING",
                "Mode: shadow/paper",
                f"Identity: {compact_identity(selected)}",
                f"Reason: {reason}",
            ]
        )
    return "\n".join(
        [
            "⚠️ Momentum warning",
            compact_identity(selected),
            f"Reason: {reason}",
        ]
    )


def render_failed_message(selected: dict[str, Any], *, session_id: str, reason: str, style: str) -> str:
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session FAILED",
                "Mode: shadow/paper",
                f"Session ID: {session_id}",
                f"Identity: {compact_identity(selected)}",
                f"Reason: {reason}",
            ]
        )
    return "\n".join(
        [
            "🔴 Momentum session failed",
            compact_identity(selected),
            f"Session {short_session_id(session_id)}",
            f"Reason: {reason}",
        ]
    )


def render_stopped_message(selected: dict[str, Any], *, session_id: str, stop_reason: str, style: str) -> str:
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session STOPPED",
                "Mode: shadow/paper",
                f"Session ID: {session_id}",
                f"Identity: {compact_identity(selected)}",
                f"Stop Reason: {stop_reason}",
            ]
        )
    return "\n".join(
        [
            "⚠️ Momentum session stopped",
            compact_identity(selected),
            f"Session {short_session_id(session_id)}",
            f"Reason: {stop_reason}",
        ]
    )


def render_summary_message(state: dict[str, Any], selected: dict[str, Any], *, style: str) -> str:
    pnl_text = usd_text(parse_float(state.get("current_pnl_usd")))
    if style == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session SUMMARY",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Identity: {compact_identity(selected)}",
                f"Status: {state['session_status']}",
                f"Processed Events: {state['processed_event_count']}",
                f"Decisions/Fills: {state['decision_event_count']}/{state['fill_event_count']}",
                f"Trades Opened/Exited/Reversed: {state['trade_open_count']}/{state['trade_exit_count']}/{state['trade_reversal_count']}",
                f"PnL: {pnl_text}",
                f"Profitability: {state['profitability_status']}",
                f"Funding: {state['funding_support_status']}",
                "Caveat: flat fee model; no leverage/margin/liquidation realism; no live exchange execution semantics.",
            ]
        )
    return "\n".join(
        [
            "📋 Momentum session summary",
            f"Status {state['session_status']} | session {short_session_id(state['session_id'])}",
            f"Position {state['current_position_direction']} qty={state['current_position_qty']} | fills {state['fill_event_count']}",
            f"Opens {state['trade_open_count']} | exits {state['trade_exit_count']} | reversals {state['trade_reversal_count']}",
            f"PnL: {pnl_text}",
            f"Funding: {state['funding_support_status']}",
            "Caveat: flat fees; no leverage/margin/liquidation realism.",
        ]
    )


def initial_session_state(*, session_id: str, selected: dict[str, Any], telegram: dict[str, Any], args: argparse.Namespace, paths: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "last_updated_ts_utc": utc_now_iso(),
        "session_id": session_id,
        "session_status": "STARTING",
        "start_ts_utc": utc_now_iso(),
        "stop_ts_utc": "",
        "stop_reason": "",
        "selection_mode": selection_mode(args),
        "selected_strategy_id": row_strategy_id(selected),
        "selected_pack_id": row_pack_id(selected),
        "selected_family_id": TARGET_FAMILY_ID,
        "selected_exchange": str(selected.get("exchange") or "").strip(),
        "selected_stream": str(selected.get("stream") or "").strip(),
        "selected_symbols": list(selected.get("symbols") or []),
        "run_max_duration_sec": int(args.run_max_duration_sec),
        "heartbeat_ms": int(args.heartbeat_ms),
        "heartbeat_timeout_sec": int(args.heartbeat_timeout_sec),
        "session_max_runtime_sec": int(args.session_max_runtime_sec),
        "session_json": str(Path(args.session_json).resolve()),
        "session_artifacts_dir": str(paths["session_dir"].resolve()),
        "session_binding_artifact": str(paths["session_binding_json"].resolve()),
        "summary_json_path": str(paths["summary_json"].resolve()),
        "stdout_log_path": str(paths["stdout_log"].resolve()),
        "stderr_log_path": str(paths["stderr_log"].resolve()),
        "audit_spool_dir": str(paths["audit_spool_dir"].resolve()),
        "telegram_send_mode": telegram["send_mode"],
        "telegram_api_base_url": telegram["api_base_url"],
        "telegram_bot_token_masked": telegram["token_masked"],
        "telegram_chat_id_masked": telegram["chat_id_masked"],
        "processed_event_count": 0,
        "decision_event_count": 0,
        "risk_reject_event_count": 0,
        "fill_event_count": 0,
        "mark_price_event_count": 0,
        "funding_events_count": 0,
        "trade_open_count": 0,
        "trade_exit_count": 0,
        "trade_reversal_count": 0,
        "current_live_run_id": "",
        "current_position_direction": "FLAT",
        "current_position_qty": 0.0,
        "current_avg_entry_price": None,
        "current_mark_price": None,
        "current_pnl_usd": None,
        "current_pnl_source_field": "",
        "profitability_status": "NO_FILL_ACTIVITY",
        "funding_support_status": "NO_FILL_ACTIVITY",
        "funding_alignment_status": "NO_FILL_ACTIVITY",
        "funding_cost_quote": 0.0,
        "current_paper_run_status": "NO_FILL_ACTIVITY",
        "funding_windows_crossed_count": 0,
        "funding_applied_count": 0,
        "last_action": "STAY_FLAT",
        "last_action_ts_event": "",
        "action_sequence": [],
        "telegram_messages_attempted": 0,
        "telegram_messages_sent": 0,
        "telegram_error_count": 0,
        "telegram_event_types_attempted": [],
        "telegram_event_types_sent": [],
        "latest_message_results": [],
        "child_command": "",
        "child_pid": None,
        "child_exit_code": None,
        "soft_live_done": None,
        "soft_live_error": None,
    }


def apply_metrics_to_state(state: dict[str, Any], live_state: LivePaperState, metrics: dict[str, Any]) -> None:
    state["current_live_run_id"] = live_state.live_run_id
    state["decision_event_count"] = live_state.decision_event_count
    state["risk_reject_event_count"] = live_state.risk_reject_event_count
    state["fill_event_count"] = live_state.fill_event_count
    state["mark_price_event_count"] = live_state.mark_price_event_count
    state["funding_events_count"] = live_state.funding_events_count
    state["processed_event_count"] = max(
        int(state.get("processed_event_count") or 0),
        live_state.decision_event_count
        + live_state.risk_reject_event_count
        + live_state.fill_event_count
        + live_state.mark_price_event_count
        + live_state.funding_events_count,
    )
    state["trade_open_count"] = live_state.trade_open_count
    state["trade_exit_count"] = live_state.trade_exit_count
    state["trade_reversal_count"] = live_state.trade_reversal_count
    state["current_position_direction"] = str(metrics.get("final_position_direction") or "FLAT")
    state["current_position_qty"] = parse_float(metrics.get("final_position_qty")) or 0.0
    state["current_avg_entry_price"] = parse_float(metrics.get("final_avg_entry_price"))
    state["current_mark_price"] = parse_float(metrics.get("final_mark_price"))
    state["current_pnl_usd"] = parse_float(metrics.get("operator_pnl_usd"))
    state["current_pnl_source_field"] = str(metrics.get("operator_pnl_source_field") or "")
    state["profitability_status"] = str(metrics.get("profitability_status") or "NO_FILL_ACTIVITY")
    state["funding_support_status"] = str(metrics.get("funding_support_status") or "NO_FILL_ACTIVITY")
    state["funding_alignment_status"] = str(metrics.get("funding_alignment_status") or "NO_FILL_ACTIVITY")
    state["funding_cost_quote"] = parse_float(metrics.get("funding_cost_quote")) or 0.0
    state["current_paper_run_status"] = str(metrics.get("paper_run_status") or "NO_FILL_ACTIVITY")
    state["funding_windows_crossed_count"] = int(metrics.get("funding_windows_crossed_count") or 0)
    state["funding_applied_count"] = int(metrics.get("funding_applied_count") or 0)
    state["last_action"] = live_state.last_action
    state["last_action_ts_event"] = live_state.last_action_ts_event
    state["action_sequence"] = list(metrics.get("action_sequence") or [])


def session_payload(state: dict[str, Any]) -> dict[str, Any]:
    payload = dict(state)
    payload["last_updated_ts_utc"] = utc_now_iso()
    return payload


def tee_stream(stream: Any, log_path: Path, *, callback: Any) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        for line in iter(stream.readline, ""):
            handle.write(line)
            handle.flush()
            callback(line.rstrip("\n"))
    stream.close()


def build_child_callbacks(shared: dict[str, Any], *, lock: threading.Lock) -> tuple[Any, Any]:
    def stdout_callback(line: str) -> None:
        if not line:
            return
        if line.startswith("total_processed:"):
            try:
                processed = int(line.split(":", 1)[1].strip())
            except (TypeError, ValueError):
                return
            with lock:
                shared["processed_event_count"] = processed
            return
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return
        if not isinstance(obj, dict):
            return
        event = str(obj.get("event") or "").strip()
        with lock:
            if event == "soft_live_heartbeat":
                shared["last_heartbeat_monotonic"] = now_monotonic()
                shared["heartbeat_payload"] = obj
            elif event == "soft_live_done":
                shared["soft_live_done"] = obj
            elif event == "soft_live_error":
                shared["soft_live_error"] = obj

    def stderr_callback(line: str) -> None:
        if not line:
            return
        with lock:
            shared["stderr_tail"].append(line)
            shared["stderr_tail"] = shared["stderr_tail"][-20:]

    return stdout_callback, stderr_callback


def main(argv: list[str] | None = None) -> int:
    load_env_defaults_from_file(resolve_env_file())
    args = parse_args(argv or sys.argv[1:])
    session_json = Path(args.session_json).resolve()
    session_artifacts_dir = Path(args.session_artifacts_dir).resolve()
    binding_artifact = load_binding_artifact(Path(args.binding_artifact).resolve())
    selected = select_momentum_item(list(binding_artifact.get("items") or []), args)
    telegram = telegram_config(args)
    session_id = create_session_id()
    paths = build_session_paths(session_artifacts_dir=session_artifacts_dir, session_id=session_id)
    for path in paths.values():
        if path.suffix:
            path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path.mkdir(parents=True, exist_ok=True)

    write_json(
        paths["session_binding_json"],
        build_session_binding_artifact(binding_artifact, selected, generated_ts_utc=utc_now_iso()),
    )

    state = initial_session_state(
        session_id=session_id,
        selected=selected,
        telegram=telegram,
        args=args,
        paths=paths,
    )
    live_state = LivePaperState(selected=selected)
    metrics = live_state.current_metrics()
    apply_metrics_to_state(state, live_state, metrics)
    write_json(session_json, session_payload(state))

    send_session_message(
        state,
        telegram,
        event_type="session_started",
        text=render_session_started_message(selected, session_id=session_id, args=args),
    )
    write_json(session_json, session_payload(state))

    child_env = build_child_env(selected, paths, args)
    child_command = build_child_command(args)
    state["child_command"] = command_preview(child_command)

    proc = start_child_process(child_command, env=child_env)
    state["child_pid"] = proc.pid
    state["session_status"] = "RUNNING"
    write_json(session_json, session_payload(state))

    shared = {
        "last_heartbeat_monotonic": now_monotonic(),
        "heartbeat_payload": {},
        "processed_event_count": 0,
        "soft_live_done": None,
        "soft_live_error": None,
        "stderr_tail": [],
    }
    lock = threading.Lock()
    stdout_callback, stderr_callback = build_child_callbacks(shared, lock=lock)
    stdout_thread = threading.Thread(target=tee_stream, args=(proc.stdout, paths["stdout_log"]), kwargs={"callback": stdout_callback}, daemon=True)
    stderr_thread = threading.Thread(target=tee_stream, args=(proc.stderr, paths["stderr_log"]), kwargs={"callback": stderr_callback}, daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    stop_file = Path(args.stop_file).resolve() if str(args.stop_file or "").strip() else None
    cursor = AuditCursor()
    session_started_monotonic = now_monotonic()
    final_error = ""

    def emit_profitability_if_needed(*, force: bool = False) -> None:
        nonlocal metrics
        metrics = live_state.current_metrics()
        apply_metrics_to_state(state, live_state, metrics)
        key = json.dumps(
            {
                "status": metrics.get("profitability_status"),
                "funding": metrics.get("funding_support_status"),
                "pnl": round(parse_float(metrics.get("operator_pnl_usd")) or 0.0, 4) if parse_float(metrics.get("operator_pnl_usd")) is not None else None,
            },
            sort_keys=True,
        )
        if not force and key == live_state.last_profitability_key:
            return
        live_state.last_profitability_key = key
        send_session_message(
            state,
            telegram,
            event_type="profitability_update",
            text=render_profitability_message(selected, metrics, style=args.telegram_message_style),
        )

    def emit_trade_action(action: str, fill: dict[str, Any]) -> None:
        nonlocal metrics
        metrics = live_state.current_metrics()
        apply_metrics_to_state(state, live_state, metrics)
        if action == "LONG_OPEN":
            event_type = "trade_opened_long"
        elif action == "SHORT_OPEN":
            event_type = "trade_opened_short"
        elif action in {"LONG_EXIT", "SHORT_EXIT"}:
            event_type = "trade_exited"
        elif action in {"LONG_TO_SHORT_REVERSAL", "SHORT_TO_LONG_REVERSAL"}:
            event_type = "trade_reversed"
        else:
            return
        send_session_message(
            state,
            telegram,
            event_type=event_type,
            text=render_trade_message(event_type, selected, metrics, action=action, fill=fill, style=args.telegram_message_style),
        )
        emit_profitability_if_needed(force=True)

    def emit_funding_if_needed(previous_applied_count: int) -> None:
        nonlocal metrics
        metrics = live_state.current_metrics()
        apply_metrics_to_state(state, live_state, metrics)
        if int(metrics.get("funding_applied_count") or 0) <= previous_applied_count:
            return
        funding_cost = parse_float(metrics.get("funding_cost_quote"))
        if funding_cost is None or abs(funding_cost) <= EPSILON:
            return
        send_session_message(
            state,
            telegram,
            event_type="funding_cost_update",
            text=render_funding_message(selected, metrics, style=args.telegram_message_style),
        )
        emit_profitability_if_needed(force=True)

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        while proc.poll() is None:
            if STOP_REQUESTED:
                state["session_status"] = "STOPPING"
                terminate_child_process(proc)
                break
            if stop_file_requested(stop_file):
                request_stop(f"stop_file:{stop_file}")
                state["session_status"] = "STOPPING"
                terminate_child_process(proc)
                break
            if args.session_max_runtime_sec > 0 and (now_monotonic() - session_started_monotonic) >= args.session_max_runtime_sec:
                request_stop(f"session_max_runtime_sec:{args.session_max_runtime_sec}")
                state["session_status"] = "STOPPING"
                terminate_child_process(proc)
                break
            with lock:
                last_heartbeat_monotonic = float(shared["last_heartbeat_monotonic"])
                state["processed_event_count"] = int(shared["processed_event_count"])
                state["soft_live_done"] = shared["soft_live_done"]
                state["soft_live_error"] = shared["soft_live_error"]
            if (now_monotonic() - last_heartbeat_monotonic) > float(args.heartbeat_timeout_sec):
                final_error = f"heartbeat_timeout:{args.heartbeat_timeout_sec}"
                send_session_message(
                    state,
                    telegram,
                    event_type="session_warning",
                    text=render_warning_message(selected, reason=final_error, style=args.telegram_message_style),
                )
                terminate_child_process(proc)
                break
            new_rows = cursor.read_new_rows(paths["audit_spool_dir"])
            for row in new_rows:
                previous_applied_count = live_state.funding_applied_count
                notifications = live_state.process_audit_row(row)
                metrics = live_state.current_metrics()
                apply_metrics_to_state(state, live_state, metrics)
                emit_funding_if_needed(previous_applied_count)
                for notification in notifications:
                    if notification.get("type") == "trade_action":
                        emit_trade_action(str(notification.get("action") or ""), dict(notification.get("fill") or {}))
            write_json(session_json, session_payload(state))
            time.sleep(float(args.poll_interval_sec))
    finally:
        if proc.poll() is None:
            terminate_child_process(proc)
        stdout_thread.join(timeout=10)
        stderr_thread.join(timeout=10)

    state["child_exit_code"] = proc.poll()
    with lock:
        if shared["processed_event_count"]:
            state["processed_event_count"] = int(shared["processed_event_count"])
        state["soft_live_done"] = shared["soft_live_done"]
        state["soft_live_error"] = shared["soft_live_error"]
    new_rows = cursor.read_new_rows(paths["audit_spool_dir"])
    for row in new_rows:
        previous_applied_count = live_state.funding_applied_count
        notifications = live_state.process_audit_row(row)
        metrics = live_state.current_metrics()
        apply_metrics_to_state(state, live_state, metrics)
        emit_funding_if_needed(previous_applied_count)
        for notification in notifications:
            if notification.get("type") == "trade_action":
                emit_trade_action(str(notification.get("action") or ""), dict(notification.get("fill") or {}))

    metrics = live_state.current_metrics()
    apply_metrics_to_state(state, live_state, metrics)

    summary_exists = paths["summary_json"].exists()

    if STOP_REQUESTED:
        stop_reason = STOP_REASON or "manual_stop"
        state["session_status"] = "STOPPED_BY_USER" if stop_reason.startswith("signal:") or stop_reason.startswith("stop_file:") else "COMPLETED_LIMIT"
        state["stop_reason"] = stop_reason
        send_session_message(
            state,
            telegram,
            event_type="session_stopped_by_user",
            text=render_stopped_message(selected, session_id=session_id, stop_reason=stop_reason, style=args.telegram_message_style),
        )
    elif state["child_exit_code"] == 0 and summary_exists:
        state["session_status"] = "STOPPED_BY_CHILD"
        state["stop_reason"] = "child_exit_clean"
    else:
        state["session_status"] = "FAILED"
        state["stop_reason"] = final_error or ("summary_json_missing" if not summary_exists else f"child_exit_code:{state['child_exit_code']}")
        send_session_message(
            state,
            telegram,
            event_type="session_failed",
            text=render_failed_message(selected, session_id=session_id, reason=state["stop_reason"], style=args.telegram_message_style),
        )

    state["stop_ts_utc"] = utc_now_iso()
    write_json(session_json, session_payload(state))
    send_session_message(
        state,
        telegram,
        event_type="session_summary",
        text=render_summary_message(state, selected, style=args.telegram_message_style),
    )
    write_json(session_json, session_payload(state))

    print(f"session_json={session_json}")
    print(f"session_status={state['session_status']}")
    print(f"selected_strategy_id={state['selected_strategy_id']}")
    print(f"processed_event_count={state['processed_event_count']}")
    print(f"fill_event_count={state['fill_event_count']}")
    print(f"trade_open_count={state['trade_open_count']}")
    print(f"trade_exit_count={state['trade_exit_count']}")
    print(f"trade_reversal_count={state['trade_reversal_count']}")
    print(f"telegram_messages_attempted={state['telegram_messages_attempted']}")
    print(f"telegram_messages_sent={state['telegram_messages_sent']}")
    print(f"telegram_error_count={state['telegram_error_count']}")
    print(f"profitability_status={state['profitability_status']}")
    print(f"current_pnl_usd={state['current_pnl_usd']}")

    if state["session_status"] == "FAILED":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
