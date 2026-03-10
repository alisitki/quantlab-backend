import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-momentum-continuous-shadow-session-v1.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def binding_artifact(items: list[dict]) -> dict:
    return {
        "schema_version": "candidate_strategy_runtime_binding_v0",
        "generated_ts_utc": "2026-03-09T00:00:00Z",
        "source_candidate_strategy_contract_json": "/tmp/candidate_strategy_contract_v0.json",
        "source_binding_map_json": "/tmp/family_shadow_runtime_binding_map_v0.json",
        "source_row_count": len(items),
        "translated_spec_count": sum(1 for item in items if item.get("translation_status") == "TRANSLATABLE"),
        "bound_shadow_runnable_count": sum(1 for item in items if item.get("runtime_binding_status") == "BOUND_SHADOW_RUNNABLE"),
        "unbound_no_runtime_impl_count": 0,
        "unbound_config_gap_count": 0,
        "unbound_translation_rejected_count": 0,
        "bindable_family_ids": ["spread_reversion_v1", "momentum_v1"],
        "items": items,
    }


def bound_item(
    *,
    family_id: str,
    pack_id: str,
    strategy_id: str,
    stream: str,
    strategy_file: str,
    binding_mode: str,
    rank: int = 1,
    symbol: str = "btcusdt",
) -> dict:
    selected_cell = {
        "exchange": "binance",
        "stream": stream,
        "symbol": symbol,
        "delta_ms": 1000,
        "h_ms": 1000,
        "event_count": 1000,
        "mean_product": 0.25 if family_id == "momentum_v1" else -0.01,
        "t_stat": 4.0 if family_id == "momentum_v1" else -4.0,
    }
    return {
        "rank": rank,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": strategy_id,
        "family_id": family_id,
        "exchange": "binance",
        "stream": stream,
        "symbols": [symbol],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "runtime_strategy_file": strategy_file,
        "runtime_strategy_config": {
            "binding_mode": binding_mode,
            "family_id": family_id,
            "source_pack_id": pack_id,
            "source_decision_tier": "PROMOTE_STRONG",
            "exchange": "binance",
            "stream": stream,
            "symbols": [symbol],
            "source_family_report_path": f"/tmp/{pack_id}.json",
            "window": "20260107..20260107",
            "params": {"delta_ms_list": [1000], "h_ms_list": [1000], "tolerance_ms": 0},
            "selected_cell": selected_cell,
            "orderQty": 1,
        },
        "binding_reason": "",
    }


def write_fake_child_tool(path: Path, config_path: Path) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import os\n"
            "import signal\n"
            "import sys\n"
            "import time\n"
            "from pathlib import Path\n"
            "config = json.loads(Path(%r).read_text(encoding='utf-8'))\n"
            "audit_root = Path(os.environ['AUDIT_SPOOL_DIR']) / 'date=20260309'\n"
            "audit_root.mkdir(parents=True, exist_ok=True)\n"
            "audit_path = audit_root / 'part-0001.jsonl'\n"
            "summary_path = Path(os.environ['SOFT_LIVE_SUMMARY_JSON'])\n"
            "summary_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "stop_requested = False\n"
            "def on_stop(_signum, _frame):\n"
            "    global stop_requested\n"
            "    stop_requested = True\n"
            "signal.signal(signal.SIGTERM, on_stop)\n"
            "signal.signal(signal.SIGINT, on_stop)\n"
            "for line in config.get('stdout_lines', []):\n"
            "    print(line, flush=True)\n"
            "for row in config.get('audit_rows', []):\n"
            "    with audit_path.open('a', encoding='utf-8') as handle:\n"
            "        handle.write(json.dumps(row) + '\\n')\n"
            "    time.sleep(config.get('between_audit_sec', 0))\n"
            "mode = config.get('mode', 'oneshot')\n"
            "if mode == 'sleep_until_signal':\n"
            "    while not stop_requested:\n"
            "        print(json.dumps({'event': 'soft_live_heartbeat', 'live_run_id': config['live_run_id'], 'decision_count': 0}), flush=True)\n"
            "        time.sleep(config.get('heartbeat_sleep_sec', 0.2))\n"
            "else:\n"
            "    time.sleep(config.get('sleep_sec', 0.05))\n"
            "summary = dict(config.get('summary_payload', {}))\n"
            "summary.setdefault('live_run_id', config['live_run_id'])\n"
            "summary_path.write_text(json.dumps(summary) + '\\n', encoding='utf-8')\n"
            "print(config.get('total_processed_line', 'total_processed: 123'), flush=True)\n"
            "print(json.dumps(config.get('done_payload', {'event': 'soft_live_done', 'live_run_id': config['live_run_id']})), flush=True)\n"
            "raise SystemExit(int(config.get('exit_code', 0)))\n"
        )
        % str(config_path),
        encoding="utf-8",
    )
    path.chmod(0o755)


class _TelegramHandler(BaseHTTPRequestHandler):
    messages: list[str] = []

    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")) or 0)
        payload = json.loads(body.decode("utf-8"))
        self.__class__.messages.append(str(payload.get("text") or ""))
        encoded = json.dumps({"ok": True, "result": {"message_id": len(self.__class__.messages)}}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):  # noqa: A003
        return


class RunMomentumContinuousShadowSessionV1Tests(unittest.TestCase):
    def _run(self, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        run_env = os.environ.copy()
        if env is not None:
            run_env.update(env)
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
            env=run_env,
        )

    def test_one_shot_session_emits_trade_and_funding_aware_profitability(self):
        with tempfile.TemporaryDirectory(prefix="momentum_continuous_session_oneshot_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_config = root / "child_config.json"
            child_tool = root / "fake_child.py"
            session_json = root / "session.json"
            session_artifacts = root / "artifacts"

            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="spread_reversion_v1",
                            pack_id="pack_spread",
                            strategy_id="strategy_spread",
                            stream="bbo",
                            strategy_file="core/strategy/strategies/SpreadReversionV1Strategy.js",
                            binding_mode="OBSERVE_ONLY",
                            rank=1,
                            symbol="bnbusdt",
                        ),
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=2,
                            symbol="btcusdt",
                        ),
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "mode": "oneshot",
                    "live_run_id": "live_momentum_demo",
                    "stdout_lines": [
                        json.dumps({"event": "soft_live_heartbeat", "live_run_id": "live_momentum_demo", "decision_count": 1}),
                    ],
                    "audit_rows": [
                        {
                            "actor": "system",
                            "action": "RUN_START",
                            "metadata": {"live_run_id": "live_momentum_demo", "strategy_id": "strategy_momentum"},
                        },
                        {
                            "actor": "system",
                            "action": "MARK_PRICE",
                            "metadata": {
                                "live_run_id": "live_momentum_demo",
                                "symbol": "BTCUSDT",
                                "ts_event": "1700000000000000000",
                                "mark_price": 100.0,
                            },
                        },
                        {
                            "actor": "system",
                            "action": "FILL",
                            "metadata": {
                                "live_run_id": "live_momentum_demo",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "qty": 1.0,
                                "fill_price": 100.0,
                                "fill_fee": 0.04,
                                "fill_value": 100.0,
                                "ts_event": "1700000000000000100",
                            },
                        },
                        {
                            "actor": "system",
                            "action": "MARK_PRICE",
                            "metadata": {
                                "live_run_id": "live_momentum_demo",
                                "symbol": "BTCUSDT",
                                "ts_event": "1700000000000000200",
                                "mark_price": 101.0,
                            },
                        },
                        {
                            "actor": "system",
                            "action": "FUNDING",
                            "metadata": {
                                "live_run_id": "live_momentum_demo",
                                "exchange": "binance",
                                "symbol": "BTCUSDT",
                                "ts_event": "1700000000000000300",
                                "funding_rate": 0.0001,
                                "next_funding_ts": "1700000000000000200",
                            },
                        },
                    ],
                    "summary_payload": {
                        "started_at": "2026-03-09T00:00:00Z",
                        "finished_at": "2026-03-09T00:01:00Z",
                        "stop_reason": "STREAM_END",
                        "execution_summary": {
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 1,
                            "total_realized_pnl": -0.04,
                            "total_unrealized_pnl": 1.0,
                            "equity": 10000.96,
                            "max_position_value": 101.0,
                            "positions": {
                                "BTCUSDT": {
                                    "symbol": "BTCUSDT",
                                    "size": 1.0,
                                    "avg_entry_price": 100.0,
                                    "realized_pnl": -0.04,
                                    "unrealized_pnl": 1.0,
                                    "current_price": 101.0,
                                }
                            },
                        },
                    },
                    "done_payload": {"event": "soft_live_done", "live_run_id": "live_momentum_demo"},
                    "total_processed_line": "total_processed: 321",
                },
            )
            write_fake_child_tool(child_tool, child_config)

            res = self._run(
                "--telegram-dry-run",
                "--binding-artifact",
                str(binding_json),
                "--child-tool",
                str(child_tool),
                "--session-json",
                str(session_json),
                "--session-artifacts-dir",
                str(session_artifacts),
                "--poll-interval-sec",
                "0.05",
                "--heartbeat-timeout-sec",
                "5",
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "STOPPED_BY_CHILD")
            self.assertEqual(payload["fill_event_count"], 1)
            self.assertEqual(payload["trade_open_count"], 1)
            self.assertEqual(payload["trade_reversal_count"], 0)
            self.assertEqual(payload["current_position_direction"], "LONG")
            self.assertEqual(payload["processed_event_count"], 321)
            self.assertEqual(payload["funding_support_status"], "FUNDING_COST_BACKED")
            self.assertEqual(payload["profitability_status"], "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE")
            self.assertEqual(payload["current_pnl_source_field"], "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate")
            self.assertIn("session_started", payload["telegram_event_types_attempted"])
            self.assertIn("trade_opened_long", payload["telegram_event_types_attempted"])
            self.assertIn("profitability_update", payload["telegram_event_types_attempted"])
            self.assertIn("funding_cost_update", payload["telegram_event_types_attempted"])
            self.assertIn("session_summary", payload["telegram_event_types_attempted"])

    def test_stop_file_stops_continuous_session(self):
        with tempfile.TemporaryDirectory(prefix="momentum_continuous_session_stopfile_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_config = root / "child_config.json"
            child_tool = root / "fake_child.py"
            session_json = root / "session.json"
            session_artifacts = root / "artifacts"
            stop_file = root / "stop.now"

            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=1,
                            symbol="btcusdt",
                        )
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "mode": "sleep_until_signal",
                    "live_run_id": "live_stop_demo",
                    "heartbeat_sleep_sec": 0.1,
                    "summary_payload": {
                        "started_at": "2026-03-09T00:00:00Z",
                        "finished_at": "2026-03-09T00:01:00Z",
                        "stop_reason": "MANUAL_STOP",
                        "execution_summary": {
                            "snapshot_present": False,
                            "positions_count": 0,
                            "fills_count": 0,
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10000.0,
                            "max_position_value": 0.0,
                            "positions": {},
                        },
                    },
                    "done_payload": {"event": "soft_live_done", "live_run_id": "live_stop_demo"},
                    "total_processed_line": "total_processed: 22",
                },
            )
            write_fake_child_tool(child_tool, child_config)

            proc = subprocess.Popen(
                [
                    "python3",
                    str(SCRIPT),
                    "--telegram-dry-run",
                    "--binding-artifact",
                    str(binding_json),
                    "--child-tool",
                    str(child_tool),
                    "--session-json",
                    str(session_json),
                    "--session-artifacts-dir",
                    str(session_artifacts),
                    "--poll-interval-sec",
                    "0.05",
                    "--heartbeat-timeout-sec",
                    "5",
                    "--stop-file",
                    str(stop_file),
                ],
                cwd=str(REPO),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            time.sleep(0.6)
            stop_file.write_text("stop\n", encoding="utf-8")
            stdout, stderr = proc.communicate(timeout=15)

            self.assertEqual(proc.returncode, 0, msg=stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "STOPPED_BY_USER")
            self.assertTrue(str(payload["stop_reason"]).startswith("stop_file:"))
            self.assertIn("session_stopped_by_user", payload["telegram_event_types_attempted"])

    def test_one_shot_session_can_reverse_and_exit_in_same_session(self):
        with tempfile.TemporaryDirectory(prefix="momentum_continuous_session_reversal_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_config = root / "child_config.json"
            child_tool = root / "fake_child.py"
            session_json = root / "session.json"
            session_artifacts = root / "artifacts"

            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=1,
                            symbol="btcusdt",
                        )
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "mode": "oneshot",
                    "live_run_id": "live_reversal_demo",
                    "stdout_lines": [
                        json.dumps({"event": "soft_live_heartbeat", "live_run_id": "live_reversal_demo", "decision_count": 3}),
                    ],
                    "audit_rows": [
                        {
                            "actor": "system",
                            "action": "MARK_PRICE",
                            "metadata": {
                                "live_run_id": "live_reversal_demo",
                                "symbol": "BTCUSDT",
                                "ts_event": "1700000000000000000",
                                "mark_price": 100.0,
                            },
                        },
                        {
                            "actor": "system",
                            "action": "FILL",
                            "metadata": {
                                "live_run_id": "live_reversal_demo",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "qty": 1.0,
                                "fill_price": 100.0,
                                "fill_fee": 0.04,
                                "fill_value": 100.0,
                                "ts_event": "1700000000000000100",
                            },
                        },
                        {
                            "actor": "system",
                            "action": "FILL",
                            "metadata": {
                                "live_run_id": "live_reversal_demo",
                                "symbol": "BTCUSDT",
                                "side": "SELL",
                                "qty": 2.0,
                                "fill_price": 99.0,
                                "fill_fee": 0.0792,
                                "fill_value": 198.0,
                                "ts_event": "1700000000000000200",
                            },
                        },
                        {
                            "actor": "system",
                            "action": "FILL",
                            "metadata": {
                                "live_run_id": "live_reversal_demo",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "qty": 1.0,
                                "fill_price": 98.0,
                                "fill_fee": 0.0392,
                                "fill_value": 98.0,
                                "ts_event": "1700000000000000300",
                            },
                        },
                    ],
                    "summary_payload": {
                        "started_at": "2026-03-09T00:00:00Z",
                        "finished_at": "2026-03-09T00:01:00Z",
                        "stop_reason": "STREAM_END",
                        "execution_summary": {
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 3,
                            "total_realized_pnl": -3.1584,
                            "total_unrealized_pnl": 0.0,
                            "equity": 9996.8416,
                            "max_position_value": 100.0,
                            "positions": {},
                        },
                    },
                    "done_payload": {"event": "soft_live_done", "live_run_id": "live_reversal_demo"},
                    "total_processed_line": "total_processed: 444",
                },
            )
            write_fake_child_tool(child_tool, child_config)

            res = self._run(
                "--telegram-dry-run",
                "--binding-artifact",
                str(binding_json),
                "--child-tool",
                str(child_tool),
                "--session-json",
                str(session_json),
                "--session-artifacts-dir",
                str(session_artifacts),
                "--poll-interval-sec",
                "0.05",
                "--heartbeat-timeout-sec",
                "5",
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "STOPPED_BY_CHILD")
            self.assertEqual(payload["fill_event_count"], 3)
            self.assertEqual(payload["trade_open_count"], 1)
            self.assertEqual(payload["trade_reversal_count"], 1)
            self.assertEqual(payload["trade_exit_count"], 1)
            self.assertEqual(payload["current_position_direction"], "FLAT")
            self.assertEqual(payload["last_action"], "SHORT_EXIT")
            self.assertEqual(payload["processed_event_count"], 444)
            self.assertIn("trade_opened_long", payload["telegram_event_types_attempted"])
            self.assertIn("trade_reversed", payload["telegram_event_types_attempted"])
            self.assertIn("trade_exited", payload["telegram_event_types_attempted"])
            self.assertIn("SHORT_EXIT", [item.get("action") for item in payload["action_sequence"]])

    def test_live_telegram_posts_compact_messages(self):
        with tempfile.TemporaryDirectory(prefix="momentum_continuous_session_live_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_config = root / "child_config.json"
            child_tool = root / "fake_child.py"
            session_json = root / "session.json"
            session_artifacts = root / "artifacts"
            env_file = root / "telegram.env"

            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=1,
                            symbol="btcusdt",
                        )
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "mode": "oneshot",
                    "live_run_id": "live_message_demo",
                    "audit_rows": [],
                    "summary_payload": {
                        "started_at": "2026-03-09T00:00:00Z",
                        "finished_at": "2026-03-09T00:00:02Z",
                        "stop_reason": "STREAM_END",
                        "execution_summary": {
                            "snapshot_present": False,
                            "positions_count": 0,
                            "fills_count": 0,
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10000.0,
                            "max_position_value": 0.0,
                            "positions": {},
                        },
                    },
                    "done_payload": {"event": "soft_live_done", "live_run_id": "live_message_demo"},
                    "total_processed_line": "total_processed: 5",
                },
            )
            write_fake_child_tool(child_tool, child_config)

            _TelegramHandler.messages = []
            server = HTTPServer(("127.0.0.1", 0), _TelegramHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                env_file.write_text(
                    "TELEGRAM_BOT_TOKEN=123456:TESTTOKEN\nTELEGRAM_CHAT_ID=123456789\n",
                    encoding="utf-8",
                )
                env = {"QUANTLAB_ENV_FILE": str(env_file)}
                res = self._run(
                    "--binding-artifact",
                    str(binding_json),
                    "--child-tool",
                    str(child_tool),
                    "--session-json",
                    str(session_json),
                    "--session-artifacts-dir",
                    str(session_artifacts),
                    "--poll-interval-sec",
                    "0.05",
                    "--heartbeat-timeout-sec",
                    "5",
                    "--telegram-api-base-url",
                    f"http://127.0.0.1:{server.server_port}",
                    env=env,
                )
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["telegram_send_mode"], "LIVE")
            self.assertGreaterEqual(payload["telegram_messages_sent"], 2)
            self.assertTrue(any("Momentum session started" in message for message in _TelegramHandler.messages))
            self.assertTrue(any("Momentum session summary" in message for message in _TelegramHandler.messages))


if __name__ == "__main__":
    unittest.main()
