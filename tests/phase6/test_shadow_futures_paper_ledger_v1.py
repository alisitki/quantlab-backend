import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_futures_paper_ledger_v1.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def make_history_entry(
    *,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    symbols: list[str],
    execution_summary: dict,
    execution_events: list[dict],
    funding_events: list[dict] | None = None,
    mark_price_events: list[dict] | None = None,
) -> dict:
    return {
        "schema_version": "shadow_observation_history_v0",
        "observation_key": observation_key,
        "observed_at": observed_at,
        "selected_pack_id": pack_id,
        "selected_rank": 1,
        "selected_exchange": "bybit",
        "selected_symbols": symbols,
        "selected_decision_tier": "PROMOTE_STRONG",
        "selected_selection_slot": "bybit/bbo",
        "live_run_id": live_run_id,
        "started_at": "2026-03-09T00:00:00Z",
        "finished_at": "2026-03-09T00:01:00Z",
        "stop_reason": "STREAM_END",
        "run_duration_sec": 60.0,
        "verify_soft_live_pass": True,
        "processed_event_count": 10,
        "heartbeat_seen": True,
        "execution_summary": execution_summary,
        "execution_events": execution_events,
        "funding_events": funding_events if funding_events is not None else [],
        "mark_price_events": mark_price_events if mark_price_events is not None else [],
    }


def make_binding_item(*, pack_id: str, family_id: str, binding_mode: str = "OBSERVE_ONLY") -> dict:
    return {
        "rank": 1,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": f"candidate_strategy::{family_id}::{pack_id}::bnbusdt",
        "family_id": family_id,
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": ["bnbusdt"],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "runtime_strategy_file": "core/strategy/strategies/SpreadReversionV1Strategy.js",
        "runtime_strategy_config": {
            "binding_mode": binding_mode,
            "family_id": family_id,
            "exchange": "bybit",
            "stream": "bbo",
            "symbols": ["bnbusdt"],
            "params": {"delta_ms_list": [1000], "h_ms_list": [1000], "tolerance_ms": 0},
            "selected_cell": {
                "symbol": "bnbusdt",
                "exchange": "bybit",
                "stream": "bbo",
                "delta_ms": 1000,
                "h_ms": 1000,
                "mean_product": -0.001,
                "t_stat": -3.0,
                "event_count": 200,
            },
        },
        "binding_reason": "",
    }


class ShadowFuturesPaperLedgerV1Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_empty_history_writes_empty_payload(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_empty_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            history.write_text("", encoding="utf-8")
            events.write_text("", encoding="utf-8")
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["run_count"], 0)
            self.assertEqual(payload["items"], [])

    def test_spread_reversion_observe_only_no_fill_run_is_honest(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_no_fill_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-09T00:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 0,
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10000.0,
                            "max_position_value": 0.0,
                            "positions": {},
                        },
                        execution_events=[
                            {
                                "event_seq": 1,
                                "event_type": "DECISION",
                                "ts_event": "1700000000",
                                "symbol": "BNBUSDT",
                                "side": "BUY",
                                "qty": 1.0,
                                "fill_price": None,
                                "fill_fee": None,
                                "fill_value": None,
                                "reason": "",
                            }
                        ],
                    )
                ],
            )
            events.write_text("", encoding="utf-8")
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_a", family_id="spread_reversion_v1")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertEqual(item["family_directionality_status"], "NON_DIRECTIONAL_SIGNAL_ONLY")
            self.assertEqual(item["binding_mode"], "OBSERVE_ONLY")
            self.assertEqual(item["paper_run_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["final_position_direction"], "FLAT")
            self.assertEqual(item["fill_event_count"], 0)
            self.assertEqual(item["action_sequence"], [])
            self.assertEqual(item["episodes"], [])
            self.assertEqual(item["turnover_quote"], 0.0)
            self.assertEqual(item["turnover_support_status"], "NO_FILL_ACTIVITY")
            self.assertIsNone(item["effective_fee_rate"])
            self.assertEqual(item["final_position_notional_quote"], 0.0)
            self.assertEqual(item["position_notional_support_status"], "NO_OPEN_POSITION")
            self.assertEqual(item["exposure_to_equity_ratio"], 0.0)
            self.assertEqual(item["exposure_ratio_status"], "NO_OPEN_POSITION")
            self.assertEqual(item["mark_to_market_pnl_quote_gross"], 0.0)
            self.assertEqual(item["mark_to_market_pnl_quote_net_paid_fees"], 0.0)
            self.assertEqual(item["estimated_exit_fee_quote"], 0.0)
            self.assertEqual(item["mark_to_market_pnl_quote_net_after_exit_estimate"], 0.0)
            self.assertEqual(item["fee_support_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["cost_accounting_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["profitability_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["funding_cost_quote"], 0.0)
            self.assertEqual(item["funding_support_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["funding_alignment_status"], "NO_FILL_ACTIVITY")
            self.assertEqual(item["leverage_support_status"], "UNSUPPORTED")
            self.assertEqual(item["margin_support_status"], "UNSUPPORTED")

    def test_momentum_binding_reports_directional_family_status(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_momentum_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_momo|run_1",
                        observed_at="2026-03-09T00:01:00Z",
                        pack_id="pack_momo",
                        live_run_id="run_1",
                        symbols=["BTCUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 0,
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10000.0,
                            "max_position_value": 0.0,
                            "positions": {},
                        },
                        execution_events=[],
                    )
                ],
            )
            events.write_text("", encoding="utf-8")
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_momo", family_id="momentum_v1", binding_mode="PAPER_DIRECTIONAL_V1")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertEqual(item["family_id"], "momentum_v1")
            self.assertEqual(item["binding_mode"], "PAPER_DIRECTIONAL_V1")
            self.assertEqual(item["family_directionality_status"], "PRIMARY_DIRECTIONAL_LONG_SHORT")

    def test_fill_backed_long_run_replays_position_and_pnl(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_long_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_long|run_open",
                        observed_at="2026-03-09T00:02:00Z",
                        pack_id="pack_long",
                        live_run_id="run_open",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 1,
                            "total_realized_pnl": -0.04,
                            "total_unrealized_pnl": 2.0,
                            "equity": 10001.96,
                            "max_position_value": 102.0,
                            "positions": {
                                "BNBUSDT": {
                                    "symbol": "BNBUSDT",
                                    "size": 1.0,
                                    "avg_entry_price": 100.0,
                                    "realized_pnl": -0.04,
                                    "unrealized_pnl": 2.0,
                                    "current_price": 102.0,
                                }
                            },
                        },
                        execution_events=[],
                        funding_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "funding_rate": 0.0001,
                                "next_funding_ts": "1700003600000",
                            }
                        ],
                        mark_price_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "mark_price": 102.0,
                                "index_price": 101.9,
                            }
                        ],
                    )
                ],
            )
            write_jsonl(
                events,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_long|run_open|event|1",
                        "observation_key": "pack_long|run_open",
                        "observed_at": "2026-03-09T00:02:00Z",
                        "selected_pack_id": "pack_long",
                        "live_run_id": "run_open",
                        "event_seq": 1,
                        "event_type": "FILL",
                        "ts_event": "1700000001",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": 100.0,
                        "fill_fee": 0.04,
                        "fill_value": 100.0,
                        "reason": "",
                    }
                ],
            )
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_long", family_id="spread_reversion_v1", binding_mode="PAPER_EXECUTION")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertEqual(item["paper_run_status"], "FILL_BACKED_POSITION_OPEN")
            self.assertEqual(item["final_position_direction"], "LONG")
            self.assertEqual(item["final_position_qty"], 1.0)
            self.assertEqual(item["final_avg_entry_price"], 100.0)
            self.assertEqual(item["final_mark_price"], 102.0)
            self.assertEqual(item["mark_price_source"], "SUMMARY_POSITION_CURRENT_PRICE")
            self.assertEqual(item["replayed_realized_pnl_quote_gross"], 0.0)
            self.assertEqual(item["replayed_realized_pnl_quote_net"], -0.04)
            self.assertEqual(item["replayed_unrealized_pnl_quote"], 2.0)
            self.assertEqual(item["turnover_quote"], 100.0)
            self.assertEqual(item["opening_turnover_quote"], 100.0)
            self.assertEqual(item["closing_turnover_quote"], 0.0)
            self.assertEqual(item["turnover_support_status"], "FILL_VALUE_BACKED")
            self.assertAlmostEqual(item["effective_fee_rate"], 0.0004)
            self.assertEqual(item["total_fee_quote"], 0.04)
            self.assertEqual(item["final_position_notional_quote"], 102.0)
            self.assertEqual(item["position_notional_support_status"], "SUMMARY_MARK_PRICE_BACKED")
            self.assertEqual(item["summary_equity_quote"], 10001.96)
            self.assertEqual(item["max_position_value_quote"], 102.0)
            self.assertAlmostEqual(item["exposure_to_equity_ratio"], 102.0 / 10001.96)
            self.assertEqual(item["exposure_ratio_status"], "SUMMARY_EQUITY_BACKED")
            self.assertEqual(item["mark_to_market_pnl_quote_gross"], 2.0)
            self.assertEqual(item["mark_to_market_pnl_quote_net_paid_fees"], 1.96)
            self.assertEqual(item["mark_to_market_pnl_quote_net_after_funding"], 1.96)
            self.assertAlmostEqual(item["estimated_exit_fee_quote"], 0.0408)
            self.assertAlmostEqual(item["mark_to_market_pnl_quote_net_after_exit_estimate"], 1.9192)
            self.assertAlmostEqual(item["mark_to_market_pnl_quote_net_after_funding_and_exit_estimate"], 1.9192)
            self.assertEqual(item["fee_support_status"], "FILL_FEE_BACKED")
            self.assertEqual(item["funding_cost_quote"], 0.0)
            self.assertEqual(item["funding_support_status"], "NO_FUNDING_WINDOW_CROSSED")
            self.assertEqual(item["funding_alignment_status"], "NO_FUNDING_WINDOW_CROSSED")
            self.assertEqual(item["funding_rate_source"], "LIVE_STREAM_FUNDING")
            self.assertEqual(item["funding_events_count"], 1)
            self.assertEqual(item["funding_windows_crossed_count"], 0)
            self.assertEqual(item["funding_applied_count"], 0)
            self.assertEqual(item["cost_accounting_status"], "NET_FEE_BACKED_MARK_TO_MARKET_FUNDING_AWARE")
            self.assertEqual(item["profitability_status"], "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE")
            self.assertEqual(item["position_reconciliation_status"], "MATCHED_TO_SUMMARY")
            self.assertEqual(item["pnl_reconciliation_status"], "MATCHED_TO_SUMMARY")
            self.assertEqual(item["action_sequence"][0]["action"], "LONG_OPEN")
            self.assertEqual(item["episodes"][0]["direction"], "LONG")
            self.assertEqual(item["episodes"][0]["status"], "OPEN")

    def test_missing_fill_fee_keeps_net_pnl_null(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_fee_gap_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_fee|run_close",
                        observed_at="2026-03-09T00:03:00Z",
                        pack_id="pack_fee",
                        live_run_id="run_close",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 2,
                            "total_realized_pnl": 4.5,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10004.5,
                            "max_position_value": 105.0,
                            "positions": {},
                        },
                        execution_events=[],
                        funding_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "funding_rate": 0.0001,
                                "next_funding_ts": "1700003600000",
                            }
                        ],
                        mark_price_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "mark_price": 105.0,
                                "index_price": 104.9,
                            }
                        ],
                    )
                ],
            )
            write_jsonl(
                events,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_fee|run_close|event|1",
                        "observation_key": "pack_fee|run_close",
                        "observed_at": "2026-03-09T00:03:00Z",
                        "selected_pack_id": "pack_fee",
                        "live_run_id": "run_close",
                        "event_seq": 1,
                        "event_type": "FILL",
                        "ts_event": "1700000010",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": 100.0,
                        "fill_fee": None,
                        "fill_value": 100.0,
                        "reason": "",
                    },
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_fee|run_close|event|2",
                        "observation_key": "pack_fee|run_close",
                        "observed_at": "2026-03-09T00:03:00Z",
                        "selected_pack_id": "pack_fee",
                        "live_run_id": "run_close",
                        "event_seq": 2,
                        "event_type": "FILL",
                        "ts_event": "1700000020",
                        "symbol": "BNBUSDT",
                        "side": "SELL",
                        "qty": 1.0,
                        "fill_price": 105.0,
                        "fill_fee": None,
                        "fill_value": 105.0,
                        "reason": "",
                    },
                ],
            )
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_fee", family_id="spread_reversion_v1", binding_mode="PAPER_EXECUTION")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertEqual(item["paper_run_status"], "FILL_BACKED_FLAT")
            self.assertEqual(item["final_position_direction"], "FLAT")
            self.assertEqual(item["turnover_quote"], 205.0)
            self.assertEqual(item["opening_turnover_quote"], 100.0)
            self.assertEqual(item["closing_turnover_quote"], 105.0)
            self.assertEqual(item["turnover_support_status"], "FILL_VALUE_BACKED")
            self.assertIsNone(item["effective_fee_rate"])
            self.assertEqual(item["replayed_realized_pnl_quote_gross"], 5.0)
            self.assertIsNone(item["replayed_realized_pnl_quote_net"])
            self.assertEqual(item["mark_to_market_pnl_quote_gross"], 5.0)
            self.assertIsNone(item["mark_to_market_pnl_quote_net_paid_fees"])
            self.assertEqual(item["estimated_exit_fee_quote"], 0.0)
            self.assertIsNone(item["mark_to_market_pnl_quote_net_after_exit_estimate"])
            self.assertIsNone(item["total_fee_quote"])
            self.assertEqual(item["fee_support_status"], "FILL_FEE_PARTIAL")
            self.assertEqual(item["cost_accounting_status"], "GROSS_ONLY_FEE_PARTIAL")
            self.assertEqual(item["profitability_status"], "GROSS_ONLY_FEE_PARTIAL")
            self.assertEqual(item["pnl_reconciliation_status"], "NET_UNAVAILABLE_FEE_MISSING")
            self.assertEqual([action["action"] for action in item["action_sequence"]], ["LONG_OPEN", "LONG_CLOSE"])
            self.assertEqual(item["episodes"][0]["close_action"], "LONG_CLOSE")
            self.assertIsNone(item["episodes"][0]["realized_pnl_quote_net"])

    def test_turnover_recomputes_from_price_and_qty_when_fill_value_missing(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_turnover_recompute_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_turnover|run_open",
                        observed_at="2026-03-09T00:04:00Z",
                        pack_id="pack_turnover",
                        live_run_id="run_open",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 1,
                            "total_realized_pnl": -0.04,
                            "total_unrealized_pnl": 1.0,
                            "equity": 10000.96,
                            "max_position_value": 101.0,
                            "positions": {
                                "BNBUSDT": {
                                    "symbol": "BNBUSDT",
                                    "size": 1.0,
                                    "avg_entry_price": 100.0,
                                    "realized_pnl": -0.04,
                                    "unrealized_pnl": 1.0,
                                    "current_price": 101.0,
                                }
                            },
                        },
                        execution_events=[],
                        funding_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "funding_rate": 0.0001,
                                "next_funding_ts": "1700003600000",
                            }
                        ],
                        mark_price_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1700000005",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "mark_price": 101.0,
                                "index_price": 100.9,
                            }
                        ],
                    )
                ],
            )
            write_jsonl(
                events,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_turnover|run_open|event|1",
                        "observation_key": "pack_turnover|run_open",
                        "observed_at": "2026-03-09T00:04:00Z",
                        "selected_pack_id": "pack_turnover",
                        "live_run_id": "run_open",
                        "event_seq": 1,
                        "event_type": "FILL",
                        "ts_event": "1700000100",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": 100.0,
                        "fill_fee": 0.04,
                        "fill_value": None,
                        "reason": "",
                    }
                ],
            )
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_turnover", family_id="momentum_v1", binding_mode="PAPER_DIRECTIONAL_V1")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertEqual(item["turnover_quote"], 100.0)
            self.assertEqual(item["turnover_support_status"], "PRICE_QTY_RECOMPUTED")
            self.assertAlmostEqual(item["effective_fee_rate"], 0.0004)

    def test_funding_cost_applies_when_open_position_crosses_boundary(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_funding_apply_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_funding|run_1",
                        observed_at="2026-03-09T00:00:00Z",
                        pack_id="pack_funding",
                        live_run_id="run_1",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 1,
                            "total_realized_pnl": -0.04,
                            "total_unrealized_pnl": 2.0,
                            "equity": 10001.96,
                            "max_position_value": 102.0,
                            "positions": {
                                "BNBUSDT": {
                                    "symbol": "BNBUSDT",
                                    "size": 1.0,
                                    "avg_entry_price": 100.0,
                                    "realized_pnl": -0.04,
                                    "unrealized_pnl": 2.0,
                                    "current_price": 102.0,
                                }
                            },
                        },
                        execution_events=[],
                        funding_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1773014401990",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "funding_rate": 0.001,
                                "next_funding_ts": "1773014402000",
                            }
                        ],
                        mark_price_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1773014401995",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "mark_price": 102.0,
                                "index_price": 101.9,
                            }
                        ],
                    )
                ],
            )
            write_jsonl(
                events,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_funding|run_1|event|1",
                        "observation_key": "pack_funding|run_1",
                        "observed_at": "2026-03-09T00:00:00Z",
                        "selected_pack_id": "pack_funding",
                        "live_run_id": "run_1",
                        "event_seq": 1,
                        "event_type": "FILL",
                        "ts_event": "1773014401000",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": 100.0,
                        "fill_fee": 0.04,
                        "fill_value": 100.0,
                        "reason": "",
                    }
                ],
            )
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_funding", family_id="momentum_v1", binding_mode="PAPER_DIRECTIONAL_V1")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertAlmostEqual(item["funding_cost_quote"], 0.102)
            self.assertEqual(item["funding_support_status"], "FUNDING_COST_BACKED")
            self.assertEqual(item["funding_alignment_status"], "ALL_APPLIED_WINDOWS_MARK_PRICE_BACKED")
            self.assertEqual(item["funding_events_count"], 1)
            self.assertEqual(item["funding_windows_crossed_count"], 1)
            self.assertEqual(item["funding_applied_count"], 1)
            self.assertAlmostEqual(item["mark_to_market_pnl_quote_net_after_funding"], 1.858)
            self.assertAlmostEqual(item["mark_to_market_pnl_quote_net_after_funding_and_exit_estimate"], 1.8172)
            self.assertEqual(item["profitability_status"], "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE")
            self.assertEqual(item["funding_windows"][0]["alignment_status"], "APPLIED")

    def test_short_position_receives_positive_funding(self):
        with tempfile.TemporaryDirectory(prefix="shadow_futures_paper_funding_short_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            events = root / "events.jsonl"
            bindings = root / "bindings.json"
            out_json = root / "futures.json"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_short|run_1",
                        observed_at="2026-03-09T00:00:00Z",
                        pack_id="pack_short",
                        live_run_id="run_1",
                        symbols=["BNBUSDT"],
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 1,
                            "total_realized_pnl": -0.04,
                            "total_unrealized_pnl": 2.0,
                            "equity": 10001.96,
                            "max_position_value": 98.0,
                            "positions": {
                                "BNBUSDT": {
                                    "symbol": "BNBUSDT",
                                    "size": -1.0,
                                    "avg_entry_price": 100.0,
                                    "realized_pnl": -0.04,
                                    "unrealized_pnl": 2.0,
                                    "current_price": 98.0,
                                }
                            },
                        },
                        execution_events=[],
                        funding_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1773014401990",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "funding_rate": 0.001,
                                "next_funding_ts": "1773014402000",
                            }
                        ],
                        mark_price_events=[
                            {
                                "event_seq": 1,
                                "ts_event": "1773014401995",
                                "exchange": "bybit",
                                "symbol": "BNBUSDT",
                                "mark_price": 98.0,
                                "index_price": 98.1,
                            }
                        ],
                    )
                ],
            )
            write_jsonl(
                events,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_short|run_1|event|1",
                        "observation_key": "pack_short|run_1",
                        "observed_at": "2026-03-09T00:00:00Z",
                        "selected_pack_id": "pack_short",
                        "live_run_id": "run_1",
                        "event_seq": 1,
                        "event_type": "FILL",
                        "ts_event": "1773014401000",
                        "symbol": "BNBUSDT",
                        "side": "SELL",
                        "qty": 1.0,
                        "fill_price": 100.0,
                        "fill_fee": 0.04,
                        "fill_value": 100.0,
                        "reason": "",
                    }
                ],
            )
            write_json(
                bindings,
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": [make_binding_item(pack_id="pack_short", family_id="momentum_v1", binding_mode="PAPER_DIRECTIONAL_V1")],
                },
            )

            res = self._run(
                "--history-jsonl",
                str(history),
                "--execution-events-jsonl",
                str(events),
                "--binding-artifact",
                str(bindings),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            item = json.loads(out_json.read_text(encoding="utf-8"))["items"][0]
            self.assertAlmostEqual(item["funding_cost_quote"], -0.098)
            self.assertEqual(item["funding_support_status"], "FUNDING_COST_BACKED")
            self.assertEqual(item["funding_windows"][0]["position_direction"], "SHORT")
            self.assertAlmostEqual(item["mark_to_market_pnl_quote_net_after_funding"], 2.058)


if __name__ == "__main__":
    unittest.main()
