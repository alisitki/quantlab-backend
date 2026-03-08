import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_trade_ledger_v1.py"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_ledger_row(
    *,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    snapshot_present: bool = True,
    positions_count: int = 0,
    fills_count: int = 0,
    total_realized_pnl=None,
    total_unrealized_pnl=None,
    max_position_value=None,
) -> dict:
    return {
        "schema_version": "shadow_execution_ledger_v0",
        "observation_key": observation_key,
        "observed_at": observed_at,
        "selected_pack_id": pack_id,
        "live_run_id": live_run_id,
        "stop_reason": "STREAM_END",
        "snapshot_present": snapshot_present,
        "positions_count": positions_count,
        "fills_count": fills_count,
        "total_realized_pnl": total_realized_pnl,
        "total_unrealized_pnl": total_unrealized_pnl,
        "equity": None,
        "max_position_value": max_position_value,
        "pnl_state": "UNKNOWN",
    }


def make_event_row(
    *,
    event_id: str,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    event_seq: int,
    event_type: str,
    ts_event: str,
    side: str,
    qty: float,
    fill_price,
    reason: str = "",
    symbol: str = "BNBUSDT",
) -> dict:
    return {
        "schema_version": "shadow_execution_events_v1",
        "event_id": event_id,
        "observation_key": observation_key,
        "observed_at": observed_at,
        "selected_pack_id": pack_id,
        "live_run_id": live_run_id,
        "event_seq": event_seq,
        "event_type": event_type,
        "ts_event": ts_event,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "fill_price": fill_price,
        "reason": reason,
    }


class ShadowTradeLedgerV1Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_missing_ledger_produces_empty_output(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_empty_") as td:
            root = Path(td)
            out_jsonl = root / "trade_ledger.jsonl"
            res = self._run(
                "--execution-ledger-jsonl",
                str(root / "missing.jsonl"),
                "--execution-events-jsonl",
                str(root / "missing-events.jsonl"),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertEqual(load_jsonl(out_jsonl), [])

    def test_position_window_closes_trade_on_flat_snapshot_with_state_fallbacks(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_close_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=1.5,
                        max_position_value=100.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=2.0,
                        max_position_value=125.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        positions_count=0,
                        fills_count=2,
                        total_realized_pnl=3.0,
                        total_unrealized_pnl=0.0,
                        max_position_value=50.0,
                    ),
                ],
            )
            write_jsonl(events, [])
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(out_jsonl)
            self.assertEqual(len(rows), 1)
            trade = rows[0]
            self.assertEqual(trade["trade_mode"], "POSITION_LIFECYCLE")
            self.assertEqual(trade["status"], "CLOSED")
            self.assertEqual(trade["opened_at"], "2026-03-08T12:00:00Z")
            self.assertEqual(trade["closed_at"], "2026-03-08T12:02:00Z")
            self.assertEqual(trade["last_live_run_id"], "run_3")
            self.assertEqual(trade["observation_count"], 3)
            self.assertEqual(trade["realized_pnl_delta"], 3.0)
            self.assertEqual(trade["latest_unrealized_pnl"], 0.0)
            self.assertEqual(trade["max_position_value_seen"], 125.0)
            self.assertIsNone(trade["side"])
            self.assertEqual(trade["open_reason"], "STATE_POSITION_OPENED")
            self.assertEqual(trade["close_reason"], "STATE_POSITION_CLOSED")
            self.assertIsNone(trade["entry_event_type"])
            self.assertIsNone(trade["exit_event_type"])
            self.assertIsNone(trade["entry_ts_event"])
            self.assertIsNone(trade["exit_ts_event"])
            self.assertIsNone(trade["entry_price"])
            self.assertIsNone(trade["exit_price"])

    def test_open_position_persists_at_end_of_history(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_open_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=-0.5,
                        max_position_value=80.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        snapshot_present=False,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.5,
                        total_unrealized_pnl=1.25,
                        max_position_value=95.0,
                    ),
                ],
            )
            write_jsonl(
                events,
                [
                    make_event_row(
                        event_id="pack_a|run_1|event|1",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000000",
                        side="BUY",
                        qty=1.0,
                        fill_price=None,
                    ),
                    make_event_row(
                        event_id="pack_a|run_1|event|2",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=2,
                        event_type="FILL",
                        ts_event="1700000001",
                        side="BUY",
                        qty=1.0,
                        fill_price=612.5,
                    ),
                ],
            )
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(out_jsonl)
            self.assertEqual(len(rows), 1)
            trade = rows[0]
            self.assertEqual(trade["status"], "OPEN")
            self.assertIsNone(trade["closed_at"])
            self.assertEqual(trade["last_observed_at"], "2026-03-08T12:02:00Z")
            self.assertEqual(trade["observation_count"], 2)
            self.assertEqual(trade["realized_pnl_delta"], 0.5)
            self.assertEqual(trade["latest_unrealized_pnl"], 1.25)
            self.assertEqual(trade["max_position_value_seen"], 95.0)
            self.assertEqual(trade["side"], "BUY")
            self.assertEqual(trade["open_reason"], "EVENT_DECISION")
            self.assertIsNone(trade["close_reason"])
            self.assertEqual(trade["entry_event_type"], "DECISION")
            self.assertIsNone(trade["exit_event_type"])
            self.assertEqual(trade["entry_ts_event"], "1700000000")
            self.assertIsNone(trade["exit_ts_event"])
            self.assertEqual(trade["entry_price"], 612.5)
            self.assertIsNone(trade["exit_price"])

    def test_closed_trade_uses_event_supported_side_reason_and_fill_prices(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_event_close_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=1.5,
                        max_position_value=100.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=2.0,
                        max_position_value=125.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        positions_count=0,
                        fills_count=2,
                        total_realized_pnl=3.0,
                        total_unrealized_pnl=0.0,
                        max_position_value=50.0,
                    ),
                ],
            )
            write_jsonl(
                events,
                [
                    make_event_row(
                        event_id="pack_a|run_1|event|1",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000000",
                        side="BUY",
                        qty=1.0,
                        fill_price=None,
                    ),
                    make_event_row(
                        event_id="pack_a|run_1|event|2",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=2,
                        event_type="FILL",
                        ts_event="1700000001",
                        side="BUY",
                        qty=1.0,
                        fill_price=612.5,
                    ),
                    make_event_row(
                        event_id="pack_a|run_3|event|1",
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000002",
                        side="SELL",
                        qty=1.0,
                        fill_price=None,
                    ),
                    make_event_row(
                        event_id="pack_a|run_3|event|2",
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        event_seq=2,
                        event_type="FILL",
                        ts_event="1700000003",
                        side="SELL",
                        qty=1.0,
                        fill_price=620.0,
                    ),
                ],
            )
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            trade = load_jsonl(out_jsonl)[0]
            self.assertEqual(trade["side"], "BUY")
            self.assertEqual(trade["open_reason"], "EVENT_DECISION")
            self.assertEqual(trade["close_reason"], "EVENT_FILL")
            self.assertEqual(trade["entry_event_type"], "DECISION")
            self.assertEqual(trade["exit_event_type"], "FILL")
            self.assertEqual(trade["entry_ts_event"], "1700000000")
            self.assertEqual(trade["exit_ts_event"], "1700000003")
            self.assertEqual(trade["entry_price"], 612.5)
            self.assertEqual(trade["exit_price"], 620.0)

    def test_intrarun_realized_trade_emits_closed_trade_with_event_enrichment(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_intrarun_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=0,
                        fills_count=2,
                        total_realized_pnl=1.5,
                        total_unrealized_pnl=0.0,
                        max_position_value=60.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        positions_count=0,
                        fills_count=2,
                        total_realized_pnl=2.0,
                        total_unrealized_pnl=0.0,
                        max_position_value=55.0,
                    ),
                ],
            )
            write_jsonl(
                events,
                [
                    make_event_row(
                        event_id="pack_a|run_1|event|1",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000000",
                        side="BUY",
                        qty=1.0,
                        fill_price=None,
                    ),
                    make_event_row(
                        event_id="pack_a|run_1|event|2",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=2,
                        event_type="FILL",
                        ts_event="1700000001",
                        side="BUY",
                        qty=1.0,
                        fill_price=600.0,
                    ),
                    make_event_row(
                        event_id="pack_a|run_1|event|3",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=3,
                        event_type="FILL",
                        ts_event="1700000002",
                        side="SELL",
                        qty=1.0,
                        fill_price=601.5,
                    ),
                ],
            )
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(out_jsonl)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["trade_mode"], "INTRARUN_REALIZED")
            self.assertEqual(rows[0]["status"], "CLOSED")
            self.assertEqual(rows[0]["realized_pnl_delta"], 1.5)
            self.assertEqual(rows[0]["side"], "BUY")
            self.assertEqual(rows[0]["open_reason"], "EVENT_DECISION")
            self.assertEqual(rows[0]["close_reason"], "EVENT_FILL")
            self.assertEqual(rows[0]["entry_price"], 600.0)
            self.assertEqual(rows[0]["exit_price"], 601.5)
            self.assertEqual(rows[1]["realized_pnl_delta"], 0.5)
            self.assertEqual(rows[1]["opened_at"], "2026-03-08T12:01:00Z")
            self.assertIsNone(rows[1]["side"])
            self.assertEqual(rows[1]["open_reason"], "STATE_INTRARUN_REALIZED")
            self.assertEqual(rows[1]["close_reason"], "STATE_INTRARUN_REALIZED")
            self.assertIsNone(rows[1]["entry_price"])
            self.assertIsNone(rows[1]["exit_price"])

    def test_decision_only_exit_keeps_prices_null(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_decision_only_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=1.0,
                        max_position_value=50.0,
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        positions_count=0,
                        fills_count=1,
                        total_realized_pnl=0.75,
                        total_unrealized_pnl=0.0,
                        max_position_value=25.0,
                    ),
                ],
            )
            write_jsonl(
                events,
                [
                    make_event_row(
                        event_id="pack_a|run_1|event|1",
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000100",
                        side="BUY",
                        qty=1.0,
                        fill_price=None,
                    ),
                    make_event_row(
                        event_id="pack_a|run_2|event|1",
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        event_seq=1,
                        event_type="DECISION",
                        ts_event="1700000101",
                        side="SELL",
                        qty=1.0,
                        fill_price=None,
                    ),
                ],
            )
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            trade = load_jsonl(out_jsonl)[0]
            self.assertEqual(trade["close_reason"], "EVENT_DECISION")
            self.assertEqual(trade["exit_event_type"], "DECISION")
            self.assertIsNone(trade["entry_price"])
            self.assertIsNone(trade["exit_price"])

    def test_schema_mismatch_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_schema_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    {
                        "schema_version": "wrong_schema",
                        "observation_key": "pack_a|run_1",
                        "observed_at": "2026-03-08T12:00:00Z",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_1",
                        "snapshot_present": True,
                        "positions_count": 1,
                        "fills_count": 1,
                        "total_realized_pnl": 0.0,
                        "total_unrealized_pnl": 1.0,
                        "max_position_value": 10.0,
                    }
                ],
            )
            write_jsonl(events, [])
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 1)
            self.assertIn("SHADOW_TRADE_LEDGER_V1_ERROR", res.stderr)

    def test_bad_event_schema_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_trade_ledger_events_schema_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            events = root / "events.jsonl"
            out_jsonl = root / "trade_ledger.jsonl"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:00:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        positions_count=1,
                        fills_count=1,
                        total_realized_pnl=0.0,
                        total_unrealized_pnl=1.0,
                        max_position_value=50.0,
                    )
                ],
            )
            write_jsonl(events, [{"schema_version": "wrong_events", "event_id": "bad"}])
            res = self._run(
                "--execution-ledger-jsonl",
                str(ledger),
                "--execution-events-jsonl",
                str(events),
                "--out-jsonl",
                str(out_jsonl),
            )
            self.assertEqual(res.returncode, 1)
            self.assertIn("execution_events_schema_mismatch:", res.stderr)


if __name__ == "__main__":
    unittest.main()
