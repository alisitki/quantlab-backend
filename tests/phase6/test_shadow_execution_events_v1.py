import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_execution_events_v1.py"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_history_entry(
    *,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    execution_events,
) -> dict:
    return {
        "schema_version": "shadow_observation_history_v0",
        "observation_key": observation_key,
        "observed_at": observed_at,
        "selected_pack_id": pack_id,
        "selected_rank": 1,
        "selected_exchange": "bybit",
        "selected_symbols": ["BNBUSDT"],
        "selected_decision_tier": "PROMOTE_STRONG",
        "selected_selection_slot": "bybit/bbo",
        "live_run_id": live_run_id,
        "started_at": "2026-03-08T12:00:00Z",
        "finished_at": "2026-03-08T12:00:10Z",
        "stop_reason": "STREAM_END",
        "run_duration_sec": 10.0,
        "verify_soft_live_pass": True,
        "processed_event_count": 4,
        "heartbeat_seen": True,
        "execution_summary": {
            "snapshot_present": True,
            "positions_count": 1,
            "fills_count": 1,
            "total_realized_pnl": 0.0,
            "total_unrealized_pnl": 0.0,
            "equity": 10000.0,
            "max_position_value": 100.0,
        },
        "execution_events": execution_events,
    }


class ShadowExecutionEventsV1Tests(unittest.TestCase):
    def _run(self, *args: str):
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_empty_history_writes_empty_jsonl(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_events_empty_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            out_jsonl = root / "events.jsonl"
            history.write_text("", encoding="utf-8")

            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(out_jsonl))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertEqual(load_jsonl(out_jsonl), [])
            self.assertIn("event_count=0", res.stdout)

    def test_flattened_rows_preserve_observation_order_and_event_seq(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_events_flatten_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            out_jsonl = root / "events.jsonl"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_b|run_2",
                        observed_at="2026-03-08T12:02:00Z",
                        pack_id="pack_b",
                        live_run_id="run_2",
                        execution_events=[
                            {
                                "event_seq": 2,
                                "event_type": "FILL",
                                "ts_event": "1700000002",
                                "symbol": "ETHUSDT",
                                "side": "SELL",
                                "qty": 2,
                                "fill_price": 2001.5,
                                "fill_fee": 1.6012,
                                "fill_value": 4003.0,
                                "reason": "",
                            }
                        ],
                    ),
                    make_history_entry(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-08T12:01:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        execution_events=[
                            {
                                "event_seq": 1,
                                "event_type": "DECISION",
                                "ts_event": "1700000000",
                                "symbol": "BNBUSDT",
                                "side": "BUY",
                                "qty": 1,
                                "fill_price": None,
                                "reason": "",
                            },
                            {
                                "event_seq": 2,
                                "event_type": "RISK_REJECT",
                                "ts_event": "1700000001",
                                "symbol": "BNBUSDT",
                                "side": "BUY",
                                "qty": 1,
                                "fill_price": None,
                                "reason": "max_position_exceeded",
                            },
                        ],
                    ),
                ],
            )

            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(out_jsonl))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(out_jsonl)
            self.assertEqual([row["selected_pack_id"] for row in rows], ["pack_a", "pack_a", "pack_b"])
            self.assertEqual(rows[0]["event_id"], "pack_a|run_1|event|1")
            self.assertEqual(rows[0]["event_type"], "DECISION")
            self.assertEqual(rows[1]["event_type"], "RISK_REJECT")
            self.assertEqual(rows[1]["reason"], "max_position_exceeded")
            self.assertEqual(rows[2]["fill_price"], 2001.5)
            self.assertEqual(rows[2]["fill_fee"], 1.6012)
            self.assertEqual(rows[2]["fill_value"], 4003.0)
            self.assertIn("event_count=3", res.stdout)

    def test_missing_execution_events_is_deterministic_empty(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_events_missing_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            out_jsonl = root / "events.jsonl"
            row = make_history_entry(
                observation_key="pack_a|run_1",
                observed_at="2026-03-08T12:01:00Z",
                pack_id="pack_a",
                live_run_id="run_1",
                execution_events=[],
            )
            row.pop("execution_events", None)
            write_jsonl(history, [row])

            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(out_jsonl))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertEqual(load_jsonl(out_jsonl), [])
            self.assertIn("history_count=1", res.stdout)

    def test_schema_mismatch_fails_fast(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_events_bad_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            out_jsonl = root / "events.jsonl"
            write_jsonl(history, [{"schema_version": "not_history", "observation_key": "x"}])

            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(out_jsonl))
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("history_schema_mismatch:", res.stderr)


if __name__ == "__main__":
    unittest.main()
