import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_observation_history_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_summary(*, pack_id: str, live_run_id: str, generated_ts_utc: str, processed_event_count):
    return {
        "schema_version": "shadow_observation_summary_v0",
        "generated_ts_utc": generated_ts_utc,
        "selected_rank": 1,
        "selected_pack_id": pack_id,
        "selected_pack_path": f"/tmp/{pack_id}",
        "selected_exchange": "bybit",
        "selected_symbols": ["BNBUSDT"],
        "selected_decision_tier": "PROMOTE_STRONG",
        "selected_selection_slot": "bybit/bbo",
        "live_run_id": live_run_id,
        "started_at": "2026-03-07T07:16:21.367Z",
        "finished_at": "2026-03-07T07:16:43.420Z",
        "stop_reason": "STREAM_END",
        "run_duration_sec": 22.053,
        "audit_run_start_seen": True,
        "audit_run_stop_seen": True,
        "verify_soft_live_pass": True,
        "processed_event_count": processed_event_count,
        "heartbeat_seen": True,
        "heartbeat_count": 5,
        "funding_events": [
            {
                "event_seq": 1,
                "ts_event": "1700000000000",
                "exchange": "bybit",
                "symbol": "BNBUSDT",
                "funding_rate": 0.0001,
                "next_funding_ts": "1700003600000",
            }
        ],
        "mark_price_events": [
            {
                "event_seq": 1,
                "ts_event": "1700000000000",
                "exchange": "bybit",
                "symbol": "BNBUSDT",
                "mark_price": 612.45,
                "index_price": 612.4,
            }
        ],
        "execution_summary": {
            "snapshot_present": True,
            "positions_count": 1,
            "fills_count": 2,
            "total_realized_pnl": 1.25,
            "total_unrealized_pnl": -0.1,
            "equity": 10001.15,
            "max_position_value": 250.0,
            "positions": {
                "BNBUSDT": {
                    "symbol": "BNBUSDT",
                    "size": 1.0,
                    "avg_entry_price": 612.5,
                    "realized_pnl": 1.25,
                    "unrealized_pnl": -0.1,
                    "current_price": 612.4,
                }
            },
        },
        "execution_events": [
            {
                "event_seq": 1,
                "event_type": "DECISION",
                "ts_event": "1700000000000000000",
                "symbol": "BNBUSDT",
                "side": "BUY",
                "qty": 1.0,
                "fill_price": None,
                "reason": "",
            },
            {
                "event_seq": 2,
                "event_type": "FILL",
                "ts_event": "1700000000000000100",
                "symbol": "BNBUSDT",
                "side": "BUY",
                "qty": 1.0,
                "fill_price": 612.5,
                "fill_fee": 0.245,
                "fill_value": 612.5,
                "reason": "",
            },
        ],
        "note": "verify_soft_live_pass_inferred_from_summary_json_and_audit",
    }


class ShadowObservationHistoryV0Tests(unittest.TestCase):
    def _run(self, *args: str):
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_first_insert_writes_history_and_index(self):
        with tempfile.TemporaryDirectory(prefix="shadow_history_first_") as td:
            root = Path(td)
            summary = root / "summary_a.json"
            history = root / "history.jsonl"
            index = root / "index.json"
            write_json(
                summary,
                make_summary(
                    pack_id="pack_a",
                    live_run_id="run_a",
                    generated_ts_utc="2026-03-07T07:24:58Z",
                    processed_event_count=16,
                ),
            )

            res = self._run(
                "--summary-json",
                str(summary),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("inserted_count=1", res.stdout)
            entries = load_jsonl(history)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["observation_key"], "pack_a|run_a")
            self.assertEqual(entries[0]["execution_summary"]["fills_count"], 2)
            self.assertEqual(entries[0]["execution_summary"]["positions"]["BNBUSDT"]["current_price"], 612.4)
            self.assertEqual(entries[0]["funding_events"][0]["funding_rate"], 0.0001)
            self.assertEqual(entries[0]["mark_price_events"][0]["mark_price"], 612.45)
            self.assertEqual(len(entries[0]["execution_events"]), 2)
            self.assertEqual(entries[0]["execution_events"][0]["event_type"], "DECISION")
            self.assertEqual(entries[0]["execution_events"][1]["fill_fee"], 0.245)
            payload = load_json(index)
            self.assertEqual(payload["schema_version"], "shadow_observation_index_v0")
            self.assertEqual(payload["record_count"], 1)
            self.assertEqual(payload["pack_count"], 1)
            self.assertEqual(payload["latest_by_pack_id"]["pack_a"]["last_live_run_id"], "run_a")
            self.assertEqual(payload["latest_by_pack_id"]["pack_a"]["last_processed_event_count"], 16)
            self.assertEqual(payload["latest_by_pack_id"]["pack_a"]["observation_count"], 1)
            self.assertEqual(payload["latest_by_pack_id"]["pack_a"]["last_execution_summary"]["positions_count"], 1)

    def test_duplicate_insert_skips_existing_key(self):
        with tempfile.TemporaryDirectory(prefix="shadow_history_dup_") as td:
            root = Path(td)
            summary = root / "summary_a.json"
            history = root / "history.jsonl"
            index = root / "index.json"
            write_json(
                summary,
                make_summary(
                    pack_id="pack_a",
                    live_run_id="run_a",
                    generated_ts_utc="2026-03-07T07:24:58Z",
                    processed_event_count=16,
                ),
            )

            first = self._run(
                "--summary-json",
                str(summary),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            second = self._run(
                "--summary-json",
                str(summary),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            self.assertEqual(first.returncode, 0, msg=first.stderr)
            self.assertEqual(second.returncode, 0, msg=second.stderr)
            self.assertIn("inserted_count=0", second.stdout)
            self.assertIn("skipped_duplicate_count=1", second.stdout)
            self.assertEqual(len(load_jsonl(history)), 1)

    def test_second_distinct_insert_updates_latest_index(self):
        with tempfile.TemporaryDirectory(prefix="shadow_history_second_") as td:
            root = Path(td)
            summary_a = root / "summary_a.json"
            summary_b = root / "summary_b.json"
            history = root / "history.jsonl"
            index = root / "index.json"
            write_json(
                summary_a,
                make_summary(
                    pack_id="pack_a",
                    live_run_id="run_a",
                    generated_ts_utc="2026-03-07T07:24:58Z",
                    processed_event_count=16,
                ),
            )
            write_json(
                summary_b,
                make_summary(
                    pack_id="pack_a",
                    live_run_id="run_b",
                    generated_ts_utc="2026-03-07T07:30:00Z",
                    processed_event_count=25,
                ),
            )

            first = self._run(
                "--summary-json",
                str(summary_a),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            second = self._run(
                "--summary-json",
                str(summary_b),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            self.assertEqual(first.returncode, 0, msg=first.stderr)
            self.assertEqual(second.returncode, 0, msg=second.stderr)
            entries = load_jsonl(history)
            self.assertEqual(len(entries), 2)
            payload = load_json(index)
            latest = payload["latest_by_pack_id"]["pack_a"]
            self.assertEqual(latest["last_live_run_id"], "run_b")
            self.assertEqual(latest["last_processed_event_count"], 25)
            self.assertEqual(latest["observation_count"], 2)
            self.assertEqual(latest["last_execution_summary"]["fills_count"], 2)
            self.assertEqual(payload["record_count"], 2)

    def test_missing_execution_summary_uses_deterministic_fallback(self):
        with tempfile.TemporaryDirectory(prefix="shadow_history_exec_fallback_") as td:
            root = Path(td)
            summary = root / "summary_a.json"
            history = root / "history.jsonl"
            index = root / "index.json"
            payload = make_summary(
                pack_id="pack_a",
                live_run_id="run_a",
                generated_ts_utc="2026-03-07T07:24:58Z",
                processed_event_count=16,
            )
            payload.pop("execution_summary", None)
            payload.pop("execution_events", None)
            payload.pop("funding_events", None)
            payload.pop("mark_price_events", None)
            write_json(summary, payload)

            res = self._run(
                "--summary-json",
                str(summary),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            entries = load_jsonl(history)
            self.assertEqual(
                entries[0]["execution_summary"],
                {
                    "snapshot_present": False,
                    "positions_count": 0,
                    "fills_count": 0,
                    "total_realized_pnl": None,
                    "total_unrealized_pnl": None,
                    "equity": None,
                    "max_position_value": None,
                    "positions": {},
                },
            )
            latest = load_json(index)["latest_by_pack_id"]["pack_a"]["last_execution_summary"]
            self.assertEqual(
                latest,
                {
                    "snapshot_present": False,
                    "positions_count": 0,
                    "fills_count": 0,
                    "total_realized_pnl": None,
                    "total_unrealized_pnl": None,
                    "equity": None,
                    "max_position_value": None,
                    "positions": {},
                },
            )
            self.assertEqual(entries[0]["execution_events"], [])
            self.assertEqual(entries[0]["funding_events"], [])
            self.assertEqual(entries[0]["mark_price_events"], [])

    def test_bad_summary_shape_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_history_bad_") as td:
            root = Path(td)
            summary = root / "bad_summary.json"
            history = root / "history.jsonl"
            index = root / "index.json"
            write_json(
                summary,
                {
                    "schema_version": "not_shadow_summary",
                    "selected_pack_id": "pack_a",
                },
            )

            res = self._run(
                "--summary-json",
                str(summary),
                "--history-jsonl",
                str(history),
                "--index-json",
                str(index),
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("summary_schema_mismatch:", res.stderr)


if __name__ == "__main__":
    unittest.main()
