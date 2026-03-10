import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_observation_summary_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def make_watchlist_item(*, rank: int, pack_id: str) -> dict:
    return {
        "rank": rank,
        "selection_slot": "bybit/bbo" if rank == 1 else "overall_fill",
        "pack_id": pack_id,
        "pack_path": f"/tmp/{pack_id}",
        "decision_tier": "PROMOTE_STRONG",
        "score": "64.534273",
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": ["bnbusdt"],
        "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
        "watch_status": "ACTIVE",
        "notes": "",
    }


class ShadowObservationSummaryV0Tests(unittest.TestCase):
    def _run(self, *args: str):
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_good_path_writes_summary_json_with_exact_fields(self):
        with tempfile.TemporaryDirectory(prefix="shadow_summary_good_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            summary_json = root / "quantlab-soft-live.json"
            audit_dir = root / "audit"
            stdout_log = root / "run.stdout.log"
            out_json = root / "shadow_observation_summary_v0.json"

            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T06:22:36Z",
                    "source": "tools/phase6_state/candidate_review.tsv",
                    "selection_policy": {},
                    "items": [
                        make_watchlist_item(rank=1, pack_id="pack_a"),
                        make_watchlist_item(rank=2, pack_id="pack_b"),
                    ],
                },
            )
            write_json(
                summary_json,
                {
                    "live_run_id": "live_run_123",
                    "started_at": "2026-03-07T07:16:21.367Z",
                    "finished_at": "2026-03-07T07:16:43.420Z",
                    "stop_reason": "STREAM_END",
                    "funding_events": [
                        {
                            "event_seq": 1,
                            "ts_event": "1700000000000",
                            "exchange": "bybit",
                            "symbol": "bnbusdt",
                            "funding_rate": 0.0001,
                            "next_funding_ts": "1700003600000",
                        }
                    ],
                    "mark_price_events": [
                        {
                            "event_seq": 1,
                            "ts_event": "1700000000000",
                            "exchange": "bybit",
                            "symbol": "bnbusdt",
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
                            "bnbusdt": {
                                "size": 1.0,
                                "avg_entry_price": 612.5,
                                "realized_pnl": 1.25,
                                "unrealized_pnl": -0.1,
                                "current_price": 612.4,
                            }
                        },
                    },
                },
            )
            write_jsonl(
                audit_dir / "date=20260307" / "part-a.jsonl",
                [
                    {
                        "action": "RUN_START",
                        "metadata": {"live_run_id": "live_run_123"},
                    },
                    {
                        "action": "DECISION",
                        "metadata": {
                            "live_run_id": "live_run_123",
                            "ts_event": "1700000000000000000",
                            "symbol": "bnbusdt",
                            "side": "buy",
                            "qty": 1,
                        },
                    },
                    {
                        "action": "RISK_REJECT",
                        "metadata": {
                            "live_run_id": "live_run_123",
                            "ts_event": "1700000000000000100",
                            "symbol": "bnbusdt",
                            "side": "sell",
                            "qty": 0.5,
                            "risk_reason": "max_position_exceeded",
                        },
                    },
                    {
                        "action": "FILL",
                        "metadata": {
                            "live_run_id": "live_run_123",
                            "ts_event": "1700000000000000200",
                            "symbol": "bnbusdt",
                            "side": "buy",
                            "qty": 1,
                            "fill_price": 612.5,
                            "fill_fee": 0.245,
                            "fill_value": 612.5,
                        },
                    },
                    {
                        "action": "FILL",
                        "metadata": {
                            "live_run_id": "live_run_123",
                            "ts_event": "1700000000000000300",
                            "symbol": "bnbusdt",
                            "side": "buy",
                            "qty": 0,
                            "fill_price": 0,
                        },
                    },
                    {
                        "action": "RUN_STOP",
                        "metadata": {"live_run_id": "live_run_123"},
                    },
                ],
            )
            stdout_log.write_text(
                "\n".join(
                    [
                        '{"event":"soft_live_heartbeat","live_run_id":"live_run_123","status":"RUNNING"}',
                        "[run_abc] [INFO] total_processed: 16",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            res = self._run(
                "--watchlist",
                str(watchlist),
                "--summary-json",
                str(summary_json),
                "--audit-spool-dir",
                str(audit_dir),
                "--stdout-log",
                str(stdout_log),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertTrue(out_json.exists())

            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_observation_summary_v0")
            self.assertEqual(payload["selected_rank"], 1)
            self.assertEqual(payload["selected_pack_id"], "pack_a")
            self.assertEqual(payload["selected_exchange"], "bybit")
            self.assertEqual(payload["selected_symbols"], ["BNBUSDT"])
            self.assertEqual(payload["selected_decision_tier"], "PROMOTE_STRONG")
            self.assertEqual(payload["selected_selection_slot"], "bybit/bbo")
            self.assertEqual(payload["live_run_id"], "live_run_123")
            self.assertEqual(payload["stop_reason"], "STREAM_END")
            self.assertEqual(payload["run_duration_sec"], 22.053)
            self.assertTrue(payload["audit_run_start_seen"])
            self.assertTrue(payload["audit_run_stop_seen"])
            self.assertTrue(payload["verify_soft_live_pass"])
            self.assertEqual(payload["processed_event_count"], 16)
            self.assertTrue(payload["heartbeat_seen"])
            self.assertEqual(payload["heartbeat_count"], 1)
            self.assertEqual(
                payload["funding_events"],
                [
                    {
                        "event_seq": 1,
                        "ts_event": "1700000000000",
                        "exchange": "bybit",
                        "symbol": "BNBUSDT",
                        "funding_rate": 0.0001,
                        "next_funding_ts": "1700003600000",
                    }
                ],
            )
            self.assertEqual(
                payload["mark_price_events"],
                [
                    {
                        "event_seq": 1,
                        "ts_event": "1700000000000",
                        "exchange": "bybit",
                        "symbol": "BNBUSDT",
                        "mark_price": 612.45,
                        "index_price": 612.4,
                    }
                ],
            )
            self.assertEqual(
                payload["execution_summary"],
                {
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
            )
            self.assertEqual(
                payload["execution_events"],
                [
                    {
                        "event_seq": 1,
                        "event_type": "DECISION",
                        "ts_event": "1700000000000000000",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": None,
                        "fill_fee": None,
                        "fill_value": None,
                        "reason": "",
                    },
                    {
                        "event_seq": 2,
                        "event_type": "RISK_REJECT",
                        "ts_event": "1700000000000000100",
                        "symbol": "BNBUSDT",
                        "side": "SELL",
                        "qty": 0.5,
                        "fill_price": None,
                        "fill_fee": None,
                        "fill_value": None,
                        "reason": "max_position_exceeded",
                    },
                    {
                        "event_seq": 3,
                        "event_type": "FILL",
                        "ts_event": "1700000000000000200",
                        "symbol": "BNBUSDT",
                        "side": "BUY",
                        "qty": 1.0,
                        "fill_price": 612.5,
                        "fill_fee": 0.245,
                        "fill_value": 612.5,
                        "reason": "",
                    },
                ],
            )

    def test_missing_summary_file_fails_fast(self):
        with tempfile.TemporaryDirectory(prefix="shadow_summary_missing_summary_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T06:22:36Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {},
                    "items": [make_watchlist_item(rank=1, pack_id="pack_a")],
                },
            )
            res = self._run(
                "--watchlist",
                str(watchlist),
                "--summary-json",
                str(root / "missing-summary.json"),
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("summary_json_missing:", res.stderr)

    def test_missing_audit_dir_sets_false_and_note(self):
        with tempfile.TemporaryDirectory(prefix="shadow_summary_missing_audit_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            summary_json = root / "quantlab-soft-live.json"
            out_json = root / "shadow_observation_summary_v0.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T06:22:36Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {},
                    "items": [make_watchlist_item(rank=1, pack_id="pack_a")],
                },
            )
            write_json(
                summary_json,
                {
                    "live_run_id": "live_run_missing_audit",
                    "started_at": "2026-03-07T07:16:21.367Z",
                    "finished_at": "2026-03-07T07:16:43.420Z",
                    "stop_reason": "STREAM_END",
                },
            )
            res = self._run(
                "--watchlist",
                str(watchlist),
                "--summary-json",
                str(summary_json),
                "--audit-spool-dir",
                str(root / "missing-audit"),
                "--out-json",
                str(out_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertFalse(payload["audit_run_start_seen"])
            self.assertFalse(payload["audit_run_stop_seen"])
            self.assertFalse(payload["verify_soft_live_pass"])
            self.assertEqual(payload["processed_event_count"], "unknown")
            self.assertEqual(payload["heartbeat_seen"], "unknown")
            self.assertEqual(payload["execution_events"], [])
            self.assertEqual(payload["funding_events"], [])
            self.assertEqual(payload["mark_price_events"], [])
            self.assertIn("audit_spool_missing", payload["note"])
            self.assertEqual(
                payload["execution_summary"],
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

    def test_bad_rank_and_bad_pack_id_fail(self):
        with tempfile.TemporaryDirectory(prefix="shadow_summary_bad_selection_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            summary_json = root / "quantlab-soft-live.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T06:22:36Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {},
                    "items": [make_watchlist_item(rank=1, pack_id="pack_a")],
                },
            )
            write_json(
                summary_json,
                {
                    "live_run_id": "live_run_123",
                    "started_at": "2026-03-07T07:16:21.367Z",
                    "finished_at": "2026-03-07T07:16:43.420Z",
                    "stop_reason": "STREAM_END",
                },
            )

            res_rank = self._run(
                "--watchlist",
                str(watchlist),
                "--summary-json",
                str(summary_json),
                "--rank",
                "5",
            )
            self.assertNotEqual(res_rank.returncode, 0)
            self.assertIn("rank_not_found:5", res_rank.stderr)

            res_pack = self._run(
                "--watchlist",
                str(watchlist),
                "--summary-json",
                str(summary_json),
                "--pack-id",
                "missing_pack",
            )
            self.assertNotEqual(res_pack.returncode, 0)
            self.assertIn("pack_id_not_found:missing_pack", res_pack.stderr)


if __name__ == "__main__":
    unittest.main()
