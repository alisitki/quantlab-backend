import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_execution_ledger_v0.py"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_history_entry(
    *,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    stop_reason: str = "STREAM_END",
    execution_summary: dict | None = None,
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
        "started_at": "2026-03-07T13:14:30Z",
        "finished_at": "2026-03-07T13:15:00Z",
        "stop_reason": stop_reason,
        "run_duration_sec": 30.0,
        "verify_soft_live_pass": True,
        "processed_event_count": 16,
        "heartbeat_seen": True,
        "execution_summary": execution_summary,
    }


class ShadowExecutionLedgerV0Tests(unittest.TestCase):
    def _run(self, *args: str):
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_populated_execution_summary_writes_ledger_row(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_ledger_populated_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            ledger = root / "ledger.jsonl"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 1,
                            "fills_count": 2,
                            "total_realized_pnl": 1.25,
                            "total_unrealized_pnl": -0.1,
                            "equity": 10001.15,
                            "max_position_value": 250.0,
                        },
                    )
                ],
            )
            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(ledger))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(ledger)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["selected_pack_id"], "pack_a")
            self.assertEqual(rows[0]["fills_count"], 2)
            self.assertEqual(rows[0]["pnl_state"], "ACTIVE_POSITION")

    def test_missing_execution_summary_uses_no_snapshot_fallback(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_ledger_missing_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            ledger = root / "ledger.jsonl"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        execution_summary=None,
                    )
                ],
            )
            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(ledger))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(ledger)
            self.assertEqual(rows[0]["snapshot_present"], False)
            self.assertEqual(rows[0]["fills_count"], 0)
            self.assertEqual(rows[0]["pnl_state"], "NO_SNAPSHOT")

    def test_pnl_state_derivation_variants(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_ledger_pnl_state_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            ledger = root / "ledger.jsonl"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 0,
                            "total_realized_pnl": None,
                            "total_unrealized_pnl": None,
                            "equity": None,
                            "max_position_value": None,
                        },
                    ),
                    make_history_entry(
                        observation_key="pack_b|run_b",
                        observed_at="2026-03-07T13:16:00Z",
                        pack_id="pack_b",
                        live_run_id="run_b",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 2,
                            "total_realized_pnl": 3.5,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10003.5,
                            "max_position_value": 200.0,
                        },
                    ),
                    make_history_entry(
                        observation_key="pack_c|run_c",
                        observed_at="2026-03-07T13:17:00Z",
                        pack_id="pack_c",
                        live_run_id="run_c",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 2,
                            "total_realized_pnl": -1.5,
                            "total_unrealized_pnl": 0.0,
                            "equity": 9998.5,
                            "max_position_value": 200.0,
                        },
                    ),
                    make_history_entry(
                        observation_key="pack_d|run_d",
                        observed_at="2026-03-07T13:18:00Z",
                        pack_id="pack_d",
                        live_run_id="run_d",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 2,
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10000.0,
                            "max_position_value": 200.0,
                        },
                    ),
                ],
            )
            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(ledger))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = {row["selected_pack_id"]: row for row in load_jsonl(ledger)}
            self.assertEqual(rows["pack_a"]["pnl_state"], "FLAT_NO_FILLS")
            self.assertEqual(rows["pack_b"]["pnl_state"], "REALIZED_GAIN")
            self.assertEqual(rows["pack_c"]["pnl_state"], "REALIZED_LOSS")
            self.assertEqual(rows["pack_d"]["pnl_state"], "REALIZED_FLAT")

    def test_duplicate_observation_key_is_deduped_deterministically(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_ledger_dup_") as td:
            root = Path(td)
            history = root / "history.jsonl"
            ledger = root / "ledger.jsonl"
            write_jsonl(
                history,
                [
                    make_history_entry(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 1,
                            "total_realized_pnl": 1.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10001.0,
                            "max_position_value": 100.0,
                        },
                    ),
                    make_history_entry(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        execution_summary={
                            "snapshot_present": True,
                            "positions_count": 0,
                            "fills_count": 2,
                            "total_realized_pnl": 2.0,
                            "total_unrealized_pnl": 0.0,
                            "equity": 10002.0,
                            "max_position_value": 150.0,
                        },
                    ),
                ],
            )
            res = self._run("--history-jsonl", str(history), "--out-jsonl", str(ledger))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_jsonl(ledger)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["fills_count"], 2)
            self.assertEqual(rows[0]["total_realized_pnl"], 2.0)


if __name__ == "__main__":
    unittest.main()
