import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_execution_pack_summary_v0.py"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def make_ledger_row(
    *,
    observation_key: str,
    observed_at: str,
    pack_id: str,
    live_run_id: str,
    stop_reason: str = "STREAM_END",
    snapshot_present: bool = True,
    positions_count: int = 0,
    fills_count: int = 0,
    total_realized_pnl=None,
    total_unrealized_pnl=None,
    equity=None,
    max_position_value=None,
    pnl_state: str = "FLAT_NO_FILLS",
) -> dict:
    return {
        "schema_version": "shadow_execution_ledger_v0",
        "observation_key": observation_key,
        "observed_at": observed_at,
        "selected_pack_id": pack_id,
        "live_run_id": live_run_id,
        "stop_reason": stop_reason,
        "snapshot_present": snapshot_present,
        "positions_count": positions_count,
        "fills_count": fills_count,
        "total_realized_pnl": total_realized_pnl,
        "total_unrealized_pnl": total_unrealized_pnl,
        "equity": equity,
        "max_position_value": max_position_value,
        "pnl_state": pnl_state,
    }


class ShadowExecutionPackSummaryV0Tests(unittest.TestCase):
    def _run(self, *args: str):
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_single_pack_single_run(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_single_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            out_json = root / "summary.json"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        positions_count=1,
                        fills_count=2,
                        total_realized_pnl=1.25,
                        total_unrealized_pnl=-0.1,
                        equity=10001.15,
                        max_position_value=250.0,
                        pnl_state="ACTIVE_POSITION",
                    )
                ],
            )
            res = self._run("--ledger-jsonl", str(ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = load_json(out_json)
            self.assertEqual(payload["record_count"], 1)
            self.assertEqual(payload["pack_count"], 1)
            latest = payload["latest_by_pack_id"]["pack_a"]
            self.assertEqual(latest["last_live_run_id"], "run_a")
            self.assertEqual(latest["last_fills_count"], 2)
            self.assertEqual(latest["last_pnl_state"], "ACTIVE_POSITION")
            self.assertEqual(latest["latest_realized_sign"], "GAIN")
            self.assertEqual(latest["latest_unrealized_sign"], "LOSS")
            self.assertEqual(latest["pnl_interpretation"], "ACTIVE_LOSING")
            self.assertEqual(latest["pnl_attention_flag"], True)
            self.assertEqual(latest["run_count"], 1)
            self.assertEqual(latest["recent_run_count"], 1)
            self.assertEqual(latest["recent_gain_count"], 0)
            self.assertEqual(latest["recent_loss_count"], 1)
            self.assertEqual(latest["recent_flat_count"], 0)
            self.assertEqual(latest["recent_attention_count"], 1)
            self.assertEqual(latest["recent_pnl_bias"], "LOSS_BIAS")

    def test_same_pack_multiple_runs_uses_latest_observed_at_then_live_run_id(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_latest_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            out_json = root / "summary.json"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_a",
                        fills_count=1,
                        total_realized_pnl=1.0,
                        pnl_state="REALIZED_GAIN",
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_b",
                        observed_at="2026-03-07T13:16:00Z",
                        pack_id="pack_a",
                        live_run_id="run_b",
                        fills_count=2,
                        total_realized_pnl=-1.0,
                        pnl_state="REALIZED_LOSS",
                    ),
                ],
            )
            res = self._run("--ledger-jsonl", str(ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            latest = load_json(out_json)["latest_by_pack_id"]["pack_a"]
            self.assertEqual(latest["last_live_run_id"], "run_b")
            self.assertEqual(latest["last_pnl_state"], "REALIZED_LOSS")
            self.assertEqual(latest["latest_realized_sign"], "LOSS")
            self.assertEqual(latest["pnl_interpretation"], "REALIZED_LOSS")
            self.assertEqual(latest["pnl_attention_flag"], True)
            self.assertEqual(latest["run_count"], 2)
            self.assertEqual(latest["recent_run_count"], 2)
            self.assertEqual(latest["recent_gain_count"], 1)
            self.assertEqual(latest["recent_loss_count"], 1)
            self.assertEqual(latest["recent_flat_count"], 0)
            self.assertEqual(latest["recent_attention_count"], 1)
            self.assertEqual(latest["recent_pnl_bias"], "MIXED")

    def test_interpretation_variants_and_isolation(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_interpretation_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            out_json = root / "summary.json"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_active_gain|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_active_gain",
                        live_run_id="run_a",
                        positions_count=1,
                        total_unrealized_pnl=1.5,
                        pnl_state="ACTIVE_POSITION",
                    ),
                    make_ledger_row(
                        observation_key="pack_active_flat|run_b",
                        observed_at="2026-03-07T13:17:00Z",
                        pack_id="pack_active_flat",
                        live_run_id="run_b",
                        positions_count=1,
                        total_unrealized_pnl=0.0,
                        pnl_state="ACTIVE_POSITION",
                    ),
                    make_ledger_row(
                        observation_key="pack_realized_gain|run_c",
                        observed_at="2026-03-07T13:18:00Z",
                        pack_id="pack_realized_gain",
                        live_run_id="run_c",
                        fills_count=2,
                        total_realized_pnl=3.0,
                        pnl_state="REALIZED_GAIN",
                    ),
                    make_ledger_row(
                        observation_key="pack_realized_flat|run_d",
                        observed_at="2026-03-07T13:19:00Z",
                        pack_id="pack_realized_flat",
                        live_run_id="run_d",
                        fills_count=2,
                        total_realized_pnl=0.0,
                        pnl_state="REALIZED_FLAT",
                    ),
                    make_ledger_row(
                        observation_key="pack_no_snapshot|run_e",
                        observed_at="2026-03-07T13:20:00Z",
                        pack_id="pack_no_snapshot",
                        live_run_id="run_e",
                        pnl_state="NO_SNAPSHOT",
                        snapshot_present=False,
                    ),
                ],
            )
            res = self._run("--ledger-jsonl", str(ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = load_json(out_json)
            self.assertEqual(payload["pack_count"], 5)
            self.assertEqual(payload["latest_by_pack_id"]["pack_active_gain"]["pnl_interpretation"], "ACTIVE_GAINING")
            self.assertEqual(payload["latest_by_pack_id"]["pack_active_gain"]["pnl_attention_flag"], False)
            self.assertEqual(payload["latest_by_pack_id"]["pack_active_gain"]["recent_pnl_bias"], "GAIN_BIAS")
            self.assertEqual(payload["latest_by_pack_id"]["pack_active_flat"]["pnl_interpretation"], "ACTIVE_FLAT")
            self.assertEqual(payload["latest_by_pack_id"]["pack_active_flat"]["recent_pnl_bias"], "FLAT_BIAS")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_gain"]["pnl_interpretation"], "REALIZED_GAIN")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_gain"]["latest_realized_sign"], "GAIN")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_gain"]["recent_pnl_bias"], "GAIN_BIAS")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_flat"]["pnl_interpretation"], "REALIZED_FLAT")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_flat"]["latest_realized_sign"], "FLAT")
            self.assertEqual(payload["latest_by_pack_id"]["pack_realized_flat"]["recent_pnl_bias"], "FLAT_BIAS")
            self.assertEqual(payload["latest_by_pack_id"]["pack_no_snapshot"]["pnl_interpretation"], "NO_SNAPSHOT")
            self.assertEqual(payload["latest_by_pack_id"]["pack_no_snapshot"]["pnl_attention_flag"], True)
            self.assertEqual(payload["latest_by_pack_id"]["pack_no_snapshot"]["recent_attention_count"], 1)
            self.assertEqual(payload["latest_by_pack_id"]["pack_no_snapshot"]["recent_pnl_bias"], "MIXED")

    def test_missing_value_fallbacks_produce_unknown_interpretation_fields(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_missing_values_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            out_json = root / "summary.json"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_unknown|run_a",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_unknown",
                        live_run_id="run_a",
                        positions_count=1,
                        total_unrealized_pnl=None,
                        pnl_state="ACTIVE_POSITION",
                    )
                ],
            )
            res = self._run("--ledger-jsonl", str(ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            latest = load_json(out_json)["latest_by_pack_id"]["pack_unknown"]
            self.assertEqual(latest["latest_unrealized_sign"], "UNKNOWN")
            self.assertEqual(latest["pnl_interpretation"], "ACTIVE_UNKNOWN")
            self.assertEqual(latest["pnl_attention_flag"], True)
            self.assertEqual(latest["recent_attention_count"], 1)
            self.assertEqual(latest["recent_pnl_bias"], "MIXED")

    def test_recent_window_uses_latest_three_runs_per_pack(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_recent_window_") as td:
            root = Path(td)
            ledger = root / "ledger.jsonl"
            out_json = root / "summary.json"
            write_jsonl(
                ledger,
                [
                    make_ledger_row(
                        observation_key="pack_a|run_1",
                        observed_at="2026-03-07T13:15:00Z",
                        pack_id="pack_a",
                        live_run_id="run_1",
                        fills_count=1,
                        total_realized_pnl=1.0,
                        pnl_state="REALIZED_GAIN",
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_2",
                        observed_at="2026-03-07T13:16:00Z",
                        pack_id="pack_a",
                        live_run_id="run_2",
                        fills_count=1,
                        total_realized_pnl=-1.0,
                        pnl_state="REALIZED_LOSS",
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_3",
                        observed_at="2026-03-07T13:17:00Z",
                        pack_id="pack_a",
                        live_run_id="run_3",
                        fills_count=0,
                        total_realized_pnl=0.0,
                        pnl_state="FLAT_NO_FILLS",
                    ),
                    make_ledger_row(
                        observation_key="pack_a|run_4",
                        observed_at="2026-03-07T13:18:00Z",
                        pack_id="pack_a",
                        live_run_id="run_4",
                        positions_count=1,
                        total_unrealized_pnl=2.0,
                        pnl_state="ACTIVE_POSITION",
                    ),
                ],
            )
            res = self._run("--ledger-jsonl", str(ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            latest = load_json(out_json)["latest_by_pack_id"]["pack_a"]
            self.assertEqual(latest["run_count"], 4)
            self.assertEqual(latest["recent_run_count"], 3)
            self.assertEqual(latest["recent_gain_count"], 1)
            self.assertEqual(latest["recent_loss_count"], 1)
            self.assertEqual(latest["recent_flat_count"], 1)
            self.assertEqual(latest["recent_attention_count"], 1)
            self.assertEqual(latest["recent_pnl_bias"], "MIXED")

    def test_missing_or_empty_ledger_produces_empty_summary(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_pack_empty_") as td:
            root = Path(td)
            missing_ledger = root / "missing.jsonl"
            out_json = root / "summary.json"
            res = self._run("--ledger-jsonl", str(missing_ledger), "--out-json", str(out_json))
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = load_json(out_json)
            self.assertEqual(payload["record_count"], 0)
            self.assertEqual(payload["pack_count"], 0)
            self.assertEqual(payload["latest_by_pack_id"], {})


if __name__ == "__main__":
    unittest.main()
