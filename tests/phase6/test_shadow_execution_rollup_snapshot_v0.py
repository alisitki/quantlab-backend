import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SNAPSHOT_SCRIPT = REPO / "tools" / "shadow_execution_rollup_snapshot_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def pack_item(
    *,
    pack_id: str,
    last_observed_at: str = "2026-03-07T13:39:26Z",
    last_pnl_state: str = "FLAT_NO_FILLS",
    pnl_interpretation: str = "FLAT_NO_FILLS",
    pnl_attention_flag=False,
    recent_run_count: int = 3,
    recent_gain_count: int = 0,
    recent_loss_count: int = 0,
    recent_flat_count: int = 1,
    recent_attention_count: int = 2,
    recent_pnl_bias: str = "FLAT_BIAS",
) -> dict:
    return {
        "selected_pack_id": pack_id,
        "last_observed_at": last_observed_at,
        "last_live_run_id": "run_a",
        "last_stop_reason": "STREAM_END",
        "last_snapshot_present": True,
        "last_positions_count": 0,
        "last_fills_count": 0,
        "last_total_realized_pnl": 0.0,
        "last_total_unrealized_pnl": 0.0,
        "last_equity": None,
        "last_max_position_value": None,
        "last_pnl_state": last_pnl_state,
        "latest_realized_sign": "FLAT",
        "latest_unrealized_sign": "FLAT",
        "pnl_interpretation": pnl_interpretation,
        "pnl_attention_flag": pnl_attention_flag,
        "run_count": 4,
        "recent_run_count": recent_run_count,
        "recent_gain_count": recent_gain_count,
        "recent_loss_count": recent_loss_count,
        "recent_flat_count": recent_flat_count,
        "recent_attention_count": recent_attention_count,
        "recent_pnl_bias": recent_pnl_bias,
    }


class ShadowExecutionRollupSnapshotV0Tests(unittest.TestCase):
    def _run(self, pack_summary: Path, out_json: Path):
        return subprocess.run(
            [
                "python3",
                str(SNAPSHOT_SCRIPT),
                "--pack-summary",
                str(pack_summary),
                "--out-json",
                str(out_json),
            ],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_good_path_snapshot_generation(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_rollup_snapshot_") as td:
            root = Path(td)
            pack_summary = root / "pack_summary.json"
            out_json = root / "snapshot.json"
            write_json(
                pack_summary,
                {
                    "schema_version": "shadow_execution_pack_summary_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "record_count": 2,
                    "pack_count": 2,
                    "latest_by_pack_id": {
                        "pack_a": pack_item(pack_id="pack_a"),
                        "pack_b": pack_item(
                            pack_id="pack_b",
                            last_pnl_state="REALIZED_LOSS",
                            pnl_interpretation="REALIZED_LOSS",
                            pnl_attention_flag=True,
                            recent_gain_count=1,
                            recent_loss_count=1,
                            recent_flat_count=0,
                            recent_attention_count=1,
                            recent_pnl_bias="MIXED",
                        ),
                    },
                },
            )
            result = self._run(pack_summary, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_execution_rollup_snapshot_v0")
            self.assertEqual(payload["selected_count"], 2)
            self.assertEqual(payload["items"][0]["selected_pack_id"], "pack_a")
            self.assertEqual(payload["items"][0]["combined_pnl_status_short"], "FLAT_NO_FILLS/FLAT_BIAS")
            self.assertEqual(payload["items"][0]["recent_rollup_short"], "r3:g0/l0/f1/a2")
            self.assertEqual(payload["items"][0]["pnl_rollup_attention"], "true")
            self.assertEqual(payload["items"][1]["combined_pnl_status_short"], "REALIZED_LOSS/MIXED")

    def test_empty_pack_summary_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_rollup_snapshot_empty_") as td:
            root = Path(td)
            pack_summary = root / "pack_summary.json"
            out_json = root / "snapshot.json"
            write_json(
                pack_summary,
                {
                    "schema_version": "shadow_execution_pack_summary_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "record_count": 0,
                    "pack_count": 0,
                    "latest_by_pack_id": {},
                },
            )
            result = self._run(pack_summary, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_count"], 0)
            self.assertEqual(payload["items"], [])

    def test_invalid_pack_summary_shape_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_rollup_snapshot_bad_") as td:
            root = Path(td)
            pack_summary = root / "pack_summary.json"
            out_json = root / "snapshot.json"
            write_json(
                pack_summary,
                {
                    "schema_version": "shadow_execution_pack_summary_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "record_count": 1,
                    "pack_count": 1,
                    "latest_by_pack_id": {
                        "pack_a": {
                            "selected_pack_id": "pack_a",
                        }
                    },
                },
            )
            result = self._run(pack_summary, out_json)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("pack_summary_item_missing_fields", result.stderr)

    def test_attention_flag_and_consistency_are_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_rollup_snapshot_attention_") as td:
            root = Path(td)
            pack_summary = root / "pack_summary.json"
            out_json = root / "snapshot.json"
            write_json(
                pack_summary,
                {
                    "schema_version": "shadow_execution_pack_summary_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "record_count": 1,
                    "pack_count": 1,
                    "latest_by_pack_id": {
                        "pack_a": pack_item(
                            pack_id="pack_a",
                            last_pnl_state="REALIZED_GAIN",
                            pnl_interpretation="REALIZED_GAIN",
                            pnl_attention_flag=False,
                            recent_run_count=2,
                            recent_gain_count=2,
                            recent_loss_count=0,
                            recent_flat_count=0,
                            recent_attention_count=0,
                            recent_pnl_bias="GAIN_BIAS",
                        )
                    },
                },
            )
            result = self._run(pack_summary, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["combined_pnl_status_short"], "REALIZED_GAIN/GAIN_BIAS")
            self.assertEqual(item["recent_rollup_short"], "r2:g2/l0/f0/a0")
            self.assertEqual(item["pnl_rollup_attention"], "false")


if __name__ == "__main__":
    unittest.main()
