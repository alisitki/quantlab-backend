import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_execution_outcome_review_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def rollup_item(
    *,
    pack_id: str,
    last_observed_at: str = "2026-03-07T13:39:26Z",
    pnl_interpretation: str = "FLAT_NO_FILLS",
    recent_pnl_bias: str = "FLAT_BIAS",
    recent_run_count: int = 3,
    recent_attention_count: int = 0,
    pnl_rollup_attention: str = "false",
) -> dict:
    return {
        "selected_pack_id": pack_id,
        "last_observed_at": last_observed_at,
        "last_pnl_state": pnl_interpretation,
        "pnl_interpretation": pnl_interpretation,
        "recent_pnl_bias": recent_pnl_bias,
        "recent_run_count": recent_run_count,
        "recent_attention_count": recent_attention_count,
        "combined_pnl_status_short": f"{pnl_interpretation}/{recent_pnl_bias}",
        "recent_rollup_short": "r3:g1/l1/f1/a0",
        "pnl_rollup_attention": pnl_rollup_attention,
    }


class ShadowExecutionOutcomeReviewV0Tests(unittest.TestCase):
    def _run(self, rollup_snapshot: Path, out_json: Path):
        return subprocess.run(
            [
                "python3",
                str(SCRIPT),
                "--rollup-snapshot",
                str(rollup_snapshot),
                "--out-json",
                str(out_json),
            ],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_good_path_populated_review_generation(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_outcome_review_") as td:
            root = Path(td)
            rollup_snapshot = root / "rollup_snapshot.json"
            out_json = root / "review.json"
            write_json(
                rollup_snapshot,
                {
                    "schema_version": "shadow_execution_rollup_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 1,
                    "items": [
                        rollup_item(
                            pack_id="pack_a",
                            pnl_interpretation="FLAT_NO_FILLS",
                            recent_pnl_bias="FLAT_BIAS",
                            recent_run_count=3,
                            recent_attention_count=2,
                            pnl_rollup_attention="true",
                        )
                    ],
                },
            )
            result = self._run(rollup_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_execution_outcome_review_v0")
            self.assertEqual(payload["selected_count"], 1)
            item = payload["items"][0]
            self.assertEqual(item["selected_pack_id"], "pack_a")
            self.assertEqual(item["outcome_class"], "STABLE_FLAT")
            self.assertEqual(item["latest_vs_recent_consistency"], "CONSISTENT")
            self.assertEqual(item["outcome_attention_flag"], "true")
            self.assertEqual(
                item["outcome_review_short"],
                "latest and recent outcomes both lean flat, but attention remains active",
            )

    def test_empty_rollup_snapshot_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_outcome_review_empty_") as td:
            root = Path(td)
            rollup_snapshot = root / "rollup_snapshot.json"
            out_json = root / "review.json"
            write_json(
                rollup_snapshot,
                {
                    "schema_version": "shadow_execution_rollup_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 0,
                    "items": [],
                },
            )
            result = self._run(rollup_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_count"], 0)
            self.assertEqual(payload["items"], [])

    def test_invalid_rollup_shape_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_outcome_review_bad_") as td:
            root = Path(td)
            rollup_snapshot = root / "rollup_snapshot.json"
            out_json = root / "review.json"
            write_json(
                rollup_snapshot,
                {
                    "schema_version": "shadow_execution_rollup_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 1,
                    "items": [
                        {
                            "selected_pack_id": "pack_a",
                        }
                    ],
                },
            )
            result = self._run(rollup_snapshot, out_json)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("rollup_snapshot_item_missing_fields", result.stderr)

    def test_consistency_divergence_and_attention_are_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_outcome_review_consistency_") as td:
            root = Path(td)
            rollup_snapshot = root / "rollup_snapshot.json"
            out_json = root / "review.json"
            write_json(
                rollup_snapshot,
                {
                    "schema_version": "shadow_execution_rollup_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 4,
                    "items": [
                        rollup_item(
                            pack_id="pack_gain",
                            pnl_interpretation="REALIZED_GAIN",
                            recent_pnl_bias="GAIN_BIAS",
                            recent_run_count=2,
                            pnl_rollup_attention="false",
                        ),
                        rollup_item(
                            pack_id="pack_mixed",
                            pnl_interpretation="REALIZED_GAIN",
                            recent_pnl_bias="MIXED",
                            recent_run_count=3,
                            pnl_rollup_attention="false",
                        ),
                        rollup_item(
                            pack_id="pack_unknown",
                            pnl_interpretation="ACTIVE_UNKNOWN",
                            recent_pnl_bias="LOSS_BIAS",
                            recent_run_count=3,
                            pnl_rollup_attention="true",
                        ),
                        rollup_item(
                            pack_id="pack_history",
                            pnl_interpretation="REALIZED_LOSS",
                            recent_pnl_bias="NO_HISTORY",
                            recent_run_count=0,
                            pnl_rollup_attention="false",
                        ),
                    ],
                },
            )
            result = self._run(rollup_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            items = {item["selected_pack_id"]: item for item in payload["items"]}

            self.assertEqual(items["pack_gain"]["outcome_class"], "STABLE_GAINING")
            self.assertEqual(items["pack_gain"]["latest_vs_recent_consistency"], "CONSISTENT")
            self.assertEqual(items["pack_gain"]["outcome_attention_flag"], "false")
            self.assertEqual(
                items["pack_gain"]["outcome_review_short"],
                "latest and recent outcomes both lean gaining",
            )

            self.assertEqual(items["pack_mixed"]["outcome_class"], "MIXED_RECENT")
            self.assertEqual(items["pack_mixed"]["latest_vs_recent_consistency"], "DIVERGENT")
            self.assertEqual(items["pack_mixed"]["outcome_attention_flag"], "true")
            self.assertEqual(
                items["pack_mixed"]["outcome_review_short"],
                "recent window is mixed against the latest outcome, but attention remains active",
            )

            self.assertEqual(items["pack_unknown"]["outcome_class"], "ATTENTION_REQUIRED")
            self.assertEqual(items["pack_unknown"]["latest_vs_recent_consistency"], "UNKNOWN")
            self.assertEqual(items["pack_unknown"]["outcome_attention_flag"], "true")
            self.assertEqual(
                items["pack_unknown"]["outcome_review_short"],
                "latest outcome is attention-class or unclear",
            )

            self.assertEqual(items["pack_history"]["outcome_class"], "NO_RECENT_HISTORY")
            self.assertEqual(items["pack_history"]["latest_vs_recent_consistency"], "UNKNOWN")
            self.assertEqual(items["pack_history"]["outcome_attention_flag"], "false")
            self.assertEqual(
                items["pack_history"]["outcome_review_short"],
                "no recent execution outcome history",
            )


if __name__ == "__main__":
    unittest.main()
