import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "shadow_execution_review_queue_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def operator_snapshot_item(
    *,
    rank: int,
    pack_id: str,
    pnl_interpretation: str = "UNKNOWN",
    outcome_class: str = "UNKNOWN",
    latest_vs_recent_consistency: str = "UNKNOWN",
    outcome_attention_flag: str = "false",
    outcome_review_short: str = "",
) -> dict:
    return {
        "rank": rank,
        "pack_id": pack_id,
        "decision_tier": "PROMOTE_STRONG",
        "score": "84.000000",
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": ["bnbusdt"],
        "observation_status": "NEW",
        "next_action_hint": "READY_TO_OBSERVE",
        "reobserve_status": "NOT_OBSERVED",
        "observation_last_outcome_short": "NO_HISTORY",
        "pnl_interpretation": pnl_interpretation,
        "pnl_attention_flag": "false",
        "latest_realized_sign": "UNKNOWN",
        "latest_unrealized_sign": "UNKNOWN",
        "outcome_class": outcome_class,
        "latest_vs_recent_consistency": latest_vs_recent_consistency,
        "outcome_attention_flag": outcome_attention_flag,
        "outcome_review_short": outcome_review_short,
        "recent_observation_trail": "",
        "combined_status_short": f"NEW/{pnl_interpretation}",
    }


class ShadowExecutionReviewQueueV0Tests(unittest.TestCase):
    def _run(self, operator_snapshot: Path, out_json: Path):
        return subprocess.run(
            [
                "python3",
                str(SCRIPT),
                "--operator-snapshot",
                str(operator_snapshot),
                "--out-json",
                str(out_json),
            ],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_single_pack_populated_case(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_review_queue_single_") as td:
            root = Path(td)
            operator_snapshot = root / "operator_snapshot.json"
            out_json = root / "review_queue.json"
            write_json(
                operator_snapshot,
                {
                    "schema_version": "shadow_operator_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T15:20:12Z",
                    "selected_count": 1,
                    "items": [
                        operator_snapshot_item(
                            rank=1,
                            pack_id="pack_a",
                            pnl_interpretation="FLAT_NO_FILLS",
                            outcome_class="STABLE_FLAT",
                            latest_vs_recent_consistency="CONSISTENT",
                            outcome_attention_flag="true",
                            outcome_review_short="latest and recent outcomes both lean flat, but attention remains active",
                        )
                    ],
                },
            )
            result = self._run(operator_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_execution_review_queue_v0")
            self.assertEqual(payload["selected_count"], 1)
            item = payload["items"][0]
            self.assertEqual(item["review_rank"], 1)
            self.assertEqual(item["source_rank"], 1)
            self.assertEqual(item["pack_id"], "pack_a")
            self.assertEqual(item["trend_class"], "STABLE")
            self.assertEqual(item["trend_direction"], "FLAT")
            self.assertEqual(item["trend_attention_flag"], "true")
            self.assertEqual(item["review_priority_bucket"], "HIGH")
            self.assertEqual(item["review_reason_short"], "stable flat execution trend, attention active")

    def test_empty_operator_snapshot_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_review_queue_empty_") as td:
            root = Path(td)
            operator_snapshot = root / "operator_snapshot.json"
            out_json = root / "review_queue.json"
            write_json(
                operator_snapshot,
                {
                    "schema_version": "shadow_operator_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T15:20:12Z",
                    "selected_count": 0,
                    "items": [],
                },
            )
            result = self._run(operator_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_count"], 0)
            self.assertEqual(payload["items"], [])

    def test_multi_pack_ordering_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_review_queue_ordering_") as td:
            root = Path(td)
            operator_snapshot = root / "operator_snapshot.json"
            out_json = root / "review_queue.json"
            write_json(
                operator_snapshot,
                {
                    "schema_version": "shadow_operator_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T15:20:12Z",
                    "selected_count": 4,
                    "items": [
                        operator_snapshot_item(
                            rank=3,
                            pack_id="pack_low",
                            pnl_interpretation="UNKNOWN",
                            outcome_class="UNKNOWN",
                        ),
                        operator_snapshot_item(
                            rank=2,
                            pack_id="pack_mixed",
                            pnl_interpretation="REALIZED_GAIN",
                            outcome_class="MIXED_RECENT",
                            latest_vs_recent_consistency="DIVERGENT",
                            outcome_attention_flag="false",
                        ),
                        operator_snapshot_item(
                            rank=4,
                            pack_id="pack_high_b",
                            pnl_interpretation="ACTIVE_UNKNOWN",
                            outcome_class="ATTENTION_REQUIRED",
                            outcome_attention_flag="true",
                        ),
                        operator_snapshot_item(
                            rank=1,
                            pack_id="pack_high_a",
                            pnl_interpretation="FLAT_NO_FILLS",
                            outcome_class="STABLE_FLAT",
                            latest_vs_recent_consistency="CONSISTENT",
                            outcome_attention_flag="true",
                        ),
                    ],
                },
            )
            result = self._run(operator_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            items = payload["items"]
            self.assertEqual(
                [item["pack_id"] for item in items],
                ["pack_high_a", "pack_high_b", "pack_mixed", "pack_low"],
            )
            self.assertEqual(
                [item["review_priority_bucket"] for item in items],
                ["HIGH", "HIGH", "NORMAL", "LOW"],
            )
            self.assertEqual(
                [item["review_rank"] for item in items],
                [1, 2, 3, 4],
            )

    def test_trend_buckets_are_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_review_queue_buckets_") as td:
            root = Path(td)
            operator_snapshot = root / "operator_snapshot.json"
            out_json = root / "review_queue.json"
            write_json(
                operator_snapshot,
                {
                    "schema_version": "shadow_operator_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T15:20:12Z",
                    "selected_count": 4,
                    "items": [
                        operator_snapshot_item(
                            rank=1,
                            pack_id="pack_stable_gain",
                            pnl_interpretation="REALIZED_GAIN",
                            outcome_class="STABLE_GAINING",
                            outcome_attention_flag="false",
                        ),
                        operator_snapshot_item(
                            rank=2,
                            pack_id="pack_stable_loss",
                            pnl_interpretation="REALIZED_LOSS",
                            outcome_class="STABLE_LOSING",
                            outcome_attention_flag="false",
                        ),
                        operator_snapshot_item(
                            rank=3,
                            pack_id="pack_attention",
                            pnl_interpretation="ACTIVE_LOSING",
                            outcome_class="ATTENTION_REQUIRED",
                            outcome_attention_flag="false",
                        ),
                        operator_snapshot_item(
                            rank=4,
                            pack_id="pack_no_history",
                            pnl_interpretation="UNKNOWN",
                            outcome_class="UNKNOWN",
                            outcome_attention_flag="false",
                        ),
                    ],
                },
            )
            result = self._run(operator_snapshot, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            items = {item["pack_id"]: item for item in payload["items"]}
            self.assertEqual(items["pack_stable_gain"]["trend_class"], "STABLE")
            self.assertEqual(items["pack_stable_gain"]["trend_direction"], "GAINING")
            self.assertEqual(items["pack_stable_gain"]["review_reason_short"], "stable gaining execution trend")
            self.assertEqual(items["pack_stable_loss"]["trend_class"], "STABLE")
            self.assertEqual(items["pack_stable_loss"]["trend_direction"], "LOSING")
            self.assertEqual(items["pack_stable_loss"]["review_reason_short"], "stable losing execution trend")
            self.assertEqual(items["pack_attention"]["trend_class"], "ATTENTION")
            self.assertEqual(items["pack_attention"]["trend_direction"], "LOSING")
            self.assertEqual(
                items["pack_attention"]["review_reason_short"],
                "losing or unclear latest execution outcome",
            )
            self.assertEqual(items["pack_no_history"]["trend_class"], "NO_HISTORY")
            self.assertEqual(items["pack_no_history"]["trend_direction"], "UNKNOWN")
            self.assertEqual(items["pack_no_history"]["review_reason_short"], "no execution trend history")

    def test_invalid_operator_snapshot_shape_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_exec_review_queue_bad_") as td:
            root = Path(td)
            operator_snapshot = root / "operator_snapshot.json"
            out_json = root / "review_queue.json"
            write_json(
                operator_snapshot,
                {
                    "schema_version": "shadow_operator_snapshot_v0",
                    "generated_ts_utc": "2026-03-07T15:20:12Z",
                    "selected_count": 1,
                    "items": [
                        {
                            "rank": 1,
                            "pack_id": "pack_a",
                        }
                    ],
                },
            )
            result = self._run(operator_snapshot, out_json)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("operator_snapshot_item_missing_fields", result.stderr)


if __name__ == "__main__":
    unittest.main()
