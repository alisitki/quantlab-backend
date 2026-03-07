import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SNAPSHOT_SCRIPT = REPO / "tools" / "shadow_operator_snapshot_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def watchlist_item(
    *,
    rank: int,
    pack_id: str,
    decision_tier: str = "PROMOTE_STRONG",
    score: str = "84.000000",
    exchange: str = "bybit",
    stream: str = "bbo",
    symbols: list[str] | None = None,
    observation_status: str = "NEW",
    next_action_hint: str = "READY_TO_OBSERVE",
    reobserve_status: str = "NOT_OBSERVED",
    observation_last_outcome_short: str = "NO_HISTORY",
    pnl_interpretation: str = "UNKNOWN",
    pnl_attention_flag: str = "false",
    latest_realized_sign: str = "UNKNOWN",
    latest_unrealized_sign: str = "UNKNOWN",
    recent_observation_trail: str = "",
) -> dict:
    return {
        "rank": rank,
        "selection_slot": "overall_fill",
        "pack_id": pack_id,
        "pack_path": f"/tmp/{pack_id}",
        "decision_tier": decision_tier,
        "score": score,
        "exchange": exchange,
        "stream": stream,
        "symbols": list(symbols or ["bnbusdt"]),
        "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
        "watch_status": "ACTIVE",
        "observed_before": False,
        "observation_count": 0,
        "last_observed_at": "",
        "last_verify_soft_live_pass": "unknown",
        "last_stop_reason": "",
        "last_processed_event_count": "unknown",
        "last_observation_age_hours": "unknown",
        "observation_recency_bucket": "NEVER_OBSERVED",
        "observation_last_outcome_short": observation_last_outcome_short,
        "observation_attention_flag": "false",
        "observation_status": observation_status,
        "next_action_hint": next_action_hint,
        "reobserve_status": reobserve_status,
        "recent_observation_trail": recent_observation_trail,
        "last_pnl_state": "UNKNOWN",
        "pnl_interpretation": pnl_interpretation,
        "pnl_attention_flag": pnl_attention_flag,
        "latest_realized_sign": latest_realized_sign,
        "latest_unrealized_sign": latest_unrealized_sign,
        "notes": "",
    }


def outcome_review_item(
    *,
    pack_id: str,
    outcome_class: str = "STABLE_FLAT",
    latest_vs_recent_consistency: str = "CONSISTENT",
    outcome_attention_flag: str = "false",
    outcome_review_short: str = "latest and recent outcomes both lean flat",
) -> dict:
    return {
        "selected_pack_id": pack_id,
        "last_observed_at": "2026-03-07T13:39:26Z",
        "outcome_class": outcome_class,
        "latest_vs_recent_consistency": latest_vs_recent_consistency,
        "outcome_attention_flag": outcome_attention_flag,
        "outcome_review_short": outcome_review_short,
    }


class ShadowOperatorSnapshotV0Tests(unittest.TestCase):
    def _run(self, watchlist: Path, outcome_review: Path, out_json: Path, *extra: str):
        cmd = [
            "python3",
            str(SNAPSHOT_SCRIPT),
            "--watchlist",
            str(watchlist),
            "--outcome-review",
            str(outcome_review),
            "--out-json",
            str(out_json),
            *extra,
        ]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_good_path_snapshot_generation(self):
        with tempfile.TemporaryDirectory(prefix="shadow_operator_snapshot_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            outcome_review = root / "outcome_review.json"
            out_json = root / "snapshot.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {"top_n": 3},
                    "items": [
                        watchlist_item(
                            rank=1,
                            pack_id="pack_a",
                            observation_status="OBSERVED_PASS",
                            next_action_hint="ALREADY_OBSERVED_GOOD",
                            reobserve_status="RECENTLY_OBSERVED",
                            observation_last_outcome_short="PASS(16)",
                            pnl_interpretation="FLAT_NO_FILLS",
                            latest_realized_sign="FLAT",
                            latest_unrealized_sign="FLAT",
                            recent_observation_trail="2026-03-07T07:24:58Z/PASS(16)/STREAM_END",
                        ),
                        watchlist_item(rank=2, pack_id="pack_b"),
                    ],
                },
            )
            write_json(
                outcome_review,
                {
                    "schema_version": "shadow_execution_outcome_review_v0",
                    "generated_ts_utc": "2026-03-07T15:14:02Z",
                    "source_rollup_snapshot": "tools/shadow_state/shadow_execution_rollup_snapshot_v0.json",
                    "rollup_snapshot_generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 1,
                    "items": [
                        outcome_review_item(
                            pack_id="pack_a",
                            outcome_class="STABLE_FLAT",
                            latest_vs_recent_consistency="CONSISTENT",
                            outcome_attention_flag="true",
                            outcome_review_short="latest and recent outcomes both lean flat, but attention remains active",
                        )
                    ],
                },
            )
            result = self._run(watchlist, outcome_review, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_operator_snapshot_v0")
            self.assertEqual(payload["selected_count"], 2)
            self.assertEqual(payload["items"][0]["pack_id"], "pack_a")
            self.assertEqual(payload["items"][0]["combined_status_short"], "OBSERVED_PASS/FLAT_NO_FILLS")
            self.assertEqual(payload["items"][0]["outcome_class"], "STABLE_FLAT")
            self.assertEqual(payload["items"][0]["latest_vs_recent_consistency"], "CONSISTENT")
            self.assertEqual(payload["items"][0]["outcome_attention_flag"], "true")
            self.assertEqual(
                payload["items"][0]["outcome_review_short"],
                "latest and recent outcomes both lean flat, but attention remains active",
            )
            self.assertEqual(payload["items"][1]["combined_status_short"], "NEW/UNKNOWN")
            self.assertEqual(payload["items"][1]["outcome_class"], "UNKNOWN")
            self.assertEqual(payload["items"][1]["latest_vs_recent_consistency"], "UNKNOWN")
            self.assertEqual(payload["items"][1]["outcome_attention_flag"], "false")
            self.assertEqual(payload["items"][1]["outcome_review_short"], "")

    def test_top_n_selection_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_operator_snapshot_topn_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            outcome_review = root / "outcome_review.json"
            out_json = root / "snapshot.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {"top_n": 3},
                    "items": [
                        watchlist_item(rank=1, pack_id="pack_a"),
                        watchlist_item(rank=2, pack_id="pack_b"),
                        watchlist_item(rank=3, pack_id="pack_c"),
                    ],
                },
            )
            write_json(
                outcome_review,
                {
                    "schema_version": "shadow_execution_outcome_review_v0",
                    "generated_ts_utc": "2026-03-07T15:14:02Z",
                    "source_rollup_snapshot": "tools/shadow_state/shadow_execution_rollup_snapshot_v0.json",
                    "rollup_snapshot_generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 3,
                    "items": [
                        outcome_review_item(pack_id="pack_a", outcome_class="STABLE_GAINING"),
                        outcome_review_item(pack_id="pack_b", outcome_class="MIXED_RECENT"),
                        outcome_review_item(pack_id="pack_c", outcome_class="STABLE_LOSING"),
                    ],
                },
            )
            result = self._run(watchlist, outcome_review, out_json, "--max-items", "2")
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual([item["pack_id"] for item in payload["items"]], ["pack_a", "pack_b"])
            self.assertEqual([item["outcome_class"] for item in payload["items"]], ["STABLE_GAINING", "MIXED_RECENT"])

    def test_empty_watchlist_is_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_operator_snapshot_empty_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            outcome_review = root / "outcome_review.json"
            out_json = root / "snapshot.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {"top_n": 3},
                    "items": [],
                },
            )
            write_json(
                outcome_review,
                {
                    "schema_version": "shadow_execution_outcome_review_v0",
                    "generated_ts_utc": "2026-03-07T15:14:02Z",
                    "source_rollup_snapshot": "tools/shadow_state/shadow_execution_rollup_snapshot_v0.json",
                    "rollup_snapshot_generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 0,
                    "items": [],
                },
            )
            result = self._run(watchlist, outcome_review, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_count"], 0)
            self.assertEqual(payload["items"], [])

    def test_missing_outcome_review_uses_deterministic_fallbacks(self):
        with tempfile.TemporaryDirectory(prefix="shadow_operator_snapshot_missing_outcome_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            missing_outcome_review = root / "missing_outcome_review.json"
            out_json = root / "snapshot.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {"top_n": 3},
                    "items": [watchlist_item(rank=1, pack_id="pack_a")],
                },
            )
            result = self._run(watchlist, missing_outcome_review, out_json)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["items"][0]["outcome_class"], "UNKNOWN")
            self.assertEqual(payload["items"][0]["latest_vs_recent_consistency"], "UNKNOWN")
            self.assertEqual(payload["items"][0]["outcome_attention_flag"], "false")
            self.assertEqual(payload["items"][0]["outcome_review_short"], "")

    def test_invalid_watchlist_shape_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_operator_snapshot_bad_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            outcome_review = root / "outcome_review.json"
            out_json = root / "snapshot.json"
            write_json(
                watchlist,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T14:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {"top_n": 3},
                    "items": [
                        {
                            "rank": 1,
                            "pack_id": "pack_a",
                        }
                    ],
                },
            )
            write_json(
                outcome_review,
                {
                    "schema_version": "shadow_execution_outcome_review_v0",
                    "generated_ts_utc": "2026-03-07T15:14:02Z",
                    "source_rollup_snapshot": "tools/shadow_state/shadow_execution_rollup_snapshot_v0.json",
                    "rollup_snapshot_generated_ts_utc": "2026-03-07T14:35:30Z",
                    "selected_count": 0,
                    "items": [],
                },
            )
            result = self._run(watchlist, outcome_review, out_json)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("watchlist_item_missing_fields", result.stderr)


if __name__ == "__main__":
    unittest.main()
