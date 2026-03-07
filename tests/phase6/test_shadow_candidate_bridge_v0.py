import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
BRIDGE_SCRIPT = REPO / "tools" / "shadow_candidate_bridge_v0.py"
OBSERVER_SCRIPT = REPO / "tools" / "shadow_observe_mock_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def make_pack(root: Path, name: str, symbols: list[str]) -> Path:
    pack_path = root / name
    for symbol in symbols:
        (pack_path / "runs" / symbol).mkdir(parents=True, exist_ok=True)
    return pack_path


def candidate_record(
    *,
    pack_id: str,
    pack_path: Path,
    decision_tier: str,
    guards: dict | None = None,
) -> dict:
    return {
        "candidate_status": "NEW",
        "context_policy_hash": "ctx-hash" if decision_tier == "PROMOTE_STRONG" else "",
        "decision_tier": decision_tier,
        "det_pass": 5,
        "det_skipped": 1,
        "det_supported": 5,
        "export_ts_utc": "2026-03-07T06:00:00Z",
        "guards": dict(guards or {}),
        "max_elapsed_sec": 10.0,
        "max_rss_kb": 100000.0,
        "notes": "",
        "pack_id": pack_id,
        "pack_path": str(pack_path),
        "policy_hash": "policy-hash",
        "source_decision": decision_tier,
    }


def promotion_record(
    *,
    pack_id: str,
    pack_path: Path,
    decision_tier: str,
) -> dict:
    return {
        "decision_tier": decision_tier,
        "guards": {
            "G4_MARK_CONTEXT": "PASS",
            "G5_FUNDING_CONTEXT": "PASS",
            "G6_OI_CONTEXT": "PASS",
        },
        "pack_id": pack_id,
        "pack_path": str(pack_path),
    }


def write_candidate_review(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "rank",
                "score",
                "decision_tier",
                "pack_id",
                "pack_path",
                "det_ratio",
                "det_pass",
                "det_supported",
                "det_skipped",
                "max_rss_kb",
                "max_elapsed_sec",
                "context_flags",
                "candidate_status",
                "observed_before",
                "observation_count",
                "last_observed_at",
                "last_verify_soft_live_pass",
                "last_stop_reason",
                "last_processed_event_count",
                "last_observation_age_hours",
                "observation_recency_bucket",
                "observation_last_outcome_short",
                "observation_attention_flag",
                "observation_status",
                "next_action_hint",
                "reobserve_status",
                "recent_observation_trail",
                "last_pnl_state",
                "pnl_interpretation",
                "pnl_attention_flag",
                "latest_realized_sign",
                "latest_unrealized_sign",
            ],
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_watchlist(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_observation_log(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class ShadowCandidateBridgeV0Tests(unittest.TestCase):
    def _write_state(self, state_dir: Path, review_rows: list[dict], candidate_records: list[dict], promotion_records: list[dict]) -> None:
        write_candidate_review(state_dir / "candidate_review.tsv", review_rows)
        write_json(
            state_dir / "candidate_index.json",
            {
                "record_count": len(candidate_records),
                "by_tier": {
                    "PROMOTE": sum(1 for rec in candidate_records if rec["decision_tier"] == "PROMOTE"),
                    "PROMOTE_STRONG": sum(1 for rec in candidate_records if rec["decision_tier"] == "PROMOTE_STRONG"),
                },
                "candidate_pack_ids": [rec["pack_id"] for rec in candidate_records],
                "latest_by_pack_id": {rec["pack_id"]: rec for rec in candidate_records},
                "latest_by_tier": {},
                "latest_export_ts_utc": "2026-03-07T06:00:00Z",
            },
        )
        write_json(
            state_dir / "promotion_index.json",
            {
                "record_count": len(promotion_records),
                "pack_latest": {rec["pack_id"]: rec for rec in promotion_records},
                "promote_pack_ids": [],
                "promote_packs": [],
                "promote_strong_pack_ids": [],
                "promote_strong_packs": [],
            },
        )

    def _run_bridge(self, state_dir: Path, out_dir: Path, *extra_args: str):
        cmd = [
            "python3",
            str(BRIDGE_SCRIPT),
            "--state-dir",
            str(state_dir),
            "--out-dir",
            str(out_dir),
            *extra_args,
        ]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def _run_observer(self, watchlist_path: Path, out_log_path: Path):
        cmd = [
            "python3",
            str(OBSERVER_SCRIPT),
            "--watchlist",
            str(watchlist_path),
            "--out-log",
            str(out_log_path),
        ]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_promote_strong_priority_and_diversity_slots(self):
        with tempfile.TemporaryDirectory(prefix="shadow_bridge_slots_") as td:
            root = Path(td)
            state_dir = root / "state"
            out_dir = root / "shadow"

            bybit_bbo = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-bbo-20260101..20260101__FULLSCAN_MAJOR", ["ethusdt"])
            binance_bbo = make_pack(root, "multi-hypothesis-phase5-bighunt-binance-bbo-20260101..20260101__FULLSCAN_MAJOR", ["btcusdt"])
            bybit_trade = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-trade-20260101..20260101__FULLSCAN_MAJOR", ["xrpusdt"])
            promote_pack = make_pack(root, "multi-hypothesis-phase5-bighunt-okx-trade-20260101..20260101__FULLSCAN_MAJOR", ["solusdt"])

            candidate_records = [
                candidate_record(pack_id="pack_bybit_bbo", pack_path=bybit_bbo, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_binance_bbo", pack_path=binance_bbo, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_promote", pack_path=promote_pack, decision_tier="PROMOTE"),
            ]
            promotion_records = [
                promotion_record(pack_id="pack_bybit_bbo", pack_path=bybit_bbo, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_binance_bbo", pack_path=binance_bbo, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_promote", pack_path=promote_pack, decision_tier="PROMOTE"),
            ]
            review_rows = [
                {
                    "rank": "1",
                    "score": "64.900000",
                    "decision_tier": "PROMOTE",
                    "pack_id": "pack_bybit_bbo",
                    "pack_path": str(bybit_bbo),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "10.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
                {
                    "rank": "2",
                    "score": "63.500000",
                    "decision_tier": "PROMOTE",
                    "pack_id": "pack_binance_bbo",
                    "pack_path": str(binance_bbo),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "11.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=NEUTRAL",
                    "candidate_status": "NEW",
                },
                {
                    "rank": "3",
                    "score": "62.200000",
                    "decision_tier": "PROMOTE",
                    "pack_id": "pack_bybit_trade",
                    "pack_path": str(bybit_trade),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "12.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
                {
                    "rank": "4",
                    "score": "99.000000",
                    "decision_tier": "PROMOTE",
                    "pack_id": "pack_promote",
                    "pack_path": str(promote_pack),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "5.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
            ]
            self._write_state(state_dir, review_rows, candidate_records, promotion_records)

            res = self._run_bridge(state_dir, out_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            watchlist = load_watchlist(out_dir / "shadow_watchlist_v0.json")
            items = watchlist["items"]
            self.assertEqual([item["pack_id"] for item in items], ["pack_bybit_bbo", "pack_binance_bbo", "pack_bybit_trade"])
            self.assertEqual([item["selection_slot"] for item in items], ["bybit/bbo", "binance/bbo", "*/trade"])
            self.assertTrue(all(item["decision_tier"] == "PROMOTE_STRONG" for item in items))

    def test_fallback_fill_and_canonical_filtering_are_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="shadow_bridge_fallback_") as td:
            root = Path(td)
            state_dir = root / "state"
            out_dir = root / "shadow"

            bybit_trade = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-trade-20260101..20260101__FULLSCAN_MAJOR", ["xrpusdt"])
            okx_trade = make_pack(root, "multi-hypothesis-phase5-bighunt-okx-trade-20260101..20260101__FULLSCAN_MAJOR", ["solusdt"])
            noncanonical = make_pack(root, "misc-pack-without-canonical-lane", ["adausdt"])

            candidate_records = [
                candidate_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_okx_trade", pack_path=okx_trade, decision_tier="PROMOTE"),
                candidate_record(pack_id="pack_misc", pack_path=noncanonical, decision_tier="PROMOTE_STRONG"),
            ]
            promotion_records = [
                promotion_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_okx_trade", pack_path=okx_trade, decision_tier="PROMOTE"),
                promotion_record(pack_id="pack_misc", pack_path=noncanonical, decision_tier="PROMOTE_STRONG"),
            ]
            review_rows = [
                {
                    "rank": "1",
                    "score": "50.000000",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_misc",
                    "pack_path": str(noncanonical),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "8.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
                {
                    "rank": "2",
                    "score": "40.000000",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_bybit_trade",
                    "pack_path": str(bybit_trade),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "12.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
                {
                    "rank": "3",
                    "score": "39.500000",
                    "decision_tier": "PROMOTE",
                    "pack_id": "pack_okx_trade",
                    "pack_path": str(okx_trade),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "14.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
            ]
            self._write_state(state_dir, review_rows, candidate_records, promotion_records)

            r1 = self._run_bridge(state_dir, out_dir, "--top-n", "2")
            self.assertEqual(r1.returncode, 0, msg=r1.stderr)
            first = (out_dir / "shadow_watchlist_v0.tsv").read_text(encoding="utf-8")
            r2 = self._run_bridge(state_dir, out_dir, "--top-n", "2")
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            second = (out_dir / "shadow_watchlist_v0.tsv").read_text(encoding="utf-8")
            self.assertEqual(first, second)
            watchlist = load_watchlist(out_dir / "shadow_watchlist_v0.json")
            items = watchlist["items"]
            self.assertEqual([item["pack_id"] for item in items], ["pack_bybit_trade", "pack_okx_trade"])
            self.assertEqual(items[0]["selection_slot"], "*/trade")
            self.assertIn(items[1]["selection_slot"], {"*/trade", "overall_fill"})

    def test_observation_fields_pass_through_and_fallback(self):
        with tempfile.TemporaryDirectory(prefix="shadow_bridge_observation_") as td:
            root = Path(td)
            state_dir = root / "state"
            out_dir = root / "shadow"

            bybit_bbo = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-bbo-20260101..20260101__FULLSCAN_MAJOR", ["ethusdt"])
            binance_bbo = make_pack(root, "multi-hypothesis-phase5-bighunt-binance-bbo-20260101..20260101__FULLSCAN_MAJOR", ["btcusdt"])
            bybit_trade = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-trade-20260101..20260101__FULLSCAN_MAJOR", ["xrpusdt"])

            candidate_records = [
                candidate_record(pack_id="pack_bybit_bbo", pack_path=bybit_bbo, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_binance_bbo", pack_path=binance_bbo, decision_tier="PROMOTE_STRONG"),
                candidate_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
            ]
            promotion_records = [
                promotion_record(pack_id="pack_bybit_bbo", pack_path=bybit_bbo, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_binance_bbo", pack_path=binance_bbo, decision_tier="PROMOTE_STRONG"),
                promotion_record(pack_id="pack_bybit_trade", pack_path=bybit_trade, decision_tier="PROMOTE_STRONG"),
            ]
            review_rows = [
                {
                    "rank": "1",
                    "score": "64.900000",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_bybit_bbo",
                    "pack_path": str(bybit_bbo),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "10.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                    "observation_status": "OBSERVED_PASS",
                    "next_action_hint": "ALREADY_OBSERVED_GOOD",
                    "observed_before": "true",
                    "observation_count": "2",
                    "last_observed_at": "2026-03-07T07:24:58Z",
                    "last_verify_soft_live_pass": "true",
                    "last_stop_reason": "STREAM_END",
                    "last_processed_event_count": "16",
                    "last_observation_age_hours": "1.000",
                    "observation_recency_bucket": "WITHIN_24H",
                    "observation_last_outcome_short": "PASS(16)",
                    "observation_attention_flag": "false",
                    "reobserve_status": "RECENTLY_OBSERVED",
                    "recent_observation_trail": "2026-03-07T07:24:58Z/PASS(16)/STREAM_END",
                    "last_pnl_state": "FLAT_NO_FILLS",
                    "pnl_interpretation": "FLAT_NO_FILLS",
                    "pnl_attention_flag": "false",
                    "latest_realized_sign": "FLAT",
                    "latest_unrealized_sign": "FLAT",
                },
                {
                    "rank": "2",
                    "score": "63.500000",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_binance_bbo",
                    "pack_path": str(binance_bbo),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "11.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=NEUTRAL",
                    "candidate_status": "NEW",
                    "observation_status": "NEW",
                    "next_action_hint": "READY_TO_OBSERVE",
                    "observed_before": "false",
                    "observation_count": "0",
                    "last_observed_at": "",
                    "last_verify_soft_live_pass": "unknown",
                    "last_stop_reason": "",
                    "last_processed_event_count": "unknown",
                    "last_observation_age_hours": "unknown",
                    "observation_recency_bucket": "NEVER_OBSERVED",
                    "observation_last_outcome_short": "NO_HISTORY",
                    "observation_attention_flag": "false",
                    "reobserve_status": "NOT_OBSERVED",
                    "recent_observation_trail": "",
                    "last_pnl_state": "UNKNOWN",
                    "pnl_interpretation": "UNKNOWN",
                    "pnl_attention_flag": "false",
                    "latest_realized_sign": "UNKNOWN",
                    "latest_unrealized_sign": "UNKNOWN",
                },
                {
                    "rank": "3",
                    "score": "62.200000",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_bybit_trade",
                    "pack_path": str(bybit_trade),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "12.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                    "observation_status": "NEW",
                    "next_action_hint": "READY_TO_OBSERVE",
                    "last_observation_age_hours": "unknown",
                    "observation_recency_bucket": "NEVER_OBSERVED",
                    "observation_last_outcome_short": "NO_HISTORY",
                    "observation_attention_flag": "false",
                    "reobserve_status": "NOT_OBSERVED",
                    "recent_observation_trail": "",
                    "last_pnl_state": "UNKNOWN",
                    "pnl_interpretation": "UNKNOWN",
                    "pnl_attention_flag": "false",
                    "latest_realized_sign": "UNKNOWN",
                    "latest_unrealized_sign": "UNKNOWN",
                },
            ]
            self._write_state(state_dir, review_rows, candidate_records, promotion_records)

            res = self._run_bridge(state_dir, out_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            watchlist = load_watchlist(out_dir / "shadow_watchlist_v0.json")
            items = watchlist["items"]
            self.assertEqual([item["pack_id"] for item in items], ["pack_bybit_bbo", "pack_binance_bbo", "pack_bybit_trade"])
            self.assertTrue(items[0]["observed_before"])
            self.assertEqual(items[0]["observation_count"], 2)
            self.assertEqual(items[0]["last_observed_at"], "2026-03-07T07:24:58Z")
            self.assertEqual(items[0]["last_verify_soft_live_pass"], "true")
            self.assertEqual(items[0]["last_stop_reason"], "STREAM_END")
            self.assertEqual(items[0]["last_processed_event_count"], "16")
            self.assertEqual(items[0]["last_observation_age_hours"], "1.000")
            self.assertEqual(items[0]["observation_recency_bucket"], "WITHIN_24H")
            self.assertEqual(items[0]["observation_last_outcome_short"], "PASS(16)")
            self.assertEqual(items[0]["observation_attention_flag"], "false")
            self.assertEqual(items[0]["observation_status"], "OBSERVED_PASS")
            self.assertEqual(items[0]["next_action_hint"], "ALREADY_OBSERVED_GOOD")
            self.assertEqual(items[0]["reobserve_status"], "RECENTLY_OBSERVED")
            self.assertEqual(items[0]["recent_observation_trail"], "2026-03-07T07:24:58Z/PASS(16)/STREAM_END")
            self.assertEqual(items[0]["last_pnl_state"], "FLAT_NO_FILLS")
            self.assertEqual(items[0]["pnl_interpretation"], "FLAT_NO_FILLS")
            self.assertEqual(items[0]["pnl_attention_flag"], "false")
            self.assertEqual(items[0]["latest_realized_sign"], "FLAT")
            self.assertEqual(items[0]["latest_unrealized_sign"], "FLAT")
            self.assertFalse(items[1]["observed_before"])
            self.assertEqual(items[1]["observation_count"], 0)
            self.assertEqual(items[1]["last_verify_soft_live_pass"], "unknown")
            self.assertEqual(items[1]["last_observation_age_hours"], "unknown")
            self.assertEqual(items[1]["observation_recency_bucket"], "NEVER_OBSERVED")
            self.assertEqual(items[1]["observation_last_outcome_short"], "NO_HISTORY")
            self.assertEqual(items[1]["observation_attention_flag"], "false")
            self.assertEqual(items[1]["observation_status"], "NEW")
            self.assertEqual(items[1]["next_action_hint"], "READY_TO_OBSERVE")
            self.assertEqual(items[1]["reobserve_status"], "NOT_OBSERVED")
            self.assertEqual(items[1]["recent_observation_trail"], "")
            self.assertEqual(items[1]["last_pnl_state"], "UNKNOWN")
            self.assertEqual(items[1]["pnl_interpretation"], "UNKNOWN")
            self.assertEqual(items[1]["pnl_attention_flag"], "false")
            self.assertEqual(items[1]["latest_realized_sign"], "UNKNOWN")
            self.assertEqual(items[1]["latest_unrealized_sign"], "UNKNOWN")
            self.assertFalse(items[2]["observed_before"])
            self.assertEqual(items[2]["observation_count"], 0)
            self.assertEqual(items[2]["last_observation_age_hours"], "unknown")
            self.assertEqual(items[2]["observation_recency_bucket"], "NEVER_OBSERVED")
            self.assertEqual(items[2]["observation_last_outcome_short"], "NO_HISTORY")
            self.assertEqual(items[2]["observation_attention_flag"], "false")
            self.assertEqual(items[2]["observation_status"], "NEW")
            self.assertEqual(items[2]["next_action_hint"], "READY_TO_OBSERVE")
            self.assertEqual(items[2]["reobserve_status"], "NOT_OBSERVED")
            self.assertEqual(items[2]["recent_observation_trail"], "")
            self.assertEqual(items[2]["last_pnl_state"], "UNKNOWN")
            self.assertEqual(items[2]["pnl_interpretation"], "UNKNOWN")
            self.assertEqual(items[2]["pnl_attention_flag"], "false")
            self.assertEqual(items[2]["latest_realized_sign"], "UNKNOWN")
            self.assertEqual(items[2]["latest_unrealized_sign"], "UNKNOWN")
            tsv_lines = (out_dir / "shadow_watchlist_v0.tsv").read_text(encoding="utf-8").splitlines()
            self.assertIn("observed_before", tsv_lines[0])
            self.assertIn("last_processed_event_count", tsv_lines[0])
            self.assertIn("last_observation_age_hours", tsv_lines[0])
            self.assertIn("observation_recency_bucket", tsv_lines[0])
            self.assertIn("observation_last_outcome_short", tsv_lines[0])
            self.assertIn("observation_attention_flag", tsv_lines[0])
            self.assertIn("next_action_hint", tsv_lines[0])
            self.assertIn("reobserve_status", tsv_lines[0])
            self.assertIn("recent_observation_trail", tsv_lines[0])
            self.assertIn("last_pnl_state", tsv_lines[0])
            self.assertIn("pnl_interpretation", tsv_lines[0])
            self.assertIn("pnl_attention_flag", tsv_lines[0])
            self.assertIn("latest_realized_sign", tsv_lines[0])
            self.assertIn("latest_unrealized_sign", tsv_lines[0])
            self.assertIn(
                "\ttrue\t2\t2026-03-07T07:24:58Z\ttrue\tSTREAM_END\t16\t1.000\tWITHIN_24H\tPASS(16)\tfalse\tOBSERVED_PASS\tALREADY_OBSERVED_GOOD\tRECENTLY_OBSERVED\t2026-03-07T07:24:58Z/PASS(16)/STREAM_END\tFLAT_NO_FILLS\tFLAT_NO_FILLS\tfalse\tFLAT\tFLAT\t",
                tsv_lines[1],
            )

    def test_watchlist_schema_and_symbols_present(self):
        with tempfile.TemporaryDirectory(prefix="shadow_bridge_schema_") as td:
            root = Path(td)
            state_dir = root / "state"
            out_dir = root / "shadow"
            pack = make_pack(root, "multi-hypothesis-phase5-bighunt-bybit-bbo-20260101..20260101__FULLSCAN_MAJOR", ["ethusdt", "btcusdt"])

            candidate_records = [
                candidate_record(pack_id="pack_schema", pack_path=pack, decision_tier="PROMOTE_STRONG"),
            ]
            promotion_records = [
                promotion_record(pack_id="pack_schema", pack_path=pack, decision_tier="PROMOTE_STRONG"),
            ]
            review_rows = [
                {
                    "rank": "1",
                    "score": "64.530507",
                    "decision_tier": "PROMOTE_STRONG",
                    "pack_id": "pack_schema",
                    "pack_path": str(pack),
                    "det_ratio": "1.000000",
                    "det_pass": "5",
                    "det_supported": "5",
                    "det_skipped": "1",
                    "max_rss_kb": "100000.0",
                    "max_elapsed_sec": "2.0",
                    "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                    "candidate_status": "NEW",
                },
            ]
            self._write_state(state_dir, review_rows, candidate_records, promotion_records)

            res = self._run_bridge(state_dir, out_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            watchlist = load_watchlist(out_dir / "shadow_watchlist_v0.json")
            item = watchlist["items"][0]
            self.assertEqual(
                sorted(item.keys()),
                sorted(
                    [
                        "rank",
                        "selection_slot",
                        "pack_id",
                        "pack_path",
                        "decision_tier",
                        "score",
                        "exchange",
                        "stream",
                        "symbols",
                        "context_flags",
                        "watch_status",
                        "observed_before",
                        "observation_count",
                        "last_observed_at",
                        "last_verify_soft_live_pass",
                        "last_stop_reason",
                        "last_processed_event_count",
                        "last_observation_age_hours",
                        "observation_recency_bucket",
                        "observation_last_outcome_short",
                        "observation_attention_flag",
                        "observation_status",
                        "next_action_hint",
                        "reobserve_status",
                        "recent_observation_trail",
                        "last_pnl_state",
                        "pnl_interpretation",
                        "pnl_attention_flag",
                        "latest_realized_sign",
                        "latest_unrealized_sign",
                        "notes",
                    ]
                ),
            )
            self.assertEqual(item["symbols"], ["btcusdt", "ethusdt"])
            self.assertEqual(item["score"], "64.530507")
            self.assertEqual(item["last_observation_age_hours"], "unknown")
            self.assertEqual(item["observation_recency_bucket"], "NEVER_OBSERVED")
            self.assertEqual(item["observation_last_outcome_short"], "NO_HISTORY")
            self.assertEqual(item["observation_attention_flag"], "false")
            self.assertEqual(item["observation_status"], "NEW")
            self.assertEqual(item["next_action_hint"], "READY_TO_OBSERVE")
            self.assertEqual(item["reobserve_status"], "NOT_OBSERVED")
            self.assertEqual(item["recent_observation_trail"], "")
            self.assertEqual(item["last_pnl_state"], "UNKNOWN")
            self.assertEqual(item["pnl_interpretation"], "UNKNOWN")
            self.assertEqual(item["pnl_attention_flag"], "false")
            self.assertEqual(item["latest_realized_sign"], "UNKNOWN")
            self.assertEqual(item["latest_unrealized_sign"], "UNKNOWN")

    def test_mock_observer_reads_watchlist_and_writes_events(self):
        with tempfile.TemporaryDirectory(prefix="shadow_observer_") as td:
            root = Path(td)
            watchlist_path = root / "shadow_watchlist_v0.json"
            out_log_path = root / "shadow_observation_log_v0.jsonl"
            write_json(
                watchlist_path,
                {
                    "schema_version": "shadow_watchlist_v0",
                    "generated_ts_utc": "2026-03-07T06:00:00Z",
                    "source": "candidate_review.tsv",
                    "selection_policy": {},
                    "items": [
                        {
                            "rank": 1,
                            "selection_slot": "bybit/bbo",
                            "pack_id": "pack_strong",
                            "pack_path": "/tmp/pack_strong",
                            "decision_tier": "PROMOTE_STRONG",
                            "score": "64.000000",
                            "exchange": "bybit",
                            "stream": "bbo",
                            "symbols": ["ethusdt"],
                            "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
                            "watch_status": "ACTIVE",
                            "observed_before": True,
                            "observation_count": 1,
                            "last_observed_at": "2026-03-07T07:24:58Z",
                            "last_verify_soft_live_pass": "true",
                            "last_stop_reason": "STREAM_END",
                            "last_processed_event_count": "16",
                            "last_observation_age_hours": "1.000",
                            "observation_recency_bucket": "WITHIN_24H",
                            "observation_last_outcome_short": "PASS(16)",
                            "observation_attention_flag": "false",
                            "observation_status": "OBSERVED_PASS",
                            "next_action_hint": "ALREADY_OBSERVED_GOOD",
                            "reobserve_status": "RECENTLY_OBSERVED",
                            "recent_observation_trail": "2026-03-07T07:24:58Z/PASS(16)/STREAM_END",
                            "notes": "",
                        },
                        {
                            "rank": 2,
                            "selection_slot": "overall_fill",
                            "pack_id": "pack_promote",
                            "pack_path": "/tmp/pack_promote",
                            "decision_tier": "PROMOTE",
                            "score": "50.000000",
                            "exchange": "binance",
                            "stream": "trade",
                            "symbols": ["btcusdt"],
                            "context_flags": "MARK=WARN;FUNDING=PASS;OI=NEUTRAL",
                            "watch_status": "ACTIVE",
                            "observed_before": False,
                            "observation_count": 0,
                            "last_observed_at": "",
                            "last_verify_soft_live_pass": "unknown",
                            "last_stop_reason": "",
                            "last_processed_event_count": "unknown",
                            "last_observation_age_hours": "unknown",
                            "observation_recency_bucket": "NEVER_OBSERVED",
                            "observation_last_outcome_short": "NO_HISTORY",
                            "observation_attention_flag": "false",
                            "observation_status": "NEW",
                            "next_action_hint": "READY_TO_OBSERVE",
                            "reobserve_status": "NOT_OBSERVED",
                            "recent_observation_trail": "",
                            "notes": "",
                        },
                    ],
                },
            )

            res = self._run_observer(watchlist_path, out_log_path)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            events = load_observation_log(out_log_path)
            self.assertEqual(len(events), 6)
            self.assertEqual([event["event_type"] for event in events[:3]], ["watch_started", "signal_seen", "would_trade"])
            self.assertEqual([event["event_type"] for event in events[3:]], ["watch_started", "signal_seen", "would_skip"])


if __name__ == "__main__":
    unittest.main()
