import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.phase6_backfill_v2 import run_backfill


REPO = Path(__file__).resolve().parents[2]


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_base_policy(path: Path) -> None:
    write_json(
        path,
        {
            "pass_ratio": 1.0,
            "max_rss_kb": 2500000,
            "max_elapsed_sec": 900,
            "exclude_statuses": ["SKIPPED_UNSUPPORTED_STREAM"],
            "supported_statuses": ["PASS", "FAIL", "MISMATCH", "SKIPPED_UNSUPPORTED_STREAM"],
            "require_sha_ok_lines": 1,
        },
    )


def write_context_policy(path: Path) -> None:
    write_json(
        path,
        {
            "base_decision_if_v1_pass": "PROMOTE",
            "strong_promotion_requires": {
                "max_mark_trade_basis_max_abs_bps": 25.0,
                "max_abs_funding_mean": 0.0005,
                "oi_confirmation_required_exchanges": ["bybit", "okx"],
            },
            "hold_conditions": {
                "max_mark_trade_basis_max_abs_bps": 75.0,
                "max_abs_funding_mean": 0.0015,
                "min_oi_change_pct_confirm": 0.0,
            },
            "unsupported_oi_behavior": "NEUTRAL",
            "absent_context_behavior": "NEUTRAL",
        },
    )


def make_pack(root: Path, name: str, *, exchange: str = "binance", stream: str = "trade") -> Path:
    pack = root / f"multi-hypothesis-phase5-bighunt-{exchange}-{stream}-20260301..20260301-{name}__FULLSCAN_MAJOR"
    pack.mkdir(parents=True, exist_ok=True)
    (pack / "sha_verify.txt").write_text("/tmp/fake.tar.gz: OK\n", encoding="utf-8")
    write_tsv(pack / "campaign_meta.tsv", ["run_id", "category"], [[pack.name, "FULLSCAN_MAJOR"]])
    write_tsv(
        pack / "run_summary.tsv",
        ["symbol", "exit_code", "elapsed_sec", "max_rss_kb", "determinism_statuses", "label"],
        [["btcusdt", "0", "12.5", "10000", "PASS|SKIPPED_UNSUPPORTED_STREAM", "label=PASS/MULTI_HYPOTHESIS_READY"]],
    )
    write_tsv(
        pack / "runs" / "btcusdt" / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv",
        ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
        [["w", "f1", "a", "a", "PASS", "cols"]],
    )
    return pack


def write_context_summary(pack: Path, *, exchange: str = "binance", oi_status: str = "UNSUPPORTED_EXCHANGE", oi_change_pct: str = "NA") -> None:
    write_tsv(
        pack / "runs" / "btcusdt" / "artifacts" / "context" / "context_summary.tsv",
        [
            "exchange",
            "symbol",
            "date_start",
            "date_end",
            "core_stream",
            "ctx_mark_price_status",
            "ctx_mark_price_first",
            "ctx_mark_price_last",
            "ctx_mark_price_change_bps",
            "ctx_mark_trade_basis_mean_bps",
            "ctx_mark_trade_basis_max_abs_bps",
            "ctx_funding_status",
            "ctx_funding_count",
            "ctx_funding_first",
            "ctx_funding_last",
            "ctx_funding_mean",
            "ctx_funding_min",
            "ctx_funding_max",
            "ctx_oi_status",
            "ctx_oi_count",
            "ctx_oi_first",
            "ctx_oi_last",
            "ctx_oi_change_pct",
            "ctx_oi_min",
            "ctx_oi_max",
            "notes",
        ],
        [
            [
                exchange,
                "btcusdt",
                "20260301",
                "20260301",
                "trade",
                "OK",
                "100.000000000000000",
                "101.000000000000000",
                "100.000000000000000",
                "5.000000000000000",
                "12.000000000000000",
                "OK",
                "3",
                "0.000100000000000",
                "0.000500000000000",
                "0.000300000000000",
                "0.000100000000000",
                "0.000500000000000",
                oi_status,
                "2" if oi_status == "OK" else "NA",
                "10.000000000000000" if oi_status == "OK" else "NA",
                "11.000000000000000" if oi_status == "OK" else "NA",
                oi_change_pct,
                "10.000000000000000" if oi_status == "OK" else "NA",
                "11.000000000000000" if oi_status == "OK" else "NA",
                "",
            ]
        ],
    )


class Phase6BackfillV2Tests(unittest.TestCase):
    def _args(self, archive_glob: str, state_dir: Path):
        return SimpleNamespace(
            archive_root_glob=archive_glob,
            state_dir=str(state_dir),
            policy="",
            context_policy="",
            pack=[],
        )

    def test_eligible_pack_backfilled(self):
        with tempfile.TemporaryDirectory(prefix="phase6_backfill_ok_") as td:
            root = Path(td)
            archive_root = root / "archive" / "20260307_slim"
            state_dir = root / "state"
            write_base_policy(state_dir / "promotion_policy.json")
            write_context_policy(state_dir / "context_policy_v2.json")
            pack = make_pack(archive_root, "120000-aa", exchange="bybit", stream="trade")
            write_context_summary(pack, exchange="bybit", oi_status="OK", oi_change_pct="4.500000000000000")

            exit_code, report, report_path = run_backfill(self._args(str(root / "archive" / "*_slim"), state_dir), repo=REPO)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["eligible_count"], 1)
            self.assertEqual(report["applied_count"], 1)
            self.assertTrue(report_path.exists())
            self.assertTrue((pack / "guards" / "decision_report.txt").exists())
            records = (state_dir / "promotion_records.jsonl").read_text(encoding="utf-8")
            self.assertIn("\"decision_tier\":\"PROMOTE_STRONG\"", records)

    def test_already_applied_is_skipped_and_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="phase6_backfill_idem_") as td:
            root = Path(td)
            archive_root = root / "archive" / "20260307_slim"
            state_dir = root / "state"
            write_base_policy(state_dir / "promotion_policy.json")
            write_context_policy(state_dir / "context_policy_v2.json")
            pack = make_pack(archive_root, "120100-bb", exchange="bybit", stream="trade")
            write_context_summary(pack, exchange="bybit", oi_status="OK", oi_change_pct="4.500000000000000")

            args = self._args(str(root / "archive" / "*_slim"), state_dir)
            first_exit, first_report, _first_path = run_backfill(args, repo=REPO)
            self.assertEqual(first_exit, 0)
            self.assertEqual(first_report["applied_count"], 1)
            first_lines = (state_dir / "promotion_records.jsonl").read_text(encoding="utf-8").strip().splitlines()

            second_exit, second_report, _second_path = run_backfill(args, repo=REPO)
            self.assertEqual(second_exit, 0)
            self.assertEqual(second_report["applied_count"], 0)
            self.assertEqual(second_report["skipped_already_applied_count"], 1)
            second_lines = (state_dir / "promotion_records.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(first_lines, second_lines)

    def test_insufficient_context_is_skipped(self):
        with tempfile.TemporaryDirectory(prefix="phase6_backfill_skip_") as td:
            root = Path(td)
            archive_root = root / "archive" / "20260307_slim"
            state_dir = root / "state"
            write_base_policy(state_dir / "promotion_policy.json")
            write_context_policy(state_dir / "context_policy_v2.json")
            make_pack(archive_root, "120200-cc", exchange="okx", stream="bbo")

            exit_code, report, _report_path = run_backfill(self._args(str(root / "archive" / "*_slim"), state_dir), repo=REPO)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["applied_count"], 0)
            self.assertEqual(report["skipped_insufficient_context_count"], 1)


if __name__ == "__main__":
    unittest.main()
