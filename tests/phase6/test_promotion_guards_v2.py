import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_promotion_guards_v2.py"


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def write_base_policy(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pass_ratio": 1.0,
                "max_rss_kb": 2500000,
                "max_elapsed_sec": 900,
                "exclude_statuses": ["SKIPPED_UNSUPPORTED_STREAM"],
                "supported_statuses": ["PASS", "FAIL", "MISMATCH", "SKIPPED_UNSUPPORTED_STREAM"],
                "require_sha_ok_lines": 1,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def write_context_policy(path: Path, *, absent_behavior: str = "NEUTRAL") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
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
                "absent_context_behavior": absent_behavior,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def make_pack(root: Path, name: str, *, sha_ok: bool = True, det_status: str = "PASS") -> Path:
    pack = root / name
    pack.mkdir(parents=True, exist_ok=True)
    sha_line = "/tmp/fake.tar.gz: OK\n" if sha_ok else "/tmp/fake.tar.gz: FAILED\n"
    (pack / "sha_verify.txt").write_text(sha_line, encoding="utf-8")
    write_tsv(pack / "campaign_meta.tsv", ["run_id", "category"], [[name, "FULLSCAN_MAJOR"]])
    write_tsv(
        pack / "run_summary.tsv",
        ["symbol", "exit_code", "elapsed_sec", "max_rss_kb", "determinism_statuses", "label"],
        [["btcusdt", "0", "12.5", "10000", "PASS|SKIPPED_UNSUPPORTED_STREAM", "label=PASS/MULTI_HYPOTHESIS_READY"]],
    )
    write_tsv(
        pack / "runs" / "btcusdt" / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv",
        ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
        [["w", "f1", "a", "a", det_status, "cols"]],
    )
    return pack


def write_context_summary(
    pack: Path,
    *,
    symbol: str = "btcusdt",
    exchange: str = "binance",
    core_stream: str = "trade",
    mark_status: str = "OK",
    mark_basis_max_abs_bps: str = "10.000000000000000",
    funding_status: str = "OK",
    funding_mean: str = "0.000400000000000",
    oi_status: str = "UNSUPPORTED_EXCHANGE",
    oi_change_pct: str = "NA",
) -> None:
    write_tsv(
        pack / "runs" / symbol / "artifacts" / "context" / "context_summary.tsv",
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
                symbol,
                "20260301",
                "20260302",
                core_stream,
                mark_status,
                "100.000000000000000",
                "101.000000000000000",
                "100.000000000000000",
                "5.000000000000000",
                mark_basis_max_abs_bps,
                funding_status,
                "3",
                "0.000100000000000",
                "0.000500000000000",
                funding_mean,
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


class PromotionGuardsV2Tests(unittest.TestCase):
    def _run(self, pack: Path, state_dir: Path, base_policy: Path, context_policy: Path):
        cmd = [
            "python3",
            str(SCRIPT),
            "--pack",
            str(pack),
            "--state-dir",
            str(state_dir),
            "--policy",
            str(base_policy),
            "--context-policy",
            str(context_policy),
        ]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_v1_hold_remains_hold(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_base_hold_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_hold", sha_ok=False)
            write_context_summary(pack, exchange="bybit", oi_status="OK", oi_change_pct="5.000000000000000")

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=HOLD", res.stdout)
            detail = json.loads((pack / "guards" / "guard_details.json").read_text(encoding="utf-8"))
            self.assertEqual(detail["decision_tier"], "HOLD")
            self.assertEqual(detail["base_v1_decision"], "HOLD")

    def test_good_context_promotes_strong(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_strong_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_strong")
            write_context_summary(
                pack,
                exchange="bybit",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="OK",
                oi_change_pct="4.500000000000000",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=PROMOTE_STRONG", res.stdout)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("G4_MARK_CONTEXT=PASS", report)
            self.assertIn("G5_FUNDING_CONTEXT=PASS", report)
            self.assertIn("G6_OI_CONTEXT=PASS", report)

    def test_high_basis_holds(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_basis_hold_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_basis_hold")
            write_context_summary(
                pack,
                exchange="bybit",
                mark_basis_max_abs_bps="90.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="OK",
                oi_change_pct="4.500000000000000",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=HOLD", res.stdout)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("G4_MARK_CONTEXT=FAIL", report)

    def test_high_funding_downgrades_to_promote(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_funding_warn_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_funding_warn")
            write_context_summary(
                pack,
                exchange="bybit",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000800000000000",
                oi_status="OK",
                oi_change_pct="4.500000000000000",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=PROMOTE", res.stdout)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("G5_FUNDING_CONTEXT=WARN", report)

    def test_binance_oi_unsupported_is_neutral(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_binance_oi_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_binance_strong")
            write_context_summary(
                pack,
                exchange="binance",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="UNSUPPORTED_EXCHANGE",
                oi_change_pct="NA",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=PROMOTE_STRONG", res.stdout)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("G6_OI_CONTEXT=PASS", report)

    def test_required_oi_absent_drops_to_promote(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_oi_absent_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_oi_absent")
            write_context_summary(
                pack,
                exchange="bybit",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="ABSENT",
                oi_change_pct="NA",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=PROMOTE", res.stdout)
            detail = json.loads((pack / "guards" / "guard_details.json").read_text(encoding="utf-8"))
            self.assertEqual(detail["guards"]["G6_OI_CONTEXT"]["absent_row_count"], 1)

    def test_required_oi_weak_holds(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_oi_weak_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_oi_weak")
            write_context_summary(
                pack,
                exchange="okx",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="OK",
                oi_change_pct="-1.000000000000000",
            )

            res = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("decision=HOLD", res.stdout)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("G6_OI_CONTEXT=FAIL", report)

    def test_idempotent_state_append_still_works(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v2_idem_") as td:
            root = Path(td)
            state_dir = root / "state"
            base_policy = state_dir / "promotion_policy.json"
            context_policy = state_dir / "context_policy_v2.json"
            write_base_policy(base_policy)
            write_context_policy(context_policy)
            pack = make_pack(root, "pack_idem")
            write_context_summary(
                pack,
                exchange="bybit",
                mark_basis_max_abs_bps="12.000000000000000",
                funding_mean="0.000300000000000",
                oi_status="OK",
                oi_change_pct="4.500000000000000",
            )

            r1 = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(r1.returncode, 0, msg=r1.stderr)
            self.assertIn("record_appended=true", r1.stdout)

            r2 = self._run(pack, state_dir, base_policy, context_policy)
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            self.assertIn("record_appended=false", r2.stdout)

            records = (state_dir / "promotion_records.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len([line for line in records if line.strip()]), 1)
            index_obj = json.loads((state_dir / "promotion_index.json").read_text(encoding="utf-8"))
            self.assertEqual(len(index_obj["promote_strong_packs"]), 1)
            self.assertEqual(len(index_obj["promote_packs"]), 1)


if __name__ == "__main__":
    unittest.main()
