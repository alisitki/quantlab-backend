import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_promotion_guards_v0.py"


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


class PromotionGuardsV0Tests(unittest.TestCase):
    def _mk_pack(self, root: Path) -> Path:
        pack = root / "pack"
        pack.mkdir(parents=True, exist_ok=True)
        (pack / "sha_verify.txt").write_text("/tmp/pack.tar.gz: OK\n", encoding="utf-8")
        write_tsv(
            pack / "campaign_meta.tsv",
            ["run_id", "category"],
            [["pack", "FULLSCAN_MAJOR"]],
        )
        write_tsv(
            pack / "run_summary.tsv",
            ["symbol", "exit_code", "elapsed_sec", "max_rss_kb", "determinism_statuses", "label"],
            [["btcusdt", "0", "10.0", "1000", "PASS", "label=PASS/MULTI_HYPOTHESIS_READY"]],
        )
        write_tsv(
            pack / "runs" / "btcusdt" / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv",
            ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
            [["w", "f1", "a", "a", "PASS", "cols"]],
        )
        return pack

    def _run(self, pack: Path, *extra):
        cmd = ["python3", str(SCRIPT), "--pack", str(pack), *extra]
        return subprocess.run(cmd, cwd=str(REPO), text=True, capture_output=True)

    def test_promote_happy_path(self):
        with tempfile.TemporaryDirectory(prefix="phase6_promote_") as td:
            pack = self._mk_pack(Path(td))
            res = self._run(pack)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("decision=PROMOTE", report)
            tsv = (pack / "guards" / "decision_report.tsv").read_text(encoding="utf-8")
            self.assertIn("FINAL_DECISION\tPASS\tPROMOTE", tsv)
            detail = json.loads((pack / "guards" / "guard_details.json").read_text(encoding="utf-8"))
            self.assertEqual(detail["decision"], "PROMOTE")

    def test_hold_when_evidence_has_no_ok_line(self):
        with tempfile.TemporaryDirectory(prefix="phase6_evidence_hold_") as td:
            pack = self._mk_pack(Path(td))
            (pack / "sha_verify.txt").write_text("/tmp/pack.tar.gz: FAILED\n", encoding="utf-8")
            res = self._run(pack)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("decision=HOLD", report)
            self.assertIn("G1_EVIDENCE=FAIL", report)

    def test_hold_when_determinism_ratio_below_threshold(self):
        with tempfile.TemporaryDirectory(prefix="phase6_ratio_hold_") as td:
            pack = self._mk_pack(Path(td))
            write_tsv(
                pack / "runs" / "ethusdt" / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv",
                ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
                [["w", "f1", "a", "b", "FAIL", "cols"]],
            )
            res = self._run(pack, "--pass-ratio", "0.75")
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("decision=HOLD", report)
            self.assertIn("G2_DETERMINISM=FAIL", report)

    def test_skipped_status_excluded_from_determinism_ratio(self):
        with tempfile.TemporaryDirectory(prefix="phase6_skipped_") as td:
            pack = self._mk_pack(Path(td))
            write_tsv(
                pack / "runs" / "ethusdt" / "artifacts" / "multi_hypothesis" / "determinism_compare.tsv",
                ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
                [["w", "f2", "-", "-", "SKIPPED_UNSUPPORTED_STREAM", "cols"]],
            )
            res = self._run(pack, "--pass-ratio", "1.0")
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("decision=PROMOTE", report)

    def test_hold_when_resource_limit_exceeded(self):
        with tempfile.TemporaryDirectory(prefix="phase6_resource_hold_") as td:
            pack = self._mk_pack(Path(td))
            write_tsv(
                pack / "run_summary.tsv",
                ["symbol", "exit_code", "elapsed_sec", "max_rss_kb", "determinism_statuses", "label"],
                [["btcusdt", "0", "1200.0", "1000", "PASS", "label=PASS/MULTI_HYPOTHESIS_READY"]],
            )
            res = self._run(pack)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            report = (pack / "guards" / "decision_report.txt").read_text(encoding="utf-8")
            self.assertIn("decision=HOLD", report)
            self.assertIn("G3_RESOURCE=FAIL", report)

    def test_contract_mismatch_reports_found_vs_expected(self):
        with tempfile.TemporaryDirectory(prefix="phase6_contract_mismatch_") as td:
            pack = self._mk_pack(Path(td))
            (pack / "run_summary.tsv").unlink()
            res = self._run(pack)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("STOP: PACK_CONTRACT_MISMATCH", res.stderr)
            self.assertIn("expected=", res.stderr)
            self.assertIn("found=", res.stderr)


if __name__ == "__main__":
    unittest.main()
