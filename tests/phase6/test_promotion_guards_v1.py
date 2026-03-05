import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from tools.phase6_promotion_guards_v1 import canonical_policy_hash, read_jsonl_records, rebuild_index


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_promotion_guards_v1.py"


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def write_default_policy(path: Path) -> None:
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


class PromotionGuardsV1Tests(unittest.TestCase):
    def _run(self, pack: Path, state_dir: Path, policy_path: Path):
        cmd = [
            "python3",
            str(SCRIPT),
            "--pack",
            str(pack),
            "--state-dir",
            str(state_dir),
            "--policy",
            str(policy_path),
        ]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_policy_hash_stable(self):
        p1 = {
            "pass_ratio": 1.0,
            "max_rss_kb": 2500000,
            "max_elapsed_sec": 900,
            "exclude_statuses": ["SKIPPED_UNSUPPORTED_STREAM"],
            "supported_statuses": ["PASS", "FAIL", "MISMATCH", "SKIPPED_UNSUPPORTED_STREAM"],
            "require_sha_ok_lines": 1,
        }
        p2 = {
            "supported_statuses": ["PASS", "FAIL", "MISMATCH", "SKIPPED_UNSUPPORTED_STREAM"],
            "exclude_statuses": ["SKIPPED_UNSUPPORTED_STREAM"],
            "pass_ratio": 1.0,
            "max_elapsed_sec": 900,
            "max_rss_kb": 2500000,
            "require_sha_ok_lines": 1,
        }
        self.assertEqual(canonical_policy_hash(p1), canonical_policy_hash(p2))

    def test_record_append_and_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v1_idem_") as td:
            root = Path(td)
            state_dir = root / "state"
            policy = state_dir / "promotion_policy.json"
            write_default_policy(policy)
            pack = make_pack(root, "pack_promote")

            r1 = self._run(pack, state_dir, policy)
            self.assertEqual(r1.returncode, 0, msg=r1.stderr)
            self.assertIn("record_appended=true", r1.stdout)
            records_path = state_dir / "promotion_records.jsonl"
            self.assertEqual(len(read_jsonl_records(records_path)), 1)

            r2 = self._run(pack, state_dir, policy)
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            self.assertIn("record_appended=false", r2.stdout)
            self.assertEqual(len(read_jsonl_records(records_path)), 1)

    def test_index_rebuild_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v1_index_") as td:
            root = Path(td)
            state_dir = root / "state"
            policy = state_dir / "promotion_policy.json"
            write_default_policy(policy)
            p1 = make_pack(root, "pack1", sha_ok=True, det_status="PASS")
            p2 = make_pack(root, "pack2", sha_ok=False, det_status="PASS")
            self.assertEqual(self._run(p1, state_dir, policy).returncode, 0)
            self.assertEqual(self._run(p2, state_dir, policy).returncode, 0)
            records = read_jsonl_records(state_dir / "promotion_records.jsonl")
            idx1 = rebuild_index(records)
            idx2 = rebuild_index(records)
            self.assertEqual(
                json.dumps(idx1, sort_keys=True),
                json.dumps(idx2, sort_keys=True),
            )

    def test_promote_packs_contains_promote_only(self):
        with tempfile.TemporaryDirectory(prefix="phase6_v1_promote_list_") as td:
            root = Path(td)
            state_dir = root / "state"
            policy = state_dir / "promotion_policy.json"
            write_default_policy(policy)
            promote_pack = make_pack(root, "pack_promote", sha_ok=True, det_status="PASS")
            hold_pack = make_pack(root, "pack_hold", sha_ok=False, det_status="PASS")
            self.assertEqual(self._run(promote_pack, state_dir, policy).returncode, 0)
            self.assertEqual(self._run(hold_pack, state_dir, policy).returncode, 0)

            idx = json.loads((state_dir / "promotion_index.json").read_text(encoding="utf-8"))
            self.assertIn(str(promote_pack), idx["promote_packs"])
            self.assertNotIn(str(hold_pack), idx["promote_packs"])


if __name__ == "__main__":
    unittest.main()
