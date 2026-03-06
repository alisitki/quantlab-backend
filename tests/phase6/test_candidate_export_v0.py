import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_candidate_export_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")


def read_jsonl(path: Path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def promotion_record(
    *,
    pack_id: str,
    pack_path: str,
    decision: str,
    decision_tier: str | None = None,
    ts_utc: str = "2026-03-06T00:00:00Z",
    policy_hash: str = "policy-hash",
    context_policy_hash: str = "",
    det_pass: int = 5,
    det_supported: int = 5,
    det_skipped: int = 1,
    max_rss_kb: float = 12345.0,
    max_elapsed_sec: float = 12.5,
) -> dict:
    rec = {
        "ts_utc": ts_utc,
        "pack_id": pack_id,
        "pack_path": pack_path,
        "decision": decision,
        "policy_hash": policy_hash,
        "det_pass": det_pass,
        "det_supported": det_supported,
        "det_skipped": det_skipped,
        "max_rss_kb": max_rss_kb,
        "max_elapsed_sec": max_elapsed_sec,
        "guards": {"G1_EVIDENCE": "PASS"},
        "sha_tar_ok": True,
    }
    if decision_tier is not None:
        rec["decision_tier"] = decision_tier
    if context_policy_hash:
        rec["context_policy_hash"] = context_policy_hash
    return rec


def promotion_index(pack_latest: dict) -> dict:
    promote_packs = sorted(
        {
            str(rec.get("pack_path", ""))
            for rec in pack_latest.values()
            if str(rec.get("decision_tier") or rec.get("decision") or "").strip().upper() in {"PROMOTE", "PROMOTE_STRONG"}
        }
    )
    promote_strong_packs = sorted(
        {
            str(rec.get("pack_path", ""))
            for rec in pack_latest.values()
            if str(rec.get("decision_tier") or rec.get("decision") or "").strip().upper() == "PROMOTE_STRONG"
        }
    )
    return {
        "record_count": len(pack_latest),
        "pack_latest": pack_latest,
        "promote_pack_ids": sorted(pack_latest.keys()),
        "promote_packs": promote_packs,
        "promote_strong_packs": promote_strong_packs,
    }


class CandidateExportV0Tests(unittest.TestCase):
    def _run(self, state_dir: Path):
        cmd = ["python3", str(SCRIPT), "--state-dir", str(state_dir)]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_hold_excluded_and_promote_exported(self):
        with tempfile.TemporaryDirectory(prefix="candidate_export_hold_") as td:
            state_dir = Path(td)
            p1 = promotion_record(pack_id="pack_promote", pack_path="/tmp/promote", decision="PROMOTE")
            p2 = promotion_record(pack_id="pack_hold", pack_path="/tmp/hold", decision="HOLD")
            write_jsonl(state_dir / "promotion_records.jsonl", [p1, p2])
            write_json(state_dir / "promotion_index.json", promotion_index({"pack_promote": p1, "pack_hold": p2}))

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            queue = read_jsonl(state_dir / "candidate_queue.jsonl")
            self.assertEqual(len(queue), 1)
            self.assertEqual(queue[0]["pack_id"], "pack_promote")
            report = (state_dir / "candidate_report.tsv").read_text(encoding="utf-8")
            self.assertIn("pack_promote", report)
            self.assertNotIn("pack_hold", report)

    def test_promote_strong_preserved_and_reported_first(self):
        with tempfile.TemporaryDirectory(prefix="candidate_export_strong_") as td:
            state_dir = Path(td)
            strong = promotion_record(
                pack_id="pack_strong",
                pack_path="/tmp/a_strong",
                decision="PROMOTE_STRONG",
                decision_tier="PROMOTE_STRONG",
                context_policy_hash="ctx",
            )
            promote = promotion_record(pack_id="pack_promote", pack_path="/tmp/z_promote", decision="PROMOTE")
            write_jsonl(state_dir / "promotion_records.jsonl", [promote, strong])
            write_json(
                state_dir / "promotion_index.json",
                promotion_index({"pack_promote": promote, "pack_strong": strong}),
            )

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            idx = json.loads((state_dir / "candidate_index.json").read_text(encoding="utf-8"))
            self.assertEqual(idx["by_tier"]["PROMOTE_STRONG"], 1)
            with (state_dir / "candidate_report.tsv").open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f, delimiter="\t"))
            self.assertEqual(rows[0]["pack_id"], "pack_strong")
            self.assertEqual(rows[0]["decision_tier"], "PROMOTE_STRONG")

    def test_rerun_is_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="candidate_export_idem_") as td:
            state_dir = Path(td)
            promote = promotion_record(pack_id="pack_promote", pack_path="/tmp/promote", decision="PROMOTE")
            write_jsonl(state_dir / "promotion_records.jsonl", [promote])
            write_json(state_dir / "promotion_index.json", promotion_index({"pack_promote": promote}))

            r1 = self._run(state_dir)
            self.assertEqual(r1.returncode, 0, msg=r1.stderr)
            self.assertIn("exported_count=1", r1.stdout)
            r2 = self._run(state_dir)
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            self.assertIn("exported_count=0", r2.stdout)
            self.assertIn("skipped_existing_count=1", r2.stdout)
            queue = read_jsonl(state_dir / "candidate_queue.jsonl")
            self.assertEqual(len(queue), 1)

    def test_latest_record_per_pack_id_wins(self):
        with tempfile.TemporaryDirectory(prefix="candidate_export_latest_") as td:
            state_dir = Path(td)
            older = promotion_record(
                pack_id="pack_flip",
                pack_path="/tmp/pack_flip",
                decision="PROMOTE",
                ts_utc="2026-03-06T00:00:00Z",
            )
            newer_hold = promotion_record(
                pack_id="pack_flip",
                pack_path="/tmp/pack_flip",
                decision="HOLD",
                ts_utc="2026-03-06T01:00:00Z",
            )
            newer_strong = promotion_record(
                pack_id="pack_strong",
                pack_path="/tmp/pack_strong",
                decision="PROMOTE_STRONG",
                decision_tier="PROMOTE_STRONG",
                ts_utc="2026-03-06T02:00:00Z",
                context_policy_hash="ctx-2",
            )
            write_jsonl(state_dir / "promotion_records.jsonl", [older, newer_hold, newer_strong])
            write_json(
                state_dir / "promotion_index.json",
                promotion_index({"pack_flip": newer_hold, "pack_strong": newer_strong}),
            )

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            queue = read_jsonl(state_dir / "candidate_queue.jsonl")
            self.assertEqual(len(queue), 1)
            self.assertEqual(queue[0]["pack_id"], "pack_strong")
            self.assertEqual(queue[0]["decision_tier"], "PROMOTE_STRONG")


if __name__ == "__main__":
    unittest.main()
