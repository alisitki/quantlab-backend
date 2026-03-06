import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_candidate_review_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")


def candidate_record(
    *,
    pack_id: str,
    pack_path: str,
    decision_tier: str,
    det_pass: int = 5,
    det_supported: int = 5,
    det_skipped: int = 1,
    max_rss_kb: float = 100000.0,
    max_elapsed_sec: float = 30.0,
    guards: dict | None = None,
    export_ts_utc: str = "2026-03-06T09:15:42Z",
) -> dict:
    return {
        "export_ts_utc": export_ts_utc,
        "pack_id": pack_id,
        "pack_path": pack_path,
        "decision_tier": decision_tier,
        "source_decision": decision_tier,
        "context_policy_hash": "ctx-hash" if decision_tier == "PROMOTE_STRONG" else "",
        "policy_hash": "policy-hash",
        "det_pass": det_pass,
        "det_supported": det_supported,
        "det_skipped": det_skipped,
        "max_rss_kb": max_rss_kb,
        "max_elapsed_sec": max_elapsed_sec,
        "guards": dict(guards or {"G1_EVIDENCE": "PASS"}),
        "candidate_status": "NEW",
        "notes": "",
    }


def write_candidate_state(state_dir: Path, records) -> None:
    rows = sorted(
        records,
        key=lambda rec: (
            0 if str(rec["decision_tier"]).strip().upper() == "PROMOTE_STRONG" else 1,
            str(rec["pack_path"]),
            str(rec["pack_id"]),
        ),
    )
    write_jsonl(state_dir / "candidate_queue.jsonl", rows)
    index_payload = {
        "record_count": len(rows),
        "by_tier": {
            "PROMOTE": sum(1 for rec in rows if rec["decision_tier"] == "PROMOTE"),
            "PROMOTE_STRONG": sum(1 for rec in rows if rec["decision_tier"] == "PROMOTE_STRONG"),
        },
        "candidate_pack_ids": [rec["pack_id"] for rec in rows],
        "latest_by_pack_id": {rec["pack_id"]: rec for rec in rows},
        "latest_by_tier": {
            "PROMOTE": [rec for rec in rows if rec["decision_tier"] == "PROMOTE"],
            "PROMOTE_STRONG": [rec for rec in rows if rec["decision_tier"] == "PROMOTE_STRONG"],
        },
        "latest_export_ts_utc": max(str(rec["export_ts_utc"]) for rec in rows) if rows else "",
    }
    write_json(state_dir / "candidate_index.json", index_payload)
    with (state_dir / "candidate_report.tsv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "pack_id",
                "decision_tier",
                "pack_path",
                "det_pass",
                "det_supported",
                "det_skipped",
                "max_rss_kb",
                "max_elapsed_sec",
                "candidate_status",
            ]
        )
        for rec in rows:
            writer.writerow(
                [
                    rec["pack_id"],
                    rec["decision_tier"],
                    rec["pack_path"],
                    rec["det_pass"],
                    rec["det_supported"],
                    rec["det_skipped"],
                    rec["max_rss_kb"],
                    rec["max_elapsed_sec"],
                    rec["candidate_status"],
                ]
            )


def write_context_guard_report(pack_path: Path, *, mark: str, funding: str, oi: str) -> None:
    guards_dir = pack_path / "guards"
    guards_dir.mkdir(parents=True, exist_ok=True)
    (guards_dir / "decision_report.txt").write_text(
        "\n".join(
            [
                "decision=PROMOTE",
                f"G4_MARK_CONTEXT={mark} observed=[x] threshold=[x] detail=[x]",
                f"G5_FUNDING_CONTEXT={funding} observed=[x] threshold=[x] detail=[x]",
                f"G6_OI_CONTEXT={oi} observed=[x] threshold=[x] detail=[x]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    write_json(guards_dir / "guard_details.json", {"decision_tier": "PROMOTE", "guards": {}})


def load_review_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


class CandidateReviewV0Tests(unittest.TestCase):
    def _run(self, state_dir: Path):
        cmd = ["python3", str(SCRIPT), "--state-dir", str(state_dir)]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_promote_strong_outranks_promote(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_tier_") as td:
            root = Path(td)
            state_dir = root / "state"
            strong = candidate_record(pack_id="pack_strong", pack_path=str(root / "pack_a"), decision_tier="PROMOTE_STRONG")
            promote = candidate_record(pack_id="pack_promote", pack_path=str(root / "pack_b"), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [promote, strong])

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_strong")
            self.assertEqual(rows[0]["decision_tier"], "PROMOTE_STRONG")

    def test_better_determinism_and_context_rank_higher(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_context_") as td:
            root = Path(td)
            state_dir = root / "state"
            top_pack = root / "pack_top"
            low_pack = root / "pack_low"
            top = candidate_record(
                pack_id="pack_top",
                pack_path=str(top_pack),
                decision_tier="PROMOTE",
                det_pass=10,
                det_supported=10,
            )
            low = candidate_record(
                pack_id="pack_low",
                pack_path=str(low_pack),
                decision_tier="PROMOTE",
                det_pass=6,
                det_supported=10,
            )
            write_candidate_state(state_dir, [top, low])
            write_context_guard_report(top_pack, mark="PASS", funding="PASS", oi="PASS")

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_top")
            self.assertEqual(rows[0]["context_flags"], "MARK=PASS;FUNDING=PASS;OI=PASS")

    def test_lower_resource_wins_tie(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_resource_") as td:
            root = Path(td)
            state_dir = root / "state"
            light = candidate_record(
                pack_id="pack_light",
                pack_path=str(root / "pack_a"),
                decision_tier="PROMOTE",
                max_rss_kb=80000.0,
                max_elapsed_sec=20.0,
            )
            heavy = candidate_record(
                pack_id="pack_heavy",
                pack_path=str(root / "pack_b"),
                decision_tier="PROMOTE",
                max_rss_kb=300000.0,
                max_elapsed_sec=80.0,
            )
            write_candidate_state(state_dir, [heavy, light])

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_light")

    def test_missing_context_produces_stable_order_and_na_flags(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_missing_") as td:
            root = Path(td)
            state_dir = root / "state"
            first = candidate_record(pack_id="pack_a", pack_path=str(root / "a_pack"), decision_tier="PROMOTE")
            second = candidate_record(pack_id="pack_b", pack_path=str(root / "b_pack"), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [second, first])

            res = self._run(state_dir)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_a")
            self.assertEqual(rows[0]["context_flags"], "MARK=NA;FUNDING=NA;OI=NA")
            self.assertEqual(rows[1]["pack_id"], "pack_b")

    def test_rerun_is_stable(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_rerun_") as td:
            root = Path(td)
            state_dir = root / "state"
            first = candidate_record(pack_id="pack_a", pack_path=str(root / "pack_a"), decision_tier="PROMOTE")
            second = candidate_record(
                pack_id="pack_b",
                pack_path=str(root / "pack_b"),
                decision_tier="PROMOTE",
                det_pass=9,
                det_supported=10,
            )
            write_candidate_state(state_dir, [first, second])

            r1 = self._run(state_dir)
            self.assertEqual(r1.returncode, 0, msg=r1.stderr)
            tsv_first = (state_dir / "candidate_review.tsv").read_text(encoding="utf-8")
            r2 = self._run(state_dir)
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            tsv_second = (state_dir / "candidate_review.tsv").read_text(encoding="utf-8")
            self.assertEqual(tsv_first, tsv_second)


if __name__ == "__main__":
    unittest.main()
