import csv
import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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


def write_observation_index(path: Path, latest_by_pack_id: dict) -> None:
    write_json(
        path,
        {
            "schema_version": "shadow_observation_index_v0",
            "generated_ts_utc": "2026-03-07T07:31:18Z",
            "record_count": len(latest_by_pack_id),
            "pack_count": len(latest_by_pack_id),
            "observation_keys": [],
            "pack_ids": sorted(latest_by_pack_id.keys()),
            "latest_by_pack_id": latest_by_pack_id,
        },
    )


def write_observation_history(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries)
    path.write_text(payload, encoding="utf-8")


def write_execution_pack_summary(path: Path, latest_by_pack_id: dict) -> None:
    write_json(
        path,
        {
            "schema_version": "shadow_execution_pack_summary_v0",
            "generated_ts_utc": "2026-03-07T12:00:00Z",
            "record_count": len(latest_by_pack_id),
            "pack_count": len(latest_by_pack_id),
            "latest_by_pack_id": latest_by_pack_id,
        },
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
    def _run(
        self,
        state_dir: Path,
        *,
        observation_index: Path | None = None,
        observation_history: Path | None = None,
        execution_pack_summary: Path | None = None,
        recent_observation_hours: float | None = None,
    ):
        cmd = ["python3", str(SCRIPT), "--state-dir", str(state_dir)]
        if observation_index is not None:
            cmd.extend(["--observation-index", str(observation_index)])
        if observation_history is not None:
            cmd.extend(["--observation-history", str(observation_history)])
        if execution_pack_summary is not None:
            cmd.extend(["--execution-pack-summary", str(execution_pack_summary)])
        if recent_observation_hours is not None:
            cmd.extend(["--recent-observation-hours", str(recent_observation_hours)])
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
            self.assertEqual(rows[0]["observed_before"], "false")
            self.assertEqual(rows[0]["observation_count"], "0")
            self.assertEqual(rows[0]["last_observed_at"], "")
            self.assertEqual(rows[0]["last_verify_soft_live_pass"], "unknown")
            self.assertEqual(rows[0]["last_stop_reason"], "")
            self.assertEqual(rows[0]["last_processed_event_count"], "unknown")
            self.assertEqual(rows[0]["last_observation_age_hours"], "unknown")
            self.assertEqual(rows[0]["observation_recency_bucket"], "NEVER_OBSERVED")
            self.assertEqual(rows[0]["observation_last_outcome_short"], "NO_HISTORY")
            self.assertEqual(rows[0]["observation_attention_flag"], "false")
            self.assertEqual(rows[0]["observation_status"], "NEW")
            self.assertEqual(rows[0]["next_action_hint"], "READY_TO_OBSERVE")
            self.assertEqual(rows[0]["reobserve_status"], "NOT_OBSERVED")
            self.assertEqual(rows[0]["recent_observation_trail"], "")
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

    def test_matching_history_enriches_without_reordering(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_history_match_") as td:
            root = Path(td)
            state_dir = root / "state"
            observation_index = root / "shadow_observation_index_v0.json"
            pack_top = candidate_record(
                pack_id="pack_top",
                pack_path=str(root / "pack_top"),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
            )
            pack_low = candidate_record(
                pack_id="pack_low",
                pack_path=str(root / "pack_low"),
                decision_tier="PROMOTE",
            )
            write_candidate_state(state_dir, [pack_low, pack_top])
            recent_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            write_observation_index(
                observation_index,
                {
                    "pack_top": {
                        "selected_pack_id": "pack_top",
                        "last_observed_at": recent_ts,
                        "last_live_run_id": "run_top",
                        "last_verify_soft_live_pass": True,
                        "last_stop_reason": "STREAM_END",
                        "last_processed_event_count": 16,
                        "observation_count": 2,
                    }
                },
            )

            res = self._run(state_dir, observation_index=observation_index)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_top")
            self.assertEqual(rows[0]["observed_before"], "true")
            self.assertEqual(rows[0]["observation_count"], "2")
            self.assertEqual(rows[0]["last_observed_at"], recent_ts)
            self.assertEqual(rows[0]["last_verify_soft_live_pass"], "true")
            self.assertEqual(rows[0]["last_stop_reason"], "STREAM_END")
            self.assertEqual(rows[0]["last_processed_event_count"], "16")
            self.assertNotEqual(rows[0]["last_observation_age_hours"], "unknown")
            self.assertLess(float(rows[0]["last_observation_age_hours"]), 24.0)
            self.assertEqual(rows[0]["observation_recency_bucket"], "WITHIN_24H")
            self.assertEqual(rows[0]["observation_last_outcome_short"], "PASS(16)")
            self.assertEqual(rows[0]["observation_attention_flag"], "false")
            self.assertEqual(rows[0]["observation_status"], "OBSERVED_PASS")
            self.assertEqual(rows[0]["next_action_hint"], "ALREADY_OBSERVED_GOOD")
            self.assertEqual(rows[0]["reobserve_status"], "RECENTLY_OBSERVED")
            self.assertEqual(rows[1]["pack_id"], "pack_low")

    def test_no_matching_pack_in_history_produces_empty_enrichment(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_history_no_match_") as td:
            root = Path(td)
            state_dir = root / "state"
            observation_index = root / "shadow_observation_index_v0.json"
            record = candidate_record(pack_id="pack_a", pack_path=str(root / "pack_a"), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [record])
            write_observation_index(
                observation_index,
                {
                    "other_pack": {
                        "selected_pack_id": "other_pack",
                        "last_observed_at": "2026-03-07T07:24:58Z",
                        "last_live_run_id": "run_other",
                        "last_verify_soft_live_pass": True,
                        "last_stop_reason": "STREAM_END",
                        "last_processed_event_count": 9,
                        "observation_count": 1,
                    }
                },
            )

            res = self._run(state_dir, observation_index=observation_index)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_a")
            self.assertEqual(rows[0]["observed_before"], "false")
            self.assertEqual(rows[0]["observation_count"], "0")
            self.assertEqual(rows[0]["last_verify_soft_live_pass"], "unknown")
            self.assertEqual(rows[0]["last_observation_age_hours"], "unknown")
            self.assertEqual(rows[0]["observation_recency_bucket"], "NEVER_OBSERVED")
            self.assertEqual(rows[0]["observation_last_outcome_short"], "NO_HISTORY")
            self.assertEqual(rows[0]["observation_attention_flag"], "false")
            self.assertEqual(rows[0]["observation_status"], "NEW")
            self.assertEqual(rows[0]["next_action_hint"], "READY_TO_OBSERVE")
            self.assertEqual(rows[0]["reobserve_status"], "NOT_OBSERVED")
            self.assertEqual(rows[0]["recent_observation_trail"], "")

    def test_observed_pass_no_events_and_observed_fail_statuses(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_history_status_") as td:
            root = Path(td)
            state_dir = root / "state"
            observation_index = root / "shadow_observation_index_v0.json"
            pass_no_events = candidate_record(
                pack_id="pack_pass_no_events",
                pack_path=str(root / "pack_pass_no_events"),
                decision_tier="PROMOTE",
                max_rss_kb=80000.0,
                max_elapsed_sec=10.0,
            )
            fail_record = candidate_record(
                pack_id="pack_fail",
                pack_path=str(root / "pack_fail"),
                decision_tier="PROMOTE",
                max_rss_kb=90000.0,
                max_elapsed_sec=11.0,
            )
            write_candidate_state(state_dir, [pass_no_events, fail_record])
            stale_ts = (datetime.now(timezone.utc) - timedelta(hours=49)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            write_observation_index(
                observation_index,
                {
                    "pack_pass_no_events": {
                        "selected_pack_id": "pack_pass_no_events",
                        "last_observed_at": stale_ts,
                        "last_live_run_id": "run_pass_no_events",
                        "last_verify_soft_live_pass": True,
                        "last_stop_reason": "STREAM_END",
                        "last_processed_event_count": 0,
                        "observation_count": 1,
                    },
                    "pack_fail": {
                        "selected_pack_id": "pack_fail",
                        "last_observed_at": stale_ts,
                        "last_live_run_id": "run_fail",
                        "last_verify_soft_live_pass": False,
                        "last_stop_reason": "ERROR",
                        "last_processed_event_count": "unknown",
                        "observation_count": 3,
                    },
                    "pack_unknown": {
                        "selected_pack_id": "pack_unknown",
                        "last_observed_at": "bad-ts",
                        "last_live_run_id": "run_unknown",
                        "last_verify_soft_live_pass": "unknown",
                        "last_stop_reason": "",
                        "last_processed_event_count": "unknown",
                        "observation_count": 1,
                    },
                },
            )
            unknown_record = candidate_record(
                pack_id="pack_unknown",
                pack_path=str(root / "pack_unknown"),
                decision_tier="PROMOTE",
                max_rss_kb=85000.0,
                max_elapsed_sec=9.0,
            )
            write_candidate_state(state_dir, [pass_no_events, fail_record, unknown_record])
            res = self._run(state_dir, observation_index=observation_index)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = {row["pack_id"]: row for row in load_review_rows(state_dir / "candidate_review.tsv")}
            self.assertEqual(rows["pack_pass_no_events"]["observation_status"], "OBSERVED_PASS_NO_EVENTS")
            self.assertGreater(float(rows["pack_pass_no_events"]["last_observation_age_hours"]), 24.0)
            self.assertEqual(rows["pack_pass_no_events"]["observation_recency_bucket"], "WITHIN_7D")
            self.assertEqual(rows["pack_pass_no_events"]["observation_last_outcome_short"], "PASS_NO_EVENTS")
            self.assertEqual(rows["pack_pass_no_events"]["observation_attention_flag"], "false")
            self.assertEqual(rows["pack_pass_no_events"]["next_action_hint"], "REOBSERVE_CANDIDATE")
            self.assertEqual(rows["pack_pass_no_events"]["reobserve_status"], "STALE_OBSERVATION")
            self.assertEqual(rows["pack_fail"]["observation_status"], "OBSERVED_FAIL")
            self.assertGreater(float(rows["pack_fail"]["last_observation_age_hours"]), 24.0)
            self.assertEqual(rows["pack_fail"]["observation_recency_bucket"], "WITHIN_7D")
            self.assertEqual(rows["pack_fail"]["observation_last_outcome_short"], "FAIL")
            self.assertEqual(rows["pack_fail"]["observation_attention_flag"], "true")
            self.assertEqual(rows["pack_fail"]["next_action_hint"], "NEEDS_ATTENTION")
            self.assertEqual(rows["pack_fail"]["reobserve_status"], "STALE_OBSERVATION")
            self.assertEqual(rows["pack_unknown"]["observation_status"], "OBSERVED_UNKNOWN")
            self.assertEqual(rows["pack_unknown"]["last_observation_age_hours"], "unknown")
            self.assertEqual(rows["pack_unknown"]["observation_recency_bucket"], "UNKNOWN")
            self.assertEqual(rows["pack_unknown"]["observation_last_outcome_short"], "UNKNOWN")
            self.assertEqual(rows["pack_unknown"]["observation_attention_flag"], "true")
            self.assertEqual(rows["pack_unknown"]["next_action_hint"], "REVIEW_OBSERVATION_STATE")
            self.assertEqual(rows["pack_unknown"]["reobserve_status"], "OBSERVATION_TIME_UNKNOWN")

    def test_recent_observation_trail_handles_single_multiple_and_isolation(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_history_trail_") as td:
            root = Path(td)
            state_dir = root / "state"
            observation_history = root / "shadow_observation_history_v0.jsonl"
            pack_a = candidate_record(pack_id="pack_a", pack_path=str(root / "pack_a"), decision_tier="PROMOTE")
            pack_b = candidate_record(pack_id="pack_b", pack_path=str(root / "pack_b"), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [pack_a, pack_b])
            write_observation_history(
                observation_history,
                [
                    {
                        "schema_version": "shadow_observation_history_v0",
                        "observation_key": "pack_a|run_old",
                        "observed_at": "2026-03-05T10:00:00Z",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_old",
                        "verify_soft_live_pass": True,
                        "processed_event_count": 0,
                        "stop_reason": "STREAM_END",
                    },
                    {
                        "schema_version": "shadow_observation_history_v0",
                        "observation_key": "pack_a|run_mid",
                        "observed_at": "2026-03-06T10:00:00Z",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_mid",
                        "verify_soft_live_pass": False,
                        "processed_event_count": "unknown",
                        "stop_reason": "ERROR",
                    },
                    {
                        "schema_version": "shadow_observation_history_v0",
                        "observation_key": "pack_a|run_new",
                        "observed_at": "2026-03-07T10:00:00Z",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_new",
                        "verify_soft_live_pass": True,
                        "processed_event_count": 16,
                        "stop_reason": "STREAM_END",
                    },
                    {
                        "schema_version": "shadow_observation_history_v0",
                        "observation_key": "pack_a|run_ignored",
                        "observed_at": "2026-03-04T10:00:00Z",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_ignored",
                        "verify_soft_live_pass": True,
                        "processed_event_count": 9,
                        "stop_reason": "STREAM_END",
                    },
                    {
                        "schema_version": "shadow_observation_history_v0",
                        "observation_key": "pack_b|run_b",
                        "observed_at": "2026-03-07T09:00:00Z",
                        "selected_pack_id": "pack_b",
                        "live_run_id": "run_b",
                        "verify_soft_live_pass": True,
                        "processed_event_count": 5,
                        "stop_reason": "STREAM_END",
                    },
                ],
            )

            res = self._run(state_dir, observation_history=observation_history)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = {row["pack_id"]: row for row in load_review_rows(state_dir / "candidate_review.tsv")}
            self.assertEqual(
                rows["pack_a"]["recent_observation_trail"],
                "2026-03-07T10:00:00Z/PASS(16)/STREAM_END | 2026-03-06T10:00:00Z/FAIL/ERROR | 2026-03-05T10:00:00Z/PASS_NO_EVENTS/STREAM_END",
            )
            self.assertEqual(
                rows["pack_b"]["recent_observation_trail"],
                "2026-03-07T09:00:00Z/PASS(5)/STREAM_END",
            )

    def test_matching_execution_pack_summary_enriches_without_reordering(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_exec_match_") as td:
            root = Path(td)
            state_dir = root / "state"
            execution_pack_summary = root / "shadow_execution_pack_summary_v0.json"
            pack_top = candidate_record(
                pack_id="pack_top",
                pack_path=str(root / "pack_top"),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
            )
            pack_low = candidate_record(
                pack_id="pack_low",
                pack_path=str(root / "pack_low"),
                decision_tier="PROMOTE",
            )
            write_candidate_state(state_dir, [pack_low, pack_top])
            write_execution_pack_summary(
                execution_pack_summary,
                {
                    "pack_top": {
                        "selected_pack_id": "pack_top",
                        "last_pnl_state": "ACTIVE_POSITION",
                        "pnl_interpretation": "ACTIVE_LOSING",
                        "pnl_attention_flag": True,
                        "latest_realized_sign": "GAIN",
                        "latest_unrealized_sign": "LOSS",
                    }
                },
            )

            res = self._run(state_dir, execution_pack_summary=execution_pack_summary)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_top")
            self.assertEqual(rows[0]["last_pnl_state"], "ACTIVE_POSITION")
            self.assertEqual(rows[0]["pnl_interpretation"], "ACTIVE_LOSING")
            self.assertEqual(rows[0]["pnl_attention_flag"], "true")
            self.assertEqual(rows[0]["latest_realized_sign"], "GAIN")
            self.assertEqual(rows[0]["latest_unrealized_sign"], "LOSS")
            self.assertEqual(rows[1]["pack_id"], "pack_low")

    def test_missing_or_non_matching_execution_pack_summary_uses_unknown_fallbacks(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_exec_fallback_") as td:
            root = Path(td)
            state_dir = root / "state"
            execution_pack_summary = root / "shadow_execution_pack_summary_v0.json"
            record = candidate_record(pack_id="pack_a", pack_path=str(root / "pack_a"), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [record])
            write_execution_pack_summary(
                execution_pack_summary,
                {
                    "other_pack": {
                        "selected_pack_id": "other_pack",
                        "last_pnl_state": "REALIZED_GAIN",
                        "pnl_interpretation": "REALIZED_GAIN",
                        "pnl_attention_flag": False,
                        "latest_realized_sign": "GAIN",
                        "latest_unrealized_sign": "FLAT",
                    }
                },
            )

            res = self._run(state_dir, execution_pack_summary=execution_pack_summary)
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_a")
            self.assertEqual(rows[0]["last_pnl_state"], "UNKNOWN")
            self.assertEqual(rows[0]["pnl_interpretation"], "UNKNOWN")
            self.assertEqual(rows[0]["pnl_attention_flag"], "false")
            self.assertEqual(rows[0]["latest_realized_sign"], "UNKNOWN")
            self.assertEqual(rows[0]["latest_unrealized_sign"], "UNKNOWN")

            missing_res = self._run(state_dir, execution_pack_summary=root / "missing_pack_summary.json")
            self.assertEqual(missing_res.returncode, 0, msg=missing_res.stderr)
            missing_rows = load_review_rows(state_dir / "candidate_review.tsv")
            self.assertEqual(missing_rows[0]["last_pnl_state"], "UNKNOWN")
            self.assertEqual(missing_rows[0]["pnl_interpretation"], "UNKNOWN")
            self.assertEqual(missing_rows[0]["pnl_attention_flag"], "false")
            self.assertEqual(missing_rows[0]["latest_realized_sign"], "UNKNOWN")
            self.assertEqual(missing_rows[0]["latest_unrealized_sign"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
