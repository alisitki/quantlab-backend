import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_candidate_review_v2.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def candidate_record(
    *,
    pack_id: str,
    pack_path: str,
    decision_tier: str,
    det_pass: int = 5,
    det_supported: int = 5,
    det_skipped: int = 0,
    max_rss_kb: float = 100000.0,
    max_elapsed_sec: float = 30.0,
    guards: dict | None = None,
    export_ts_utc: str = "2026-03-16T09:15:42Z",
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
    with (state_dir / "candidate_report.tsv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
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


def write_context_guard_report(pack_path: Path, *, mark: str = "PASS", funding: str = "PASS", oi: str = "PASS") -> None:
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


def contract_item(
    *,
    pack_id: str,
    pack_path: str,
    family_id: str,
    translation_status: str = "TRANSLATABLE",
    reject_reason: str = "",
) -> dict:
    return {
        "contract_row_id": f"contract::{pack_id}::btcusdt",
        "rank": 1,
        "pack_id": pack_id,
        "source_pack_id": pack_id,
        "pack_path": pack_path,
        "decision_tier": "PROMOTE",
        "selected_symbol": "btcusdt",
        "selected_symbol_index": 0,
        "selected_family_id": family_id,
        "selected_family_report_path": f"{pack_path}/runs/btcusdt/artifacts/multi_hypothesis/family_{family_id}_report.json",
        "translation_status": translation_status,
        "reject_reason": reject_reason,
        "strategy_spec": {
            "strategy_id": f"candidate_strategy::{family_id}::{pack_id}::btcusdt",
            "family_id": family_id,
            "exchange": "binance",
            "stream": "trade",
            "symbols": ["btcusdt"],
        },
    }


def binding_item(
    *,
    pack_id: str,
    family_id: str,
    runtime_binding_status: str,
    shadow_tradeability_class: str,
    binding_mode: str | None,
    binding_reason: str = "",
) -> dict:
    return {
        "rank": 1,
        "pack_id": pack_id,
        "source_pack_id": pack_id,
        "contract_row_id": f"contract::{pack_id}::btcusdt",
        "selected_symbol": "btcusdt",
        "translation_status": "TRANSLATABLE",
        "strategy_id": f"candidate_strategy::{family_id}::{pack_id}::btcusdt",
        "family_id": family_id,
        "exchange": "binance",
        "stream": "trade",
        "symbols": ["btcusdt"],
        "runtime_binding_status": runtime_binding_status,
        "runtime_strategy_file": None,
        "runtime_strategy_config": {"binding_mode": binding_mode} if binding_mode else None,
        "binding_mode": binding_mode,
        "shadow_tradeability_class": shadow_tradeability_class,
        "binding_reason": binding_reason,
    }


def family_role_item(*, family_id: str, role: str) -> dict:
    return {
        "family_id": family_id,
        "role": role,
        "runtime_bindable_now": role == "PRIMARY_DIRECTIONAL",
        "support_only_now": role == "CONTEXT_GUARD",
    }


def futures_row(
    *,
    pack_id: str,
    observed_at: str,
    live_run_id: str,
    fill_event_count: int,
    paper_run_status: str,
    profitability_status: str,
) -> dict:
    return {
        "selected_pack_id": pack_id,
        "observed_at": observed_at,
        "live_run_id": live_run_id,
        "fill_event_count": fill_event_count,
        "paper_run_status": paper_run_status,
        "profitability_status": profitability_status,
    }


def trade_row(
    *,
    pack_id: str,
    trade_id: str,
    closed_at: str,
    realized_pnl_delta: float,
    status: str = "CLOSED",
) -> dict:
    return {
        "schema_version": "shadow_trade_ledger_v1",
        "selected_pack_id": pack_id,
        "trade_id": trade_id,
        "status": status,
        "closed_at": closed_at,
        "realized_pnl_delta": realized_pnl_delta,
    }


def pack_summary_row(
    *,
    observed_at: str,
    pnl_interpretation: str,
    recent_pnl_bias: str,
    pnl_attention_flag: str = "false",
) -> dict:
    return {
        "last_observed_at": observed_at,
        "pnl_interpretation": pnl_interpretation,
        "recent_pnl_bias": recent_pnl_bias,
        "pnl_attention_flag": pnl_attention_flag,
    }


def outcome_item(
    *,
    pack_id: str,
    outcome_class: str,
    outcome_attention_flag: str = "false",
    last_observed_at: str = "2026-03-16T06:29:12Z",
) -> dict:
    return {
        "selected_pack_id": pack_id,
        "last_observed_at": last_observed_at,
        "outcome_class": outcome_class,
        "outcome_attention_flag": outcome_attention_flag,
    }


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


class CandidateReviewV2Tests(unittest.TestCase):
    def _run(
        self,
        *,
        state_dir: Path,
        contract_json: Path,
        binding_json: Path,
        family_role_json: Path,
        watchlist_json: Path,
        pack_summary_json: Path,
        outcome_review_json: Path,
        trade_ledger_jsonl: Path,
        futures_paper_ledger_json: Path,
        candidate_review_v0_tsv: Path,
    ):
        return subprocess.run(
            [
                "python3",
                str(SCRIPT),
                "--state-dir",
                str(state_dir),
                "--candidate-review-v0-tsv",
                str(candidate_review_v0_tsv),
                "--strategy-contract-json",
                str(contract_json),
                "--runtime-binding-json",
                str(binding_json),
                "--family-role-json",
                str(family_role_json),
                "--watchlist-json",
                str(watchlist_json),
                "--execution-pack-summary-json",
                str(pack_summary_json),
                "--execution-outcome-review-json",
                str(outcome_review_json),
                "--trade-ledger-jsonl",
                str(trade_ledger_jsonl),
                "--futures-paper-ledger-json",
                str(futures_paper_ledger_json),
            ],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def _write_optional_artifacts(
        self,
        root: Path,
        *,
        contract_items: list[dict],
        binding_items: list[dict],
        family_role_items: list[dict],
        watchlist_items: list[dict] | None = None,
        pack_summary_latest: dict | None = None,
        outcome_items: list[dict] | None = None,
        futures_items: list[dict] | None = None,
        trade_items: list[dict] | None = None,
    ) -> dict[str, Path]:
        contract_json = root / "contract.json"
        binding_json = root / "binding.json"
        family_role_json = root / "family_roles.json"
        watchlist_json = root / "watchlist.json"
        pack_summary_json = root / "pack_summary.json"
        outcome_review_json = root / "outcome_review.json"
        trade_ledger_jsonl = root / "trade_ledger.jsonl"
        futures_paper_ledger_json = root / "futures_ledger.json"
        missing_v0_tsv = root / "missing_candidate_review.tsv"

        write_json(contract_json, {"schema_version": "candidate_strategy_contract_v0", "items": contract_items})
        write_json(binding_json, {"schema_version": "candidate_strategy_runtime_binding_v0", "items": binding_items})
        write_json(family_role_json, {"schema_version": "hypothesis_family_role_classification_v0", "items": family_role_items})
        write_json(watchlist_json, {"schema_version": "shadow_watchlist_v0", "items": watchlist_items or []})
        write_json(
            pack_summary_json,
            {
                "schema_version": "shadow_execution_pack_summary_v0",
                "latest_by_pack_id": pack_summary_latest or {},
            },
        )
        write_json(
            outcome_review_json,
            {
                "schema_version": "shadow_execution_outcome_review_v0",
                "items": outcome_items or [],
            },
        )
        write_json(
            futures_paper_ledger_json,
            {
                "schema_version": "shadow_futures_paper_ledger_v1",
                "items": futures_items or [],
            },
        )
        write_jsonl(trade_ledger_jsonl, trade_items or [])

        return {
            "contract_json": contract_json,
            "binding_json": binding_json,
            "family_role_json": family_role_json,
            "watchlist_json": watchlist_json,
            "pack_summary_json": pack_summary_json,
            "outcome_review_json": outcome_review_json,
            "trade_ledger_jsonl": trade_ledger_jsonl,
            "futures_paper_ledger_json": futures_paper_ledger_json,
            "candidate_review_v0_tsv": missing_v0_tsv,
        }

    def test_high_research_observe_only_cannot_outrank_runnable_directional(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_v2_observe_only_") as td:
            root = Path(td)
            state_dir = root / "state"
            pack_obs = root / "pack_obs"
            pack_dir = root / "pack_dir"
            write_context_guard_report(pack_obs)
            write_context_guard_report(pack_dir)
            observe_only = candidate_record(
                pack_id="pack_obs",
                pack_path=str(pack_obs),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
                max_rss_kb=80000.0,
                max_elapsed_sec=15.0,
            )
            directional = candidate_record(
                pack_id="pack_dir",
                pack_path=str(pack_dir),
                decision_tier="PROMOTE",
                det_pass=5,
                det_supported=5,
                max_rss_kb=100000.0,
                max_elapsed_sec=30.0,
            )
            write_candidate_state(state_dir, [observe_only, directional])
            paths = self._write_optional_artifacts(
                root,
                contract_items=[
                    contract_item(pack_id="pack_obs", pack_path=str(pack_obs), family_id="spread_reversion_v1"),
                    contract_item(pack_id="pack_dir", pack_path=str(pack_dir), family_id="momentum_v1"),
                ],
                binding_items=[
                    binding_item(
                        pack_id="pack_obs",
                        family_id="spread_reversion_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="OBSERVE_ONLY",
                        binding_mode="OBSERVE_ONLY",
                    ),
                    binding_item(
                        pack_id="pack_dir",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                ],
                family_role_items=[
                    family_role_item(family_id="spread_reversion_v1", role="CONTEXT_GUARD"),
                    family_role_item(family_id="momentum_v1", role="PRIMARY_DIRECTIONAL"),
                ],
            )
            result = self._run(state_dir=state_dir, **paths)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            rows = load_rows(state_dir / "candidate_review_v2.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_dir")
            self.assertEqual(rows[0]["review_class"], "UNSEEN")
            self.assertEqual(rows[1]["pack_id"], "pack_obs")
            self.assertEqual(rows[1]["review_class"], "OBSERVE_ONLY")

    def test_shadow_weak_cannot_stay_top_on_research_score_alone(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_v2_weak_") as td:
            root = Path(td)
            state_dir = root / "state"
            pack_weak = root / "pack_weak"
            pack_neutral = root / "pack_neutral"
            write_context_guard_report(pack_weak)
            write_context_guard_report(pack_neutral)
            weak = candidate_record(
                pack_id="pack_weak",
                pack_path=str(pack_weak),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
                max_rss_kb=75000.0,
                max_elapsed_sec=12.0,
            )
            neutral = candidate_record(
                pack_id="pack_neutral",
                pack_path=str(pack_neutral),
                decision_tier="PROMOTE",
                det_pass=5,
                det_supported=5,
                max_rss_kb=105000.0,
                max_elapsed_sec=28.0,
            )
            write_candidate_state(state_dir, [weak, neutral])
            paths = self._write_optional_artifacts(
                root,
                contract_items=[
                    contract_item(pack_id="pack_weak", pack_path=str(pack_weak), family_id="momentum_v1"),
                    contract_item(pack_id="pack_neutral", pack_path=str(pack_neutral), family_id="momentum_v1"),
                ],
                binding_items=[
                    binding_item(
                        pack_id="pack_weak",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                    binding_item(
                        pack_id="pack_neutral",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                ],
                family_role_items=[family_role_item(family_id="momentum_v1", role="PRIMARY_DIRECTIONAL")],
                pack_summary_latest={
                    "pack_weak": pack_summary_row(
                        observed_at="2026-03-16T06:00:00Z",
                        pnl_interpretation="REALIZED_LOSS",
                        recent_pnl_bias="FLAT_BIAS",
                        pnl_attention_flag="true",
                    ),
                    "pack_neutral": pack_summary_row(
                        observed_at="2026-03-16T06:10:00Z",
                        pnl_interpretation="FLAT_NO_FILLS",
                        recent_pnl_bias="FLAT_BIAS",
                    ),
                },
                outcome_items=[
                    outcome_item(pack_id="pack_weak", outcome_class="MIXED_RECENT", outcome_attention_flag="true"),
                    outcome_item(pack_id="pack_neutral", outcome_class="STABLE_FLAT"),
                ],
                futures_items=[
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T05:50:00Z",
                        live_run_id="run_1",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T05:55:00Z",
                        live_run_id="run_2",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T06:00:00Z",
                        live_run_id="run_3",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_FLAT",
                        profitability_status="NET_AFTER_FEES_AND_FUNDING",
                    ),
                    futures_row(
                        pack_id="pack_neutral",
                        observed_at="2026-03-16T05:50:00Z",
                        live_run_id="run_a",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_neutral",
                        observed_at="2026-03-16T05:55:00Z",
                        live_run_id="run_b",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_neutral",
                        observed_at="2026-03-16T06:00:00Z",
                        live_run_id="run_c",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                ],
                trade_items=[
                    trade_row(
                        pack_id="pack_weak",
                        trade_id="pack_weak|1",
                        closed_at="2026-03-16T06:01:00Z",
                        realized_pnl_delta=-1.0,
                    ),
                    trade_row(
                        pack_id="pack_weak",
                        trade_id="pack_weak|2",
                        closed_at="2026-03-16T06:02:00Z",
                        realized_pnl_delta=-0.5,
                    ),
                ],
            )
            result = self._run(state_dir=state_dir, **paths)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            rows = load_rows(state_dir / "candidate_review_v2.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_neutral")
            self.assertEqual(rows[0]["review_class"], "NEUTRAL")
            weak_row = next(row for row in rows if row["pack_id"] == "pack_weak")
            self.assertEqual(weak_row["review_class"], "WEAK")
            self.assertGreater(int(weak_row["rank"]), int(rows[0]["rank"]))

    def test_bindable_directional_with_promising_shadow_surfaces_correctly(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_v2_promising_") as td:
            root = Path(td)
            state_dir = root / "state"
            pack_promising = root / "pack_promising"
            pack_unseen = root / "pack_unseen"
            write_context_guard_report(pack_promising)
            write_context_guard_report(pack_unseen)
            promising = candidate_record(
                pack_id="pack_promising",
                pack_path=str(pack_promising),
                decision_tier="PROMOTE",
                det_pass=5,
                det_supported=5,
            )
            unseen = candidate_record(
                pack_id="pack_unseen",
                pack_path=str(pack_unseen),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
            )
            write_candidate_state(state_dir, [promising, unseen])
            paths = self._write_optional_artifacts(
                root,
                contract_items=[
                    contract_item(pack_id="pack_promising", pack_path=str(pack_promising), family_id="momentum_v1"),
                    contract_item(pack_id="pack_unseen", pack_path=str(pack_unseen), family_id="momentum_v1"),
                ],
                binding_items=[
                    binding_item(
                        pack_id="pack_promising",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                    binding_item(
                        pack_id="pack_unseen",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                ],
                family_role_items=[family_role_item(family_id="momentum_v1", role="PRIMARY_DIRECTIONAL")],
                pack_summary_latest={
                    "pack_promising": pack_summary_row(
                        observed_at="2026-03-16T06:15:00Z",
                        pnl_interpretation="REALIZED_GAIN",
                        recent_pnl_bias="GAIN_BIAS",
                    )
                },
                outcome_items=[outcome_item(pack_id="pack_promising", outcome_class="STABLE_GAINING")],
                futures_items=[
                    futures_row(
                        pack_id="pack_promising",
                        observed_at="2026-03-16T06:00:00Z",
                        live_run_id="run_1",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_promising",
                        observed_at="2026-03-16T06:05:00Z",
                        live_run_id="run_2",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_promising",
                        observed_at="2026-03-16T06:10:00Z",
                        live_run_id="run_3",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_FLAT",
                        profitability_status="NET_AFTER_FEES_AND_FUNDING",
                    ),
                ],
                trade_items=[
                    trade_row(
                        pack_id="pack_promising",
                        trade_id="pack_promising|1",
                        closed_at="2026-03-16T06:11:00Z",
                        realized_pnl_delta=1.25,
                    ),
                    trade_row(
                        pack_id="pack_promising",
                        trade_id="pack_promising|2",
                        closed_at="2026-03-16T06:12:00Z",
                        realized_pnl_delta=0.75,
                    ),
                ],
            )
            result = self._run(state_dir=state_dir, **paths)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            rows = load_rows(state_dir / "candidate_review_v2.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_promising")
            self.assertEqual(rows[0]["review_class"], "PROMISING")
            unseen_row = next(row for row in rows if row["pack_id"] == "pack_unseen")
            self.assertEqual(unseen_row["review_class"], "UNSEEN")

    def test_no_evidence_is_not_the_same_as_weak(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_v2_unseen_vs_weak_") as td:
            root = Path(td)
            state_dir = root / "state"
            pack_unseen = root / "pack_unseen"
            pack_weak = root / "pack_weak"
            write_context_guard_report(pack_unseen)
            write_context_guard_report(pack_weak)
            unseen = candidate_record(pack_id="pack_unseen", pack_path=str(pack_unseen), decision_tier="PROMOTE")
            weak = candidate_record(pack_id="pack_weak", pack_path=str(pack_weak), decision_tier="PROMOTE")
            write_candidate_state(state_dir, [unseen, weak])
            paths = self._write_optional_artifacts(
                root,
                contract_items=[
                    contract_item(pack_id="pack_unseen", pack_path=str(pack_unseen), family_id="momentum_v1"),
                    contract_item(pack_id="pack_weak", pack_path=str(pack_weak), family_id="momentum_v1"),
                ],
                binding_items=[
                    binding_item(
                        pack_id="pack_unseen",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                    binding_item(
                        pack_id="pack_weak",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                ],
                family_role_items=[family_role_item(family_id="momentum_v1", role="PRIMARY_DIRECTIONAL")],
                pack_summary_latest={
                    "pack_weak": pack_summary_row(
                        observed_at="2026-03-16T06:00:00Z",
                        pnl_interpretation="REALIZED_LOSS",
                        recent_pnl_bias="LOSS_BIAS",
                        pnl_attention_flag="true",
                    )
                },
                outcome_items=[outcome_item(pack_id="pack_weak", outcome_class="STABLE_LOSING", outcome_attention_flag="true")],
                futures_items=[
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T05:50:00Z",
                        live_run_id="run_1",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T05:55:00Z",
                        live_run_id="run_2",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_POSITION_OPEN",
                        profitability_status="NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    ),
                    futures_row(
                        pack_id="pack_weak",
                        observed_at="2026-03-16T06:00:00Z",
                        live_run_id="run_3",
                        fill_event_count=1,
                        paper_run_status="FILL_BACKED_FLAT",
                        profitability_status="NET_AFTER_FEES_AND_FUNDING",
                    ),
                ],
                trade_items=[
                    trade_row(
                        pack_id="pack_weak",
                        trade_id="pack_weak|1",
                        closed_at="2026-03-16T06:01:00Z",
                        realized_pnl_delta=-0.5,
                    ),
                    trade_row(
                        pack_id="pack_weak",
                        trade_id="pack_weak|2",
                        closed_at="2026-03-16T06:02:00Z",
                        realized_pnl_delta=-0.5,
                    ),
                ],
            )
            result = self._run(state_dir=state_dir, **paths)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            rows = {row["pack_id"]: row for row in load_rows(state_dir / "candidate_review_v2.tsv")}
            self.assertEqual(rows["pack_unseen"]["review_class"], "UNSEEN")
            self.assertEqual(rows["pack_weak"]["review_class"], "WEAK")
            self.assertNotEqual(rows["pack_unseen"]["review_class"], rows["pack_weak"]["review_class"])

    def test_unrunnable_cannot_outrank_runnable_directional(self):
        with tempfile.TemporaryDirectory(prefix="candidate_review_v2_unrunnable_") as td:
            root = Path(td)
            state_dir = root / "state"
            pack_unrun = root / "pack_unrun"
            pack_dir = root / "pack_dir"
            write_context_guard_report(pack_unrun)
            write_context_guard_report(pack_dir)
            unrunnable = candidate_record(
                pack_id="pack_unrun",
                pack_path=str(pack_unrun),
                decision_tier="PROMOTE_STRONG",
                det_pass=10,
                det_supported=10,
            )
            directional = candidate_record(
                pack_id="pack_dir",
                pack_path=str(pack_dir),
                decision_tier="PROMOTE",
                det_pass=5,
                det_supported=5,
            )
            write_candidate_state(state_dir, [unrunnable, directional])
            paths = self._write_optional_artifacts(
                root,
                contract_items=[
                    contract_item(pack_id="pack_unrun", pack_path=str(pack_unrun), family_id="momentum_v1"),
                    contract_item(pack_id="pack_dir", pack_path=str(pack_dir), family_id="momentum_v1"),
                ],
                binding_items=[
                    binding_item(
                        pack_id="pack_unrun",
                        family_id="momentum_v1",
                        runtime_binding_status="UNBOUND_NO_RUNTIME_IMPL",
                        shadow_tradeability_class="UNBOUND",
                        binding_mode=None,
                        binding_reason="NO_RUNTIME_IMPL",
                    ),
                    binding_item(
                        pack_id="pack_dir",
                        family_id="momentum_v1",
                        runtime_binding_status="BOUND_SHADOW_RUNNABLE",
                        shadow_tradeability_class="DIRECTIONAL",
                        binding_mode="PAPER_DIRECTIONAL_V1",
                    ),
                ],
                family_role_items=[family_role_item(family_id="momentum_v1", role="PRIMARY_DIRECTIONAL")],
            )
            result = self._run(state_dir=state_dir, **paths)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            rows = load_rows(state_dir / "candidate_review_v2.tsv")
            self.assertEqual(rows[0]["pack_id"], "pack_dir")
            self.assertEqual(rows[0]["review_class"], "UNSEEN")
            self.assertEqual(rows[1]["pack_id"], "pack_unrun")
            self.assertEqual(rows[1]["review_class"], "UNRUNNABLE")


if __name__ == "__main__":
    unittest.main()
