import json
import tempfile
import unittest
import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-directional-expectancy-campaign-v0.py"
SPEC = importlib.util.spec_from_file_location("run_directional_expectancy_campaign_v0", SCRIPT)
campaign_module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(campaign_module)


def make_binding_item(
    *,
    strategy_id: str,
    pack_id: str,
    family_id: str,
    symbol: str,
    review_priority: int = 3,
    score: float = 70.0,
    decision_tier: str = "PROMOTE",
    selected_cell: dict | None = None,
) -> dict:
    return {
        "strategy_id": strategy_id,
        "pack_id": pack_id,
        "pack_path": f"/tmp/{pack_id}",
        "family_id": family_id,
        "selected_symbol": symbol,
        "exchange": "binance",
        "stream": "trade",
        "symbols": [symbol],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "shadow_tradeability_class": "DIRECTIONAL",
        "binding_mode": "PAPER_DIRECTIONAL_V1",
        "decision_tier": decision_tier,
        "source_review_class_priority": review_priority,
        "source_review_class": "UNSEEN",
        "source_review_score": score,
        "runtime_strategy_file": "core/strategy/strategies/MomentumV1Strategy.js",
        "runtime_strategy_config": {
            "binding_mode": "PAPER_DIRECTIONAL_V1",
            "source_decision_tier": decision_tier,
            "selected_cell": selected_cell or {},
        },
    }


class RunDirectionalExpectancyCampaignV0Tests(unittest.TestCase):
    def test_classify_row_contract(self):
        row = {
            "completed_horizon_sec": 86400,
            "fills_count": 4,
            "closed_cycle_count": 2,
            "net_pnl": 1.0,
            "net_pnl_bps_turnover": 3.0,
        }
        self.assertEqual(campaign_module.classify_row(row)[0], "PROMISING")
        row["net_pnl"] = -0.1
        row["net_pnl_bps_turnover"] = -1.5
        self.assertEqual(campaign_module.classify_row(row)[0], "NEUTRAL")
        row["net_pnl_bps_turnover"] = -3.0
        self.assertEqual(campaign_module.classify_row(row)[0], "WEAK")
        row["fills_count"] = 0
        self.assertEqual(campaign_module.classify_row(row)[0], "NO_SIGNAL")
        row["fills_count"] = 1
        row["closed_cycle_count"] = 0
        self.assertEqual(campaign_module.classify_row(row)[0], "INSUFFICIENT_EVIDENCE")
        row["completed_horizon_sec"] = 100
        self.assertEqual(campaign_module.classify_row(row)[0], "BROKEN")

    def test_prepare_selection_respects_per_family_quota(self):
        binding_payload = {
            "items": [
                make_binding_item(
                    strategy_id=f"candidate_strategy::momentum_v1::pack_m{i}::sym{i}",
                    pack_id=f"pack_m{i}",
                    family_id="momentum_v1",
                    symbol=f"sym{i}",
                    selected_cell={"mean_product": 0.5 - i * 0.01, "t_stat": 5 + i, "event_count": 1000 + i},
                )
                for i in range(1, 6)
            ] + [
                make_binding_item(
                    strategy_id=f"candidate_strategy::return_reversal_v1::pack_r{i}::rsym{i}",
                    pack_id=f"pack_r{i}",
                    family_id="return_reversal_v1",
                    symbol=f"rsym{i}",
                    selected_cell={"mean_product": -0.5 + i * 0.01, "t_stat": -5 - i, "event_count": 900 + i},
                )
                for i in range(1, 6)
            ] + [
                make_binding_item(
                    strategy_id=f"candidate_strategy::jump_reversion_v1::pack_j{i}::jsym{i}",
                    pack_id=f"pack_j{i}",
                    family_id="jump_reversion_v1",
                    symbol=f"jsym{i}",
                    selected_cell={"mean_signed_reversal": 0.3 - i * 0.01, "t_stat": 4 + i, "jump_count": 250 + i},
                )
                for i in range(1, 6)
            ] + [
                make_binding_item(
                    strategy_id=f"candidate_strategy::family_b_simple_momentum::pack_b{i}::bsym{i}",
                    pack_id=f"pack_b{i}",
                    family_id="family_b_simple_momentum",
                    symbol=f"bsym{i}",
                    selected_cell={"mean_forward_return": 0.01 - i * 0.001, "t_stat": 3 + i, "signal_support": 400 + i},
                )
                for i in range(1, 6)
            ]
        }
        baseline_results = {
            "items": [
                {"strategy_id": "candidate_strategy::momentum_v1::pack_m1::sym1", "net_pnl": -5, "turnover": 1000, "prior_2h_classification": "WEAK", "fills_count": 4},
                {"strategy_id": "candidate_strategy::momentum_v1::pack_m2::sym2", "net_pnl": -1, "turnover": 1000, "prior_2h_classification": "NO_SIGNAL", "fills_count": 3},
                {"strategy_id": "candidate_strategy::momentum_v1::pack_m3::sym3", "net_pnl": -2, "turnover": 1000, "prior_2h_classification": "NEUTRAL", "fills_count": 5},
            ]
        }

        rows = campaign_module.prepare_selection(binding_payload, baseline_results, 3)
        counts = {}
        for row in rows:
            counts[row["family_id"]] = counts.get(row["family_id"], 0) + 1
        self.assertEqual(counts["momentum_v1"], 3)
        self.assertEqual(counts["return_reversal_v1"], 3)
        self.assertEqual(counts["jump_reversion_v1"], 3)
        self.assertEqual(counts["family_b_simple_momentum"], 3)

    def test_hourly_report_sends_once_per_hour(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_telegram_") as td:
            path = Path(td) / "telegram_reports.jsonl"
            status = {
                "elapsed_sec": 3700,
                "completed_rows": 1,
                "total_rows": 4,
                "active_rows": 3,
                "failed_rows": 0,
                "family_activity_summary_so_far": "momentum_v1:1fill/0done",
                "rows_with_fills_so_far": ["btcusdt"],
                "aggregate_fills_so_far": 2,
                "hourly_reports_attempted": 0,
                "hourly_reports_sent": 0,
                "sent_hours": [],
            }
            result = campaign_module.maybe_send_hourly_report(
                status,
                campaign_id="campaign_x",
                telegram_reports_jsonl=path,
                telegram_api_base_url="https://api.telegram.org",
                telegram_dry_run=True,
            )
            self.assertIsNotNone(result)
            self.assertEqual(status["hourly_reports_sent"], 1)
            self.assertEqual(status["sent_hours"], [1])

            result_again = campaign_module.maybe_send_hourly_report(
                status,
                campaign_id="campaign_x",
                telegram_reports_jsonl=path,
                telegram_api_base_url="https://api.telegram.org",
                telegram_dry_run=True,
            )
            self.assertIsNone(result_again)
            self.assertEqual(status["hourly_reports_sent"], 1)

    def test_build_row_result_uses_fractional_summary_timestamps_for_completed_horizon(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_row_result_") as td:
            root = Path(td)
            artifact = root / "run01"
            shadow_state = artifact / "shadow_state_local"
            shadow_state.mkdir(parents=True, exist_ok=True)
            (shadow_state / "shadow_execution_events_v1.jsonl").write_text("", encoding="utf-8")
            (artifact / "summary_capture.json").write_text(
                json.dumps(
                    {
                        "started_at": "2026-03-17T17:39:23.579Z",
                        "finished_at": "2026-03-18T17:39:28.973Z",
                        "stop_reason": "STREAM_END",
                        "execution_summary": {
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "positions": {},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (artifact / "batch_result.json").write_text(
                json.dumps({"generated_ts_utc": "2026-03-18T17:39:43Z"}),
                encoding="utf-8",
            )
            selection_row = {
                "run_label": "run01",
                "strategy_id": "candidate_strategy::momentum_v1::pack::btcusdt",
                "pack_id": "pack",
                "family_id": "momentum_v1",
                "symbol": "btcusdt",
                "exchange": "binance",
                "binding_mode": "PAPER_DIRECTIONAL_V1",
            }
            manifest_item = {
                "artifact_path": str(artifact),
                "summary_json_path": str(artifact / "summary_capture.json"),
                "batch_result_json_path": str(artifact / "batch_result.json"),
                "shadow_state_dir": str(shadow_state),
                "launched_status": "BACKGROUND_LAUNCHED",
                "launch_started_ts_utc": "2026-03-17T17:39:21Z",
            }

            row = campaign_module.build_row_result(selection_row, manifest_item)

            self.assertGreaterEqual(row["completed_horizon_sec"], 86400)
            self.assertEqual(row["classification"], "NO_SIGNAL")
            self.assertEqual(row["classification_reason"], "no_fills_observed")

    def test_build_row_result_falls_back_to_manifest_and_batch_timestamps(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_row_result_fallback_") as td:
            root = Path(td)
            artifact = root / "run01"
            shadow_state = artifact / "shadow_state_local"
            shadow_state.mkdir(parents=True, exist_ok=True)
            (shadow_state / "shadow_execution_events_v1.jsonl").write_text("", encoding="utf-8")
            (artifact / "summary_capture.json").write_text(
                json.dumps(
                    {
                        "stop_reason": "STREAM_END",
                        "execution_summary": {
                            "total_realized_pnl": 0.0,
                            "total_unrealized_pnl": 0.0,
                            "positions": {},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (artifact / "batch_result.json").write_text(
                json.dumps({"generated_ts_utc": "2026-03-18T17:39:43Z"}),
                encoding="utf-8",
            )
            selection_row = {
                "run_label": "run01",
                "strategy_id": "candidate_strategy::momentum_v1::pack::btcusdt",
                "pack_id": "pack",
                "family_id": "momentum_v1",
                "symbol": "btcusdt",
                "exchange": "binance",
                "binding_mode": "PAPER_DIRECTIONAL_V1",
            }
            manifest_item = {
                "artifact_path": str(artifact),
                "summary_json_path": str(artifact / "summary_capture.json"),
                "batch_result_json_path": str(artifact / "batch_result.json"),
                "shadow_state_dir": str(shadow_state),
                "launched_status": "BACKGROUND_LAUNCHED",
                "launch_started_ts_utc": "2026-03-17T17:39:21Z",
            }

            row = campaign_module.build_row_result(selection_row, manifest_item)

            self.assertGreaterEqual(row["completed_horizon_sec"], 86400)
            self.assertEqual(row["classification"], "NO_SIGNAL")


if __name__ == "__main__":
    unittest.main()
