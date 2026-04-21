import json
import tempfile
import unittest
import importlib.util
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-momentum-normalized-subset-confirmation-v0.py"
SPEC = importlib.util.spec_from_file_location("run_momentum_normalized_subset_confirmation_v0", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(module)


class RunMomentumNormalizedSubsetConfirmationV0Tests(unittest.TestCase):
    def test_normalized_qty_for_price_rounds_down_and_respects_min_qty(self):
        qty = module.normalized_qty_for_price(
            676.78,
            target_quote_notional=10.275,
            qty_round_decimals=8,
            min_order_qty=1e-8,
        )
        self.assertEqual(qty, 0.01518218)
        tiny_qty = module.normalized_qty_for_price(
            1e12,
            target_quote_notional=10.275,
            qty_round_decimals=8,
            min_order_qty=1e-8,
        )
        self.assertEqual(tiny_qty, 1e-8)

    def test_simulate_normalized_row_rebuilds_promising_result(self):
        with tempfile.TemporaryDirectory(prefix="momentum_normalized_confirmation_") as td:
            root = Path(td)
            artifact = root / "source_row"
            shadow_state = artifact / "shadow_state_local"
            shadow_state.mkdir(parents=True, exist_ok=True)
            (artifact / "summary_capture.json").write_text(
                json.dumps(
                    {
                        "started_at": "2026-03-17T00:00:00.000Z",
                        "finished_at": "2026-03-18T00:00:05.000Z",
                        "stop_reason": "STREAM_END",
                    }
                ),
                encoding="utf-8",
            )
            (shadow_state / "shadow_futures_paper_ledger_v1.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "effective_fee_rate": 0.0004,
                                "final_mark_price": 103.0,
                                "episodes": [
                                    {
                                        "episode_id": "ep1",
                                        "direction": "LONG",
                                        "status": "CLOSED",
                                        "entry_price": 100.0,
                                        "exit_price": 101.0,
                                    },
                                    {
                                        "episode_id": "ep2",
                                        "direction": "LONG",
                                        "status": "CLOSED",
                                        "entry_price": 102.0,
                                        "exit_price": 103.0,
                                    },
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            source_row = {
                "strategy_id": "candidate_strategy::momentum_v1::pack::btcusdt",
                "symbol": "btcusdt",
                "artifact_path": str(artifact),
                "binding_mode": "PAPER_DIRECTIONAL_V1",
                "exchange": "binance",
                "family_id": "momentum_v1",
                "pack_id": "pack",
                "fills_count": 4,
                "opens_count": 2,
                "exits_count": 2,
                "reversals_count": 0,
                "closed_cycle_count": 2,
                "risk_reject_summary": {"risk_reject_event_count": 0},
                "stop_reason": "STREAM_END",
                "paper_run_status": "FLAT_RUN",
                "profitability_status": "NET_AFTER_FEES_AND_FUNDING",
                "classification": "PROMISING",
                "net_pnl": 1.0,
                "fees": 0.1,
                "turnover": 10.0,
                "net_pnl_bps_turnover": 1000.0,
            }
            row = module.simulate_normalized_row(
                source_row,
                target_quote_notional=10.0,
                qty_round_decimals=8,
                min_order_qty=1e-8,
                row_output_dir=root / "row_result",
            )

            self.assertGreaterEqual(row["completed_horizon_sec"], 86400)
            self.assertEqual(row["classification"], "PROMISING")
            self.assertEqual(row["classification_reason"], "positive_closed_cycle_result_above_2bps")
            self.assertAlmostEqual(row["turnover"], 40.19803805, places=8)
            self.assertEqual(row["fills_count"], 4)
            self.assertEqual(row["closed_cycle_count"], 2)


if __name__ == "__main__":
    unittest.main()
