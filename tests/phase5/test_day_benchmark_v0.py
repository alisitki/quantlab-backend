import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.phase5_big_hunt_plan_v2 import parse_args as parse_plan_args
from tools.phase5_day_benchmark_v0 import build_summary_row, discover_benchmark_day, parse_time_v_metrics
from tools.phase5_state_selection_v1 import load_inventory


def write_inventory(path: Path, partitions: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"partitions": partitions}, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class DayBenchmarkV0Tests(unittest.TestCase):
    def test_planner_default_window_days_is_one(self):
        with patch.object(sys, "argv", ["phase5_big_hunt_plan_v2.py", "--exchange", "binance", "--stream", "trade"]):
            args = parse_plan_args()
        self.assertEqual(args.window_days, 1)

    def test_benchmark_day_discovery_prefers_oldest_full_coverage(self):
        with tempfile.TemporaryDirectory(prefix="day_benchmark_discovery_") as td:
            state = Path(td) / "inventory_state.json"
            write_inventory(
                state,
                {
                    "binance/trade/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "binance/bbo/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "bybit/trade/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "bybit/bbo/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "okx/trade/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "okx/bbo/btcusdt/20260104": {"status": "success", "day_quality_post": "GOOD"},
                    "binance/trade/btcusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
                    "bybit/trade/btcusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
                    "okx/trade/btcusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
                },
            )
            rows = load_inventory(state)
            out = discover_benchmark_day(
                rows,
                [
                    ("binance", "trade"),
                    ("binance", "bbo"),
                    ("bybit", "trade"),
                    ("bybit", "bbo"),
                    ("okx", "trade"),
                    ("okx", "bbo"),
                ],
                require_quality_pass=True,
            )
            self.assertEqual(out["chosen_day"], "20260104")
            self.assertEqual(out["coverage_score"], 6)
            self.assertEqual(out["selection_rule"], "oldest_full_coverage_score=6")

    def test_summary_row_generation_is_deterministic(self):
        row = build_summary_row(
            date="20260104",
            exchange="binance",
            stream="trade",
            status="OK",
            selected_symbol_count=20,
            selected_row_count=20,
            elapsed_sec=12.3456789,
            max_rss_kb=123456.0,
            archive_dir="/tmp/archive",
            phase6_v2_decision="PROMOTE",
            candidate_export_delta=1,
        )
        self.assertEqual(
            row,
            {
                "date": "20260104",
                "exchange": "binance",
                "stream": "trade",
                "status": "OK",
                "selected_symbol_count": "20",
                "selected_row_count": "20",
                "elapsed_sec": "12.345679",
                "max_rss_kb": "123456.0",
                "archive_dir": "/tmp/archive",
                "phase6_v2_decision": "PROMOTE",
                "candidate_export_delta": "1",
            },
        )

    def test_parse_time_v_metrics_handles_elapsed_label_with_colons(self):
        with tempfile.TemporaryDirectory(prefix="day_benchmark_timev_") as td:
            path = Path(td) / "time-v.log"
            path.write_text(
                "Elapsed (wall clock) time (h:mm:ss or m:ss): 12:55.52\n"
                "Maximum resident set size (kbytes): 123456\n",
                encoding="utf-8",
            )
            out = parse_time_v_metrics(path)
            self.assertAlmostEqual(out["elapsed_sec"], 775.52, places=2)
            self.assertEqual(out["max_rss_kb"], 123456.0)


if __name__ == "__main__":
    unittest.main()
