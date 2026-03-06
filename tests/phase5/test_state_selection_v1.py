import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from tools.phase5_state_selection_v1 import build_object_keys_tsv, filter_rows, load_inventory


class StateSelectionV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="state_sel_v1_"))
        self.state_json = self.tmpdir / "state.json"
        obj = {
            "partitions": {
                "binance/trade/adausdt/20260127": {
                    "status": "success",
                    "day_quality_post": "GOOD",
                },
                "binance/trade/btcusdt/20260127": {
                    "status": "success",
                    "day_quality_post": "GOOD",
                },
                "binance/trade/btcusdt/20260128": {
                    "status": "success",
                    "day_quality_post": "GOOD",
                },
                "binance/trade/ethusdt/20260127": {
                    "status": "success",
                    "day_quality_post": "DEGRADED",
                },
                "binance/trade/ethusdt/20260128": {
                    "status": "success",
                    "day_quality_post": "DEGRADED",
                },
                "binance/trade/xrpusdt/20260127": {
                    "status": "failed",
                    "day_quality_post": "GOOD",
                },
                "binance/trade/xrpusdt/20260128": {
                    "status": "success",
                    "day_quality_post": "BAD",
                },
            }
        }
        self.state_json.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_missing_day_symbol_excluded(self):
        rows = load_inventory(self.state_json, bucket="quantlab-compact")
        selected_rows, selected_symbols, days = filter_rows(
            rows,
            exchange="binance",
            stream="trade",
            start="20260127",
            end="20260128",
            require_status="success",
            require_quality_pass=True,
            max_symbols=20,
        )
        self.assertEqual(days, ["20260127", "20260128"])
        self.assertNotIn("adausdt", selected_symbols)
        self.assertEqual(selected_symbols, ["btcusdt", "ethusdt"])
        self.assertTrue(all(r.symbol in {"btcusdt", "ethusdt"} for r in selected_rows))

    def test_tsv_written_deterministic(self):
        rows = load_inventory(self.state_json, bucket="quantlab-compact")
        selected_rows, _, _ = filter_rows(
            rows,
            exchange="binance",
            stream="trade",
            start="20260127",
            end="20260128",
            require_status="success",
            require_quality_pass=True,
            max_symbols=20,
        )
        out = self.tmpdir / "object_keys_selected.tsv"
        build_object_keys_tsv(selected_rows, out)

        with out.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f, delimiter="\t")
            got = [(row["date"], row["partition_key"], row["data_key"]) for row in r]

        expected = sorted(got, key=lambda x: (x[0], x[1].split("/")[2], x[2]))
        self.assertEqual(got, expected)


if __name__ == "__main__":
    unittest.main()

