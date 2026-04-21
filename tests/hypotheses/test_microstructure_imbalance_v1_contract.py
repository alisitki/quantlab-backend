import csv
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "hypotheses" / "microstructure_imbalance_v1.py"
RUNNER = REPO / "tools" / "run-multi-hypothesis.js"


class MicrostructureImbalanceV1ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        token = uuid4().hex[:10]
        self.exchange = f"umi{token}"
        self.symbol = f"miv1{token}usdt"
        self.created_paths = []
        self.tmpdir = Path(tempfile.mkdtemp(prefix="miv1_contract_"))

    def tearDown(self) -> None:
        for p in reversed(self.created_paths):
            if p.is_file():
                p.unlink(missing_ok=True)
        shutil.rmtree(REPO / "data" / "curated" / f"exchange={self.exchange}", ignore_errors=True)
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_trade_day(self, day: str, ts, seq, price, qty, side) -> None:
        out = (
            REPO
            / "data"
            / "curated"
            / f"exchange={self.exchange}"
            / "stream=trade"
            / f"symbol={self.symbol}"
            / f"date={day}"
            / "data.parquet"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "ts_event": pa.array(ts, type=pa.int64()),
                "seq": pa.array(seq, type=pa.int64()),
                "price": pa.array(price, type=pa.float64()),
                "qty": pa.array(qty, type=pa.float64()),
                "side": pa.array(side, type=pa.int64()),
            }
        )
        pq.write_table(table, out)
        self.created_paths.append(out)

    def _run_family(self, *, start: str, end: str, delta_ms: str, h_ms: str, min_support: str = "2") -> tuple[Path, Path, Path]:
        results = self.tmpdir / "results.tsv"
        summary = self.tmpdir / "summary.tsv"
        report = self.tmpdir / "report.json"
        label_report = self.tmpdir / "label_report.json"
        cmd = [
            "python3",
            str(SCRIPT),
            "--exchange",
            self.exchange,
            "--symbol",
            self.symbol,
            "--stream",
            "trade",
            "--start",
            start,
            "--end",
            end,
            "--miDeltaMsList",
            delta_ms,
            "--miHMsList",
            h_ms,
            "--miPressureThresholdList",
            "0.5",
            "--miMinSupport",
            min_support,
            "--miMinEdgeBps",
            "0.01",
            "--miMinTStat",
            "0.0",
            "--results-out",
            str(results),
            "--summary-out",
            str(summary),
            "--report-out",
            str(report),
            "--label-report-out",
            str(label_report),
        ]
        subprocess.run(cmd, cwd=str(REPO), check=True)
        return results, report, label_report

    def test_golden_header_stable_order_and_directional_label(self) -> None:
        self._write_trade_day(
            "20990401",
            [0, 1, 2, 3, 4],
            [1, 2, 3, 4, 5],
            [100.0, 100.1, 100.2, 100.3, 100.4],
            [1.0, 1.0, 1.0, 1.0, 1.0],
            [1, 1, 1, 1, 1],
        )
        self._write_trade_day(
            "20990402",
            [0, 1, 2, 3],
            [1, 2, 3, 4],
            [100.0, 100.2, 100.4, 100.6],
            [1.0, 1.0, 1.0, 1.0],
            [1, 1, 1, 1],
        )
        results, report, label_report = self._run_family(start="20990401", end="20990402", delta_ms="2,1", h_ms="1")

        with results.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = list(reader)
            self.assertEqual(
                reader.fieldnames,
                [
                    "exchange",
                    "symbol",
                    "date",
                    "stream",
                    "feature",
                    "delta_ms",
                    "h_ms",
                    "pressure_threshold",
                    "event_count",
                    "mean_abs_pressure",
                    "mean_signed_fwd_return_bps",
                    "t_stat",
                    "label",
                ],
            )

        self.assertEqual(len(rows), 4)
        self.assertEqual(
            [(r["date"], int(r["delta_ms"]), int(r["h_ms"]), r["pressure_threshold"]) for r in rows],
            [
                ("20990401", 1, 1, "0.500000"),
                ("20990401", 2, 1, "0.500000"),
                ("20990402", 1, 1, "0.500000"),
                ("20990402", 2, 1, "0.500000"),
            ],
        )
        self.assertTrue(all(r["feature"] == "signed_trade_volume_imbalance" for r in rows))
        self.assertTrue(all(r["label"] == "DIRECTIONAL" for r in rows))
        for r in rows:
            self.assertRegex(r["mean_abs_pressure"], r"^\d+\.\d{15}$")
            self.assertRegex(r["mean_signed_fwd_return_bps"], r"^-?\d+\.\d{15}$")
            self.assertRegex(r["t_stat"], r"^-?\d+\.\d{15}$")

        report_obj = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual(report_obj["family_id"], "microstructure_imbalance_v1")
        self.assertEqual(report_obj["status"], "ok")
        self.assertTrue(report_obj["result"]["pass_signal"])
        self.assertEqual(report_obj["result"]["selected_cell"]["label"], "DIRECTIONAL")
        label_obj = json.loads(label_report.read_text(encoding="utf-8"))
        self.assertEqual(label_obj["selected_label"], "DIRECTIONAL")

    def test_insufficient_support_label_is_conservative(self) -> None:
        self._write_trade_day(
            "20990403",
            [0, 1, 2],
            [1, 2, 3],
            [100.0, 100.1, 100.2],
            [1.0, 1.0, 1.0],
            [1, 1, 1],
        )
        results, report, _ = self._run_family(
            start="20990403",
            end="20990403",
            delta_ms="1",
            h_ms="1",
            min_support="50",
        )
        with results.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["label"], "INSUFFICIENT_SUPPORT")
        report_obj = json.loads(report.read_text(encoding="utf-8"))
        self.assertFalse(report_obj["result"]["pass_signal"])

    def test_runner_contract_contains_microstructure_family_and_args(self) -> None:
        text = RUNNER.read_text(encoding="utf-8")
        self.assertIn("parseFamilyMicrostructureImbalance", text)
        self.assertIn("parseMicrostructureImbalanceResults", text)
        self.assertIn("--miDeltaMsList", text)
        self.assertIn("--miHMsList", text)
        self.assertIn("--miPressureThresholdList", text)
        self.assertIn("microstructure_imbalance_v1", text)


if __name__ == "__main__":
    unittest.main()
