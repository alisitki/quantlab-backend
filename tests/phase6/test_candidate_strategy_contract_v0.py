import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_candidate_strategy_contract_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_candidate_review_tsv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["rank", "decision_tier", "pack_id", "pack_path"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_row(rank: int, pack_id: str, pack_path: Path) -> dict:
    return {
        "rank": str(rank),
        "decision_tier": "PROMOTE_STRONG",
        "pack_id": pack_id,
        "pack_path": str(pack_path),
    }


def make_pack(
    pack_path: Path,
    *,
    symbols: list[str],
    reports_by_symbol: dict[str, list[dict]],
) -> None:
    pack_path.mkdir(parents=True, exist_ok=True)
    write_json(pack_path / "campaign_plan.json", {"selected_symbols": symbols})
    for symbol, reports in reports_by_symbol.items():
        report_dir = pack_path / "runs" / symbol.lower() / "artifacts" / "multi_hypothesis"
        report_dir.mkdir(parents=True, exist_ok=True)
        for index, report in enumerate(reports, start=1):
            suffix = report.get("file_suffix") or f"report_{index}"
            write_json(report_dir / f"family_{suffix}_report.json", report)


def supported_report(*, family_id: str, exchange: str, stream: str, symbol: str, pass_signal: bool = True) -> dict:
    report = {
        "family_id": family_id,
        "status": "ok",
        "exchange": exchange,
        "stream": stream,
        "symbol": symbol,
        "window": "20260123..20260123",
        "params": {"delta_ms_list": [1000], "h_ms_list": [5000], "tolerance_ms": 0},
        "result": {
            "selected_cell": {
                "date": "20260123",
                "exchange": exchange,
                "stream": stream,
                "symbol": symbol,
                "delta_ms": 1000,
                "h_ms": 5000,
            }
        },
    }
    if family_id == "momentum_v1":
        report["result"]["selected_cell"].update(
            {
                "event_count": 1000,
                "mean_product": 0.25 if pass_signal else -0.25,
                "t_stat": 4.0 if pass_signal else -4.0,
            }
        )
        report["result"]["pass_signal"] = pass_signal
    return report


def unsupported_report(*, family_id: str, exchange: str, stream: str, symbol: str) -> dict:
    return {
        "family_id": family_id,
        "status": "unsupported_stream",
        "exchange": exchange,
        "stream": stream,
        "symbol": symbol,
        "result": {},
    }


class CandidateStrategyContractV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_empty_input_generates_empty_payload(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_empty_") as td:
            root = Path(td)
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [])

            res = self._run("--candidate-review-tsv", str(candidate_review_tsv), "--out-json", str(out_json))

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["source_row_count"], 0)
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["translatable_count"], 0)

    def test_single_supported_family_translates(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_ok_") as td:
            root = Path(td)
            pack = root / "pack_supported"
            make_pack(
                pack,
                symbols=["bnbusdt"],
                reports_by_symbol={
                    "bnbusdt": [
                        supported_report(
                            family_id="spread_reversion_v1",
                            exchange="bybit",
                            stream="bbo",
                            symbol="bnbusdt",
                        )
                    ]
                },
            )
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_a", pack)])

            res = self._run("--candidate-review-tsv", str(candidate_review_tsv), "--out-json", str(out_json))

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["translatable_count"], 1)
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "TRANSLATABLE")
            self.assertEqual(item["reject_reason"], "")
            spec = item["strategy_spec"]
            self.assertEqual(spec["family_id"], "spread_reversion_v1")
            self.assertEqual(spec["exchange"], "bybit")
            self.assertEqual(spec["stream"], "bbo")
            self.assertEqual(spec["symbols"], ["bnbusdt"])
            self.assertEqual(spec["activation_mode"], "SPEC_ONLY")
            self.assertEqual(spec["runtime_binding_status"], "UNBOUND")

    def test_multi_symbol_pack_is_not_translatable_yet(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_multi_symbol_") as td:
            root = Path(td)
            pack = root / "pack_multi"
            make_pack(pack, symbols=["bnbusdt", "ethusdt"], reports_by_symbol={})
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_multi", pack)])

            res = self._run("--candidate-review-tsv", str(candidate_review_tsv), "--out-json", str(out_json))

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "NOT_TRANSLATABLE_YET")
            self.assertEqual(item["reject_reason"], "MULTI_SYMBOL_PACK_UNSUPPORTED")
            self.assertIsNone(item["strategy_spec"])

    def test_no_supported_family_is_rejected_as_unsupported(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_unsupported_") as td:
            root = Path(td)
            pack = root / "pack_unsupported"
            make_pack(
                pack,
                symbols=["bnbusdt"],
                reports_by_symbol={
                    "bnbusdt": [
                        unsupported_report(
                            family_id="momentum_v1",
                            exchange="bybit",
                            stream="bbo",
                            symbol="bnbusdt",
                        )
                    ]
                },
            )
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_unsupported", pack)])

            res = self._run("--candidate-review-tsv", str(candidate_review_tsv), "--out-json", str(out_json))

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "UNSUPPORTED_FAMILY")
            self.assertEqual(item["reject_reason"], "NO_SUPPORTED_FAMILY_REPORT")

    def test_multiple_supported_reports_are_not_translatable_yet(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_ambiguous_") as td:
            root = Path(td)
            pack = root / "pack_ambiguous"
            make_pack(
                pack,
                symbols=["bnbusdt"],
                reports_by_symbol={
                    "bnbusdt": [
                        supported_report(
                            family_id="spread_reversion_v1",
                            exchange="bybit",
                            stream="bbo",
                            symbol="bnbusdt",
                        ),
                        supported_report(
                            family_id="jump_reversion_v1",
                            exchange="bybit",
                            stream="bbo",
                            symbol="bnbusdt",
                        ),
                    ]
                },
            )
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_ambiguous", pack)])

            res = self._run("--candidate-review-tsv", str(candidate_review_tsv), "--out-json", str(out_json))

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "NOT_TRANSLATABLE_YET")
            self.assertEqual(item["reject_reason"], "MULTIPLE_SUPPORTED_FAMILY_REPORTS")

    def test_selected_primary_family_can_resolve_multi_supported_pack(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_preferred_family_") as td:
            root = Path(td)
            pack = root / "pack_preferred"
            make_pack(
                pack,
                symbols=["btcusdt"],
                reports_by_symbol={
                    "btcusdt": [
                        supported_report(
                            family_id="momentum_v1",
                            exchange="binance",
                            stream="trade",
                            symbol="btcusdt",
                            pass_signal=True,
                        ),
                        supported_report(
                            family_id="return_reversal_v1",
                            exchange="binance",
                            stream="trade",
                            symbol="btcusdt",
                        ),
                    ]
                },
            )
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_preferred", pack)])

            res = self._run(
                "--candidate-review-tsv",
                str(candidate_review_tsv),
                "--preferred-family-id",
                "momentum_v1",
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["preferred_family_id"], "momentum_v1")
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "TRANSLATABLE")
            self.assertEqual(item["strategy_spec"]["family_id"], "momentum_v1")
            self.assertEqual(item["strategy_spec"]["stream"], "trade")

    def test_momentum_report_without_pass_signal_stays_unsupported(self):
        with tempfile.TemporaryDirectory(prefix="candidate_strategy_momentum_nopass_") as td:
            root = Path(td)
            pack = root / "pack_momentum_no_pass"
            make_pack(
                pack,
                symbols=["btcusdt"],
                reports_by_symbol={
                    "btcusdt": [
                        supported_report(
                            family_id="momentum_v1",
                            exchange="binance",
                            stream="trade",
                            symbol="btcusdt",
                            pass_signal=False,
                        )
                    ]
                },
            )
            candidate_review_tsv = root / "candidate_review.tsv"
            out_json = root / "candidate_strategy_contract.json"
            write_candidate_review_tsv(candidate_review_tsv, [make_row(1, "pack_no_pass", pack)])

            res = self._run(
                "--candidate-review-tsv",
                str(candidate_review_tsv),
                "--preferred-family-id",
                "momentum_v1",
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["translation_status"], "UNSUPPORTED_FAMILY")
            self.assertEqual(item["reject_reason"], "NO_SUPPORTED_FAMILY_REPORT")
