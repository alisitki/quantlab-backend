import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from tools import phase6_directional_expectancy_contract_v0 as contract_module


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_directional_expectancy_contract_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_candidate_review_tsv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank",
        "class_priority",
        "review_class",
        "score",
        "decision_tier",
        "pack_id",
        "pack_path",
        "trade_surface_bucket",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_row(rank: int, pack_id: str, pack_path: Path) -> dict:
    return {
        "rank": str(rank),
        "class_priority": "3",
        "review_class": "UNSEEN",
        "score": "75.5",
        "decision_tier": "PROMOTE_STRONG",
        "pack_id": pack_id,
        "pack_path": str(pack_path),
        "trade_surface_bucket": "RUNNABLE_DIRECTIONAL",
    }


def make_pack(pack_path: Path, *, symbols: list[str], reports_by_symbol: dict[str, list[tuple[str, dict]]]) -> None:
    pack_path.mkdir(parents=True, exist_ok=True)
    write_json(pack_path / "campaign_plan.json", {"selected_symbols": symbols})
    for symbol, reports in reports_by_symbol.items():
        report_dir = pack_path / "runs" / symbol.lower() / "artifacts" / "multi_hypothesis"
        report_dir.mkdir(parents=True, exist_ok=True)
        for filename, report in reports:
            write_json(report_dir / filename, report)


def role_classification_payload() -> dict:
    families = []
    for family_id, role in (
        ("momentum_v1", "PRIMARY_DIRECTIONAL"),
        ("return_reversal_v1", "PRIMARY_DIRECTIONAL"),
        ("jump_reversion_v1", "PRIMARY_DIRECTIONAL"),
        ("family_b_simple_momentum", "PRIMARY_DIRECTIONAL"),
        ("spread_reversion_v1", "CONTEXT_GUARD"),
    ):
        families.append({"family_id": family_id, "role": role})
    return {"schema_version": "hypothesis_family_role_classification_v0", "families": families}


def momentum_report(symbol: str) -> dict:
    return {
        "family_id": "momentum_v1",
        "status": "ok",
        "exchange": "binance",
        "stream": "trade",
        "symbol": symbol,
        "window": "20260101..20260101",
        "params": {"delta_ms_list": [1000], "h_ms_list": [5000], "tolerance_ms": 0},
        "result": {
            "pass_signal": True,
            "selected_cell": {
                "exchange": "binance",
                "stream": "trade",
                "symbol": symbol,
                "delta_ms": 1000,
                "h_ms": 5000,
                "event_count": 500,
                "mean_product": 0.2,
                "t_stat": 3.0,
            },
        },
    }


def return_reversal_report(symbol: str) -> dict:
    return {
        "family_id": "return_reversal_v1",
        "status": "ok",
        "exchange": "binance",
        "stream": "trade",
        "symbol": symbol,
        "window": "20260101..20260101",
        "params": {"delta_ms_list": [1000], "h_ms_list": [5000], "tolerance_ms": 0},
        "result": {
            "pass_signal": True,
            "selected_cell": {
                "exchange": "binance",
                "stream": "trade",
                "symbol": symbol,
                "delta_ms": 1000,
                "h_ms": 5000,
                "event_count": 500,
                "mean_product": -0.2,
                "t_stat": -3.0,
            },
        },
    }


def jump_reversion_report(symbol: str) -> dict:
    return {
        "family_id": "jump_reversion_v1",
        "status": "ok",
        "exchange": "binance",
        "stream": "trade",
        "symbol": symbol,
        "window": "20260101..20260101",
        "params": {"jump_thresh_bps_list": [50], "h_ms_list": [5000], "cooldown_ms": 250},
        "result": {
            "pass_signal": True,
            "selected_cell": {
                "exchange": "binance",
                "stream": "trade",
                "symbol": symbol,
                "jump_thresh_bps": 50,
                "h_ms": 5000,
                "jump_count": 500,
                "mean_signed_reversal": 0.2,
                "t_stat": 3.0,
            },
        },
    }


def family_b_report(symbol: str) -> dict:
    return {
        "family_id": "family_b_simple_momentum",
        "status": "ok",
        "exchange": "binance",
        "stream": "trade",
        "symbol": symbol,
        "window": "20260101..20260101",
        "params": {"lookback_minutes": 5, "forward_minutes": 5, "signal_quantile": 0.9, "min_support": 200},
        "result": {
            "pass_signal": True,
            "signal_support": 500,
            "lookback_quantile_threshold": 0.003,
            "mean_forward_return": 0.001,
            "t_stat": 3.0,
        },
    }


class DirectionalExpectancyContractV0Tests(unittest.TestCase):
    def test_default_review_path_uses_v2_surface(self):
        self.assertTrue(str(contract_module.DEFAULT_CANDIDATE_REVIEW_TSV).endswith("candidate_review_v2.tsv"))

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_expands_one_pack_into_multiple_primary_directional_rows(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_contract_multi_") as td:
            root = Path(td)
            pack = root / "multi-hypothesis-phase5-bighunt-binance-trade-pack"
            make_pack(
                pack,
                symbols=["btcusdt"],
                reports_by_symbol={
                    "btcusdt": [
                        ("family_momentum_report.json", momentum_report("btcusdt")),
                        ("family_return_reversal_report.json", return_reversal_report("btcusdt")),
                        ("family_jump_reversion_report.json", jump_reversion_report("btcusdt")),
                        ("family_B_report.json", family_b_report("btcusdt")),
                        ("family_spread_reversion_report.json", {
                            "family_id": "spread_reversion_v1",
                            "status": "ok",
                            "exchange": "binance",
                            "stream": "bbo",
                            "symbol": "btcusdt",
                            "result": {"pass_signal": True},
                        }),
                    ]
                },
            )
            review_tsv = root / "candidate_review.tsv"
            role_json = root / "roles.json"
            out_json = root / "directional_expectancy_contract.json"
            write_candidate_review_tsv(review_tsv, [make_row(1, "multi-hypothesis-phase5-bighunt-binance-trade-pack", pack)])
            write_json(role_json, role_classification_payload())

            res = self._run(
                "--candidate-review-tsv", str(review_tsv),
                "--role-classification-json", str(role_json),
                "--out-json", str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["eligible_review_row_count"], 1)
            self.assertEqual(payload["translatable_count"], 4)
            family_ids = [item["selected_family_id"] for item in payload["items"]]
            self.assertEqual(
                family_ids,
                [
                    "momentum_v1",
                    "return_reversal_v1",
                    "jump_reversion_v1",
                    "family_b_simple_momentum",
                ],
            )

    def test_family_b_selected_cell_is_synthesized(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_contract_family_b_") as td:
            root = Path(td)
            pack = root / "multi-hypothesis-phase5-bighunt-binance-trade-pack"
            make_pack(
                pack,
                symbols=["ethusdt"],
                reports_by_symbol={
                    "ethusdt": [
                        ("family_B_report.json", family_b_report("ethusdt")),
                    ]
                },
            )
            review_tsv = root / "candidate_review.tsv"
            role_json = root / "roles.json"
            out_json = root / "directional_expectancy_contract.json"
            write_candidate_review_tsv(review_tsv, [make_row(1, "multi-hypothesis-phase5-bighunt-binance-trade-pack", pack)])
            write_json(role_json, role_classification_payload())

            res = self._run(
                "--candidate-review-tsv", str(review_tsv),
                "--role-classification-json", str(role_json),
                "--out-json", str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            selected_cell = item["strategy_spec"]["strategy_params"]["selected_cell"]
            self.assertEqual(item["selected_family_id"], "family_b_simple_momentum")
            self.assertEqual(selected_cell["symbol"], "ethusdt")
            self.assertEqual(selected_cell["lookback_minutes"], 5)
            self.assertEqual(selected_cell["forward_minutes"], 5)
            self.assertEqual(selected_cell["signal_support"], 500)
            self.assertEqual(selected_cell["event_count"], 500)

    def test_filters_non_runnable_or_non_binance_review_rows(self):
        with tempfile.TemporaryDirectory(prefix="directional_expectancy_contract_filter_") as td:
            root = Path(td)
            pack = root / "multi-hypothesis-phase5-bighunt-binance-trade-pack"
            make_pack(
                pack,
                symbols=["btcusdt"],
                reports_by_symbol={"btcusdt": [("family_momentum_report.json", momentum_report("btcusdt"))]},
            )
            review_tsv = root / "candidate_review.tsv"
            role_json = root / "roles.json"
            out_json = root / "directional_expectancy_contract.json"
            rows = [
                make_row(1, "multi-hypothesis-phase5-bighunt-binance-trade-pack", pack),
                {
                    **make_row(2, "multi-hypothesis-phase5-bighunt-bybit-trade-pack", pack),
                    "pack_id": "multi-hypothesis-phase5-bighunt-bybit-trade-pack",
                },
                {
                    **make_row(3, "multi-hypothesis-phase5-bighunt-binance-trade-pack-obs", pack),
                    "trade_surface_bucket": "OBSERVE_ONLY",
                },
            ]
            write_candidate_review_tsv(review_tsv, rows)
            write_json(role_json, role_classification_payload())

            res = self._run(
                "--candidate-review-tsv", str(review_tsv),
                "--role-classification-json", str(role_json),
                "--out-json", str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["eligible_review_row_count"], 1)
            self.assertEqual(payload["translatable_count"], 1)


if __name__ == "__main__":
    unittest.main()
