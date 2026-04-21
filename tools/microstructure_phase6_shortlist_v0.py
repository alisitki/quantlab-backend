#!/usr/bin/env python3
"""Build an exchange-aware Phase6 shortlist for microstructure_imbalance_v1.

This is a selection-only tool. It consumes existing Phase5 fullscan artifacts and
does not read compacted data or mutate ranking/promotion/runtime state.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_DIR = (
    ROOT
    / "tools/microstructure_imbalance_fullscan_output/fullscan_trade/artifacts/microstructure_imbalance"
)
DEFAULT_RESULTS_TSV = DEFAULT_ARTIFACT_DIR / "family_microstructure_imbalance_primary_results.tsv"
DEFAULT_SUMMARY_TSV = DEFAULT_ARTIFACT_DIR / "family_microstructure_imbalance_primary_summary.tsv"
DEFAULT_SYMBOL_ROLLUP_TSV = DEFAULT_ARTIFACT_DIR / "symbol_rollup.tsv"
DEFAULT_EXCHANGE_ROLLUP_TSV = DEFAULT_ARTIFACT_DIR / "exchange_rollup.tsv"
DEFAULT_OUTPUT_JSON = ROOT / "tools/microstructure_phase6_shortlist_v0.json"
DEFAULT_OUTPUT_TSV = ROOT / "tools/microstructure_phase6_shortlist_v0.tsv"
DEFAULT_REPORT_MD = ROOT / "tools/microstructure_phase6_shortlist_output/microstructure_phase6_shortlist_report_v0.md"

FAMILY_ID = "microstructure_imbalance_v1"
TARGET_EXCHANGE = "bybit"
TARGET_LABEL = "DIRECTIONAL"
MIN_T_STAT = 20.0
MIN_EVENT_COUNT = 100_000
ALLOWED_PRESSURE_THRESHOLDS = {0.1, 0.2}
ALLOWED_H_MS = {250, 500}
PREFERRED_DELTA_MS = {250, 500}
MAX_PER_SYMBOL = 2
MAX_SHORTLIST = 10
MIN_SHORTLIST = 4


@dataclass(frozen=True)
class CellKey:
    exchange: str
    symbol: str
    stream: str
    feature: str
    delta_ms: int
    h_ms: int
    pressure_threshold: float


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def as_float(value: str) -> float:
    return float(value)


def as_int(value: str) -> int:
    return int(float(value))


def threshold_token(value: float) -> str:
    return f"pt{int(round(value * 100)):03d}"


def strategy_id_for(key: CellKey) -> str:
    return (
        f"{FAMILY_ID}__{key.exchange}__{key.symbol}__{key.stream}"
        f"__d{key.delta_ms}__h{key.h_ms}__{threshold_token(key.pressure_threshold)}"
    )


def row_passes_strict_filters(row: dict[str, str]) -> tuple[bool, str]:
    if row["exchange"] != TARGET_EXCHANGE:
        return False, "excluded_exchange"
    if row["label"] != TARGET_LABEL:
        return False, "excluded_label"
    if as_float(row["t_stat"]) < MIN_T_STAT:
        return False, "below_t_stat_threshold"
    if as_float(row["mean_signed_fwd_return_bps"]) <= 0:
        return False, "non_positive_mean_signed_fwd_return_bps"
    if as_int(row["event_count"]) < MIN_EVENT_COUNT:
        return False, "below_event_count_threshold"
    if as_float(row["pressure_threshold"]) not in ALLOWED_PRESSURE_THRESHOLDS:
        return False, "excluded_pressure_threshold"
    if as_int(row["h_ms"]) not in ALLOWED_H_MS:
        return False, "excluded_h_ms"
    return True, "PASS"


def aggregate_group(key: CellKey, rows: list[dict[str, str]]) -> dict[str, Any]:
    event_counts = [as_int(row["event_count"]) for row in rows]
    total_event_count = sum(event_counts)
    weighted_mean = (
        sum(
            as_int(row["event_count"]) * as_float(row["mean_signed_fwd_return_bps"])
            for row in rows
        )
        / total_event_count
    )
    t_stats = [as_float(row["t_stat"]) for row in rows]
    t_stat = statistics.median(t_stats)
    score = t_stat * weighted_mean
    source_dates = sorted({row["date"] for row in rows})
    return {
        "strategy_id": strategy_id_for(key),
        "family_id": FAMILY_ID,
        "exchange": key.exchange,
        "symbol": key.symbol,
        "stream": key.stream,
        "label": TARGET_LABEL,
        "score": score,
        "selected_cell": {
            "delta_ms": key.delta_ms,
            "h_ms": key.h_ms,
            "pressure_threshold": key.pressure_threshold,
            "event_count": total_event_count,
            "t_stat": t_stat,
            "mean_signed_fwd_return_bps": weighted_mean,
        },
        "selection_evidence": {
            "aggregation_method": (
                "strict-filtered daily cells grouped by exchange/symbol/stream/delta_ms/h_ms/pressure_threshold; "
                "event_count=sum, mean_signed_fwd_return_bps=event-weighted mean, t_stat=median"
            ),
            "source_dates": source_dates,
            "source_row_count": len(rows),
            "supporting_days": len(source_dates),
            "min_event_count": min(event_counts),
            "max_event_count": max(event_counts),
            "min_t_stat": min(t_stats),
            "max_t_stat": max(t_stats),
            "preferred_delta_ms": key.delta_ms in PREFERRED_DELTA_MS,
        },
    }


def build_exclusions(
    rows: list[dict[str, str]],
    symbol_rollup: list[dict[str, str]],
    shortlisted_symbols: set[str],
) -> dict[str, Any]:
    excluded_by_exchange = Counter(row["exchange"] for row in rows if row["exchange"] != TARGET_EXCHANGE)
    bybit_directional_symbols = {
        row["symbol"]
        for row in symbol_rollup
        if row.get("exchange") == TARGET_EXCHANGE and as_int(row.get("directional_cells", "0")) > 0
    }
    excluded_symbols = []
    for symbol in sorted(bybit_directional_symbols - shortlisted_symbols):
        symbol_rows = [
            row
            for row in rows
            if row["exchange"] == TARGET_EXCHANGE and row["symbol"] == symbol and row["label"] == TARGET_LABEL
        ]
        reasons = Counter(row_passes_strict_filters(row)[1] for row in symbol_rows)
        reasons.pop("PASS", None)
        reason = "no row passed strict quality/parameter filters"
        if reasons:
            reason = reasons.most_common(1)[0][0]
        excluded_symbols.append({"exchange": TARGET_EXCHANGE, "symbol": symbol, "reason": reason})
    return {
        "excluded_exchange_rows": dict(sorted(excluded_by_exchange.items())),
        "excluded_symbols": excluded_symbols,
    }


def build_shortlist(rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[CellKey, list[dict[str, str]]] = defaultdict(list)
    filter_counts: Counter[str] = Counter()
    for row in rows:
        keep, reason = row_passes_strict_filters(row)
        filter_counts[reason] += 1
        if not keep:
            continue
        key = CellKey(
            exchange=row["exchange"],
            symbol=row["symbol"],
            stream=row["stream"],
            feature=row["feature"],
            delta_ms=as_int(row["delta_ms"]),
            h_ms=as_int(row["h_ms"]),
            pressure_threshold=as_float(row["pressure_threshold"]),
        )
        grouped[key].append(row)

    candidates = [aggregate_group(key, grouped[key]) for key in sorted(grouped, key=strategy_id_for)]
    candidates.sort(
        key=lambda item: (
            -item["score"],
            -item["selected_cell"]["event_count"],
            item["strategy_id"],
        )
    )

    top_by_symbol: dict[str, str] = {}
    for item in candidates:
        top_by_symbol.setdefault(item["symbol"], item["strategy_id"])

    per_symbol_count: Counter[str] = Counter()
    selected = []
    for item in candidates:
        cell = item["selected_cell"]
        symbol = item["symbol"]
        is_top_for_symbol = item["strategy_id"] == top_by_symbol[symbol]
        if cell["delta_ms"] == 100 and not is_top_for_symbol:
            continue
        if per_symbol_count[symbol] >= MAX_PER_SYMBOL:
            continue
        selected.append(item)
        per_symbol_count[symbol] += 1
        if len(selected) >= MAX_SHORTLIST:
            break

    selected.sort(
        key=lambda item: (
            -item["score"],
            -item["selected_cell"]["event_count"],
            item["strategy_id"],
        )
    )

    debug = {
        "filter_counts": dict(sorted(filter_counts.items())),
        "strict_group_count": len(candidates),
        "strict_symbol_count": len({item["symbol"] for item in candidates}),
        "selected_per_symbol": dict(sorted(per_symbol_count.items())),
        "delta_ms_100_rule": "delta_ms=100 allowed only for the top-scoring cell per symbol",
    }
    return selected, debug


def sanity_checks(shortlist: list[dict[str, Any]]) -> dict[str, Any]:
    checks = {
        "shortlist_size_min_4": len(shortlist) >= MIN_SHORTLIST,
        "shortlist_size_max_10": len(shortlist) <= MAX_SHORTLIST,
        "bybit_only": all(item["exchange"] == TARGET_EXCHANGE for item in shortlist),
        "directional_only": all(item["label"] == TARGET_LABEL for item in shortlist),
        "no_low_support_rows": all(
            item["selection_evidence"]["min_event_count"] >= MIN_EVENT_COUNT for item in shortlist
        ),
        "no_pressure_threshold_005": all(
            item["selected_cell"]["pressure_threshold"] in ALLOWED_PRESSURE_THRESHOLDS
            for item in shortlist
        ),
        "h_ms_preferred_only": all(item["selected_cell"]["h_ms"] in ALLOWED_H_MS for item in shortlist),
        "no_symbol_spam": all(count <= MAX_PER_SYMBOL for count in Counter(item["symbol"] for item in shortlist).values()),
    }
    return {"all_pass": all(checks.values()), "checks": checks}


def write_tsv(shortlist: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "strategy_id",
        "family_id",
        "exchange",
        "symbol",
        "stream",
        "delta_ms",
        "h_ms",
        "pressure_threshold",
        "event_count",
        "t_stat",
        "mean_signed_fwd_return_bps",
        "score",
        "source_dates",
        "supporting_days",
        "source_row_count",
        "min_event_count",
        "min_t_stat",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for item in shortlist:
            cell = item["selected_cell"]
            evidence = item["selection_evidence"]
            writer.writerow(
                {
                    "strategy_id": item["strategy_id"],
                    "family_id": item["family_id"],
                    "exchange": item["exchange"],
                    "symbol": item["symbol"],
                    "stream": item["stream"],
                    "delta_ms": cell["delta_ms"],
                    "h_ms": cell["h_ms"],
                    "pressure_threshold": cell["pressure_threshold"],
                    "event_count": cell["event_count"],
                    "t_stat": cell["t_stat"],
                    "mean_signed_fwd_return_bps": cell["mean_signed_fwd_return_bps"],
                    "score": item["score"],
                    "source_dates": ",".join(evidence["source_dates"]),
                    "supporting_days": evidence["supporting_days"],
                    "source_row_count": evidence["source_row_count"],
                    "min_event_count": evidence["min_event_count"],
                    "min_t_stat": evidence["min_t_stat"],
                }
            )


def write_report(payload: dict[str, Any], path: Path) -> None:
    summary = payload["summary"]
    shortlist = payload["shortlist"]
    best = shortlist[0] if shortlist else None
    excluded = payload["exclusions"]["excluded_symbols"]
    lines = [
        "# Microstructure Phase6 Shortlist v0",
        "",
        f"- generated_ts_utc: `{payload['generated_ts_utc']}`",
        "- bybit-only selection applied: `true`",
        f"- total candidates after filtering: `{summary['strict_group_count']}`",
        f"- shortlist size: `{summary['shortlist_size']}`",
        f"- symbols selected: `{', '.join(summary['symbols_selected'])}`",
        f"- relaxed thresholds used: `{str(summary['relaxed_thresholds_used']).lower()}`",
        "",
        "## Best Candidate",
    ]
    if best:
        cell = best["selected_cell"]
        lines.append(
            "- `{}` score={:.6f} mean_bps={:.6f} t_stat={:.6f} event_count={}".format(
                best["strategy_id"],
                best["score"],
                cell["mean_signed_fwd_return_bps"],
                cell["t_stat"],
                cell["event_count"],
            )
        )
    else:
        lines.append("- none")
    lines += ["", "## Excluded Symbols"]
    if excluded:
        for item in excluded:
            lines.append(f"- `{item['exchange']}/{item['symbol']}`: {item['reason']}")
    else:
        lines.append("- none")
    lines += [
        "",
        "## Sanity Checks",
        f"- all_pass: `{str(payload['sanity_checks']['all_pass']).lower()}`",
    ]
    for key, value in payload["sanity_checks"]["checks"].items():
        lines.append(f"- {key}: `{str(value).lower()}`")
    lines += [
        "",
        "## Final Note",
        "This shortlist is exchange-specific (bybit) and must be validated in Phase7 before any promotion decision.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary-results-tsv", type=Path, default=DEFAULT_RESULTS_TSV)
    parser.add_argument("--primary-summary-tsv", type=Path, default=DEFAULT_SUMMARY_TSV)
    parser.add_argument("--symbol-rollup-tsv", type=Path, default=DEFAULT_SYMBOL_ROLLUP_TSV)
    parser.add_argument("--exchange-rollup-tsv", type=Path, default=DEFAULT_EXCHANGE_ROLLUP_TSV)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-tsv", type=Path, default=DEFAULT_OUTPUT_TSV)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--generated-ts-utc", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for path in [
        args.primary_results_tsv,
        args.primary_summary_tsv,
        args.symbol_rollup_tsv,
        args.exchange_rollup_tsv,
    ]:
        if not path.exists():
            raise FileNotFoundError(path)

    rows = read_tsv(args.primary_results_tsv)
    symbol_rollup = read_tsv(args.symbol_rollup_tsv)
    exchange_rollup = read_tsv(args.exchange_rollup_tsv)
    shortlist, debug = build_shortlist(rows)
    sanity = sanity_checks(shortlist)
    excluded = build_exclusions(rows, symbol_rollup, {item["symbol"] for item in shortlist})

    if len(shortlist) < MIN_SHORTLIST:
        raise RuntimeError(
            f"strict shortlist size {len(shortlist)} is below minimum {MIN_SHORTLIST}; "
            "relaxed thresholds are intentionally not auto-applied"
        )
    if not sanity["all_pass"]:
        raise RuntimeError(f"sanity checks failed: {sanity}")

    payload = {
        "schema_version": "microstructure_phase6_shortlist_v0",
        "generated_ts_utc": args.generated_ts_utc or utc_now(),
        "governance": {
            "scope": "selection_only",
            "family_id": FAMILY_ID,
            "ranking_mutation": False,
            "promotion_mutation": False,
            "runtime_binding_mutation": False,
            "phase7_shadow": False,
        },
        "inputs": {
            "primary_results_tsv": str(args.primary_results_tsv),
            "primary_summary_tsv": str(args.primary_summary_tsv),
            "symbol_rollup_tsv": str(args.symbol_rollup_tsv),
            "exchange_rollup_tsv": str(args.exchange_rollup_tsv),
        },
        "selection_policy": {
            "exchange": TARGET_EXCHANGE,
            "label": TARGET_LABEL,
            "min_t_stat": MIN_T_STAT,
            "min_event_count_per_source_row": MIN_EVENT_COUNT,
            "mean_signed_fwd_return_bps": "> 0",
            "allowed_pressure_thresholds": sorted(ALLOWED_PRESSURE_THRESHOLDS),
            "allowed_h_ms": sorted(ALLOWED_H_MS),
            "preferred_delta_ms": sorted(PREFERRED_DELTA_MS),
            "score": "median_t_stat * event_weighted_mean_signed_fwd_return_bps",
            "max_per_symbol": MAX_PER_SYMBOL,
            "max_shortlist": MAX_SHORTLIST,
            "relaxed_thresholds_used": False,
        },
        "summary": {
            "total_input_rows": len(rows),
            "strict_group_count": debug["strict_group_count"],
            "strict_symbol_count": debug["strict_symbol_count"],
            "shortlist_size": len(shortlist),
            "symbols_selected": sorted({item["symbol"] for item in shortlist}),
            "best_strategy_id": shortlist[0]["strategy_id"] if shortlist else None,
            "best_score": shortlist[0]["score"] if shortlist else None,
            "relaxed_thresholds_used": False,
            "bybit_only_selection_applied": True,
        },
        "filter_debug": debug,
        "source_exchange_rollup": exchange_rollup,
        "exclusions": excluded,
        "sanity_checks": sanity,
        "shortlist": shortlist,
        "final_note": (
            "This shortlist is exchange-specific (bybit) and must be validated in Phase7 "
            "before any promotion decision."
        ),
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_tsv(shortlist, args.output_tsv)
    write_report(payload, args.report_md)

    print(
        json.dumps(
            {
                "status": "OK",
                "shortlist_size": len(shortlist),
                "symbols_selected": payload["summary"]["symbols_selected"],
                "output_json": str(args.output_json),
                "output_tsv": str(args.output_tsv),
                "report_md": str(args.report_md),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
