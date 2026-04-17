#!/usr/bin/env python3
"""Bounded EODHD metric failure probe for pyvalue screen-failure audits.

Author: OpenAI
"""

from __future__ import annotations

import argparse
import inspect
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
import sqlite3
import statistics
import sys
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyvalue.metrics import REGISTRY  # noqa: E402
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS  # noqa: E402
from pyvalue.normalization.eodhd import EODHD_STATEMENT_FIELDS  # noqa: E402

SPECIAL_RAW_SOURCES: dict[str, list[str]] = {
    "EnterpriseValue": ["Valuation.EnterpriseValue"],
    "CommonStockDividendsPerShareCashPaid": ["Highlights.DividendShare"],
    "CommonStockSharesOutstanding": [
        "SharesStats.SharesOutstanding",
        "SharesStats.SharesFloat",
        "outstandingShares.annual.shares",
        "outstandingShares.quarterly.shares",
        "outstandingShares.annual.sharesMln",
        "outstandingShares.quarterly.sharesMln",
    ],
    "EntityCommonStockSharesOutstanding": [
        "SharesStats.SharesOutstanding",
        "SharesStats.SharesFloat",
        "outstandingShares.annual.shares",
        "outstandingShares.quarterly.shares",
        "outstandingShares.annual.sharesMln",
        "outstandingShares.quarterly.sharesMln",
    ],
    "EarningsPerShareDiluted": [
        "Earnings.History.epsActual",
        "Earnings.Annual.epsActual",
    ],
    "EarningsPerShareBasic": [
        "Earnings.History.epsActual",
        "Earnings.Annual.epsActual",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe one pyvalue metric failure using EODHD-only, read-only, "
            "largest-market-cap samples."
        )
    )
    parser.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database path (default: %(default)s)",
    )
    parser.add_argument(
        "--metric-id",
        required=True,
        help="Metric identifier from pyvalue.metrics.REGISTRY",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Optional reported failure reason for context only",
    )
    parser.add_argument(
        "--exchange-codes",
        nargs="+",
        required=True,
        help="One or more EODHD exchange codes to sample",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Largest-market-cap sample size per exchange (max: 50)",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_FACT_AGE_DAYS,
        help="Freshness window for latest normalized facts",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sample_size = max(1, min(int(args.sample_size), 50))
    metric_cls = REGISTRY.get(args.metric_id)
    if metric_cls is None:
        raise SystemExit(f"Unknown metric id: {args.metric_id}")

    db_path = Path(args.database).expanduser().resolve()
    exchanges = [code.strip().upper() for code in args.exchange_codes if code.strip()]
    if not exchanges:
        raise SystemExit("Provide at least one --exchange-codes value")

    with connect_read_only(db_path) as conn:
        conn.row_factory = sqlite3.Row
        sample_rows = load_sample_rows(conn, exchanges, sample_size)

        symbols = [row["symbol"] for rows in sample_rows.values() for row in rows]
        security_ids_by_symbol = {
            row["symbol"]: row["security_id"]
            for rows in sample_rows.values()
            for row in rows
        }
        facts_by_symbol = load_normalized_facts(
            conn,
            security_ids_by_symbol,
            required_concepts(metric_cls),
        )
        raw_payloads = load_raw_payloads(conn, security_ids_by_symbol)

    metric_file = inspect.getsourcefile(metric_cls)
    metric_path = (
        str(Path(metric_file).resolve().relative_to(REPO_ROOT)) if metric_file else None
    )
    source_hints = collect_source_hints(metric_cls)
    concepts = required_concepts(metric_cls)
    concept_summaries = {
        concept: build_concept_summary(
            concept=concept,
            symbols=symbols,
            sample_rows=sample_rows,
            facts_by_symbol=facts_by_symbol,
            raw_payloads=raw_payloads,
            max_age_days=args.max_age_days,
        )
        for concept in concepts
    }

    result = {
        "metric": {
            "metric_id": args.metric_id,
            "reason": args.reason,
            "source_file": metric_path,
            "required_concepts": concepts,
            "uses_market_data": bool(getattr(metric_cls, "uses_market_data", False)),
            "source_hints": source_hints,
        },
        "probe": {
            "provider": "EODHD",
            "database": str(db_path),
            "exchange_codes": exchanges,
            "sample_size_per_exchange": sample_size,
            "max_age_days": int(args.max_age_days),
        },
        "sample": {
            exchange: [
                {
                    "rank": row["rank"],
                    "symbol": row["symbol"],
                    "entity_name": row["entity_name"],
                    "market_cap": row["market_cap"],
                    "market_cap_currency": row["currency"],
                    "market_cap_as_of": row["as_of"],
                }
                for row in rows
            ]
            for exchange, rows in sample_rows.items()
        },
        "concept_summaries": concept_summaries,
    }

    output = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0


def connect_read_only(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(db_path))}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def required_concepts(metric_cls: type) -> list[str]:
    concepts = getattr(metric_cls, "required_concepts", ()) or ()
    ordered: list[str] = []
    seen: set[str] = set()
    for concept in concepts:
        if concept in seen:
            continue
        ordered.append(str(concept))
        seen.add(str(concept))
    return ordered


def load_sample_rows(
    conn: sqlite3.Connection,
    exchange_codes: list[str],
    sample_size: int,
) -> dict[str, list[dict[str, Any]]]:
    placeholders = ", ".join("?" for _ in exchange_codes)
    rows = conn.execute(
        f"""
        WITH scope AS (
            SELECT DISTINCT
                st.security_id,
                st.provider_exchange_code AS exchange_code,
                s.canonical_symbol,
                COALESCE(s.entity_name, st.security_name) AS entity_name
            FROM supported_tickers st
            JOIN securities s ON s.security_id = st.security_id
            WHERE st.provider = 'EODHD'
              AND st.provider_exchange_code IN ({placeholders})
        ),
        latest AS (
            SELECT
                md.security_id,
                md.as_of,
                md.market_cap,
                md.currency,
                ROW_NUMBER() OVER (
                    PARTITION BY md.security_id
                    ORDER BY md.as_of DESC
                ) AS rn
            FROM market_data md
            JOIN scope ON scope.security_id = md.security_id
        ),
        ranked AS (
            SELECT
                scope.security_id,
                scope.exchange_code,
                scope.canonical_symbol,
                scope.entity_name,
                latest.as_of,
                latest.market_cap,
                latest.currency,
                ROW_NUMBER() OVER (
                    PARTITION BY scope.exchange_code
                    ORDER BY latest.market_cap DESC, scope.canonical_symbol ASC
                ) AS exchange_rank
            FROM scope
            JOIN latest ON latest.security_id = scope.security_id
            WHERE latest.rn = 1
              AND latest.market_cap IS NOT NULL
        )
        SELECT
            security_id,
            exchange_code,
            canonical_symbol,
            entity_name,
            as_of,
            market_cap,
            currency,
            exchange_rank
        FROM ranked
        WHERE exchange_rank <= ?
        ORDER BY exchange_code, exchange_rank
        """,
        [*exchange_codes, sample_size],
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {
        exchange: [] for exchange in exchange_codes
    }
    for row in rows:
        grouped[str(row["exchange_code"])].append(
            {
                "security_id": int(row["security_id"]),
                "symbol": str(row["canonical_symbol"]),
                "entity_name": row["entity_name"],
                "as_of": str(row["as_of"]),
                "market_cap": row["market_cap"],
                "currency": row["currency"],
                "rank": int(row["exchange_rank"]),
            }
        )
    return grouped


def load_normalized_facts(
    conn: sqlite3.Connection,
    security_ids_by_symbol: Mapping[str, int],
    concepts: list[str],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    if not security_ids_by_symbol or not concepts:
        return {symbol: {} for symbol in security_ids_by_symbol}
    symbols_by_security_id = {
        security_id: symbol for symbol, security_id in security_ids_by_symbol.items()
    }
    results: dict[str, dict[str, list[dict[str, Any]]]] = {
        symbol: defaultdict(list) for symbol in security_ids_by_symbol
    }
    security_ids = sorted(symbols_by_security_id)
    security_placeholders = ", ".join("?" for _ in security_ids)
    concept_placeholders = ", ".join("?" for _ in concepts)
    rows = conn.execute(
        f"""
        SELECT
            ff.security_id,
            ff.concept,
            ff.fiscal_period,
            ff.end_date,
            ff.frame,
            ff.currency,
            ff.unit,
            ff.value
        FROM financial_facts ff
        WHERE ff.security_id IN ({security_placeholders})
          AND ff.concept IN ({concept_placeholders})
        ORDER BY ff.security_id, ff.concept, ff.end_date DESC
        """,
        [*security_ids, *concepts],
    ).fetchall()
    for row in rows:
        symbol = symbols_by_security_id[int(row["security_id"])]
        results[symbol][str(row["concept"])].append(
            {
                "fiscal_period": row["fiscal_period"],
                "end_date": str(row["end_date"]),
                "frame": row["frame"],
                "currency": row["currency"],
                "unit": row["unit"],
                "value": row["value"],
            }
        )
    return {symbol: dict(concepts_map) for symbol, concepts_map in results.items()}


def load_raw_payloads(
    conn: sqlite3.Connection,
    security_ids_by_symbol: Mapping[str, int],
) -> dict[str, dict[str, Any]]:
    if not security_ids_by_symbol:
        return {}
    symbols_by_security_id = {
        security_id: symbol for symbol, security_id in security_ids_by_symbol.items()
    }
    security_ids = sorted(symbols_by_security_id)
    placeholders = ", ".join("?" for _ in security_ids)
    rows = conn.execute(
        f"""
        SELECT security_id, data
        FROM fundamentals_raw
        WHERE provider = 'EODHD'
          AND security_id IN ({placeholders})
        """,
        security_ids,
    ).fetchall()
    payloads: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = symbols_by_security_id[int(row["security_id"])]
        try:
            payloads[symbol] = json.loads(row["data"])
        except json.JSONDecodeError:
            payloads[symbol] = {}
    return payloads


def collect_source_hints(metric_cls: type) -> list[str]:
    try:
        source = inspect.getsource(metric_cls)
    except OSError:
        return []
    hints: list[str] = []
    for line in source.splitlines():
        stripped = line.strip()
        lowered = stripped.lower()
        if not stripped:
            continue
        if (
            "required_concepts" in lowered
            or "fallback" in lowered
            or "is_recent_fact" in lowered
            or "max_fact_age_days" in lowered
            or "max_fy_fact_age_days" in lowered
        ):
            hints.append(stripped)
    return hints[:20]


def build_concept_summary(
    *,
    concept: str,
    symbols: list[str],
    sample_rows: Mapping[str, list[dict[str, Any]]],
    facts_by_symbol: Mapping[str, Mapping[str, list[dict[str, Any]]]],
    raw_payloads: Mapping[str, Mapping[str, Any]],
    max_age_days: int,
) -> dict[str, Any]:
    fresh_latest = 0
    stale_latest = 0
    missing_latest = 0
    any_normalized = 0
    any_raw = 0
    raw_no_normalized: list[dict[str, Any]] = []
    no_raw_no_normalized: list[dict[str, Any]] = []
    stale_examples: list[dict[str, Any]] = []
    normalized_fy_counts: list[int] = []
    normalized_quarter_counts: list[int] = []
    raw_yearly_counts: list[int] = []
    raw_quarterly_counts: list[int] = []
    raw_snapshot_counts: list[int] = []

    sample_lookup = {
        row["symbol"]: row for rows in sample_rows.values() for row in rows
    }

    for symbol in symbols:
        sample_row = sample_lookup.get(symbol, {})
        records = list(facts_by_symbol.get(symbol, {}).get(concept, []))
        counts = normalized_record_counts(records)
        normalized_fy_counts.append(counts["fy"])
        normalized_quarter_counts.append(counts["quarter"])
        raw_counts = raw_record_counts(raw_payloads.get(symbol), concept)
        raw_yearly_counts.append(raw_counts["yearly"])
        raw_quarterly_counts.append(raw_counts["quarterly"])
        raw_snapshot_counts.append(raw_counts["snapshot"])

        latest_status = "missing"
        latest_end_date = None
        if records:
            any_normalized += 1
            latest_end_date = records[0]["end_date"]
            if is_recent_date(latest_end_date, max_age_days=max_age_days):
                fresh_latest += 1
                latest_status = "fresh"
            else:
                stale_latest += 1
                latest_status = "stale"
        else:
            missing_latest += 1

        raw_total = (
            raw_counts["yearly"] + raw_counts["quarterly"] + raw_counts["snapshot"]
        )
        if raw_total > 0:
            any_raw += 1
        if raw_total > 0 and not records:
            raw_no_normalized.append(example_row(sample_row, latest_end_date))
        if raw_total == 0 and not records:
            no_raw_no_normalized.append(example_row(sample_row, latest_end_date))
        if latest_status == "stale":
            stale_examples.append(example_row(sample_row, latest_end_date))

    return {
        "raw_sources": raw_sources_for_concept(concept),
        "fresh_latest": fresh_latest,
        "stale_latest": stale_latest,
        "missing_latest": missing_latest,
        "symbols_with_any_normalized": any_normalized,
        "symbols_with_any_raw": any_raw,
        "symbols_raw_but_no_normalized": len(raw_no_normalized),
        "symbols_no_raw_and_no_normalized": len(no_raw_no_normalized),
        "median_normalized_fy_count": safe_median(normalized_fy_counts),
        "median_normalized_quarter_count": safe_median(normalized_quarter_counts),
        "median_raw_yearly_count": safe_median(raw_yearly_counts),
        "median_raw_quarterly_count": safe_median(raw_quarterly_counts),
        "median_raw_snapshot_count": safe_median(raw_snapshot_counts),
        "examples_raw_but_no_normalized": raw_no_normalized[:5],
        "examples_no_raw_and_no_normalized": no_raw_no_normalized[:5],
        "stale_examples": stale_examples[:5],
    }


def normalized_record_counts(records: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    fy_dates: set[str] = set()
    quarter_dates: set[str] = set()
    other_dates: set[str] = set()
    for record in records:
        end_date = str(record.get("end_date") or "")
        fiscal_period = str(record.get("fiscal_period") or "").upper()
        if fiscal_period == "FY":
            fy_dates.add(end_date)
        elif fiscal_period.startswith("Q"):
            quarter_dates.add(end_date)
        else:
            other_dates.add(end_date)
    return {
        "fy": len(fy_dates),
        "quarter": len(quarter_dates),
        "other": len(other_dates),
    }


def raw_record_counts(
    payload: Optional[Mapping[str, Any]], concept: str
) -> dict[str, int]:
    if not payload:
        return {"yearly": 0, "quarterly": 0, "snapshot": 0}

    yearly = 0
    quarterly = 0
    snapshot = 0
    statement_sources = statement_sources_for_concept(concept)
    financials = payload.get("Financials") or {}
    for statement_name, fields in statement_sources:
        statement_payload = financials.get(statement_name) or {}
        yearly += count_statement_bucket(statement_payload.get("yearly"), fields)
        quarterly += count_statement_bucket(statement_payload.get("quarterly"), fields)

    if concept in {
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
    }:
        shares_stats = payload.get("SharesStats") or {}
        if as_float(shares_stats.get("SharesOutstanding")) is not None:
            snapshot += 1
        elif as_float(shares_stats.get("SharesFloat")) is not None:
            snapshot += 1
        outstanding = payload.get("outstandingShares") or {}
        yearly += count_outstanding_shares_bucket(outstanding.get("annual"))
        quarterly += count_outstanding_shares_bucket(outstanding.get("quarterly"))

    if concept in {"EarningsPerShareDiluted", "EarningsPerShareBasic"}:
        earnings = payload.get("Earnings") or {}
        yearly += count_eps_bucket(earnings.get("Annual"))
        quarterly += count_eps_bucket(earnings.get("History"))

    if concept == "EnterpriseValue":
        valuation = payload.get("Valuation") or {}
        if as_float(valuation.get("EnterpriseValue")) is not None:
            snapshot += 1

    if concept == "CommonStockDividendsPerShareCashPaid":
        highlights = payload.get("Highlights") or {}
        if as_float(highlights.get("DividendShare")) is not None:
            snapshot += 1

    return {"yearly": yearly, "quarterly": quarterly, "snapshot": snapshot}


def statement_sources_for_concept(concept: str) -> list[tuple[str, list[str]]]:
    sources: list[tuple[str, list[str]]] = []
    for statement_name, concept_map in EODHD_STATEMENT_FIELDS.items():
        fields = concept_map.get(concept)
        if fields:
            sources.append((statement_name, list(fields)))
    return sources


def raw_sources_for_concept(concept: str) -> list[str]:
    sources = [
        f"Financials.{statement}.{field}"
        for statement, fields in statement_sources_for_concept(concept)
        for field in fields
    ]
    sources.extend(SPECIAL_RAW_SOURCES.get(concept, []))
    return sources


def count_statement_bucket(entries: Any, fields: list[str]) -> int:
    count = 0
    for _, entry in iter_entries(entries):
        if not isinstance(entry, Mapping):
            continue
        lowered = {str(key).lower(): value for key, value in entry.items()}
        for field in fields:
            value = entry.get(field)
            if value is None:
                value = lowered.get(field.lower())
            if as_float(value) is not None:
                count += 1
                break
    return count


def count_outstanding_shares_bucket(entries: Any) -> int:
    count = 0
    for _, entry in iter_entries(entries):
        if not isinstance(entry, Mapping):
            continue
        shares = as_float(entry.get("shares"))
        if shares is not None:
            count += 1
            continue
        shares_mln = as_float(entry.get("sharesMln"))
        if shares_mln is not None:
            count += 1
    return count


def count_eps_bucket(entries: Any) -> int:
    count = 0
    for _, entry in iter_entries(entries):
        if not isinstance(entry, Mapping):
            continue
        if as_float(entry.get("epsActual")) is not None:
            count += 1
    return count


def iter_entries(entries: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(entries, Mapping):
        for key, value in entries.items():
            yield str(key), value
        return
    if isinstance(entries, list):
        for idx, value in enumerate(entries):
            yield str(idx), value


def as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_recent_date(value: str, *, max_age_days: int) -> bool:
    try:
        end_date = date.fromisoformat(value[:10])
    except ValueError:
        return False
    age_days = (date.today() - end_date).days
    return age_days <= max_age_days


def safe_median(values: list[int]) -> Optional[float]:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return float(statistics.median(cleaned))


def example_row(
    sample_row: Mapping[str, Any], latest_end_date: Optional[str]
) -> dict[str, Any]:
    return {
        "symbol": sample_row.get("symbol"),
        "entity_name": sample_row.get("entity_name"),
        "market_cap": sample_row.get("market_cap"),
        "market_cap_currency": sample_row.get("currency"),
        "market_cap_as_of": sample_row.get("as_of"),
        "latest_normalized_end_date": latest_end_date,
    }


if __name__ == "__main__":
    raise SystemExit(main())
