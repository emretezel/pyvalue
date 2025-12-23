"""Command line utilities for pyvalue.

Author: Emre Tezel
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
import logging
import re
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pyvalue.config import Config
from pyvalue.ingestion import CompaniesHouseClient, EODHDFundamentalsClient, GLEIFClient, SECCompanyFactsClient
from pyvalue.marketdata.service import MarketDataService, latest_share_count
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.normalization import EODHDFactsNormalizer, SECFactsNormalizer
from pyvalue.reporting import MetricCoverage, compute_fact_coverage
from pyvalue.screening import Criterion, evaluate_criterion, load_screen, evaluate_criterion_verbose
from pyvalue.logging_utils import setup_logging
from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import (
    EntityMetadataRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    ExchangeMetadataRepository,
    FinancialFactsRepository,
    MarketDataRepository,
    MetricsRepository,
    UKCompanyFactsRepository,
    UKFilingRepository,
    UKSymbolMapRepository,
    UniverseRepository,
)
from pyvalue.universe import UKUniverseLoader, USUniverseLoader

LOGGER = logging.getLogger(__name__)
DEFAULT_SCREEN_RESULTS_CSV = "data/screen_results.csv"


def _resolve_database_path(database: str) -> Path:
    """Resolve database path, falling back to repo data dir when using default name."""

    db_path = Path(database)
    if db_path.exists():
        return db_path
    if not db_path.is_absolute() and db_path.name == "pyvalue.db":
        repo_path = Path(__file__).resolve().parents[2] / "data" / db_path.name
        if repo_path.exists():
            return repo_path
    return db_path


def _qualify_symbol(symbol: str, exchange: Optional[str] = None, region: Optional[str] = None) -> str:
    base = symbol.strip().upper()
    if "." in base:
        return base
    if exchange:
        return f"{base}.{exchange.upper()}"
    if region:
        return f"{base}.{region.upper()}"
    return base


def _format_market_symbol(symbol: str, exchange: Optional[str], region: Optional[str]) -> str:
    """Format a symbol for market data providers (EODHD)."""

    normalized = symbol.upper()
    if "." in normalized:
        return normalized
    if exchange:
        exch = exchange.upper()
        # Use US suffix for common US exchange labels.
        if exch in {"US", "NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "CBOE BZX"}:
            return f"{normalized}.US"
        return f"{normalized}.{exch}"
    if region and region.upper() == "US":
        return f"{normalized}.US"
    return normalized


def _normalize_provider(provider: Optional[str]) -> str:
    if not provider:
        raise SystemExit("Provider is required (SEC or EODHD).")
    normalized = provider.strip().upper()
    if normalized not in {"SEC", "EODHD"}:
        raise SystemExit(f"Unsupported provider: {provider}")
    return normalized


def _symbols_for_region_or_raise(db_path: Path, region: str) -> List[str]:
    """Return symbols for region from listings or fundamentals, or raise with guidance."""

    region_label = region.upper()
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    symbols = universe_repo.fetch_symbols(region_label)
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    if not symbols:
        symbols = fund_repo.symbols("EODHD", region=region_label)
    if symbols:
        return symbols

    listing_regions: List[str] = []
    fund_regions: List[str] = []
    with universe_repo._connect() as conn:
        listing_regions = [row[0] for row in conn.execute("SELECT DISTINCT region FROM listings").fetchall()]
    with fund_repo._connect() as conn:
        fund_regions = [
            row[0] for row in conn.execute(
                "SELECT DISTINCT region FROM fundamentals_raw WHERE provider = 'EODHD' AND region IS NOT NULL"
            ).fetchall()
        ]
    available_regions = sorted({*listing_regions, *fund_regions})
    raise SystemExit(
        f"No symbols found for region {region_label}. Load a universe or ingest fundamentals first. "
        f"Available regions: {', '.join(available_regions) if available_regions else 'none'}. "
        f"Database: {db_path}"
    )


def _select_metric_classes(metric_ids: Optional[Sequence[str]]) -> List[type]:
    """Return metric classes for requested ids, raising on unknown identifiers."""

    ids = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    metric_classes: List[type] = []
    for metric_id in ids:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            raise SystemExit(f"Unknown metric id: {metric_id}")
        metric_classes.append(metric_cls)
    return metric_classes


def _parse_currency_codes(values: Optional[Sequence[str]]) -> Optional[set[str]]:
    if not values:
        return None
    codes: set[str] = set()
    for item in values:
        if not item:
            continue
        for part in re.split(r"[,\s]+", item.strip()):
            if part:
                codes.add(part.upper())
    return codes or None


def _select_exchange_listings_for_provider(
    database: str,
    provider: str,
    exchange_code: str,
    region: Optional[str],
    max_age_days: Optional[int],
    max_symbols: Optional[int],
    resume: bool,
) -> List[tuple[str, str]]:
    universe_repo = UniverseRepository(database)
    universe_repo.initialize_schema()

    now = datetime.now(timezone.utc)
    params: List[str] = [provider.upper(), provider.upper(), exchange_code.upper()]
    query = [
        "SELECT l.symbol, l.region, fr.fetched_at, fs.next_eligible_at",
        "FROM listings l",
        "LEFT JOIN fundamentals_raw fr ON fr.symbol = l.symbol AND fr.provider = ?",
        "LEFT JOIN fundamentals_fetch_state fs ON fs.symbol = l.symbol AND fs.provider = ?",
        "WHERE UPPER(l.exchange) = ?",
    ]
    if region:
        query.append("AND l.region = ?")
        params.append(region.upper())
    if max_age_days is not None:
        cutoff = (now - timedelta(days=max_age_days)).isoformat()
        query.append("AND (fr.fetched_at IS NULL OR fr.fetched_at <= ?)")
        params.append(cutoff)
    if resume:
        query.append("AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)")
        params.append(now.isoformat())
    query.append(
        "ORDER BY CASE WHEN fr.fetched_at IS NULL THEN 0 ELSE 1 END, fr.fetched_at ASC, l.symbol ASC"
    )
    if max_symbols is not None:
        query.append("LIMIT ?")
        params.append(max_symbols)
    sql = " ".join(query)
    with universe_repo._connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [(row[0], row[1]) for row in rows]


def _select_listing_symbols_by_exchange(
    database: str,
    exchange_code: str,
    region: Optional[str],
) -> List[str]:
    universe_repo = UniverseRepository(database)
    pairs = universe_repo.fetch_symbol_regions_by_exchange(exchange_code, region=region)
    return [symbol for symbol, _ in pairs]


def build_parser() -> argparse.ArgumentParser:
    """Configure the root parser with subcommands."""

    parser = argparse.ArgumentParser(description="pyvalue data utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    load_us = subparsers.add_parser(
        "load-us-universe",
        help="Download Nasdaq Trader files and persist the US equity universe.",
    )
    load_us.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    load_us.add_argument(
        "--include-etfs",
        action="store_true",
        help="Persist ETFs alongside operating companies.",
    )

    load_intl = subparsers.add_parser(
        "load-eodhd-universe",
        help="Download an EODHD exchange symbol list and persist it (non-US).",
    )
    load_intl.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    load_intl.add_argument(
        "--include-etfs",
        action="store_true",
        help="Persist ETFs alongside operating companies.",
    )
    load_intl.add_argument(
        "--exchange-code",
        default="LSE",
        help="EODHD exchange code to load (default: %(default)s)",
    )
    load_intl.add_argument(
        "--currencies",
        nargs="+",
        default=None,
        help="Limit to these currency codes (space or comma separated).",
    )

    ingest_uk = subparsers.add_parser(
        "ingest-uk-facts",
        help="Download Companies House profile for a company number and store it.",
    )
    ingest_uk.add_argument("company_number", nargs="?", help="Companies House company number, e.g. 00000000")
    ingest_uk.add_argument(
        "--symbol",
        default=None,
        help="Optional ticker symbol to associate with this company.",
    )
    ingest_uk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_uk_bulk = subparsers.add_parser(
        "ingest-uk-facts-bulk",
        help="Download Companies House profiles for all mapped UK symbols.",
    )
    ingest_uk_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_uk_filings = subparsers.add_parser(
        "ingest-uk-filings",
        help="Download latest iXBRL Companies House filing for a UK symbol.",
    )
    ingest_uk_filings.add_argument(
        "symbol",
        help="Ticker symbol mapped to a Companies House number",
    )
    ingest_uk_filings.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    refresh_map = subparsers.add_parser(
        "refresh-uk-symbol-map",
        help="Refresh UK symbol -> company number mapping using GLEIF and stored ISINs.",
    )
    refresh_map.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    refresh_map.add_argument(
        "--gleif-url",
        default=GLEIFClient.DEFAULT_URL,
        help="GLEIF golden CSV URL (default: %(default)s)",
    )
    refresh_map.add_argument(
        "--isin-date",
        default=None,
        help="Date (YYYY-MM-DD) for GLEIF ISIN mapping file (default: today UTC)",
    )
    refresh_map.add_argument(
        "--region",
        default="UK",
        help="Region tag used when loading the EODHD universe for mapping (default: %(default)s)",
    )

    ingest_facts = subparsers.add_parser(
        "ingest-us-facts",
        help="Download SEC company facts for a given ticker and store them locally.",
    )
    ingest_facts.add_argument("symbol", help="Ticker symbol, e.g. AAPL")
    ingest_facts.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    ingest_facts.add_argument(
        "--user-agent",
        default=None,
        help="Custom User-Agent string (falls back to PYVALUE_SEC_USER_AGENT env var).",
    )
    ingest_facts.add_argument(
        "--cik",
        default=None,
        help="Optional explicit CIK override (10-digit).",
    )

    bulk_ingest = subparsers.add_parser(
        "ingest-us-facts-bulk",
        help="Download SEC company facts for all stored US listings.",
    )
    bulk_ingest.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    bulk_ingest.add_argument(
        "--region",
        default="US",
        help="Universe region key stored in SQLite (default: %(default)s)",
    )
    bulk_ingest.add_argument(
        "--rate",
        type=float,
        default=9.0,
        help="Maximum SEC API calls per second (default: %(default)s)",
    )
    bulk_ingest.add_argument(
        "--user-agent",
        default=None,
        help="Custom User-Agent string (falls back to PYVALUE_SEC_USER_AGENT env var).",
    )

    bulk_market_data = subparsers.add_parser(
        "update-market-data-bulk",
        help="Fetch latest market data for all stored US listings.",
    )
    bulk_market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    bulk_market_data.add_argument(
        "--region",
        default=None,
        help="Universe region key stored in SQLite (default: US when no exchange-code).",
    )
    bulk_market_data.add_argument(
        "--exchange-code",
        default=None,
        help="Optional exchange code to select symbols from listings (e.g., US, LSE, NYSE).",
    )
    bulk_market_data.add_argument(
        "--rate",
        type=float,
        default=950.0,
        help="Throttle speed in symbols per minute (default: %(default)s)",
    )

    recalc_market_cap = subparsers.add_parser(
        "recalc-market-cap",
        help="Recompute stored market caps using latest price and share counts.",
    )
    recalc_market_cap.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_listings = subparsers.add_parser(
        "clear-listings",
        help="Delete all stored listings.",
    )
    clear_listings.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_facts = subparsers.add_parser(
        "clear-financial-facts",
        help="Delete all normalized financial facts.",
    )
    clear_facts.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_fundamentals_raw = subparsers.add_parser(
        "clear-fundamentals-raw",
        help="Delete all stored raw fundamentals.",
    )
    clear_fundamentals_raw.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_metrics = subparsers.add_parser(
        "clear-metrics",
        help="Delete all computed metrics.",
    )
    clear_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    normalize_facts = subparsers.add_parser(
        "normalize-us-facts",
        help="Transform stored SEC company facts for a ticker into structured rows.",
    )
    normalize_facts.add_argument("symbol", help="Ticker symbol already ingested via SEC API.")
    normalize_facts.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    normalize_facts_bulk = subparsers.add_parser(
        "normalize-us-facts-bulk",
        help="Normalize SEC facts for all stored tickers.",
    )
    normalize_facts_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_eodhd = subparsers.add_parser(
        "ingest-eodhd-fundamentals",
        help="Download EODHD fundamentals for a ticker and store them locally.",
    )
    ingest_eodhd.add_argument("symbol", help="Ticker symbol, e.g. SHEL")
    ingest_eodhd.add_argument(
        "--exchange-code",
        default=None,
        help="Exchange code to append if symbol lacks a suffix (e.g., LSE).",
    )
    ingest_eodhd.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_eodhd_bulk = subparsers.add_parser(
        "ingest-eodhd-fundamentals-bulk",
        help="Download EODHD fundamentals for all listings in an exchange or stored universe.",
    )
    ingest_eodhd_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    ingest_eodhd_bulk.add_argument(
        "--rate",
        type=float,
        default=600.0,
        help="Throttle speed in symbols per minute (default: %(default)s)",
    )
    ingest_eodhd_bulk.add_argument(
        "--exchange-code",
        default=None,
        help="If set, pull symbols directly from EODHD exchange list (e.g., LSE) instead of stored universe.",
    )
    ingest_eodhd_bulk.add_argument(
        "--region",
        default=None,
        help="Region key to pull symbols from the stored universe when --exchange-code is omitted.",
    )

    ingest_fundamentals = subparsers.add_parser(
        "ingest-fundamentals",
        help="Download fundamentals for a ticker from a chosen provider.",
    )
    ingest_fundamentals.add_argument(
        "--provider",
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to use.",
    )
    ingest_fundamentals.add_argument("symbol", help="Ticker symbol, e.g. AAPL or SHEL.LSE")
    ingest_fundamentals.add_argument(
        "--exchange-code",
        default=None,
        help="EODHD exchange code when symbol lacks a suffix (e.g., LSE).",
    )
    ingest_fundamentals.add_argument(
        "--user-agent",
        default=None,
        help="Custom User-Agent for SEC (falls back to PYVALUE_SEC_USER_AGENT).",
    )
    ingest_fundamentals.add_argument(
        "--cik",
        default=None,
        help="Optional SEC CIK override (10-digit).",
    )
    ingest_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_fundamentals_bulk = subparsers.add_parser(
        "ingest-fundamentals-bulk",
        help="Download fundamentals in bulk from a chosen provider.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--provider",
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to use.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    ingest_fundamentals_bulk.add_argument(
        "--region",
        default=None,
        help="Region key for universe lookup (defaults to US for SEC).",
    )
    ingest_fundamentals_bulk.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Throttle rate (SEC: req/sec, EODHD: symbols/min). Defaults depend on provider.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--exchange-code",
        default=None,
        help="Exchange code to filter stored listings (e.g., LSE, US).",
    )
    ingest_fundamentals_bulk.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to ingest in this run.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="Only ingest symbols with older fundamentals (days) or missing data.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--resume",
        action="store_true",
        help="Skip symbols that are still in backoff from prior failures.",
    )
    ingest_fundamentals_bulk.add_argument(
        "--user-agent",
        default=None,
        help="Custom User-Agent for SEC (falls back to PYVALUE_SEC_USER_AGENT).",
    )

    normalize_eodhd = subparsers.add_parser(
        "normalize-eodhd-fundamentals",
        help="Normalize stored EODHD fundamentals for a single ticker.",
    )
    normalize_eodhd.add_argument("symbol", help="Ticker symbol already ingested via EODHD.")
    normalize_eodhd.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    normalize_eodhd_bulk = subparsers.add_parser(
        "normalize-eodhd-fundamentals-bulk",
        help="Normalize stored EODHD fundamentals for all ingested tickers.",
    )
    normalize_eodhd_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    normalize_eodhd_bulk.add_argument(
        "--region",
        default=None,
        help="Optional region filter matching fundamentals_raw.region",
    )

    normalize_fundamentals = subparsers.add_parser(
        "normalize-fundamentals",
        help="Normalize stored fundamentals for a ticker using the provider-specific ruleset.",
    )
    normalize_fundamentals.add_argument(
        "--provider",
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to normalize.",
    )
    normalize_fundamentals.add_argument("symbol", help="Ticker symbol already ingested for the provider.")
    normalize_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    normalize_fundamentals_bulk = subparsers.add_parser(
        "normalize-fundamentals-bulk",
        help="Normalize stored fundamentals in bulk for a provider.",
    )
    normalize_fundamentals_bulk.add_argument(
        "--provider",
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to normalize.",
    )
    normalize_fundamentals_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    normalize_fundamentals_bulk.add_argument(
        "--region",
        default=None,
        help="Optional region filter (defaults to US for SEC).",
    )
    normalize_fundamentals_bulk.add_argument(
        "--exchange-code",
        default=None,
        help="Optional exchange code to select symbols from listings (e.g., US, LSE).",
    )

    market_data = subparsers.add_parser(
        "update-market-data",
        help="Fetch latest market data for a ticker and persist it.",
    )
    market_data.add_argument("symbol", help="Ticker symbol, e.g. AAPL")
    market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    compute_metrics = subparsers.add_parser(
        "compute-metrics",
        help="Compute one or more metrics for a ticker and store them.",
    )
    compute_metrics.add_argument("symbol", help="Ticker symbol to evaluate")
    compute_metrics.add_argument(
        "--metrics",
        nargs="+",
        default=["working_capital"],
        help="Metric identifiers to compute (default: working_capital)",
    )
    compute_metrics.add_argument(
        "--all",
        action="store_true",
        help="Compute all registered metrics",
    )
    compute_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    bulk_metrics = subparsers.add_parser(
        "compute-metrics-bulk",
        help="Compute metrics for all stored listings.",
    )
    bulk_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    bulk_metrics.add_argument(
        "--region",
        default=None,
        help="Universe region key stored in SQLite (default: US when no exchange-code).",
    )
    bulk_metrics.add_argument(
        "--exchange-code",
        default=None,
        help="Optional exchange code to select symbols from listings (e.g., US, LSE, NYSE).",
    )
    bulk_metrics.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to compute (default: all registered metrics)",
    )

    fact_report = subparsers.add_parser(
        "report-fact-freshness",
        help="List missing or stale financial facts required by metrics for a region.",
    )
    fact_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    fact_report.add_argument(
        "--region",
        default="US",
        help="Universe region key stored in SQLite (default: %(default)s)",
    )
    fact_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )
    fact_report.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_FACT_AGE_DAYS,
        help="Fact freshness window in days (default: %(default)s)",
    )
    fact_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for detailed concept coverage.",
    )
    fact_report.add_argument(
        "--show-all",
        action="store_true",
        help="Show concepts even when all symbols are fresh.",
    )

    metric_report = subparsers.add_parser(
        "report-metric-coverage",
        help="Count how many symbols can compute all requested metrics for a region without writing results.",
    )
    metric_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    metric_report.add_argument(
        "--region",
        default="US",
        help="Universe region key stored in SQLite (default: %(default)s)",
    )
    metric_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )

    failure_report = subparsers.add_parser(
        "report-metric-failures",
        help="Summarize warning reasons for metric computation failures in a region.",
    )
    failure_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    failure_report.add_argument(
        "--region",
        default="US",
        help="Universe region key stored in SQLite (default: %(default)s)",
    )
    failure_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )
    failure_report.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional list of symbols (space or comma separated) to evaluate instead of the full region universe",
    )
    failure_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for metric failure reasons.",
    )

    purge_nonfilers = subparsers.add_parser(
        "purge-us-nonfilers",
        help="Remove US listings that have no 10-K/10-Q filings in stored SEC company facts.",
    )
    purge_nonfilers.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    purge_nonfilers.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletions instead of just printing the symbols to be removed.",
    )

    run_screen = subparsers.add_parser(
        "run-screen",
        help="Evaluate screening criteria for a ticker.",
    )
    run_screen.add_argument("symbol", help="Ticker symbol")
    run_screen.add_argument("config", help="Path to screening config (YAML)")
    run_screen.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )

    run_screen_bulk = subparsers.add_parser(
        "run-screen-bulk",
        help="Evaluate screening criteria for all tickers in the stored universe.",
    )
    run_screen_bulk.add_argument("config", help="Path to screening config (YAML)")
    run_screen_bulk.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    run_screen_bulk.add_argument(
        "--region",
        default=None,
        help="Universe region key stored in SQLite (default: US when no exchange-code).",
    )
    run_screen_bulk.add_argument(
        "--exchange-code",
        default=None,
        help="Optional exchange code to select symbols from listings (e.g., US, LSE, NYSE).",
    )
    run_screen_bulk.add_argument(
        "--output-csv",
        default=DEFAULT_SCREEN_RESULTS_CSV,
        help="Path to write passing results as CSV (default: %(default)s)",
    )

    return parser


def _should_keep_listing(include_etfs: bool, listing_is_etf: bool) -> bool:
    """Return True if the listing should be kept after ETF filtering."""

    return include_etfs or not listing_is_etf


def cmd_load_us_universe(database: str, include_etfs: bool) -> int:
    """Execute the US universe load command."""

    loader = USUniverseLoader()
    listings = loader.load()
    LOGGER.info("Fetched %s US listings", len(listings))

    # Drop ETFs unless explicitly requested in the CLI arguments.
    filtered = [item for item in listings if _should_keep_listing(include_etfs, item.is_etf)]
    LOGGER.info("Remaining listings after ETF filter: %s", len(filtered))

    # Persist the normalized listings to SQLite storage.
    repo = UniverseRepository(database)
    repo.initialize_schema()
    inserted = repo.replace_universe(filtered, region="US")

    print(f"Stored {inserted} US listings in {database}")
    return 0


def cmd_load_eodhd_universe(
    database: str,
    include_etfs: bool,
    exchange_code: str,
    currencies: Optional[Sequence[str]] = None,
) -> int:
    """Execute the EODHD exchange universe load command."""

    api_key = Config().eodhd_api_key
    if not api_key:
        raise SystemExit(
            "EODHD API key missing. Add [eodhd].api_key to private/config.toml before loading an EODHD universe."
        )

    client = EODHDFundamentalsClient(api_key=api_key)
    meta = client.exchange_metadata(exchange_code) or {}
    region_label = (meta.get("Country") or exchange_code or "INTL").upper()

    exchange_repo = ExchangeMetadataRepository(database)
    exchange_repo.initialize_schema()
    exchange_repo.upsert(
        code=exchange_code,
        name=meta.get("Name"),
        country=meta.get("Country"),
        currency=meta.get("Currency"),
        operating_mic=meta.get("OperatingMIC") or meta.get("OperatingMic"),
    )

    allowed_currencies = _parse_currency_codes(currencies)
    loader = UKUniverseLoader(
        api_key=api_key,
        exchange_code=exchange_code,
        include_etfs=include_etfs,
        allowed_currencies=allowed_currencies,
    )
    listings = loader.load()
    LOGGER.info("Fetched %s listings for exchange %s", len(listings), exchange_code)

    filtered = [item for item in listings if _should_keep_listing(include_etfs, item.is_etf)]
    LOGGER.info("Remaining listings after ETF filter: %s", len(filtered))

    repo = UniverseRepository(database)
    repo.initialize_schema()
    inserted = repo.replace_universe(filtered, region=region_label)

    # Seed symbol->ISIN mapping if present in the feed.
    mapper = UKSymbolMapRepository(database)
    mapper.initialize_schema()
    rows = [
        (
            listing.symbol,
            listing.isin,
            None,
            None,
        )
        for listing in filtered
        if listing.isin
    ]
    if rows:
        mapper.bulk_upsert(rows)

    print(f"Stored {inserted} listings for {region_label} in {database}")
    return 0


def cmd_ingest_uk_facts(company_number: Optional[str], database: str, symbol: Optional[str]) -> int:
    """Fetch Companies House profile using company number or mapped symbol and persist it."""

    if not company_number and not symbol:
        raise SystemExit("Provide a company number or --symbol")

    mapper = UKSymbolMapRepository(database)
    mapper.initialize_schema()
    resolved_number = company_number
    if not resolved_number and symbol:
        resolved_number = mapper.fetch_company_number(symbol)
        if not resolved_number:
            raise SystemExit(f"No company number mapped for symbol {symbol}. Run refresh-uk-symbol-map first.")

    api_key = Config().companies_house_api_key
    if not api_key:
        raise SystemExit(
            "Companies House API key missing. Add [companies_house].api_key to private/config.toml."
        )

    client = CompaniesHouseClient(api_key=api_key)
    payload = client.fetch_company_profile(resolved_number)

    repo = UKCompanyFactsRepository(database)
    repo.initialize_schema()
    repo.upsert_company_facts(resolved_number, payload, symbol=symbol)

    print(f"Stored Companies House facts for {resolved_number} in {database}")
    return 0


def cmd_ingest_uk_facts_bulk(database: str) -> int:
    mapper = UKSymbolMapRepository(database)
    mapper.initialize_schema()
    company_numbers = mapper.fetch_symbols_with_company_number()
    if not company_numbers:
        raise SystemExit("No mapped UK symbols with company numbers. Run refresh-uk-symbol-map first.")

    api_key = Config().companies_house_api_key
    if not api_key:
        raise SystemExit(
            "Companies House API key missing. Add [companies_house].api_key to private/config.toml."
        )

    client = CompaniesHouseClient(api_key=api_key)
    repo = UKCompanyFactsRepository(database)
    repo.initialize_schema()

    for symbol in company_numbers:
        number = mapper.fetch_company_number(symbol)
        if not number:
            continue
        payload = client.fetch_company_profile(number)
        repo.upsert_company_facts(number, payload, symbol=symbol)
        LOGGER.info("Stored Companies House facts for %s (%s)", symbol, number)

    print(f"Stored Companies House facts for {len(company_numbers)} symbols in {database}")
    return 0


def _resolve_company_number(symbol: str, mapper: UKSymbolMapRepository) -> str:
    number = mapper.fetch_company_number(symbol)
    if not number:
        raise SystemExit(f"No company number mapped for symbol {symbol}. Run refresh-uk-symbol-map first.")
    return number


def _pick_latest_ixbrl_filing(filing_history: Dict) -> Optional[Dict]:
    items = filing_history.get("items") or []
    for item in items:
        links = item.get("links") or {}
        meta = links.get("document_metadata")
        if not meta:
            continue
        # Prefer items whose description indicates accounts; category already filtered.
        return {"filing_id": item.get("transaction_id"), "metadata_url": meta, "item": item}
    return None


def cmd_ingest_uk_filings(symbol: str, database: str) -> int:
    mapper = UKSymbolMapRepository(database)
    mapper.initialize_schema()
    company_number = _resolve_company_number(symbol, mapper)

    api_key = Config().companies_house_api_key
    if not api_key:
        raise SystemExit("Companies House API key missing. Add [companies_house].api_key to private/config.toml.")

    client = CompaniesHouseClient(api_key=api_key)
    history = client.fetch_filing_history(company_number, category="accounts", items=100)
    target = _pick_latest_ixbrl_filing(history)
    if target is None:
        raise SystemExit(f"No accounts filings with document metadata found for {symbol} ({company_number})")

    meta = client.fetch_document_metadata(target["metadata_url"])
    doc_url = None
    resources = meta.get("resources") or {}
    xhtml = resources.get("application/xhtml+xml") or resources.get("text/html")
    if isinstance(xhtml, dict):
        # Prefer explicit content link if present.
        doc_url = (xhtml.get("links") or {}).get("content") or xhtml.get("url")
    if not doc_url:
        links = meta.get("links") or {}
        doc_url = links.get("document")
    if not doc_url:
        raise SystemExit("No XHTML document link found in Companies House metadata.")

    content = client.fetch_document(doc_url)

    repo = UKFilingRepository(database)
    repo.initialize_schema()
    repo.upsert_document(
        company_number=company_number,
        filing_id=target["filing_id"] or doc_url,
        content=content,
        symbol=symbol,
        period_start=None,
        period_end=None,
        doc_type="application/xhtml+xml",
        is_ixbrl=True,
    )

    print(f"Stored iXBRL filing for {symbol} ({company_number})")
    return 0


def cmd_ingest_us_facts(symbol: str, database: str, user_agent: Optional[str], cik: Optional[str]) -> int:
    """Fetch SEC company facts for a ticker and persist them."""

    client = SECCompanyFactsClient(user_agent=user_agent)
    symbol = _qualify_symbol(symbol, exchange="US", region="US")
    if cik:
        cik_value = cik
    else:
        info = client.resolve_company(symbol.split(".")[0])
        cik_value = info.cik
        symbol = _qualify_symbol(info.symbol, exchange="US", region="US")
        LOGGER.info("Resolved %s to CIK %s (%s)", symbol, cik_value, info.name)

    payload = client.fetch_company_facts(cik_value)

    fundamentals_repo = FundamentalsRepository(database)
    fundamentals_repo.initialize_schema()
    fundamentals_repo.upsert("SEC", symbol.upper(), payload, region="US", exchange="US")
    print(f"Stored SEC company facts for {symbol} ({cik_value}) in {database}")
    return 0


def cmd_ingest_us_facts_bulk(
    database: str,
    region: str,
    rate: float,
    user_agent: Optional[str],
) -> int:
    """Fetch SEC company facts for every symbol in the stored universe."""

    client = SECCompanyFactsClient(user_agent=user_agent)
    fundamentals_repo = FundamentalsRepository(database)
    fundamentals_repo.initialize_schema()

    universe_repo = UniverseRepository(database)
    symbols = universe_repo.fetch_symbols(region)
    if not symbols:
        raise SystemExit(f"No universe symbols found for region {region}. Run load-us-universe first.")

    min_interval = 1.0 / rate if rate and rate > 0 else 0.0
    last_fetch = 0.0
    total = len(symbols)
    processed = 0
    print(f"Fetching SEC company facts for {total} symbols at <= {rate:.2f} req/s")

    try:
        for idx, symbol in enumerate(symbols, 1):
            try:
                info = client.resolve_company(symbol.split(".")[0])
            except Exception as exc:  # pragma: no cover - rare network errors
                LOGGER.error("Failed to resolve CIK for %s: %s", symbol, exc)
                continue

            if min_interval > 0 and last_fetch:
                elapsed = time.perf_counter() - last_fetch
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)

            try:
                payload = client.fetch_company_facts(info.cik)
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error("Failed to fetch company facts for %s: %s", info.symbol, exc)
                last_fetch = time.perf_counter()
                continue

            last_fetch = time.perf_counter()
            qualified = _qualify_symbol(info.symbol, exchange="US", region="US")
            fundamentals_repo.upsert("SEC", qualified, payload, region=region, exchange="US")
            processed += 1
            print(f"[{idx}/{total}] Stored company facts for {qualified}", flush=True)
    except KeyboardInterrupt:
        print(f"\nCancelled after {processed} of {total} symbols.")
        return 1

    print(f"Stored company facts for {processed} symbols in {database}")
    return 0


def _require_eodhd_key() -> str:
    api_key = Config().eodhd_api_key
    if not api_key:
        raise SystemExit("EODHD API key missing. Add [eodhd].api_key to private/config.toml.")
    return api_key


def cmd_ingest_eodhd_fundamentals(
    symbol: str,
    database: str,
    exchange_code: Optional[str],
) -> int:
    """Fetch EODHD fundamentals for a ticker and store raw payload."""

    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    exchange_meta_repo = ExchangeMetadataRepository(database)
    exchange_meta_repo.initialize_schema()
    base_symbol = symbol.upper()
    inferred_exchange = None
    if "." in base_symbol:
        base, suffix = base_symbol.split(".", 1)
        base_symbol = base
        inferred_exchange = suffix
    exch_code = (exchange_code or inferred_exchange or "").upper() or None
    meta = client.exchange_metadata(exch_code) if exch_code else None
    region = (meta.get("Country") if meta else None) or exch_code or "INTL"
    qualified_symbol = _qualify_symbol(base_symbol, exch_code, region)
    fetch_symbol = qualified_symbol
    payload = client.fetch_fundamentals(fetch_symbol, exchange_code=None)
    if exch_code:
        exchange_meta_repo.upsert(
            exch_code,
            meta.get("Name") if meta else None,
            meta.get("Country") if meta else None,
            meta.get("Currency") if meta else None,
            (meta.get("OperatingMIC") or meta.get("OperatingMic")) if meta else None,
        )
    storage_symbol = qualified_symbol
    repo = FundamentalsRepository(database)
    repo.initialize_schema()
    general = payload.get("General") or {}
    repo.upsert(
        "EODHD",
        storage_symbol,
        payload,
        region=region,
        currency=general.get("CurrencyCode"),
        exchange=exch_code,
    )
    print(f"Stored EODHD fundamentals for {storage_symbol} in {database}")
    return 0


def cmd_ingest_eodhd_fundamentals_bulk(
    database: str,
    rate: float,
    exchange_code: Optional[str],
    region: Optional[str] = None,
) -> int:
    """Fetch EODHD fundamentals for an exchange or stored universe."""

    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    repo = FundamentalsRepository(database)
    repo.initialize_schema()
    exchange_meta_repo = ExchangeMetadataRepository(database)
    exchange_meta_repo.initialize_schema()

    listings: List[Tuple[str, Optional[str]]] = []
    region_label = None
    exch_code = (exchange_code or "").upper() or None
    if exch_code:
        meta = client.exchange_metadata(exch_code)
        if meta is None:
            raise SystemExit(f"Exchange {exchange_code} not found in EODHD exchange list.")
        exchange_meta_repo.upsert(
            exch_code,
            meta.get("Name"),
            meta.get("Country"),
            meta.get("Currency"),
            meta.get("OperatingMIC") or meta.get("OperatingMic"),
        )
        region_label = (meta.get("Country") or exch_code or "INTL").upper()
        rows = client.list_symbols(exch_code)
        for row in rows:
            code = (row.get("Code") or "").strip()
            if not code:
                continue
            sec_type = (row.get("Type") or "").upper()
            if sec_type == "ETF":
                continue
            qualified = _qualify_symbol(code, exch_code, region_label)
            listings.append((qualified, exch_code))
        if not listings:
            raise SystemExit(f"No symbols found for exchange {exchange_code} from EODHD.")
    else:
        if not region:
            raise SystemExit(
                "Provide --exchange-code (e.g., LSE) or --region to ingest EODHD fundamentals."
            )
        region_label = region.upper()
        universe_repo = UniverseRepository(database)
        universe_repo.initialize_schema()
        listings = universe_repo.fetch_symbol_exchanges(region_label)
        if not listings:
            raise SystemExit(
                f"No listings found for region {region_label}. Run load-eodhd-universe or load-us-universe first."
            )

    interval = 60.0 / rate if rate and rate > 0 else 0.0
    total = len(listings)
    processed = 0
    print(f"Fetching EODHD fundamentals for {total} symbols at <= {rate:.2f} per minute")

    try:
        for idx, (symbol, exchange) in enumerate(listings, 1):
            start = time.perf_counter()
            try:
                payload = client.fetch_fundamentals(symbol, exchange_code=None)
                general = payload.get("General") or {}
                repo.upsert(
                    "EODHD",
                    symbol,
                    payload,
                    region=region_label,
                    currency=general.get("CurrencyCode"),
                    exchange=exchange or exch_code,
                )
                processed += 1
                print(f"[{idx}/{total}] Stored fundamentals for {symbol.upper()}", flush=True)
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error("Failed to fetch fundamentals for %s: %s", symbol, exc)

            elapsed = time.perf_counter() - start
            if interval > 0 and elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        print(f"\nCancelled after {processed} of {total} symbols.")
        return 1

    print(f"Stored fundamentals for {processed} symbols in {database}")
    return 0


def cmd_ingest_fundamentals(
    provider: str,
    symbol: str,
    database: str,
    exchange_code: Optional[str],
    user_agent: Optional[str],
    cik: Optional[str],
) -> int:
    """Fetch fundamentals for a ticker from the specified provider."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        return cmd_ingest_us_facts(symbol=symbol, database=database, user_agent=user_agent, cik=cik)
    if provider_norm == "EODHD":
        return cmd_ingest_eodhd_fundamentals(symbol=symbol, database=database, exchange_code=exchange_code)
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_ingest_fundamentals_bulk(
    provider: str,
    database: str,
    region: Optional[str],
    rate: Optional[float],
    exchange_code: Optional[str],
    user_agent: Optional[str],
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    resume: bool,
) -> int:
    """Fetch fundamentals in bulk for the specified provider."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        region_label = (region or "US").upper()
        rate_value = rate if rate is not None else 9.0
        return cmd_ingest_us_facts_bulk(
            database=database,
            region=region_label,
            rate=rate_value,
            user_agent=user_agent,
        )
    if provider_norm == "EODHD":
        rate_value = rate if rate is not None else 600.0
        if exchange_code:
            exchange_norm = exchange_code.upper()
            symbols = _select_exchange_listings_for_provider(
                database=database,
                provider=provider_norm,
                exchange_code=exchange_norm,
                region=region,
                max_age_days=max_age_days,
                max_symbols=max_symbols,
                resume=resume,
            )
            if not symbols:
                print(f"No eligible listings found for exchange {exchange_norm}.")
                return 0

            api_key = _require_eodhd_key()
            client = EODHDFundamentalsClient(api_key=api_key)
            repo = FundamentalsRepository(database)
            repo.initialize_schema()
            state_repo = FundamentalsFetchStateRepository(database)
            state_repo.initialize_schema()

            interval = 60.0 / rate_value if rate_value and rate_value > 0 else 0.0
            total = len(symbols)
            processed = 0
            print(
                f"Fetching EODHD fundamentals for {total} symbols on {exchange_norm} "
                f"at <= {rate_value:.2f} per minute"
            )

            try:
                for idx, (symbol, region_label) in enumerate(symbols, 1):
                    start = time.perf_counter()
                    try:
                        payload = client.fetch_fundamentals(symbol, exchange_code=None)
                        general = payload.get("General") or {}
                        repo.upsert(
                            "EODHD",
                            symbol,
                            payload,
                            region=region_label,
                            currency=general.get("CurrencyCode"),
                            exchange=exchange_norm,
                        )
                        state_repo.mark_success("EODHD", symbol)
                        processed += 1
                        print(f"[{idx}/{total}] Stored fundamentals for {symbol.upper()}", flush=True)
                    except Exception as exc:  # pragma: no cover - network errors
                        LOGGER.error("Failed to fetch fundamentals for %s: %s", symbol, exc)
                        state_repo.mark_failure("EODHD", symbol, str(exc))

                    elapsed = time.perf_counter() - start
                    if interval > 0 and elapsed < interval:
                        time.sleep(interval - elapsed)
            except KeyboardInterrupt:
                print(f"\nCancelled after {processed} of {total} symbols.")
                return 1

            print(f"Stored fundamentals for {processed} symbols in {database}")
            return 0
        return cmd_ingest_eodhd_fundamentals_bulk(
            database=database,
            rate=rate_value,
            exchange_code=exchange_code,
            region=region,
        )
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_normalize_us_facts(symbol: str, database: str) -> int:
    """Normalize previously ingested SEC facts for downstream metrics."""

    symbol = _qualify_symbol(symbol, exchange="US", region="US")
    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    payload = fund_repo.fetch("SEC", symbol.upper())
    if payload is None:
        raise SystemExit(
            f"No raw SEC payload found for {symbol}. Run ingest-us-facts before normalization."
        )
    normalizer = SECFactsNormalizer()
    records = normalizer.normalize(payload, symbol=symbol.upper())

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    entity_name = payload.get("entityName")
    if entity_name:
        entity_repo.upsert(symbol.upper(), entity_name)
    stored = fact_repo.replace_facts(symbol.upper(), records)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_normalize_us_facts_bulk(
    database: str, region: str = "US", symbols: Optional[Sequence[str]] = None
) -> int:
    """Normalize raw SEC facts for every stored ticker."""

    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    normalization_repo = FinancialFactsRepository(database)
    normalization_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    region_label = region.upper()
    if symbols is None:
        symbols = fund_repo.symbols("SEC", region=region_label)
        if not symbols:
            raise SystemExit(
                "No raw SEC facts found. Run ingest-us-facts or ingest-us-facts-bulk first."
            )
    else:
        symbols = [symbol.upper() for symbol in symbols]
        if not symbols:
            raise SystemExit("No symbols provided for SEC normalization.")

    normalizer = SECFactsNormalizer()
    total = len(symbols)
    print(f"Normalizing SEC facts for {total} symbols")
    try:
        for idx, symbol in enumerate(symbols, 1):
            payload = fund_repo.fetch("SEC", symbol)
            if payload is None:
                continue
            records = normalizer.normalize(payload, symbol=symbol)
            entity_name = payload.get("entityName")
            if entity_name:
                entity_repo.upsert(symbol, entity_name)
            stored = normalization_repo.replace_facts(symbol, records)
            print(f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}", flush=True)
    except KeyboardInterrupt:
        print("\nBulk normalization cancelled by user.")
        return 1

    print(f"Normalized SEC facts for {total} symbols into {database}")
    return 0


def _extract_entity_name_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    return general.get("Name") or general.get("Code")


def cmd_normalize_eodhd_fundamentals(symbol: str, database: str) -> int:
    """Normalize stored EODHD fundamentals for downstream metrics."""

    fund_repo = FundamentalsRepository(database)
    payload = fund_repo.fetch("EODHD", symbol.upper())
    if payload is None:
        raise SystemExit(f"No EODHD fundamentals found for {symbol}. Run ingest-eodhd-fundamentals first.")

    normalizer = EODHDFactsNormalizer()
    records = normalizer.normalize(payload, symbol=symbol.upper())

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    entity_name = _extract_entity_name_from_eodhd(payload)
    if entity_name:
        entity_repo.upsert(symbol.upper(), entity_name)

    stored = fact_repo.replace_facts(symbol.upper(), records)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_normalize_eodhd_fundamentals_bulk(
    database: str, region: Optional[str], symbols: Optional[Sequence[str]] = None
) -> int:
    """Normalize all stored EODHD fundamentals."""

    fund_repo = FundamentalsRepository(database)
    if symbols is None:
        symbols = fund_repo.symbols("EODHD", region=region)
        if not symbols:
            raise SystemExit(
                "No EODHD fundamentals found. Run ingest-eodhd-fundamentals(-bulk) first."
            )
    else:
        symbols = [symbol.upper() for symbol in symbols]
        if not symbols:
            raise SystemExit("No symbols provided for EODHD normalization.")

    normalizer = EODHDFactsNormalizer()
    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    total = len(symbols)
    print(f"Normalizing EODHD fundamentals for {total} symbols")
    try:
        for idx, symbol in enumerate(symbols, 1):
            payload = fund_repo.fetch("EODHD", symbol)
            if payload is None:
                continue
            records = normalizer.normalize(payload, symbol=symbol)
            entity_name = _extract_entity_name_from_eodhd(payload)
            if entity_name:
                entity_repo.upsert(symbol, entity_name)
            stored = fact_repo.replace_facts(symbol, records)
            print(f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}", flush=True)
    except KeyboardInterrupt:
        print("\nBulk normalization cancelled by user.")
        return 1

    print(f"Normalized EODHD fundamentals for {total} symbols into {database}")
    return 0


def cmd_normalize_fundamentals(provider: str, symbol: str, database: str) -> int:
    """Normalize stored fundamentals for a ticker using the provider-specific ruleset."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        return cmd_normalize_us_facts(symbol=symbol, database=database)
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals(symbol=symbol, database=database)
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_normalize_fundamentals_bulk(
    provider: str, database: str, region: Optional[str], exchange_code: Optional[str]
) -> int:
    """Normalize stored fundamentals in bulk for the specified provider."""

    provider_norm = _normalize_provider(provider)
    if exchange_code:
        exchange_norm = exchange_code.upper()
        listings = _select_listing_symbols_by_exchange(
            database=database, exchange_code=exchange_norm, region=region
        )
        if not listings:
            raise SystemExit(
                f"No listings found for exchange {exchange_norm}. "
                "Run load-eodhd-universe or load-us-universe first."
            )
        fund_repo = FundamentalsRepository(database)
        fund_repo.initialize_schema()
        raw_symbols = set(fund_repo.symbols(provider_norm, region=region))
        symbols = [symbol for symbol in listings if symbol in raw_symbols]
        if not symbols:
            raise SystemExit(
                f"No {provider_norm} fundamentals found for exchange {exchange_norm}. "
                "Run ingest-fundamentals-bulk first."
            )
        if provider_norm == "SEC":
            region_label = (region or "US").upper()
            return cmd_normalize_us_facts_bulk(database=database, region=region_label, symbols=symbols)
        if provider_norm == "EODHD":
            return cmd_normalize_eodhd_fundamentals_bulk(
                database=database, region=region, symbols=symbols
            )
        raise SystemExit(f"Unsupported provider: {provider}")
    if provider_norm == "SEC":
        region_label = (region or "US").upper()
        return cmd_normalize_us_facts_bulk(database=database, region=region_label)
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals_bulk(database=database, region=region)
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_update_market_data(symbol: str, database: str) -> int:
    """Fetch latest market data for a ticker and store it."""

    service = MarketDataService(db_path=database)
    data = service.refresh_symbol(symbol)
    print(
        f"Stored market data for {data.symbol}: price={data.price} as_of={data.as_of} in {database}"
    )
    return 0


def cmd_update_market_data_bulk(
    database: str, region: Optional[str], rate: float, exchange_code: Optional[str] = None
) -> int:
    """Fetch market data for every stored listing."""

    universe_repo = UniverseRepository(database)
    universe_repo.initialize_schema()
    pairs: List[tuple[str, Optional[str], Optional[str]]] = []
    if exchange_code:
        exchange_norm = exchange_code.upper()
        region_label = region.upper() if region else None
        listing_rows = universe_repo.fetch_symbol_regions_by_exchange(exchange_norm, region=region_label)
        if listing_rows:
            pairs = [(symbol, exchange_norm, row_region) for symbol, row_region in listing_rows]
        else:
            fund_repo = FundamentalsRepository(database)
            fund_repo.initialize_schema()
            raw_pairs = fund_repo.symbol_exchanges("EODHD", region=region_label)
            pairs = [
                (symbol, exchange_norm, region_label)
                for symbol, exchange in raw_pairs
                if exchange and exchange.upper() == exchange_norm
            ]
        if not pairs:
            raise SystemExit(
                f"No symbols found for exchange {exchange_norm}. "
                "Load a universe or ingest fundamentals first."
            )
        effective_region = region_label
    else:
        region_label = (region or "US").upper()
        listing_pairs = universe_repo.fetch_symbol_exchanges(region_label)
        if not listing_pairs:
            fund_repo = FundamentalsRepository(database)
            fund_repo.initialize_schema()
            listing_pairs = fund_repo.symbol_exchanges("EODHD", region=region_label)
        if not listing_pairs:
            raise SystemExit(
                f"No symbols found for region {region_label}. Load universe or ingest fundamentals first."
            )
        pairs = [(symbol, exchange, region_label) for symbol, exchange in listing_pairs]
        effective_region = region_label

    service = MarketDataService(db_path=database)
    interval = 60.0 / rate if rate and rate > 0 else 0.0
    total = len(pairs)
    processed = 0
    print(f"Updating market data for {total} symbols at <= {rate:.2f} per minute")

    try:
        for idx, (symbol, exchange, row_region) in enumerate(pairs, 1):
            start = time.perf_counter()
            try:
                fetch_symbol = _format_market_symbol(symbol, exchange, row_region or effective_region)
                service.refresh_symbol(symbol, fetch_symbol=fetch_symbol)
                processed += 1
                print(f"[{idx}/{total}] Stored market data for {symbol}", flush=True)
            except Exception as exc:  # pragma: no cover - network failures
                LOGGER.error("Failed to refresh market data for %s: %s", symbol, exc)
            elapsed = time.perf_counter() - start
            if interval > 0 and elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        print(f"\nCancelled after {processed} of {total} symbols.")
        return 1

    print(f"Stored market data for {processed} symbols in {database}")
    return 0


def cmd_compute_metrics(
    symbol: str,
    metric_ids: Sequence[str],
    database: str,
    run_all: bool,
) -> int:
    """Compute one or more metrics and store the results."""

    db_path = _resolve_database_path(database)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    computed = 0
    symbol_upper = symbol.upper()
    market_repo: Optional[MarketDataRepository] = None
    ids_to_compute = list(REGISTRY.keys()) if run_all else list(metric_ids)
    for metric_id in ids_to_compute:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            raise SystemExit(f"Unknown metric id: {metric_id}")
        metric = metric_cls()
        if getattr(metric, "uses_market_data", False):
            if market_repo is None:
                market_repo = MarketDataRepository(db_path)
                market_repo.initialize_schema()
            result = metric.compute(symbol_upper, fact_repo, market_repo)
        else:
            result = metric.compute(symbol_upper, fact_repo)
        if result is None:
            LOGGER.warning("Metric %s could not be computed for %s", metric_id, symbol_upper)
            continue
        metrics_repo.upsert(result.symbol, result.metric_id, result.value, result.as_of)
        computed += 1
    print(f"Computed {computed} metrics for {symbol_upper} in {database}")
    return 0


def cmd_compute_metrics_bulk(
    database: str,
    region: Optional[str],
    metric_ids: Optional[Sequence[str]],
    exchange_code: Optional[str] = None,
) -> int:
    """Compute metrics for all listings stored in the universe."""

    db_path = _resolve_database_path(database)
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    symbols: List[str] = []
    if exchange_code:
        exchange_norm = exchange_code.upper()
        region_label = region.upper() if region else None
        pairs = universe_repo.fetch_symbol_regions_by_exchange(exchange_norm, region=region_label)
        symbols = [symbol for symbol, _ in pairs]
        if not symbols:
            raise SystemExit(
                f"No listings found for exchange {exchange_norm}. "
                "Run load-eodhd-universe or load-us-universe first."
            )
    else:
        region_label = (region or "US").upper()
        symbols = universe_repo.fetch_symbols(region_label)
        if not symbols:
            fund_repo = FundamentalsRepository(db_path)
            fund_repo.initialize_schema()
            symbols = fund_repo.symbols("EODHD", region=region_label)
        if not symbols:
            with universe_repo._connect() as conn:
                available_regions = [
                    row[0] for row in conn.execute("SELECT DISTINCT region FROM listings").fetchall()
                ]
            raise SystemExit(
                f"No symbols found for region {region_label}. Load a universe or ingest fundamentals first. "
                f"Available regions: {', '.join(available_regions) if available_regions else 'none'}. "
                f"Database: {db_path}"
            )

    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    ids_to_compute = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    if not ids_to_compute:
        raise SystemExit("No metrics specified.")

    total_symbols = len(symbols)
    print(f"Computing metrics for {total_symbols} symbols ({len(ids_to_compute)} metrics each)")

    try:
        for idx, symbol in enumerate(symbols, 1):
            symbol_upper = symbol.upper()
            computed = 0
            for metric_id in ids_to_compute:
                metric_cls = REGISTRY.get(metric_id)
                if metric_cls is None:
                    LOGGER.warning("Unknown metric id: %s", metric_id)
                    continue
                metric = metric_cls()
                try:
                    if getattr(metric, "uses_market_data", False):
                        result = metric.compute(symbol_upper, fact_repo, market_repo)
                    else:
                        result = metric.compute(symbol_upper, fact_repo)
                except Exception as exc:  # pragma: no cover - metric errors
                    LOGGER.error("Metric %s failed for %s: %s", metric_id, symbol_upper, exc)
                    continue
                if result is None:
                    LOGGER.warning("Metric %s could not be computed for %s", metric_id, symbol_upper)
                    continue
                metrics_repo.upsert(result.symbol, result.metric_id, result.value, result.as_of)
                computed += 1
            print(f"[{idx}/{total_symbols}] Computed {computed} metrics for {symbol_upper}", flush=True)
    except KeyboardInterrupt:
        print("\nBulk metric computation cancelled by user.")
        return 1

    print(f"Computed metrics for {total_symbols} symbols in {database}")
    return 0


def cmd_report_fact_freshness(
    database: str,
    region: str,
    metric_ids: Optional[Sequence[str]],
    max_age_days: int,
    output_csv: Optional[str],
    show_all: bool,
) -> int:
    """Report missing or stale financial facts needed by metrics for a region."""

    db_path = _resolve_database_path(database)
    region_label = region.upper()
    symbols = _symbols_for_region_or_raise(db_path, region_label)
    if not symbols:
        raise SystemExit(f"No symbols found for region {region_label}.")

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    coverage = compute_fact_coverage(
        fact_repo,
        symbols,
        metric_classes,
        max_age_days=max_age_days,
    )

    print(f"Fact coverage for region {region_label} ({len(symbols)} symbols, max_age_days={max_age_days})")
    for entry in coverage:
        missing_total = sum(c.missing for c in entry.concepts)
        stale_total = sum(c.stale for c in entry.concepts)
        print(
            f"- {entry.metric_id}: fully_fresh={entry.fully_covered}/{entry.total_symbols}, "
            f"missing={missing_total}, stale={stale_total}"
        )
        for concept in entry.concepts:
            if not show_all and concept.missing == 0 and concept.stale == 0:
                continue
            fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
            print(f"    {concept.concept}: fresh={fresh}, stale={concept.stale}, missing={concept.missing}")
    if output_csv:
        _write_fact_report_csv(coverage, output_csv)
        print(f"Wrote concept-level coverage to {output_csv}")
    return 0


def cmd_report_metric_coverage(
    database: str,
    region: str,
    metric_ids: Optional[Sequence[str]],
) -> int:
    """Count symbols that can compute all requested metrics (without persisting results)."""

    db_path = _resolve_database_path(database)
    region_label = region.upper()
    symbols = _symbols_for_region_or_raise(db_path, region_label)
    if not symbols:
        raise SystemExit(f"No symbols found for region {region_label}.")

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    per_metric_success: Dict[str, int] = {getattr(cls, "id", cls.__name__): 0 for cls in metric_classes}
    all_success = 0

    for symbol in symbols:
        symbol_ok = True
        for metric_cls in metric_classes:
            metric = metric_cls()
            try:
                if getattr(metric, "uses_market_data", False):
                    result = metric.compute(symbol, fact_repo, market_repo)
                else:
                    result = metric.compute(symbol, fact_repo)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.error("Metric %s failed for %s: %s", getattr(metric_cls, "id", metric_cls.__name__), symbol, exc)
                result = None
            if result is None:
                symbol_ok = False
                continue
            per_metric_success[getattr(metric_cls, "id", metric_cls.__name__)] += 1
        if symbol_ok and metric_classes:
            all_success += 1

    total_symbols = len(symbols)
    print(f"Metric coverage for region {region_label} (symbols={total_symbols}, metrics={len(metric_classes)})")
    print(f"Symbols where all metrics computed: {all_success}/{total_symbols}")
    for metric_id, count in per_metric_success.items():
        print(f"- {metric_id}: {count}/{total_symbols} symbols")
    return 0


class _MetricWarningCollector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.records.append(record)

    def clear(self) -> None:
        self.records.clear()


_DATE_PATTERN = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")


def _format_failure_reason(records: Sequence[logging.LogRecord], symbol: str) -> str:
    if not records:
        return "no warning emitted"

    record = records[0]
    msg = record.msg if isinstance(record.msg, str) else str(record.msg)
    args = record.args
    if not args:
        return msg

    def transform(arg: object) -> object:
        if isinstance(arg, str):
            if arg.upper() == symbol.upper():
                return "<symbol>"
            if _DATE_PATTERN.match(arg):
                return "<date>"
            return arg
        if isinstance(arg, (int, float)):
            return "<n>"
        return arg

    try:
        if isinstance(args, dict):
            transformed = {key: transform(value) for key, value in args.items()}
            return msg % transformed
        if not isinstance(args, tuple):
            args = (args,)
        transformed = tuple(transform(value) for value in args)
        return msg % transformed
    except Exception:
        return record.getMessage()


def _write_metric_failure_report_csv(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    total_symbols: int,
    path: str,
) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "reason",
                "count",
                "total_symbols",
                "failure_rate",
                "example_symbol",
                "example_market_cap",
            ]
        )
        for metric_id, counter in failures.items():
            if not counter:
                writer.writerow([metric_id, "", 0, total_symbols, 0.0, "", ""])
                continue
            for reason, count in counter.most_common():
                rate = (count / total_symbols) if total_symbols else 0.0
                example = examples.get(metric_id, {}).get(reason)
                example_symbol = example[0] if example else ""
                example_cap = example[1] if example else None
                writer.writerow([metric_id, reason, count, total_symbols, rate, example_symbol, example_cap or ""])


def cmd_report_metric_failures(
    database: str,
    region: str,
    metric_ids: Optional[Sequence[str]],
    symbols: Optional[Sequence[str]],
    output_csv: Optional[str],
) -> int:
    """Summarize warning reasons for metric computation failures."""

    db_path = _resolve_database_path(database)
    region_label = region.upper()
    if symbols:
        selected: List[str] = []
        for entry in symbols:
            for symbol in entry.split(","):
                symbol = symbol.strip()
                if not symbol:
                    continue
                selected.append(_qualify_symbol(symbol, region=region_label))
        symbols = list(dict.fromkeys(selected))
    else:
        symbols = _symbols_for_region_or_raise(db_path, region_label)
        if not symbols:
            raise SystemExit(f"No symbols found for region {region_label}.")

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    failures: Dict[str, Counter] = {getattr(cls, "id", cls.__name__): Counter() for cls in metric_classes}
    totals: Dict[str, int] = {getattr(cls, "id", cls.__name__): 0 for cls in metric_classes}
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        getattr(cls, "id", cls.__name__): {} for cls in metric_classes
    }
    market_caps: Dict[str, Optional[float]] = {}
    handler = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        for symbol in symbols:
            symbol_upper = symbol.upper()
            for metric_cls in metric_classes:
                metric_id = getattr(metric_cls, "id", metric_cls.__name__)
                handler.clear()
                metric = metric_cls()
                try:
                    if getattr(metric, "uses_market_data", False):
                        result = metric.compute(symbol_upper, fact_repo, market_repo)
                    else:
                        result = metric.compute(symbol_upper, fact_repo)
                except Exception as exc:  # pragma: no cover - defensive
                    reason = f"exception: {exc.__class__.__name__}"
                    failures[metric_id][reason] += 1
                    totals[metric_id] += 1
                    continue
                if result is None:
                    reason = _format_failure_reason(handler.records, symbol_upper)
                    failures[metric_id][reason] += 1
                    totals[metric_id] += 1
                    cap = market_caps.get(symbol_upper)
                    if symbol_upper not in market_caps:
                        snapshot = market_repo.latest_snapshot(symbol_upper)
                        cap = snapshot.market_cap if snapshot else None
                        market_caps[symbol_upper] = cap
                    current = examples[metric_id].get(reason)
                    if current is None:
                        examples[metric_id][reason] = (symbol_upper, cap)
                    else:
                        current_cap = current[1]
                        if cap is not None and (current_cap is None or cap > current_cap):
                            examples[metric_id][reason] = (symbol_upper, cap)
    finally:
        root_logger.removeHandler(handler)

    total_symbols = len(symbols)
    print(f"Metric failure reasons for region {region_label} (symbols={total_symbols}, metrics={len(metric_classes)})")
    for metric_id in [getattr(cls, "id", cls.__name__) for cls in metric_classes]:
        total_failures = totals.get(metric_id, 0)
        print(f"- {metric_id}: failures={total_failures}/{total_symbols}")
        counter = failures.get(metric_id)
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = examples.get(metric_id, {}).get(reason)
            if example:
                example_symbol, example_cap = example
                cap_display = _format_value(example_cap) if example_cap is not None else "N/A"
                print(f"    {reason}: {count} (example={example_symbol}, market_cap={cap_display})")
            else:
                print(f"    {reason}: {count}")

    if output_csv:
        _write_metric_failure_report_csv(failures, examples, total_symbols, output_csv)
        print(f"Wrote metric failure reasons to {output_csv}")
    return 0


def _eligible_sec_filers(db_path: Path) -> List[str]:
    """Return symbols that have at least one 10-K/10-Q filing in SEC raw facts."""

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    allowed = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
    eligible: set[str] = set()
    with repo._connect() as conn:
        rows = conn.execute(
            "SELECT symbol, data FROM fundamentals_raw WHERE provider = 'SEC'"
        ).fetchall()
    for symbol, payload_json in rows:
        try:
            data = json.loads(payload_json)
        except Exception:
            continue
        facts = data.get("facts", {}).get("us-gaap", {}) or {}
        for detail in facts.values():
            units = detail.get("units", {}) if isinstance(detail, dict) else {}
            for entries in units.values():
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    form = item.get("form")
                    if form in allowed:
                        eligible.add(symbol.upper())
                        break
                if symbol.upper() in eligible:
                    break
            if symbol.upper() in eligible:
                break
    return sorted(eligible)


def cmd_purge_us_nonfilers(database: str, apply: bool) -> int:
    """Remove US listings with no 10-K/10-Q filings stored in SEC facts."""

    db_path = _resolve_database_path(database)
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    with universe_repo._connect() as conn:
        us_symbols = [row[0] for row in conn.execute("SELECT symbol FROM listings WHERE region = 'US'")]

    eligible = set(_eligible_sec_filers(db_path))
    to_remove = sorted([sym for sym in us_symbols if sym.upper() not in eligible])
    if not to_remove:
        print("No US non-filers found to purge.")
        return 0

    print(f"Found {len(to_remove)} US listings without 10-K/10-Q filings.")
    for sym in to_remove:
        print(f"- {sym}")
    if not apply:
        print("Dry run only. Re-run with --apply to delete from listings.")
        return 0

    with universe_repo._connect() as conn:
        conn.executemany("DELETE FROM listings WHERE symbol = ? AND region = 'US'", [(sym,) for sym in to_remove])
    print(f"Deleted {len(to_remove)} US listings from listings table.")
    return 0


def cmd_recalc_market_cap(database: str) -> int:
    """Recompute market cap values for stored market data."""

    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(database)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)

    with market_repo._connect() as conn:
        symbols = [row[0] for row in conn.execute("SELECT DISTINCT symbol FROM market_data ORDER BY symbol")]
    if not symbols:
        print("No market data found to update.")
        return 0

    total = len(symbols)
    updated_rows = 0
    print(f"Recomputing market cap for {total} symbols")
    try:
        with market_repo._connect() as conn:
            for idx, symbol in enumerate(symbols, 1):
                shares = latest_share_count(symbol, fact_repo)
                if shares is None or shares <= 0:
                    LOGGER.warning("Skipping %s due to missing share count", symbol)
                    continue
                cursor = conn.execute(
                    "UPDATE market_data SET market_cap = price * ? WHERE symbol = ?",
                    (shares, symbol),
                )
                updated_rows += cursor.rowcount or 0
                print(f"[{idx}/{total}] Updated market cap for {symbol}", flush=True)
    except KeyboardInterrupt:
        print("\nMarket cap recalculation cancelled by user.")
        return 1

    print(f"Updated market cap for {updated_rows} rows in {database}")
    return 0


def cmd_clear_listings(database: str) -> int:
    """Delete all stored listings."""

    repo = UniverseRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS listings")
    repo.initialize_schema()
    print(f"Cleared listings table in {database}")
    return 0


def cmd_clear_financial_facts(database: str) -> int:
    """Delete all normalized financial facts."""

    repo = FinancialFactsRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS financial_facts")
    repo.initialize_schema()
    print(f"Cleared financial_facts table in {database}")
    return 0


def cmd_clear_fundamentals_raw(database: str) -> int:
    """Delete all stored raw fundamentals."""

    repo = FundamentalsRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS fundamentals_raw")
    repo.initialize_schema()
    print(f"Cleared fundamentals_raw table in {database}")
    return 0


def cmd_clear_metrics(database: str) -> int:
    """Delete all computed metrics."""

    repo = MetricsRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS metrics")
    repo.initialize_schema()
    print(f"Cleared metrics table in {database}")
    return 0


def cmd_refresh_uk_symbol_map(database: str, gleif_url: str, isin_date: Optional[str], region: str) -> int:
    """Refresh UK symbol -> company number mapping using GLEIF and stored ISINs."""

    client = GLEIFClient(golden_url=gleif_url)
    golden_body = client.fetch_golden_csv()
    isin_body = client.fetch_isin_csv(as_of=isin_date)
    mapping = client.isin_to_company_number(golden_body, isin_body)
    if not mapping:
        golden_count = len(client._parse_golden(golden_body))
        isin_count = len(client._parse_isin(isin_body))
        raise SystemExit(f"No GLEIF mappings found (golden rows: {golden_count}, ISIN rows: {isin_count}); aborting.")

    universe_repo = UniverseRepository(database)
    universe_repo.initialize_schema()
    symbols = universe_repo.fetch_symbols(region=region)
    if not symbols:
        raise SystemExit(f"No listings stored for region {region}. Run load-eodhd-universe first.")

    # Fetch ISINs for UK listings.
    with universe_repo._connect() as conn:
        rows = conn.execute(
            "SELECT symbol, isin FROM listings WHERE region = ? AND isin IS NOT NULL",
            (region,),
        ).fetchall()
    isin_rows = [(row[0], row[1]) for row in rows]

    mapper = UKSymbolMapRepository(database)
    mapper.initialize_schema()

    updates = []
    for symbol, isin in isin_rows:
        if not isin:
            continue
        entry = mapping.get(isin)
        if not entry:
            continue
        updates.append((symbol, isin, entry.get("lei"), entry.get("company_number")))

    applied = mapper.bulk_upsert(updates)
    print(f"Updated UK symbol map for {applied} symbols")
    return 0


def cmd_run_screen(symbol: str, config_path: str, database: str) -> int:
    """Evaluate screening criteria against stored/derived metrics."""

    definition = load_screen(config_path)
    metrics_repo = MetricsRepository(database)
    metrics_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    results = []
    for criterion in definition.criteria:
        passed, left_value = evaluate_criterion_verbose(
            criterion, symbol.upper(), metrics_repo, fact_repo, market_repo
        )
        results.append((criterion.name, passed, left_value))
    passed_all = all(flag for _, _, flag in results)
    for name, passed, value in results:
        value_display = _format_value(value) if value is not None else "N/A"
        print(f"{name}: {'PASS' if passed else 'FAIL'} (value={value_display})")
    return 0 if passed_all else 1


def cmd_run_screen_bulk(
    config_path: str,
    database: str,
    region: Optional[str],
    output_csv: Optional[str],
    exchange_code: Optional[str] = None,
) -> int:
    """Evaluate screening criteria for every ticker stored in the universe."""

    definition = load_screen(config_path)
    output_csv = output_csv or DEFAULT_SCREEN_RESULTS_CSV
    universe_repo = UniverseRepository(database)
    universe_repo.initialize_schema()
    symbols: List[str] = []
    region_label = region.upper() if region else None
    if exchange_code:
        exchange_norm = exchange_code.upper()
        pairs = universe_repo.fetch_symbol_regions_by_exchange(exchange_norm, region=region_label)
        symbols = [symbol for symbol, _ in pairs]
        if not symbols:
            fund_repo = FundamentalsRepository(database)
            fund_repo.initialize_schema()
            raw_pairs = fund_repo.symbol_exchanges("EODHD", region=region_label)
            symbols = [
                symbol
                for symbol, exchange in raw_pairs
                if exchange and exchange.upper() == exchange_norm
            ]
        if not symbols:
            raise SystemExit(
                f"No symbols found for exchange {exchange_norm}. Load universe or ingest fundamentals first."
            )
    else:
        region_label = (region or "US").upper()
        symbols = universe_repo.fetch_symbols(region_label)
        if not symbols:
            fund_repo = FundamentalsRepository(database)
            fund_repo.initialize_schema()
            symbols = fund_repo.symbols("EODHD", region=region_label)
        if not symbols:
            raise SystemExit(
                f"No symbols found for region {region_label}. Load universe or ingest fundamentals first."
            )

    metrics_repo = MetricsRepository(database)
    metrics_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    with universe_repo._connect() as conn:
        if exchange_code:
            query = ["SELECT symbol, security_name FROM listings WHERE UPPER(exchange) = ?"]
            params: List[str] = [exchange_norm]
            if region_label:
                query.append("AND region = ?")
                params.append(region_label)
            sql = " ".join(query)
            name_rows = conn.execute(sql, params).fetchall()
        else:
            name_rows = conn.execute(
                "SELECT symbol, security_name FROM listings WHERE region = ?",
                (region_label,),
            ).fetchall()
    universe_names = {row[0].upper(): (row[1] or row[0].upper()) for row in name_rows}
    entity_labels: Dict[str, str] = {}
    passed_symbols: List[str] = []
    criterion_values: Dict[str, Dict[str, float]] = {c.name: {} for c in definition.criteria}

    for symbol in symbols:
        symbol_upper = symbol.upper()
        symbol_passed = True
        per_symbol_values: Dict[str, float] = {}
        label = entity_labels.get(symbol_upper)
        if label is None:
            label = entity_repo.fetch(symbol_upper) or universe_names.get(symbol_upper) or symbol_upper
            entity_labels[symbol_upper] = label
        for criterion in definition.criteria:
            passed, left_value = evaluate_criterion_verbose(
                criterion, symbol_upper, metrics_repo, fact_repo, market_repo
            )
            if not passed or left_value is None:
                symbol_passed = False
                break
            per_symbol_values[criterion.name] = left_value
        if symbol_passed:
            passed_symbols.append(symbol_upper)
            for criterion in definition.criteria:
                criterion_values[criterion.name][symbol_upper] = per_symbol_values[criterion.name]

    if not passed_symbols:
        print("No symbols satisfied all criteria.")
        if output_csv:
            _write_screen_csv(definition.criteria, [], {}, {}, output_csv)
        return 1

    selected_names = {symbol: entity_labels.get(symbol, symbol) for symbol in passed_symbols}
    _print_screen_table(definition.criteria, passed_symbols, criterion_values, selected_names)
    if output_csv:
        _write_screen_csv(definition.criteria, passed_symbols, criterion_values, selected_names, output_csv)
    return 0


def _print_screen_table(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
) -> None:
    header = ["Criterion"] + list(symbols)
    rows: List[List[str]] = [header]
    rows.append(["Entity"] + [entity_names.get(symbol, symbol) for symbol in symbols])
    for criterion in criteria:
        row = [criterion.name]
        for symbol in symbols:
            value = values.get(criterion.name, {}).get(symbol)
            row.append(_format_value(value) if value is not None else "N/A")
        rows.append(row)
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for row in rows:
        print(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def _format_value(value: float) -> str:
    formatted = f"{value:,.4f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _write_screen_csv(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    path: str,
) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Criterion", *symbols])
        writer.writerow(["Entity", *[entity_names.get(symbol, symbol) for symbol in symbols]])
        for criterion in criteria:
            row = [criterion.name]
            for symbol in symbols:
                value = values.get(criterion.name, {}).get(symbol)
                row.append("" if value is None else _format_value(value))
            writer.writerow(row)


def _write_fact_report_csv(report: Sequence[MetricCoverage], path: str) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric_id", "concept", "missing", "stale", "fresh", "fully_covered", "total_symbols"])
        for entry in report:
            if not entry.concepts:
                writer.writerow(
                    [entry.metric_id, "", 0, 0, entry.total_symbols, entry.fully_covered, entry.total_symbols]
                )
                continue
            for concept in entry.concepts:
                fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
                writer.writerow(
                    [
                        entry.metric_id,
                        concept.concept,
                        concept.missing,
                        concept.stale,
                        fresh,
                        entry.fully_covered,
                        entry.total_symbols,
                    ]
                )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    setup_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "load-us-universe":
        return cmd_load_us_universe(database=args.database, include_etfs=args.include_etfs)
    if args.command == "load-eodhd-universe":
        return cmd_load_eodhd_universe(
            database=args.database,
            include_etfs=args.include_etfs,
            exchange_code=args.exchange_code,
            currencies=args.currencies,
        )
    if args.command == "ingest-uk-facts":
        return cmd_ingest_uk_facts(
            company_number=args.company_number,
            database=args.database,
            symbol=args.symbol,
        )
    if args.command == "ingest-uk-facts-bulk":
        return cmd_ingest_uk_facts_bulk(database=args.database)
    if args.command == "ingest-uk-filings":
        return cmd_ingest_uk_filings(symbol=args.symbol, database=args.database)
    if args.command == "refresh-uk-symbol-map":
        return cmd_refresh_uk_symbol_map(
            database=args.database,
            gleif_url=args.gleif_url,
            isin_date=args.isin_date,
            region=args.region,
        )
    if args.command == "ingest-us-facts":
        return cmd_ingest_us_facts(
            symbol=args.symbol,
            database=args.database,
            user_agent=args.user_agent,
            cik=args.cik,
        )
    if args.command == "ingest-us-facts-bulk":
        return cmd_ingest_us_facts_bulk(
            database=args.database,
            region=args.region,
            rate=args.rate,
            user_agent=args.user_agent,
        )
    if args.command == "normalize-us-facts":
        return cmd_normalize_us_facts(symbol=args.symbol, database=args.database)
    if args.command == "normalize-us-facts-bulk":
        return cmd_normalize_us_facts_bulk(database=args.database)
    if args.command == "ingest-eodhd-fundamentals":
        return cmd_ingest_eodhd_fundamentals(
            symbol=args.symbol,
            database=args.database,
            exchange_code=args.exchange_code,
        )
    if args.command == "ingest-eodhd-fundamentals-bulk":
        return cmd_ingest_eodhd_fundamentals_bulk(
            database=args.database,
            rate=args.rate,
            exchange_code=args.exchange_code,
            region=args.region,
        )
    if args.command == "ingest-fundamentals":
        return cmd_ingest_fundamentals(
            provider=args.provider,
            symbol=args.symbol,
            database=args.database,
            exchange_code=args.exchange_code,
            user_agent=args.user_agent,
            cik=args.cik,
        )
    if args.command == "ingest-fundamentals-bulk":
        return cmd_ingest_fundamentals_bulk(
            provider=args.provider,
            database=args.database,
            region=args.region,
            rate=args.rate,
            exchange_code=args.exchange_code,
            user_agent=args.user_agent,
            max_symbols=args.max_symbols,
            max_age_days=args.max_age_days,
            resume=args.resume,
        )
    if args.command == "normalize-eodhd-fundamentals":
        return cmd_normalize_eodhd_fundamentals(symbol=args.symbol, database=args.database)
    if args.command == "normalize-eodhd-fundamentals-bulk":
        return cmd_normalize_eodhd_fundamentals_bulk(database=args.database, region=args.region)
    if args.command == "normalize-fundamentals":
        return cmd_normalize_fundamentals(
            provider=args.provider,
            symbol=args.symbol,
            database=args.database,
        )
    if args.command == "normalize-fundamentals-bulk":
        return cmd_normalize_fundamentals_bulk(
            provider=args.provider,
            database=args.database,
            region=args.region,
            exchange_code=args.exchange_code,
        )
    if args.command == "update-market-data":
        return cmd_update_market_data(symbol=args.symbol, database=args.database)
    if args.command == "update-market-data-bulk":
        return cmd_update_market_data_bulk(
            database=args.database,
            region=args.region,
            rate=args.rate,
            exchange_code=args.exchange_code,
        )
    if args.command == "clear-listings":
        return cmd_clear_listings(database=args.database)
    if args.command == "clear-financial-facts":
        return cmd_clear_financial_facts(database=args.database)
    if args.command == "clear-fundamentals-raw":
        return cmd_clear_fundamentals_raw(database=args.database)
    if args.command == "clear-metrics":
        return cmd_clear_metrics(database=args.database)
    if args.command == "compute-metrics":
        return cmd_compute_metrics(
            symbol=args.symbol,
            metric_ids=args.metrics,
            database=args.database,
            run_all=args.all,
        )
    if args.command == "compute-metrics-bulk":
        return cmd_compute_metrics_bulk(
            database=args.database,
            region=args.region,
            metric_ids=args.metrics,
            exchange_code=args.exchange_code,
        )
    if args.command == "report-fact-freshness":
        return cmd_report_fact_freshness(
            database=args.database,
            region=args.region,
            metric_ids=args.metrics,
            max_age_days=args.max_age_days,
            output_csv=args.output_csv,
            show_all=args.show_all,
        )
    if args.command == "report-metric-coverage":
        return cmd_report_metric_coverage(
            database=args.database,
            region=args.region,
            metric_ids=args.metrics,
        )
    if args.command == "report-metric-failures":
        return cmd_report_metric_failures(
            database=args.database,
            region=args.region,
            metric_ids=args.metrics,
            symbols=args.symbols,
            output_csv=args.output_csv,
        )
    if args.command == "recalc-market-cap":
        return cmd_recalc_market_cap(database=args.database)
    if args.command == "run-screen":
        return cmd_run_screen(symbol=args.symbol, config_path=args.config, database=args.database)
    if args.command == "run-screen-bulk":
        return cmd_run_screen_bulk(
            config_path=args.config,
            database=args.database,
            region=args.region,
            output_csv=args.output_csv,
            exchange_code=args.exchange_code,
        )
    if args.command == "purge-us-nonfilers":
        return cmd_purge_us_nonfilers(database=args.database, apply=args.apply)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
