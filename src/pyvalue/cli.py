"""Command line utilities for pyvalue.

Author: Emre Tezel
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Optional, Sequence

from pyvalue.ingestion import SECCompanyFactsClient
from pyvalue.marketdata.service import MarketDataService
from pyvalue.metrics import REGISTRY
from pyvalue.normalization import SECFactsNormalizer
from pyvalue.screening import evaluate_criterion, load_screen
from pyvalue.storage import (
    CompanyFactsRepository,
    FinancialFactsRepository,
    MarketDataRepository,
    MetricsRepository,
    UniverseRepository,
)
from pyvalue.universe import USUniverseLoader

LOGGER = logging.getLogger(__name__)


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
        default="US",
        help="Universe region key stored in SQLite (default: %(default)s)",
    )
    bulk_market_data.add_argument(
        "--rate",
        type=float,
        default=950.0,
        help="Throttle speed in symbols per minute (default: %(default)s)",
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


def cmd_ingest_us_facts(symbol: str, database: str, user_agent: Optional[str], cik: Optional[str]) -> int:
    """Fetch SEC company facts for a ticker and persist them."""

    client = SECCompanyFactsClient(user_agent=user_agent)
    if cik:
        cik_value = cik
    else:
        info = client.resolve_company(symbol)
        cik_value = info.cik
        symbol = info.symbol
        LOGGER.info("Resolved %s to CIK %s (%s)", symbol, cik_value, info.name)

    payload = client.fetch_company_facts(cik_value)

    repo = CompanyFactsRepository(database)
    repo.initialize_schema()
    repo.upsert_company_facts(symbol=symbol.upper(), cik=cik_value, payload=payload)
    print(f"Stored SEC company facts for {symbol} ({cik_value}) in {database}")
    return 0


def cmd_ingest_us_facts_bulk(
    database: str,
    region: str,
    rate: float,
    user_agent: Optional[str],
) -> int:
    """Fetch SEC company facts for every symbol in the stored universe."""

    universe_repo = UniverseRepository(database)
    symbols = universe_repo.fetch_symbols(region)
    if not symbols:
        raise SystemExit(f"No universe symbols found for region {region}. Run load-us-universe first.")

    client = SECCompanyFactsClient(user_agent=user_agent)
    company_repo = CompanyFactsRepository(database)
    company_repo.initialize_schema()

    min_interval = 1.0 / rate if rate and rate > 0 else 0.0
    last_fetch = 0.0
    total = len(symbols)
    processed = 0
    print(f"Fetching SEC company facts for {total} symbols at <= {rate:.2f} req/s")

    try:
        for idx, symbol in enumerate(symbols, 1):
            try:
                info = client.resolve_company(symbol)
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
            company_repo.upsert_company_facts(info.symbol, info.cik, payload)
            processed += 1
            print(f"[{idx}/{total}] Stored company facts for {info.symbol}", flush=True)
    except KeyboardInterrupt:
        print(f"\nCancelled after {processed} of {total} symbols.")
        return 1

    print(f"Stored company facts for {processed} symbols in {database}")
    return 0


def cmd_normalize_us_facts(symbol: str, database: str) -> int:
    """Normalize previously ingested SEC facts for downstream metrics."""

    company_repo = CompanyFactsRepository(database)
    record = company_repo.fetch_fact_record(symbol.upper())
    if record is None:
        raise SystemExit(
            f"No raw SEC payload found for {symbol}. Run ingest-us-facts before normalization."
        )
    cik_value, payload = record
    normalizer = SECFactsNormalizer()
    records = normalizer.normalize(payload, symbol=symbol.upper(), cik=cik_value)

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    stored = fact_repo.replace_facts(symbol.upper(), records)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_update_market_data(symbol: str, database: str) -> int:
    """Fetch latest market data for a ticker and store it."""

    service = MarketDataService(db_path=database)
    data = service.refresh_symbol(symbol)
    print(
        f"Stored market data for {data.symbol}: price={data.price} as_of={data.as_of} in {database}"
    )
    return 0


def cmd_update_market_data_bulk(database: str, region: str, rate: float) -> int:
    """Fetch market data for every stored listing."""

    universe_repo = UniverseRepository(database)
    symbols = universe_repo.fetch_symbols(region)
    if not symbols:
        raise SystemExit(f"No universe symbols found for region {region}. Run load-us-universe first.")

    service = MarketDataService(db_path=database)
    interval = 60.0 / rate if rate and rate > 0 else 0.0
    total = len(symbols)
    processed = 0
    print(f"Updating market data for {total} symbols at <= {rate:.2f} per minute")

    try:
        for idx, symbol in enumerate(symbols, 1):
            start = time.perf_counter()
            try:
                service.refresh_symbol(symbol)
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


def cmd_compute_metrics(symbol: str, metric_ids: Sequence[str], database: str, run_all: bool) -> int:
    """Compute one or more metrics and store the results."""

    fact_repo = FinancialFactsRepository(database)
    metrics_repo = MetricsRepository(database)
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
                market_repo = MarketDataRepository(database)
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


def cmd_run_screen(symbol: str, config_path: str, database: str) -> int:
    """Evaluate screening criteria against stored/derived metrics."""

    definition = load_screen(config_path)
    metrics_repo = MetricsRepository(database)
    metrics_repo.initialize_schema()
    fact_repo = FinancialFactsRepository(database)
    results = []
    for criterion in definition.criteria:
        passed = evaluate_criterion(criterion, symbol.upper(), metrics_repo, fact_repo)
        results.append((criterion.name, passed))
    passed_all = all(flag for _, flag in results)
    for name, flag in results:
        print(f"{name}: {'PASS' if flag else 'FAIL'}")
    return 0 if passed_all else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "load-us-universe":
        return cmd_load_us_universe(database=args.database, include_etfs=args.include_etfs)
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
    if args.command == "update-market-data":
        return cmd_update_market_data(symbol=args.symbol, database=args.database)
    if args.command == "update-market-data-bulk":
        return cmd_update_market_data_bulk(
            database=args.database,
            region=args.region,
            rate=args.rate,
        )
    if args.command == "compute-metrics":
        return cmd_compute_metrics(
            symbol=args.symbol,
            metric_ids=args.metrics,
            database=args.database,
            run_all=args.all,
        )
    if args.command == "run-screen":
        return cmd_run_screen(symbol=args.symbol, config_path=args.config, database=args.database)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
