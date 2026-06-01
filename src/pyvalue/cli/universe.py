"""CLI handlers for loading universes and refreshing supported exchanges/tickers.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.ingestion import EODHDFundamentalsClient
from pyvalue.persistence.storage import (
    ExchangeProviderRepository,
    FundamentalsFetchStateRepository,
    MarketDataFetchStateRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import USUniverseLoader

from ._common import (
    EODHD_ALLOWED_TICKER_TYPES,
    LOGGER,
    _normalize_provider,
    _parse_exchange_filters,
    _require_eodhd_key,
)
from .ingest import (
    cmd_ingest_fundamentals_bulk,
)
from .market_data import (
    cmd_update_market_data_bulk,
)
from .normalize import (
    cmd_normalize_fundamentals_bulk,
)
from .metrics import (
    cmd_compute_metrics_bulk,
)


_SKIPPED_NO_CURRENCY_PREVIEW = 20


def _refresh_supported_exchanges_for_provider(
    database: str,
    provider: str,
    client: EODHDFundamentalsClient,
) -> int:
    """Refresh and persist the supported exchange catalog for a provider."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "refresh-supported-exchanges currently only supports provider=EODHD."
        )
    repo = ExchangeProviderRepository(database)
    repo.initialize_schema()
    rows = client.list_exchanges()
    return repo.replace_for_provider(provider_norm, rows)


def _resolve_eodhd_exchange_metadata(
    database: str,
    client: EODHDFundamentalsClient,
    exchange_code: str,
) -> Optional[Dict[str, Optional[str]]]:
    """Resolve exchange metadata from the local catalog, bootstrapping on miss."""

    repo = ExchangeProviderRepository(database)
    record = repo.fetch("EODHD", exchange_code)
    if record is None:
        _refresh_supported_exchanges_for_provider(
            database=database,
            provider="EODHD",
            client=client,
        )
        record = repo.fetch("EODHD", exchange_code)
    if record is None:
        return None
    return {
        "Name": record.name,
        "Code": record.code,
        "Country": record.country,
        "Currency": record.currency,
        "OperatingMIC": record.operating_mic,
        "CountryISO2": record.country_iso2,
        "CountryISO3": record.country_iso3,
    }


def _report_skipped_no_currency(exchange_code: str, skipped: Sequence[str]) -> None:
    """Warn on provider tickers skipped because the payload carried no currency.

    ``listing.currency`` is NOT NULL with no fallback, so a catalog entry whose
    payload omits the currency is not inserted. Printing the affected tickers to
    the console lets the operator chase the data issue with the provider. The
    list is previewed (first ``_SKIPPED_NO_CURRENCY_PREVIEW``) so a large gap
    does not flood the output.
    """

    if not skipped:
        return
    preview = ", ".join(skipped[:_SKIPPED_NO_CURRENCY_PREVIEW])
    extra = len(skipped) - _SKIPPED_NO_CURRENCY_PREVIEW
    suffix = f" (+{extra} more)" if extra > 0 else ""
    print(
        f"    WARNING: {len(skipped)} ticker(s) on {exchange_code} skipped -- no "
        f"currency in the provider payload; chase with the provider: "
        f"{preview}{suffix}"
    )


def _refresh_supported_tickers_for_exchange(
    database: str,
    provider: str,
    client: EODHDFundamentalsClient,
    exchange_code: str,
) -> Tuple[int, int, Tuple[str, ...]]:
    """Refresh one exchange's supported tickers and prune stale fetch state.

    Returns ``(inserted, removed, skipped_no_currency)``.
    """

    provider_norm = provider.strip().upper()
    exchange_norm = exchange_code.strip().upper()
    rows = client.list_symbols(exchange_norm)
    filtered_rows: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = (row.get("Code") or "").strip()
        if not code:
            continue
        security_type = (row.get("Type") or "").strip()
        if security_type.upper() not in EODHD_ALLOWED_TICKER_TYPES:
            continue
        filtered_rows.append(row)

    ticker_repo = SupportedTickerRepository(database)
    existing = ticker_repo.list_for_exchange(provider_norm, exchange_norm)
    existing_symbols = {row.symbol for row in existing}
    result = ticker_repo.replace_for_exchange(
        provider_norm, exchange_norm, filtered_rows
    )
    current = ticker_repo.list_for_exchange(provider_norm, exchange_norm)
    current_symbols = {row.symbol for row in current}
    removed_symbols = sorted(existing_symbols - current_symbols)

    state_repo = FundamentalsFetchStateRepository(database)
    state_repo.delete_symbols(provider_norm, removed_symbols)
    market_state_repo = MarketDataFetchStateRepository(database)
    market_state_repo.delete_symbols(provider_norm, removed_symbols)
    return result.inserted, len(removed_symbols), result.skipped_no_currency


def _list_eodhd_exchange_codes(
    database: str,
    client: EODHDFundamentalsClient,
) -> List[str]:
    repo = ExchangeProviderRepository(database)
    exchanges = repo.list_all("EODHD")
    if not exchanges:
        _refresh_supported_exchanges_for_provider(database, "EODHD", client)
        exchanges = repo.list_all("EODHD")
    return [row.code for row in exchanges]


def _should_keep_listing(include_etfs: bool, listing_is_etf: bool) -> bool:
    """Return True if the listing should be kept after ETF filtering."""

    return include_etfs or not listing_is_etf


def cmd_load_universe(
    provider: str,
    database: str,
    include_etfs: bool,
    exchange_code: Optional[str],
    currencies: Optional[Sequence[str]] = None,
    include_exchanges: Optional[Sequence[str]] = None,
) -> int:
    """Load provider catalog data into the canonical provider_listing table."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        if exchange_code or currencies or include_exchanges:
            raise SystemExit(
                "Flags --exchange-code, --currencies, and --include-exchanges are only valid with provider=EODHD."
            )
        return cmd_load_us_universe(database=database, include_etfs=include_etfs)

    return cmd_load_eodhd_universe(
        database=database,
        include_etfs=include_etfs,
        exchange_code=exchange_code or "",
        currencies=currencies,
        include_exchanges=include_exchanges,
    )


def cmd_load_us_universe(database: str, include_etfs: bool) -> int:
    """Load the SEC-supported US catalog into provider_listing."""

    loader = USUniverseLoader()
    listings = loader.load()
    LOGGER.info("Fetched %s US listings", len(listings))

    # Drop ETFs unless explicitly requested in the CLI arguments.
    filtered = [
        item for item in listings if _should_keep_listing(include_etfs, item.is_etf)
    ]
    LOGGER.info("Remaining listings after ETF filter: %s", len(filtered))

    repo = SupportedTickerRepository(database)
    repo.initialize_schema()
    result = repo.replace_from_listings("SEC", "US", filtered)

    print(f"Stored {result.inserted} SEC supported tickers for US in {database}")
    _report_skipped_no_currency("US", result.skipped_no_currency)
    return 0


def cmd_load_eodhd_universe(
    database: str,
    include_etfs: bool,
    exchange_code: str,
    currencies: Optional[Sequence[str]] = None,
    include_exchanges: Optional[Sequence[str]] = None,
) -> int:
    """Exit with guidance because EODHD now uses refresh-supported-tickers."""

    if currencies or include_exchanges or include_etfs or exchange_code:
        raise SystemExit(
            "load-universe --provider EODHD is deprecated. "
            "Use `pyvalue refresh-supported-exchanges --provider EODHD` and "
            "`pyvalue refresh-supported-tickers --provider EODHD --exchange-code <CODE>` instead."
        )
    raise SystemExit(
        "load-universe --provider EODHD is deprecated. "
        "Use `pyvalue refresh-supported-exchanges --provider EODHD` and "
        "`pyvalue refresh-supported-tickers --provider EODHD --exchange-code <CODE>` instead."
    )


def cmd_refresh_supported_exchanges(provider: str, database: str) -> int:
    """Refresh the persisted supported exchange catalog."""

    provider_norm = provider.strip().upper()
    repo = ExchangeProviderRepository(database)
    repo.initialize_schema()
    if provider_norm == "SEC":
        repo.ensure_fixed_exchange(
            provider="SEC",
            provider_exchange_code="US",
            canonical_exchange_code="US",
            name="United States",
            country="US",
            currency="USD",
        )
        stored = len(repo.list_all("SEC"))
    elif provider_norm == "EODHD":
        api_key = _require_eodhd_key()
        client = EODHDFundamentalsClient(api_key=api_key)
        stored = _refresh_supported_exchanges_for_provider(
            database=database,
            provider=provider_norm,
            client=client,
        )
    else:
        raise SystemExit(f"Unsupported provider: {provider}")
    print(f"Stored {stored} supported exchanges for {provider_norm} in {database}")
    return 0


def cmd_refresh_supported_tickers(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    include_etfs: bool,
) -> int:
    """Refresh the persisted supported ticker catalog."""

    provider_norm = provider.strip().upper()
    requested_exchanges = _parse_exchange_filters(exchange_codes)
    if provider_norm == "SEC":
        if requested_exchanges and requested_exchanges != {"US"}:
            raise SystemExit("provider=SEC only supports --exchange-codes US.")
        exchange_list = ["US"]
        repo = ExchangeProviderRepository(database)
        repo.initialize_schema()
        repo.ensure_fixed_exchange(
            provider="SEC",
            provider_exchange_code="US",
            canonical_exchange_code="US",
            name="United States",
            country="US",
            currency="USD",
        )
        loader = USUniverseLoader()
        listings = loader.load()
        filtered = [
            item for item in listings if _should_keep_listing(include_etfs, item.is_etf)
        ]
        ticker_repo = SupportedTickerRepository(database)
        ticker_repo.initialize_schema()
        existing = ticker_repo.list_for_exchange("SEC", "US")
        existing_symbols = {row.symbol for row in existing}
        sec_result = ticker_repo.replace_from_listings("SEC", "US", filtered)
        stored = sec_result.inserted
        current = ticker_repo.list_for_exchange("SEC", "US")
        current_symbols = {row.symbol for row in current}
        removed_symbols = sorted(existing_symbols - current_symbols)
        FundamentalsFetchStateRepository(database).delete_symbols(
            "SEC", removed_symbols
        )
        MarketDataFetchStateRepository(database).delete_symbols("SEC", removed_symbols)
        print(
            f"[1/1] Stored {stored} supported tickers for US in {database} "
            f"(removed {len(removed_symbols)} unsupported tickers)"
        )
        _report_skipped_no_currency("US", sec_result.skipped_no_currency)
        return 0

    if provider_norm != "EODHD":
        raise SystemExit(f"Unsupported provider: {provider}")

    api_key = _require_eodhd_key()
    eodhd_client = EODHDFundamentalsClient(api_key=api_key)
    if all_supported or not requested_exchanges:
        exchange_list = _list_eodhd_exchange_codes(database, eodhd_client)
    else:
        exchange_list = sorted(requested_exchanges)
        for exchange_norm in exchange_list:
            meta = _resolve_eodhd_exchange_metadata(
                database, eodhd_client, exchange_norm
            )
            if meta is None:
                raise SystemExit(
                    f"Exchange {exchange_norm} not found in the EODHD exchange list."
                )

    if not exchange_list:
        print("No supported exchanges available to refresh.")
        return 0

    total = len(exchange_list)
    for idx, code in enumerate(exchange_list, 1):
        stored, removed, skipped = _refresh_supported_tickers_for_exchange(
            database=database,
            provider=provider_norm,
            client=eodhd_client,
            exchange_code=code,
        )
        print(
            f"[{idx}/{total}] Stored {stored} supported tickers for {code} "
            f"in {database} (removed {removed} unsupported tickers)"
        )
        _report_skipped_no_currency(code, skipped)
    return 0


def cmd_refresh_exchange(
    provider: str,
    exchange_code: str,
    database: str,
    include_etfs: bool,
    currencies: Optional[Sequence[str]],
    include_exchanges: Optional[Sequence[str]],
    fundamentals_rate: Optional[float],
    market_rate: float,
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
    user_agent: Optional[str],
    metrics: Optional[Sequence[str]],
) -> int:
    """Run catalog, fundamentals, market data, normalization, and metrics for an exchange."""

    provider_norm = _normalize_provider(provider)
    exchange_norm = exchange_code.upper()
    if provider_norm == "SEC" and exchange_norm != "US":
        raise SystemExit("provider=SEC only supports --exchange-code US.")
    if provider_norm == "SEC" and (currencies or include_exchanges):
        raise SystemExit(
            "--currencies/--include-exchanges are only valid with provider=EODHD."
        )
    if provider_norm == "EODHD" and (include_etfs or currencies or include_exchanges):
        raise SystemExit(
            "refresh-exchange --provider EODHD no longer supports universe filtering flags. "
            "The canonical EODHD catalog comes from refresh-supported-tickers."
        )

    print("Step 1/5: refresh catalog")
    if provider_norm == "SEC":
        result = cmd_load_universe(
            provider=provider_norm,
            database=database,
            include_etfs=include_etfs,
            exchange_code=None,
            currencies=None,
            include_exchanges=None,
        )
    else:
        result = cmd_refresh_supported_tickers(
            provider=provider_norm,
            database=database,
            exchange_codes=[exchange_norm],
            all_supported=False,
            include_etfs=False,
        )
    if result != 0:
        return result

    print("Step 2/5: ingest fundamentals")
    result = cmd_ingest_fundamentals_bulk(
        provider=provider_norm,
        database=database,
        rate=fundamentals_rate,
        exchange_code=exchange_norm,
        user_agent=user_agent,
        max_symbols=max_symbols,
        max_age_days=max_age_days,
        respect_backoff=respect_backoff,
    )
    if result != 0:
        return result

    print("Step 3/5: update market data")
    result = cmd_update_market_data_bulk(
        provider=provider_norm,
        database=database,
        rate=market_rate,
        exchange_code=exchange_norm,
    )
    if result != 0:
        return result

    print("Step 4/5: normalize fundamentals")
    result = cmd_normalize_fundamentals_bulk(
        provider=provider_norm,
        database=database,
        exchange_code=exchange_norm,
    )
    if result != 0:
        return result

    print("Step 5/5: compute metrics")
    return cmd_compute_metrics_bulk(
        provider=provider_norm,
        database=database,
        metric_ids=metrics,
        exchange_code=exchange_norm,
    )
