"""CLI handlers for refreshing supported exchanges and tickers.

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
    SupportedTickerRepository,
)

from ._common import (
    EODHD_ALLOWED_TICKER_TYPES,
    _normalize_provider,
    _parse_exchange_filters,
    _require_eodhd_key,
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
    """Refresh one exchange's supported tickers.

    Returns ``(inserted, removed, skipped_no_currency)``. ``replace_for_exchange``
    deletes the provider listings absent from the refreshed payload and cascades
    those deletes to ``fundamentals_raw`` / ``fundamentals_fetch_state`` /
    ``fundamentals_normalization_state`` / ``market_data_fetch_state``, so no
    separate fetch-state cleanup is needed here.
    """

    provider_norm = provider.strip().upper()
    exchange_norm = exchange_code.strip().upper()
    rows = client.list_symbols(exchange_norm)
    filtered_rows: List[Dict[str, object]] = []
    for row in rows:
        code = (row.get("Code") or "").strip()
        if not code:
            continue
        security_type = (row.get("Type") or "").strip()
        if security_type.upper() not in EODHD_ALLOWED_TICKER_TYPES:
            continue
        filtered_rows.append(row)

    ticker_repo = SupportedTickerRepository(database)
    result = ticker_repo.replace_for_exchange(
        provider_norm, exchange_norm, filtered_rows
    )
    return result.inserted, result.removed, result.skipped_no_currency


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


def cmd_refresh_supported_exchanges(provider: str, database: str) -> int:
    """Refresh the persisted supported exchange catalog."""

    provider_norm = _normalize_provider(provider)
    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    stored = _refresh_supported_exchanges_for_provider(
        database=database,
        provider=provider_norm,
        client=client,
    )
    print(f"Stored {stored} supported exchanges for {provider_norm} in {database}")
    return 0


def cmd_refresh_supported_tickers(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> int:
    """Refresh the persisted supported ticker catalog."""

    provider_norm = _normalize_provider(provider)
    requested_exchanges = _parse_exchange_filters(exchange_codes)

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
