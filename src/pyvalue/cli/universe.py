"""CLI handlers for refreshing supported exchanges and tickers.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import (
    Dict,
    List,
    Optional,
    Sequence,
)

import requests

from pyvalue.ingestion import (
    EODHDFundamentalsClient,
    ExchangeNotInPlanError,
    redact_api_token,
)
from pyvalue.persistence.storage import (
    ExchangeProviderRepository,
    MassDelistingError,
    SupportedTickerRefreshResult,
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
    """Warn on provider tickers skipped for lack of a usable payload currency.

    ``listing.currency`` is NOT NULL with a 3-uppercase-letter shape CHECK and
    no fallback, so a catalog entry whose payload omits the currency or carries
    a malformed placeholder (e.g. EODHD's ``'Unknown'``) is not inserted.
    Printing the affected tickers to the console lets the operator chase the
    data issue with the provider. The list is previewed (first
    ``_SKIPPED_NO_CURRENCY_PREVIEW``) so a large gap does not flood the output.
    """

    if not skipped:
        return
    preview = ", ".join(skipped[:_SKIPPED_NO_CURRENCY_PREVIEW])
    extra = len(skipped) - _SKIPPED_NO_CURRENCY_PREVIEW
    suffix = f" (+{extra} more)" if extra > 0 else ""
    print(
        f"    WARNING: {len(skipped)} ticker(s) on {exchange_code} skipped -- no "
        f"usable currency in the provider payload; chase with the provider: "
        f"{preview}{suffix}"
    )


def _refresh_supported_tickers_for_exchange(
    database: str,
    provider: str,
    client: EODHDFundamentalsClient,
    exchange_code: str,
    allow_mass_delisting: bool = False,
) -> SupportedTickerRefreshResult:
    """Refresh one exchange's supported tickers.

    ``replace_for_exchange`` prunes only the provider layer of the listings
    absent from the refreshed payload -- the ``provider_listing`` mapping plus
    its ``fundamentals_raw`` / ``fundamentals_fetch_state`` /
    ``fundamentals_normalization_state`` / ``market_data_fetch_state`` rows.
    Canonical rows and data (``listing``/``issuer``, facts, prices, metrics)
    are provider-independent and are never deleted by a refresh; a listing
    that lost its last mapping is merely reported as orphaned.
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
    return ticker_repo.replace_for_exchange(
        provider_norm,
        exchange_norm,
        filtered_rows,
        allow_mass_delisting=allow_mass_delisting,
    )


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
    allow_mass_delisting: bool = False,
) -> int:
    """Refresh the persisted supported ticker catalog.

    Provider failures never abort the multi-exchange run: an exchange the
    plan no longer covers (HTTP 404) or a transient provider error is warned
    about and skipped with its stored data untouched, and a payload tripping
    the mass-delisting guard is rolled back and skipped. Exit code is 0 when
    every failure was a not-in-plan skip (expected steady state until the
    exchange catalog is re-synced) and 1 when any exchange failed for another
    reason -- guard trips and provider errors need operator attention.
    """

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
    skipped_not_in_plan: List[str] = []
    blocked_mass_delisting: List[str] = []
    failed: List[str] = []
    for idx, code in enumerate(exchange_list, 1):
        try:
            result = _refresh_supported_tickers_for_exchange(
                database=database,
                provider=provider_norm,
                client=eodhd_client,
                exchange_code=code,
                allow_mass_delisting=allow_mass_delisting,
            )
        except ExchangeNotInPlanError:
            skipped_not_in_plan.append(code)
            print(
                f"[{idx}/{total}] WARNING: {code} is not covered by the current "
                "EODHD plan (HTTP 404) -- skipped, stored data untouched. Run "
                "refresh-supported-exchanges to drop it from the catalog."
            )
            continue
        except MassDelistingError as exc:
            blocked_mass_delisting.append(code)
            print(
                f"[{idx}/{total}] WARNING: {code} refresh blocked -- the payload "
                f"would remove {exc.removed} of {exc.existing} provider "
                "listings, which looks like a truncated response or a plan "
                "change. Nothing was changed; re-run with "
                "--allow-mass-delisting if this is intended."
            )
            continue
        except requests.RequestException as exc:
            # Transient provider trouble (5xx, timeout, connection reset) must
            # not kill the remaining exchanges. The message is echoed with the
            # api_token scrubbed -- connection errors embed the full URL.
            failed.append(code)
            print(
                f"[{idx}/{total}] WARNING: {code} refresh failed with a "
                f"provider error -- skipped, stored data untouched: "
                f"{redact_api_token(str(exc))}"
            )
            continue
        print(
            f"[{idx}/{total}] Stored {result.inserted} supported tickers for {code} "
            f"in {database} (removed {result.removed} unsupported tickers)"
        )
        if result.orphaned_listings:
            # Visibility line, not destruction: these listings lost their last
            # provider mapping; canonical rows and data are retained but are
            # unreachable through every provider-joined scope.
            print(
                f"    {result.orphaned_listings} listing(s) lost their last "
                "provider mapping -- canonical data retained (invisible to "
                "scopes)"
            )
        _report_skipped_no_currency(code, result.skipped_no_currency)

    if skipped_not_in_plan:
        print(
            f"Skipped {len(skipped_not_in_plan)} exchange(s) not covered by the "
            f"current EODHD plan: {', '.join(skipped_not_in_plan)}"
        )
    if blocked_mass_delisting:
        print(
            f"Blocked {len(blocked_mass_delisting)} exchange(s) on the "
            f"mass-delisting guard: {', '.join(blocked_mass_delisting)}"
        )
    if failed:
        print(
            f"Failed to refresh {len(failed)} exchange(s) on provider errors: "
            f"{', '.join(failed)}"
        )
    return 1 if (blocked_mass_delisting or failed) else 0
