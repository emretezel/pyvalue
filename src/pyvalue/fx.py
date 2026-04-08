"""DB-backed FX lookup and refresh helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from array import array
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional, Protocol, Sequence
import json
import logging

import requests  # type: ignore[import-untyped]

from pyvalue.config import Config
from pyvalue.currency import normalize_currency_code, normalize_monetary_amount
from pyvalue.storage import FXRateRecord, FXRatesRepository


LOGGER = logging.getLogger(__name__)
DEFAULT_PROVIDER = "EODHD"
DEFAULT_FRANKFURTER_API_BASE = "https://api.frankfurter.dev/v2"
DEFAULT_EODHD_API_BASE = "https://eodhd.com/api"


@dataclass(frozen=True)
class _EphemeralFXConfig:
    """Non-fetching FX config used for ephemeral in-memory contexts."""

    fx_provider: str = DEFAULT_PROVIDER
    fx_pivot_currency: str = "USD"
    fx_secondary_pivot_currency: Optional[str] = "EUR"
    fx_stale_warning_days: int = 7


def _to_date(value: object) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


@dataclass(frozen=True)
class FXQuote:
    """Resolved FX quote for one conversion path."""

    provider: str
    rate_date: date
    base_currency: str
    quote_currency: str
    rate: Decimal
    source_kind: str
    via_currency: Optional[str] = None


class MissingFXRateError(RuntimeError):
    """Raised when no direct, inverse, or triangulated FX quote exists."""

    def __init__(
        self,
        *,
        provider: str,
        base_currency: str,
        quote_currency: str,
        as_of: str,
    ) -> None:
        self.provider = provider
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.as_of = as_of
        super().__init__(
            "Missing FX rate "
            f"(provider={provider} base={base_currency} quote={quote_currency} as_of={as_of})"
        )

    def __reduce__(self) -> tuple[object, tuple[str, str, str, str]]:
        """Make the exception pickle-safe for ProcessPoolExecutor transport."""

        return (
            self.__class__._rebuild,
            (
                self.provider,
                self.base_currency,
                self.quote_currency,
                self.as_of,
            ),
        )

    @classmethod
    def _rebuild(
        cls,
        provider: str,
        base_currency: str,
        quote_currency: str,
        as_of: str,
    ) -> MissingFXRateError:
        return cls(
            provider=provider,
            base_currency=base_currency,
            quote_currency=quote_currency,
            as_of=as_of,
        )


@dataclass(frozen=True)
class FXCatalogEntry:
    """Parsed FX catalog metadata for one provider symbol."""

    symbol: str
    canonical_symbol: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    name: Optional[str]
    is_alias: bool
    is_refreshable: bool


@dataclass
class FXSeries:
    """Compact direct-rate history for one FX pair."""

    ordinals: array
    rates: array

    @classmethod
    def empty(cls) -> FXSeries:
        return cls(array("I"), array("d"))

    @classmethod
    def from_rows(cls, rows: Sequence[tuple[str, str]]) -> FXSeries:
        ordinals = array("I")
        rates = array("d")
        for rate_date, rate_text in rows:
            parsed = _to_date(rate_date)
            if parsed is None:
                continue
            ordinals.append(parsed.toordinal())
            rates.append(float(rate_text))
        return cls(ordinals=ordinals, rates=rates)


class FXRefreshProvider(Protocol):
    """Provider abstraction for syncing catalog entries and direct histories."""

    provider_name: str

    def list_catalog(self) -> list[FXCatalogEntry]: ...

    def fetch_history(
        self,
        *,
        canonical_symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[FXRateRecord]: ...


class FrankfurterProvider:
    """Fetch direct FX rates from the Frankfurter v2 API."""

    provider_name = "FRANKFURTER"

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        api_base: str = DEFAULT_FRANKFURTER_API_BASE,
    ) -> None:
        self.session = session or requests.Session()
        self.api_base = api_base.rstrip("/")

    def fetch_rates(
        self,
        *,
        base_currency: str,
        quote_currencies: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> list[FXRateRecord]:
        remaining_quotes = sorted(
            {
                code
                for code in (normalize_currency_code(item) for item in quote_currencies)
                if code is not None and code != normalize_currency_code(base_currency)
            }
        )
        base = normalize_currency_code(base_currency)
        if base is None or not remaining_quotes:
            return []

        while True:
            params = {
                "base": base,
                "quotes": ",".join(remaining_quotes),
            }
            if start_date == end_date:
                params["date"] = start_date.isoformat()
            else:
                params["from"] = start_date.isoformat()
                params["to"] = end_date.isoformat()

            response = self.session.get(
                f"{self.api_base}/rates",
                params=params,
                timeout=30,
            )
            if response.status_code not in {400, 404, 422}:
                break
            invalid_currencies = self._extract_invalid_currencies(response)
            invalid_quotes = [
                quote for quote in remaining_quotes if quote in invalid_currencies
            ]
            if base in invalid_currencies or not invalid_quotes:
                LOGGER.warning(
                    "Frankfurter FX request failed | base=%s quotes=%s status=%s body=%s",
                    base,
                    ",".join(remaining_quotes),
                    response.status_code,
                    response.text[:500],
                )
                return []
            LOGGER.warning(
                "Frankfurter FX request skipped unsupported currencies | base=%s unsupported_quotes=%s requested_quotes=%s status=%s",
                base,
                ",".join(invalid_quotes),
                ",".join(remaining_quotes),
                response.status_code,
            )
            remaining_quotes = [
                quote for quote in remaining_quotes if quote not in invalid_currencies
            ]
            if not remaining_quotes:
                return []

        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected Frankfurter FX response: {payload!r}")

        fetched_at = response.headers.get("Date")
        timestamp = fetched_at or start_date.isoformat()
        records: list[FXRateRecord] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            entry_date = str(entry.get("date") or "").strip()
            quote = normalize_currency_code(entry.get("quote"))
            rate = entry.get("rate")
            if not entry_date or quote is None or rate is None:
                continue
            records.append(
                FXRateRecord(
                    provider=self.provider_name,
                    rate_date=entry_date,
                    base_currency=base,
                    quote_currency=quote,
                    rate_text=str(rate),
                    fetched_at=timestamp,
                    source_kind="provider",
                    meta_json=json.dumps({"provider": self.provider_name}),
                )
            )
        return records

    def list_catalog(self) -> list[FXCatalogEntry]:
        raise NotImplementedError("Frankfurter does not support catalog sync")

    def fetch_history(
        self,
        *,
        canonical_symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[FXRateRecord]:
        base = normalize_currency_code(canonical_symbol[:3])
        quote = normalize_currency_code(canonical_symbol[3:])
        if base is None or quote is None:
            raise ValueError(f"Unexpected FX symbol: {canonical_symbol}")
        return self.fetch_rates(
            base_currency=base,
            quote_currencies=[quote],
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def _extract_invalid_currencies(response: requests.Response) -> set[str]:
        """Parse unsupported currency codes from a Frankfurter error response."""

        try:
            payload = response.json()
        except ValueError:
            return set()
        if not isinstance(payload, dict):
            return set()
        message = payload.get("message")
        if not isinstance(message, str):
            return set()
        prefix = "invalid currency:"
        if not message.lower().startswith(prefix):
            return set()
        raw_codes = message.split(":", 1)[1]
        return {code.strip().upper() for code in raw_codes.split(",") if code.strip()}


class EODHDFXProvider:
    """Fetch FX catalog metadata and direct histories from the EODHD API."""

    provider_name = DEFAULT_PROVIDER

    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        api_base: str = DEFAULT_EODHD_API_BASE,
    ) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()
        self.api_base = api_base.rstrip("/")

    def list_catalog(self) -> list[FXCatalogEntry]:
        response = self.session.get(
            f"{self.api_base}/exchange-symbol-list/FOREX",
            params={"api_token": self.api_key, "fmt": "json"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected EODHD FX catalog response: {payload!r}")
        entries: list[FXCatalogEntry] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            parsed = parse_eodhd_fx_catalog_entry(entry)
            if parsed is not None:
                entries.append(parsed)
        return entries

    def fetch_history(
        self,
        *,
        canonical_symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[FXRateRecord]:
        symbol = canonical_symbol.strip().upper()
        if len(symbol) != 6:
            raise ValueError(f"Unexpected EODHD FX symbol: {canonical_symbol}")
        base = normalize_currency_code(symbol[:3])
        quote = normalize_currency_code(symbol[3:])
        if base is None or quote is None:
            raise ValueError(f"Unexpected EODHD FX symbol: {canonical_symbol}")

        response = self.session.get(
            f"{self.api_base}/eod/{symbol}.FOREX",
            params={
                "api_token": self.api_key,
                "fmt": "json",
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
                "order": "a",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(
                f"Unexpected EODHD FX history response for {canonical_symbol}: {payload!r}"
            )
        fetched_at = response.headers.get("Date") or start_date.isoformat()
        records: list[FXRateRecord] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            rate_date = str(entry.get("date") or "").strip()
            close = entry.get("close")
            if not rate_date or close is None:
                continue
            records.append(
                FXRateRecord(
                    provider=self.provider_name,
                    rate_date=rate_date,
                    base_currency=base,
                    quote_currency=quote,
                    rate_text=str(close),
                    fetched_at=fetched_at,
                    source_kind="provider",
                    meta_json=json.dumps(
                        {"provider": self.provider_name, "symbol": symbol}
                    ),
                )
            )
        return records


def parse_eodhd_fx_catalog_entry(
    entry: dict[object, object],
) -> Optional[FXCatalogEntry]:
    """Parse one EODHD FOREX catalog row into canonical refresh metadata."""

    raw_symbol = str(entry.get("Code") or entry.get("code") or "").strip().upper()
    if not raw_symbol:
        return None
    name = str(entry.get("Name") or entry.get("name") or "").strip() or None
    if len(raw_symbol) == 6 and raw_symbol.isalpha():
        base = normalize_currency_code(raw_symbol[:3])
        quote = normalize_currency_code(raw_symbol[3:])
        return FXCatalogEntry(
            symbol=raw_symbol,
            canonical_symbol=raw_symbol,
            base_currency=base,
            quote_currency=quote,
            name=name,
            is_alias=False,
            is_refreshable=base is not None and quote is not None,
        )
    if len(raw_symbol) == 3 and raw_symbol.isalpha():
        quote = normalize_currency_code(raw_symbol)
        canonical_symbol = f"USD{quote}" if quote is not None else raw_symbol
        return FXCatalogEntry(
            symbol=raw_symbol,
            canonical_symbol=canonical_symbol,
            base_currency="USD" if quote is not None else None,
            quote_currency=quote,
            name=name,
            is_alias=True,
            is_refreshable=False,
        )
    return FXCatalogEntry(
        symbol=raw_symbol,
        canonical_symbol=raw_symbol,
        base_currency=None,
        quote_currency=None,
        name=name,
        is_alias=False,
        is_refreshable=False,
    )


class FXService:
    """Resolve FX rates from the local DB using an in-memory cache."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: Optional[FXRatesRepository] = None,
        provider: Optional[object] = None,
        provider_name: Optional[str] = None,
        config: Optional[Config] = None,
        preload_all: bool = False,
    ) -> None:
        default_config: Config | _EphemeralFXConfig
        if config is None and str(database) == ":memory:":
            default_config = _EphemeralFXConfig()
        else:
            default_config = config or Config()
        self.config = default_config
        self.repository = repository or FXRatesRepository(database)
        self.repository.initialize_schema()
        configured_provider = (
            provider_name
            or getattr(provider, "provider_name", None)
            or self.config.fx_provider
            or DEFAULT_PROVIDER
        )
        self.provider_name = str(configured_provider).strip().upper()
        self.pivot_currency = (
            normalize_currency_code(self.config.fx_pivot_currency) or "USD"
        )
        self.secondary_pivot_currency = normalize_currency_code(
            self.config.fx_secondary_pivot_currency
        )
        self.stale_warning_days = max(int(self.config.fx_stale_warning_days), 0)
        self._history_cache: dict[tuple[str, str, str], FXSeries] = {}
        self._quote_cache: dict[tuple[str, str, str, int], Optional[FXQuote]] = {}
        self._provider_fully_preloaded = False
        if preload_all:
            self.preload_provider_history()

    def preload_provider_history(self, provider_name: Optional[str] = None) -> None:
        """Load the full direct-rate history for one provider into memory."""

        provider_norm = (provider_name or self.provider_name).strip().upper()
        rows = self.repository.fetch_all_for_provider(provider_norm)
        self._history_cache.clear()
        self._quote_cache.clear()
        current_pair: Optional[tuple[str, str, str]] = None
        current_rows: list[tuple[str, str]] = []
        for base_currency, quote_currency, rate_date, rate_text in rows:
            pair_key = (provider_norm, base_currency, quote_currency)
            if current_pair != pair_key and current_pair is not None:
                self._history_cache[current_pair] = FXSeries.from_rows(current_rows)
                current_rows = []
            current_pair = pair_key
            current_rows.append((rate_date, rate_text))
        if current_pair is not None:
            self._history_cache[current_pair] = FXSeries.from_rows(current_rows)
        self._provider_fully_preloaded = True

    def get_fx_rate(
        self,
        base_currency: str,
        quote_currency: str,
        as_of_date: str | date,
    ) -> Optional[FXQuote]:
        """Return the latest available FX quote on or before ``as_of_date``."""

        as_of = _to_date(as_of_date)
        base = normalize_currency_code(base_currency)
        quote = normalize_currency_code(quote_currency)
        if as_of is None or base is None or quote is None:
            return None
        if base == quote:
            return FXQuote(
                provider=self.provider_name,
                rate_date=as_of,
                base_currency=base,
                quote_currency=quote,
                rate=Decimal("1"),
                source_kind="identity",
            )

        cache_key = (self.provider_name, base, quote, as_of.toordinal())
        if cache_key in self._quote_cache:
            quote_result = self._quote_cache[cache_key]
        else:
            quote_result = self._lookup(base, quote, as_of)
            self._quote_cache[cache_key] = quote_result
        if quote_result is None:
            LOGGER.warning(
                "Missing FX rate | provider=%s base=%s quote=%s as_of=%s operation=get_fx_rate",
                self.provider_name,
                base,
                quote,
                as_of.isoformat(),
            )
            return None

        age_days = (as_of - quote_result.rate_date).days
        if age_days > self.stale_warning_days:
            LOGGER.warning(
                "Stale FX rate used | provider=%s base=%s quote=%s requested_as_of=%s rate_date=%s age_days=%s source_kind=%s",
                self.provider_name,
                base,
                quote,
                as_of.isoformat(),
                quote_result.rate_date.isoformat(),
                age_days,
                quote_result.source_kind,
            )
        return quote_result

    def convert_amount(
        self,
        amount: float | Decimal,
        from_currency: str,
        to_currency: str,
        as_of_date: str | date,
    ) -> Optional[Decimal]:
        """Convert ``amount`` from ``from_currency`` into ``to_currency``."""

        normalized_amount, normalized_from = normalize_monetary_amount(
            amount,
            from_currency,
        )
        normalized_to = normalize_currency_code(to_currency)
        if (
            normalized_amount is None
            or normalized_from is None
            or normalized_to is None
        ):
            return None
        if normalized_from == normalized_to:
            return normalized_amount
        quote = self.get_fx_rate(normalized_from, normalized_to, as_of_date)
        if quote is None:
            return None
        return normalized_amount * quote.rate

    def _lookup(
        self,
        base_currency: str,
        quote_currency: str,
        as_of: date,
    ) -> Optional[FXQuote]:
        candidates = self._lookup_direct_and_inverse(
            base_currency,
            quote_currency,
            as_of,
        )
        for pivot in (self.pivot_currency, self.secondary_pivot_currency):
            if pivot is None or pivot in {base_currency, quote_currency}:
                continue
            for base_to_pivot in self._lookup_direct_and_inverse(
                base_currency, pivot, as_of
            ):
                for quote_to_pivot in self._lookup_direct_and_inverse(
                    quote_currency, pivot, as_of
                ):
                    if quote_to_pivot.rate == 0:
                        continue
                    candidates.append(
                        FXQuote(
                            provider=self.provider_name,
                            rate_date=min(
                                base_to_pivot.rate_date, quote_to_pivot.rate_date
                            ),
                            base_currency=base_currency,
                            quote_currency=quote_currency,
                            rate=base_to_pivot.rate / quote_to_pivot.rate,
                            source_kind="triangulated",
                            via_currency=pivot,
                        )
                    )
        if not candidates:
            return None
        return max(candidates, key=self._quote_rank)

    def _quote_rank(self, quote: FXQuote) -> tuple[date, int, int]:
        """Rank candidate quotes by freshness, then by path preference."""

        source_rank = {
            "provider": 3,
            "inverse": 2,
            "triangulated": 1,
            "identity": 4,
        }.get(quote.source_kind, 0)
        pivot_rank = 0
        if quote.source_kind == "triangulated":
            if quote.via_currency == self.pivot_currency:
                pivot_rank = 2
            elif quote.via_currency == self.secondary_pivot_currency:
                pivot_rank = 1
        return (
            quote.rate_date,
            source_rank,
            pivot_rank,
        )

    def _lookup_direct_and_inverse(
        self,
        base_currency: str,
        quote_currency: str,
        as_of: date,
    ) -> list[FXQuote]:
        """Return available direct and inverse quotes for a pair."""

        quotes: list[FXQuote] = []
        direct = self._latest_direct_quote(base_currency, quote_currency, as_of)
        if direct is not None:
            quotes.append(direct)
        inverse = self._latest_direct_quote(quote_currency, base_currency, as_of)
        if inverse is not None and inverse.rate != 0:
            quotes.append(
                FXQuote(
                    provider=inverse.provider,
                    rate_date=inverse.rate_date,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    rate=Decimal("1") / inverse.rate,
                    source_kind="inverse",
                )
            )
        return quotes

    def _latest_direct_quote(
        self,
        base_currency: str,
        quote_currency: str,
        as_of: date,
    ) -> Optional[FXQuote]:
        series = self._ensure_pair_history(base_currency, quote_currency)
        index = bisect_right(series.ordinals, as_of.toordinal()) - 1
        if index < 0:
            return None
        rate_date = date.fromordinal(int(series.ordinals[index]))
        rate = Decimal(str(series.rates[index]))
        return FXQuote(
            provider=self.provider_name,
            rate_date=rate_date,
            base_currency=base_currency,
            quote_currency=quote_currency,
            rate=rate,
            source_kind="provider",
        )

    def _ensure_pair_history(
        self,
        base_currency: str,
        quote_currency: str,
    ) -> FXSeries:
        provider_norm = self.provider_name
        pair_key = (provider_norm, base_currency, quote_currency)
        cached = self._history_cache.get(pair_key)
        if cached is not None:
            return cached
        if self._provider_fully_preloaded:
            empty = FXSeries.empty()
            self._history_cache[pair_key] = empty
            return empty
        rows = self.repository.fetch_pair_history(
            provider_norm,
            base_currency,
            quote_currency,
        )
        series = FXSeries.from_rows(rows)
        self._history_cache[pair_key] = series
        return series


__all__ = [
    "DEFAULT_PROVIDER",
    "EODHDFXProvider",
    "FXCatalogEntry",
    "FXQuote",
    "FXRefreshProvider",
    "FXService",
    "FXSeries",
    "FrankfurterProvider",
    "MissingFXRateError",
    "parse_eodhd_fx_catalog_entry",
]
