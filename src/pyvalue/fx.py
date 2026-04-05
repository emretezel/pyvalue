"""DB-backed FX lookup and conversion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional, Protocol, Sequence, Union
import json
import logging

import requests  # type: ignore[import-untyped]

from pyvalue.config import Config
from pyvalue.currency import normalize_currency_code, normalize_monetary_amount
from pyvalue.storage import FXRateRecord, FXRatesRepository


LOGGER = logging.getLogger(__name__)
DEFAULT_PROVIDER = "FRANKFURTER"
DEFAULT_API_BASE = "https://api.frankfurter.dev/v2"
DEFAULT_FETCH_LOOKBACK_DAYS = 14


@dataclass(frozen=True)
class _EphemeralFXConfig:
    """Non-fetching FX config used for ephemeral in-memory contexts."""

    fx_pivot_currency: str = "USD"
    fx_secondary_pivot_currency: Optional[str] = "EUR"
    fx_lazy_fetch: bool = False
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


class FXProvider(Protocol):
    """Provider abstraction for fetching direct FX rates."""

    provider_name: str

    def fetch_rates(
        self,
        *,
        base_currency: str,
        quote_currencies: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> list[FXRateRecord]: ...


class FrankfurterProvider:
    """Fetch direct FX rates from the Frankfurter v2 API."""

    provider_name = DEFAULT_PROVIDER

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        api_base: str = DEFAULT_API_BASE,
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
        quotes = sorted(
            {
                code
                for code in (normalize_currency_code(item) for item in quote_currencies)
                if code is not None and code != normalize_currency_code(base_currency)
            }
        )
        base = normalize_currency_code(base_currency)
        if base is None or not quotes:
            return []

        params = {
            "base": base,
            "quotes": ",".join(quotes),
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
        if response.status_code in {400, 404, 422}:
            LOGGER.warning(
                "Frankfurter FX request failed | base=%s quotes=%s status=%s body=%s",
                base,
                ",".join(quotes),
                response.status_code,
                response.text[:500],
            )
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


class FXService:
    """Resolve FX rates from the local DB, lazy-fetching gaps when configured."""

    def __init__(
        self,
        database: Union[str, Path],
        *,
        repository: Optional[FXRatesRepository] = None,
        provider: Optional[FXProvider] = None,
        config: Optional[Config] = None,
    ) -> None:
        default_config: Config | _EphemeralFXConfig
        if config is None and str(database) == ":memory:":
            default_config = _EphemeralFXConfig()
        else:
            default_config = config or Config()
        self.config = default_config
        self.repository = repository or FXRatesRepository(database)
        self.repository.initialize_schema()
        self.provider = provider or FrankfurterProvider()
        self.provider_name = self.provider.provider_name.strip().upper()
        self.pivot_currency = (
            normalize_currency_code(self.config.fx_pivot_currency) or "USD"
        )
        self.secondary_pivot_currency = normalize_currency_code(
            self.config.fx_secondary_pivot_currency
        )
        self.lazy_fetch = bool(self.config.fx_lazy_fetch)
        self.stale_warning_days = max(int(self.config.fx_stale_warning_days), 0)

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

        quote_result = self._lookup(base, quote, as_of)
        if quote_result is None and self.lazy_fetch:
            self._fetch_missing_rates(base, quote, as_of)
            quote_result = self._lookup(base, quote, as_of)
        if quote_result is None:
            LOGGER.warning(
                "Missing FX rate | base=%s quote=%s as_of=%s operation=get_fx_rate",
                base,
                quote,
                as_of.isoformat(),
            )
            return None

        age_days = (as_of - quote_result.rate_date).days
        if age_days > self.stale_warning_days:
            LOGGER.warning(
                "Stale FX rate used | base=%s quote=%s requested_as_of=%s rate_date=%s age_days=%s source_kind=%s",
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
        direct_or_inverse = self._lookup_direct_or_inverse(
            base_currency,
            quote_currency,
            as_of,
        )
        if direct_or_inverse is not None:
            return direct_or_inverse

        for pivot in (self.pivot_currency, self.secondary_pivot_currency):
            if pivot is None or pivot in {base_currency, quote_currency}:
                continue
            base_to_pivot = self._lookup_direct_or_inverse(base_currency, pivot, as_of)
            quote_to_pivot = self._lookup_direct_or_inverse(
                quote_currency,
                pivot,
                as_of,
            )
            if base_to_pivot is None or quote_to_pivot is None:
                continue
            if quote_to_pivot.rate == 0:
                continue
            return FXQuote(
                provider=self.provider_name,
                rate_date=min(base_to_pivot.rate_date, quote_to_pivot.rate_date),
                base_currency=base_currency,
                quote_currency=quote_currency,
                rate=base_to_pivot.rate / quote_to_pivot.rate,
                source_kind="triangulated",
                via_currency=pivot,
            )
        return None

    def _lookup_direct_or_inverse(
        self,
        base_currency: str,
        quote_currency: str,
        as_of: date,
    ) -> Optional[FXQuote]:
        direct = self.repository.latest_on_or_before(
            self.provider_name,
            base_currency,
            quote_currency,
            as_of.isoformat(),
        )
        if direct is not None:
            rate = Decimal(direct.rate_text)
            return FXQuote(
                provider=direct.provider,
                rate_date=date.fromisoformat(direct.rate_date),
                base_currency=direct.base_currency,
                quote_currency=direct.quote_currency,
                rate=rate,
                source_kind=direct.source_kind,
            )

        inverse = self.repository.latest_on_or_before(
            self.provider_name,
            quote_currency,
            base_currency,
            as_of.isoformat(),
        )
        if inverse is not None:
            rate = Decimal(inverse.rate_text)
            if rate == 0:
                return None
            return FXQuote(
                provider=inverse.provider,
                rate_date=date.fromisoformat(inverse.rate_date),
                base_currency=base_currency,
                quote_currency=quote_currency,
                rate=Decimal("1") / rate,
                source_kind="inverse",
            )
        return None

    def _fetch_missing_rates(
        self,
        base_currency: str,
        quote_currency: str,
        as_of: date,
    ) -> None:
        start_date = as_of - timedelta(days=DEFAULT_FETCH_LOOKBACK_DAYS)
        quote_sets = [
            {quote_currency, self.pivot_currency, self.secondary_pivot_currency},
            {self.pivot_currency, self.secondary_pivot_currency},
        ]
        base_currencies = [base_currency, quote_currency]
        for base, quotes in zip(base_currencies, quote_sets):
            cleaned_quotes = [
                code
                for code in quotes
                if code is not None and code != base and normalize_currency_code(code)
            ]
            if not cleaned_quotes:
                continue
            try:
                rows = self.provider.fetch_rates(
                    base_currency=base,
                    quote_currencies=cleaned_quotes,
                    start_date=start_date,
                    end_date=as_of,
                )
            except requests.RequestException as exc:
                LOGGER.warning(
                    "FX provider request failed | provider=%s base=%s quotes=%s as_of=%s exception=%s",
                    self.provider_name,
                    base,
                    ",".join(sorted(cleaned_quotes)),
                    as_of.isoformat(),
                    exc,
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive provider boundary
                LOGGER.warning(
                    "FX provider error | provider=%s base=%s quotes=%s as_of=%s exception=%s",
                    self.provider_name,
                    base,
                    ",".join(sorted(cleaned_quotes)),
                    as_of.isoformat(),
                    exc,
                )
                continue
            if rows:
                self.repository.upsert_many(rows)


class FXRateStore:
    """Compatibility wrapper exposing the historical ``convert`` API."""

    def __init__(
        self,
        database: Union[str, Path] = "data/pyvalue.db",
        *,
        service: Optional[FXService] = None,
        provider: Optional[FXProvider] = None,
        config: Optional[Config] = None,
    ) -> None:
        self.service = service or FXService(database, provider=provider, config=config)

    def convert(
        self,
        amount: float,
        from_currency: str,
        to_currency: str,
        as_of: str | date,
    ) -> Optional[float]:
        converted = self.service.convert_amount(
            amount, from_currency, to_currency, as_of
        )
        if converted is None:
            return None
        return float(converted)


__all__ = [
    "DEFAULT_PROVIDER",
    "FXProvider",
    "FXQuote",
    "FXRateStore",
    "FXService",
    "FrankfurterProvider",
]
