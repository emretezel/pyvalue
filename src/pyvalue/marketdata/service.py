"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.currency import (
    is_subunit_currency,
    normalize_currency_code,
    normalize_monetary_amount,
)
from pyvalue.marketdata import (
    EODHDProvider,
    MarketDataProvider,
    MarketDataUpdate,
    PriceData,
)
from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import (
    FinancialFactsRepository,
    FundamentalsRepository,
    MarketDataRepository,
    SupportedTickerRepository,
)

LOGGER = logging.getLogger(__name__)

SHARE_CONCEPTS = [
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
]
PRICE_VALIDATION_WINDOW_DAYS = 180
MAX_UNEXPLAINED_MARKET_VALUE_CHANGE_FACTOR = 50.0


class SuspiciousMarketPriceChangeError(ValueError):
    """Raised when fetched market data implies an implausible value jump."""


def latest_share_count(
    symbol: str, repo: FinancialFactsRepository | RegionFactsRepository
) -> Optional[float]:
    counts = repo.latest_share_counts_many([symbol], chunk_size=1)
    value = counts.get(symbol.upper())
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class MarketDataService:
    """Coordinates provider selection and persistence of price data."""

    def __init__(
        self,
        db_path: Union[str, Path],
        provider: Optional[MarketDataProvider] = None,
        config: Optional[Config] = None,
    ) -> None:
        self.config = config or Config()
        self.repo = MarketDataRepository(db_path)
        self.repo.initialize_schema()
        base_facts_repo = FinancialFactsRepository(db_path)
        base_facts_repo.initialize_schema()
        self.facts_repo = RegionFactsRepository(base_facts_repo)
        self.fund_repo = FundamentalsRepository(db_path)
        self.fund_repo.initialize_schema()
        self.supported_ticker_repo = SupportedTickerRepository(db_path)
        self.supported_ticker_repo.initialize_schema()
        self.provider = provider or self._default_provider()

    def _default_provider(self) -> MarketDataProvider:
        api_key = self.config.eodhd_api_key
        if api_key:
            return EODHDProvider(api_key=api_key)
        raise RuntimeError(
            "No market data API key configured. Set eodhd.api_key in private/config.toml."
        )

    def _shares_from_fundamentals(self, symbol: str) -> Optional[float]:
        record = self.fund_repo.fetch("EODHD", symbol.upper())
        if not record:
            return None
        stats = record.get("SharesStats") or {}
        general = record.get("General") or {}
        for candidate in (
            stats.get("SharesOutstanding"),
            stats.get("SharesFloat"),
            general.get("SharesOutstanding"),
        ):
            if candidate is None:
                continue
            try:
                value = float(candidate)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                continue
        return None

    def _latest_share_count(self, symbol: str) -> Optional[float]:
        shares = latest_share_count(symbol, self.facts_repo)
        if shares is not None:
            return shares
        return self._shares_from_fundamentals(symbol)

    def _validate_price_change(
        self,
        symbol: str,
        *,
        as_of: str,
        price: Optional[float],
        currency: Optional[str],
        market_cap: Optional[float],
    ) -> None:
        if price is None or price <= 0:
            return

        previous = self.repo.latest_snapshot_record(symbol)
        if previous is None or previous.price is None or previous.price <= 0:
            return
        if previous.currency and currency and previous.currency != currency:
            return

        try:
            current_date = date.fromisoformat(as_of)
            previous_date = date.fromisoformat(previous.as_of)
        except ValueError:
            return

        if current_date <= previous_date:
            return
        if (current_date - previous_date).days > PRICE_VALIDATION_WINDOW_DAYS:
            return

        price_ratio = price / previous.price
        if price_ratio <= 0:
            return

        current_market_cap = (
            market_cap if market_cap is not None and market_cap > 0 else None
        )
        previous_market_cap = (
            previous.market_cap
            if previous.market_cap is not None and previous.market_cap > 0
            else None
        )

        basis = "price"
        if current_market_cap is not None and previous_market_cap is not None:
            value_ratio = current_market_cap / previous_market_cap
            basis = "market_cap"
        else:
            current_shares = self._latest_share_count(symbol)
            previous_shares = None
            if previous_market_cap is not None:
                previous_shares = previous_market_cap / previous.price
            if (
                current_shares is not None
                and current_shares > 0
                and previous_shares is not None
                and previous_shares > 0
            ):
                value_ratio = price_ratio * (current_shares / previous_shares)
                basis = "share_adjusted_market_cap"
            else:
                value_ratio = price_ratio

        unexplained_factor = max(value_ratio, 1.0 / value_ratio)
        if unexplained_factor < MAX_UNEXPLAINED_MARKET_VALUE_CHANGE_FACTOR:
            return

        raise SuspiciousMarketPriceChangeError(
            (
                "suspicious market data for "
                f"{symbol}: {basis} changed by {unexplained_factor:.2f}x "
                f"({previous.price} on {previous.as_of} -> {price} on {as_of})"
            )
        )

    def prepare_price_data(
        self,
        symbol: str,
        data: PriceData,
        currency_hint: Optional[str] = None,
    ) -> PriceData:
        normalized_symbol = symbol.upper()
        prepared = PriceData(
            symbol=normalized_symbol,
            price=data.price,
            as_of=data.as_of,
            volume=data.volume,
            market_cap=data.market_cap,
            currency=data.currency,
        )
        raw_effective_currency = (
            prepared.currency
            or currency_hint
            or self.supported_ticker_repo.fetch_currency(normalized_symbol)
        )
        effective_currency = normalize_currency_code(raw_effective_currency)
        price = prepared.price
        if is_subunit_currency(raw_effective_currency) and price is not None:
            normalized_price, normalized_currency = normalize_monetary_amount(
                price,
                raw_effective_currency,
            )
            if normalized_price is not None:
                price = float(normalized_price)
            effective_currency = normalized_currency
        market_cap = prepared.market_cap
        if market_cap is None and price is not None:
            shares = self._latest_share_count(normalized_symbol)
            if shares is not None:
                market_cap = shares * price
        self._validate_price_change(
            normalized_symbol,
            as_of=prepared.as_of,
            price=price,
            currency=effective_currency or prepared.currency,
            market_cap=market_cap,
        )
        prepared.price = price
        prepared.market_cap = market_cap
        prepared.currency = effective_currency or prepared.currency
        return prepared

    def persist_updates(self, updates: list[MarketDataUpdate]) -> None:
        self.repo.upsert_prices(updates)

    def refresh_symbol(
        self, symbol: str, fetch_symbol: Optional[str] = None
    ) -> PriceData:
        fetch = fetch_symbol or symbol
        data = self.provider.latest_price(fetch)
        prepared = self.prepare_price_data(symbol, data)
        self.repo.upsert_price(
            symbol=prepared.symbol,
            as_of=prepared.as_of,
            price=prepared.price,
            volume=prepared.volume,
            currency=prepared.currency,
            market_cap=prepared.market_cap,
        )
        LOGGER.info("Stored market data for %s at %s", prepared.symbol, prepared.as_of)
        return prepared


__all__ = [
    "MarketDataService",
    "SuspiciousMarketPriceChangeError",
    "latest_share_count",
]
