"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
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


def latest_share_count(
    symbol: str, repo: FinancialFactsRepository | RegionFactsRepository
) -> Optional[float]:
    for concept in SHARE_CONCEPTS:
        fact = repo.latest_fact(symbol, concept)
        if fact is None or fact.value is None:
            continue
        try:
            return float(fact.value)
        except (TypeError, ValueError):
            continue
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
        effective_currency = (
            prepared.currency
            or currency_hint
            or self.supported_ticker_repo.fetch_currency(normalized_symbol)
        )
        price = prepared.price
        if (
            effective_currency
            and effective_currency.upper() in {"GBX", "GBP0.01"}
            and price is not None
        ):
            price = price / 100.0
            effective_currency = "GBP"
        market_cap = prepared.market_cap
        if market_cap is None and price is not None:
            shares = latest_share_count(normalized_symbol, self.facts_repo)
            if shares is None:
                shares = self._shares_from_fundamentals(normalized_symbol)
            if shares is not None:
                market_cap = shares * price
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


__all__ = ["MarketDataService", "latest_share_count"]
