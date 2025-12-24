"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.marketdata import EODHDProvider, MarketDataProvider, PriceData
from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import FinancialFactsRepository, FundamentalsRepository, MarketDataRepository, UniverseRepository

LOGGER = logging.getLogger(__name__)

SHARE_CONCEPTS = [
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
]


def latest_share_count(symbol: str, repo: FinancialFactsRepository) -> Optional[float]:
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
        self.universe_repo = UniverseRepository(db_path)
        self.universe_repo.initialize_schema()
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

    def refresh_symbol(self, symbol: str, fetch_symbol: Optional[str] = None) -> PriceData:
        fetch = fetch_symbol or symbol
        data = self.provider.latest_price(fetch)
        data.symbol = symbol.upper()
        currency_hint = data.currency or self.universe_repo.fetch_currency(symbol)
        price = data.price
        if currency_hint and currency_hint.upper() in {"GBX", "GBP0.01"} and price is not None:
            price = price / 100.0
            currency_hint = "GBP"
        market_cap = data.market_cap
        if market_cap is None and price is not None:
            shares = latest_share_count(symbol, self.facts_repo)
            if shares is None:
                shares = self._shares_from_fundamentals(symbol)
            if shares is not None:
                market_cap = shares * price
        self.repo.upsert_price(
            symbol=data.symbol,
            as_of=data.as_of,
            price=price,
            volume=data.volume,
            currency=currency_hint or data.currency,
            market_cap=market_cap,
        )
        data.market_cap = market_cap
        data.price = price
        data.currency = currency_hint or data.currency
        LOGGER.info("Stored market data for %s at %s", data.symbol, data.as_of)
        return data

__all__ = ["MarketDataService", "latest_share_count"]
