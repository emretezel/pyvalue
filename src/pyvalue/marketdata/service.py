"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.currency import (
    normalize_monetary_amount,
    raw_currency_code,
)
from pyvalue.marketdata import (
    EODHDProvider,
    MarketDataProvider,
    MarketDataUpdate,
    PriceData,
)
from pyvalue.facts import RegionFactsRepository
from pyvalue.persistence.storage import (
    FinancialFactsRepository,
    MarketDataRepository,
    SupportedTickerRepository,
)

LOGGER = logging.getLogger(__name__)


def latest_share_count(
    symbol: str, repo: FinancialFactsRepository | RegionFactsRepository
) -> Optional[float]:
    """Return the latest reported shares-outstanding count for ``symbol``.

    A standalone helper (no service state) so any caller holding a facts
    repository can resolve a share count without constructing a provider-backed
    service -- used both by the on-demand market-cap computation and, in future,
    by the share-count-dated price backfill in ``update-market-data``.
    """

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

    def prepare_price_data(
        self,
        symbol: str,
        data: PriceData,
        currency_hint: Optional[str] = None,
    ) -> PriceData:
        """Collapse a quoted price to its major currency for persistence.

        The provider quotes the price in a currency that may be a subunit (e.g.
        GBX pence on the LSE). We prefer the provider's own code -- it
        disambiguates pence from pounds -- then an explicit hint, then the
        listing's quote currency, and collapse that to the MAJOR currency so
        subunits never cross the data boundary: ``market_data.price`` is always
        stored in the major currency and the snapshot read path reports the same
        base currency.

        No anomaly guard runs here. Market value is derived by pairing a
        share-count fact with the price *as of that fact's date*
        (``metrics.utils.market_cap_money``), so a price and a share count are
        never multiplied across mismatched dates -- there is no cross-snapshot
        value jump left to police.
        """

        normalized_symbol = symbol.upper()
        quoted_currency = raw_currency_code(
            data.currency
            or currency_hint
            or self.supported_ticker_repo.fetch_currency(normalized_symbol)
        )
        major_amount, major_currency = normalize_monetary_amount(
            data.price, quoted_currency
        )
        price = float(major_amount) if major_amount is not None else data.price
        return PriceData(
            symbol=normalized_symbol,
            price=price,
            as_of=data.as_of,
            volume=data.volume,
            currency=major_currency,
        )

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
        )
        LOGGER.info("Stored market data for %s at %s", prepared.symbol, prepared.as_of)
        return prepared


__all__ = [
    "MarketDataService",
    "latest_share_count",
]
