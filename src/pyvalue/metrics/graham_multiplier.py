"""Graham multiplier metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    latest_quarterly_records,
    normalize_metric_amount,
    normalize_metric_record,
    resolve_metric_ticker_currency,
)
from pyvalue.marketdata.base import PriceData
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository


EPS_CONCEPTS = ["EarningsPerShare"]
EQUITY_CONCEPTS = ["StockholdersEquity"]
SHARE_CONCEPTS = ["CommonStockSharesOutstanding"]
GOODWILL_CONCEPTS = ["Goodwill"]
INTANGIBLE_CONCEPTS = ["IntangibleAssetsNetExcludingGoodwill"]

LOGGER = logging.getLogger(__name__)


@dataclass
class GrahamMultiplierMetric:
    id: str = "graham_multiplier"
    required_concepts = tuple(
        EPS_CONCEPTS
        + EQUITY_CONCEPTS
        + SHARE_CONCEPTS
        + GOODWILL_CONCEPTS
        + INTANGIBLE_CONCEPTS
    )
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        eps_records = self._latest_quarters(symbol, repo)
        if len(eps_records) >= 4:
            target_currency = resolve_metric_ticker_currency(
                symbol,
                repo,
                market_repo,
                candidate_currencies=[record.currency for record in eps_records[:4]],
            )
            ttm_eps = sum(
                normalize_metric_record(
                    record,
                    metric_id=self.id,
                    symbol=symbol,
                    expected_currency=target_currency,
                    contexts=(repo,),
                )[0]
                for record in eps_records[:4]
            )
            eps_as_of = eps_records[0].end_date
        else:
            fy_record = self._latest_fy_eps(symbol, repo)
            if fy_record is None or not is_recent_fact(fy_record):
                LOGGER.warning("graham_multiplier: missing EPS quarters for %s", symbol)
                return None
            ttm_eps, target_currency = normalize_metric_record(
                fy_record,
                metric_id=self.id,
                symbol=symbol,
                contexts=(repo, market_repo),
            )
            eps_as_of = fy_record.end_date

        if ttm_eps <= 0:
            LOGGER.warning("graham_multiplier: non-positive TTM EPS for %s", symbol)
            return None

        equity, _ = self._latest_value(
            symbol,
            repo,
            EQUITY_CONCEPTS,
            metric_id=self.id,
            target_currency=target_currency,
        )
        shares, _ = self._latest_value(symbol, repo, SHARE_CONCEPTS)
        if equity is None or shares is None or shares <= 0:
            LOGGER.warning("graham_multiplier: equity/shares missing for %s", symbol)
            return None

        goodwill, _ = self._latest_optional_value(
            symbol,
            repo,
            GOODWILL_CONCEPTS,
            metric_id=self.id,
            target_currency=target_currency,
        )
        intangibles, _ = self._latest_optional_value(
            symbol,
            repo,
            INTANGIBLE_CONCEPTS,
            metric_id=self.id,
            target_currency=target_currency,
        )

        price_data = self._latest_snapshot(market_repo, symbol)
        if price_data is None or price_data.price is None:
            LOGGER.warning("graham_multiplier: missing price for %s", symbol)
            return None
        price_currency = price_data.currency
        price, _ = normalize_metric_amount(
            price_data.price,
            price_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="price",
            as_of=price_data.as_of,
            expected_currency=target_currency,
            contexts=(market_repo, repo),
        )

        if price <= 0:
            LOGGER.warning("graham_multiplier: non-positive price for %s", symbol)
            return None

        tbvps = (equity - goodwill - intangibles) / shares
        if tbvps <= 0:
            LOGGER.warning("graham_multiplier: non-positive TBVPS for %s", symbol)
            return None

        multiplier = (price / ttm_eps) * (price / tbvps)
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=multiplier, as_of=eps_as_of
        )

    def _latest_quarters(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> list[FactRecord]:
        return latest_quarterly_records(
            repo.facts_for_concept, symbol, EPS_CONCEPTS, periods=4
        )

    def _latest_fy_eps(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[FactRecord]:
        records = repo.facts_for_concept(
            symbol, "EarningsPerShare", fiscal_period="FY", limit=1
        )
        if records:
            return records[0]
        return None

    def _latest_value(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: list[str],
        *,
        metric_id: Optional[str] = None,
        target_currency: Optional[str] = None,
    ) -> Tuple[Optional[float], Optional[str]]:
        for concept in concepts:
            fact = repo.latest_fact(symbol, concept)
            if fact is None or not is_recent_fact(fact):
                continue
            if fact.value is not None:
                try:
                    if metric_id is None:
                        return float(fact.value), fact.currency
                    value, currency = normalize_metric_record(
                        fact,
                        metric_id=metric_id,
                        symbol=symbol,
                        expected_currency=target_currency,
                        contexts=(repo,),
                    )
                    return value, currency
                except (TypeError, ValueError):
                    continue
        return None, None

    def _latest_optional_value(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: list[str],
        *,
        metric_id: str,
        target_currency: Optional[str],
    ) -> Tuple[float, Optional[str]]:
        value, currency = self._latest_value(
            symbol,
            repo,
            concepts,
            metric_id=metric_id,
            target_currency=target_currency,
        )
        if value is None:
            return 0.0, None
        return value, currency

    def _latest_snapshot(
        self, market_repo: MarketDataRepository, symbol: str
    ) -> Optional[PriceData]:
        if hasattr(market_repo, "latest_snapshot"):
            snapshot = market_repo.latest_snapshot(symbol)
            if snapshot:
                return snapshot
        if hasattr(market_repo, "latest_price"):
            price_entry = market_repo.latest_price(symbol)
            if isinstance(price_entry, PriceData):
                return price_entry
            if isinstance(price_entry, tuple) and len(price_entry) >= 2:
                as_of, price = price_entry[0], price_entry[1]
                return PriceData(symbol=symbol, price=price, as_of=as_of)
        return None
