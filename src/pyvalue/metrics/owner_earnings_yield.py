"""Owner earnings yield metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.fx import FXRateStore
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value import (
    EV_FALLBACK_REQUIRED_CONCEPTS,
    convert_denominator_amount,
    resolve_enterprise_value_denominator,
)
from pyvalue.money import ephemeral_fx_database_path
from pyvalue.metrics.owner_earnings_enterprise import (
    REQUIRED_CONCEPTS as OE_EV_REQUIRED_CONCEPTS,
    OwnerEarningsEnterpriseCalculator,
)
from pyvalue.metrics.owner_earnings_equity import (
    REQUIRED_CONCEPTS as OE_EQUITY_REQUIRED_CONCEPTS,
    OwnerEarningsEquityCalculator,
)
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

REQUIRED_CONCEPTS = OE_EQUITY_REQUIRED_CONCEPTS
REQUIRED_EV_CONCEPTS = tuple(
    dict.fromkeys(OE_EV_REQUIRED_CONCEPTS + EV_FALLBACK_REQUIRED_CONCEPTS)
)


def _convert_denominator(
    *,
    symbol: str,
    amount: float,
    source_currency: Optional[str],
    target_currency: Optional[str],
    as_of: str,
    context: str,
    database: str,
) -> Optional[float]:
    return convert_denominator_amount(
        symbol=symbol,
        amount=amount,
        source_currency=source_currency,
        target_currency=target_currency,
        as_of=as_of,
        context=context,
        converter=FXRateStore(database).convert,
    )


def _denominator_market_cap(
    *,
    symbol: str,
    market_repo: MarketDataRepository,
    target_currency: Optional[str],
    context: str,
    database: str,
) -> Optional[float]:
    snapshot = market_repo.latest_snapshot(symbol)
    if snapshot is None or snapshot.market_cap is None:
        LOGGER.warning("%s: missing market cap snapshot for %s", context, symbol)
        return None
    if snapshot.market_cap <= 0:
        LOGGER.warning("%s: non-positive market cap snapshot for %s", context, symbol)
        return None

    return _convert_denominator(
        symbol=symbol,
        amount=snapshot.market_cap,
        source_currency=getattr(snapshot, "currency", None),
        target_currency=target_currency,
        as_of=snapshot.as_of,
        context=context,
        database=database,
    )


def _denominator_enterprise_value(
    *,
    symbol: str,
    repo: FinancialFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: Optional[str],
    context: str,
) -> Optional[float]:
    return resolve_enterprise_value_denominator(
        symbol=symbol,
        repo=repo,
        market_repo=market_repo,
        target_currency=target_currency,
        context=context,
        converter=FXRateStore(
            str(getattr(repo, "db_path", ephemeral_fx_database_path()))
        ).convert,
    )


@dataclass
class OwnerEarningsYieldEquityMetric:
    """Compute owner earnings yield using TTM owner earnings equity."""

    id: str = "oey_equity"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_ttm(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_equity: missing numerator for %s", symbol)
            return None

        market_cap = _denominator_market_cap(
            symbol=symbol,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            database=str(getattr(repo, "db_path", ephemeral_fx_database_path())),
        )
        if market_cap is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / market_cap,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEquityFiveYearMetric:
    """Compute owner earnings yield using 5-year average owner earnings equity."""

    id: str = "oey_equity_5y"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_5y_average(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_equity_5y: missing numerator for %s", symbol)
            return None

        market_cap = _denominator_market_cap(
            symbol=symbol,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            database=str(getattr(repo, "db_path", ephemeral_fx_database_path())),
        )
        if market_cap is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / market_cap,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEVMetric:
    """Compute owner earnings yield using TTM owner earnings enterprise."""

    id: str = "oey_ev"
    required_concepts = REQUIRED_EV_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEnterpriseCalculator().compute_ttm(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_ev: missing numerator for %s", symbol)
            return None

        enterprise_value = _denominator_enterprise_value(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEVNormalizedMetric:
    """Compute normalized owner earnings yield using FY median owner earnings enterprise."""

    id: str = "oey_ev_norm"
    required_concepts = REQUIRED_EV_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEnterpriseCalculator().compute_5y_median(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_ev_norm: missing numerator for %s", symbol)
            return None

        enterprise_value = _denominator_enterprise_value(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / enterprise_value,
            as_of=numerator.as_of,
        )


__all__ = [
    "OwnerEarningsYieldEquityMetric",
    "OwnerEarningsYieldEquityFiveYearMetric",
    "OwnerEarningsYieldEVMetric",
    "OwnerEarningsYieldEVNormalizedMetric",
]
