"""Gross-margin stability metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import sqrt
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

REVENUE_CONCEPT = "Revenues"
GROSS_PROFIT_CONCEPT = "GrossProfit"
COST_OF_REVENUE_CONCEPT = "CostOfRevenue"

FY_PERIODS = {"FY"}
SERIES_YEARS = 10

REQUIRED_CONCEPTS = (
    REVENUE_CONCEPT,
    GROSS_PROFIT_CONCEPT,
    COST_OF_REVENUE_CONCEPT,
)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass(frozen=True)
class _GrossMarginFYPoint:
    year: int
    value: float
    as_of: str


@dataclass(frozen=True)
class GrossMarginTenYearSnapshot:
    points: tuple[_GrossMarginFYPoint, ...]
    as_of: str
    currency: Optional[str]


class GrossMarginTenYearCalculator:
    """Build a strict 10-year consecutive FY gross-margin series.

    Revenue and gross profit are aligned to the listing currency, so each margin
    (gross profit / revenue) is a currency-safe dimensionless ratio.
    """

    def compute_series(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[GrossMarginTenYearSnapshot]:
        target_currency = require_metric_ticker_currency(
            symbol, repo, metric_id="gm_10y_std"
        )
        revenue_map = self._fy_map(symbol, repo, REVENUE_CONCEPT, target_currency)
        if not revenue_map:
            LOGGER.warning("gm_10y_std: missing FY revenues history for %s", symbol)
            return None

        gross_profit_map = self._fy_map(
            symbol, repo, GROSS_PROFIT_CONCEPT, target_currency
        )
        cost_of_revenue_map = self._fy_map(
            symbol, repo, COST_OF_REVENUE_CONCEPT, target_currency
        )

        margins_by_year: dict[int, _GrossMarginFYPoint] = {}
        for year, revenue in revenue_map.items():
            if revenue.money.amount <= 0:
                continue

            gross_profit = gross_profit_map.get(year)
            if gross_profit is not None:
                gross_profit_money = gross_profit.money
                as_of = max(revenue.as_of, gross_profit.as_of)
            else:
                cost_of_revenue = cost_of_revenue_map.get(year)
                if cost_of_revenue is None:
                    continue
                gross_profit_money = revenue.money - cost_of_revenue.money
                as_of = max(revenue.as_of, cost_of_revenue.as_of)

            margins_by_year[year] = _GrossMarginFYPoint(
                year=year,
                value=gross_profit_money / revenue.money,
                as_of=as_of,
            )

        if not margins_by_year:
            LOGGER.warning("gm_10y_std: no FY gross-margin points for %s", symbol)
            return None

        latest_year = max(margins_by_year.keys())
        selected: list[_GrossMarginFYPoint] = []
        for year in range(latest_year, latest_year - SERIES_YEARS, -1):
            point = margins_by_year.get(year)
            if point is None:
                LOGGER.warning(
                    "gm_10y_std: missing strict consecutive FY chain for %s", symbol
                )
                return None
            selected.append(point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("gm_10y_std: latest FY point too old for %s", symbol)
            return None

        return GrossMarginTenYearSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=target_currency,
        )

    def _fy_map(
        self,
        symbol: str,
        repo: RegionFactsRepository,
        concept: str,
        target_currency: str,
    ) -> dict[int, _MoneyResult]:
        records = repo.monetary_facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _MoneyResult] = {}
        for record in ordered:
            year = self._extract_year(record.end_date)
            if year is None or year in mapped:
                continue
            mapped[year] = _MoneyResult(
                money=require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id="gm_10y_std",
                    symbol=symbol,
                    input_name=concept,
                    as_of=record.end_date,
                ),
                as_of=record.end_date,
            )
        return mapped

    def _filter_periods(
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods or record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        prefix = value[:4]
        if not prefix.isdigit():
            return None
        return int(prefix)

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))


@dataclass
class GrossMarginTenYearStdMetric:
    """Compute population stddev of FY gross margin over strict latest 10 years."""

    id: str = "gm_10y_std"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = GrossMarginTenYearCalculator().compute_series(symbol, repo)
        if snapshot is None:
            return None

        values = [point.value for point in snapshot.points]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        stddev = sqrt(variance)

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=stddev,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


__all__ = [
    "GrossMarginTenYearSnapshot",
    "GrossMarginTenYearCalculator",
    "GrossMarginTenYearStdMetric",
]
