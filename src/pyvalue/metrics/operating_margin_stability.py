"""Operating-margin stability metrics implementation.

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

OPERATING_INCOME_CONCEPT = "OperatingIncomeLoss"
REVENUE_CONCEPT = "Revenues"

FY_PERIODS = {"FY"}
TEN_YEAR_SERIES_YEARS = 10
SEVEN_YEAR_SERIES_YEARS = 7

REQUIRED_CONCEPTS = (
    OPERATING_INCOME_CONCEPT,
    REVENUE_CONCEPT,
)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass(frozen=True)
class _OperatingMarginFYPoint:
    year: int
    value: float
    as_of: str


@dataclass(frozen=True)
class OperatingMarginSeriesSnapshot:
    points: tuple[_OperatingMarginFYPoint, ...]
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class OperatingMarginSeriesCalculator:
    """Build a strict consecutive FY operating-margin series for one horizon.

    Operating income and revenue are aligned to the listing currency, so each
    margin (operating income / revenue) is a currency-safe dimensionless ratio.
    """

    metric_context: str
    series_years: int

    def compute_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OperatingMarginSeriesSnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.metric_context
        )
        operating_income_map = self._fy_map(
            listing_id, repo, OPERATING_INCOME_CONCEPT, target_currency
        )
        if not operating_income_map:
            LOGGER.warning(
                "%s: missing FY operating income history for listing_id=%s",
                self.metric_context,
                listing_id,
            )
            return None

        revenue_map = self._fy_map(listing_id, repo, REVENUE_CONCEPT, target_currency)
        if not revenue_map:
            LOGGER.warning(
                "%s: missing FY revenues history for listing_id=%s",
                self.metric_context,
                listing_id,
            )
            return None

        margins_by_year: dict[int, _OperatingMarginFYPoint] = {}
        for year, revenue in revenue_map.items():
            if revenue.money.amount <= 0:
                continue

            operating_income = operating_income_map.get(year)
            if operating_income is None:
                continue

            margins_by_year[year] = _OperatingMarginFYPoint(
                year=year,
                value=operating_income.money / revenue.money,
                as_of=max(revenue.as_of, operating_income.as_of),
            )

        if not margins_by_year:
            LOGGER.warning(
                "%s: no FY operating-margin points for listing_id=%s",
                self.metric_context,
                listing_id,
            )
            return None

        latest_year = max(margins_by_year.keys())
        selected: list[_OperatingMarginFYPoint] = []
        for year in range(latest_year, latest_year - self.series_years, -1):
            point = margins_by_year.get(year)
            if point is None:
                LOGGER.warning(
                    "%s: missing strict consecutive FY chain for listing_id=%s",
                    self.metric_context,
                    listing_id,
                )
                return None
            selected.append(point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "%s: latest FY point too old for listing_id=%s",
                self.metric_context,
                listing_id,
            )
            return None

        return OperatingMarginSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=target_currency,
        )

    def _fy_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        target_currency: str,
    ) -> dict[int, _MoneyResult]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
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
                    metric_id=self.metric_context,
                    listing_id=listing_id,
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
class OperatingMarginTenYearStdMetric:
    """Compute population stddev of FY operating margin over strict latest 10 years."""

    id: str = "opm_10y_std"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OperatingMarginSeriesCalculator(
            metric_context=self.id,
            series_years=TEN_YEAR_SERIES_YEARS,
        ).compute_series(listing_id, repo)
        if snapshot is None:
            return None

        values = [point.value for point in snapshot.points]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        stddev = sqrt(variance)

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=stddev,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


@dataclass
class OperatingMarginTenYearMinMetric:
    """Compute the minimum FY operating margin over strict latest 10 years."""

    id: str = "opm_10y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OperatingMarginSeriesCalculator(
            metric_context=self.id,
            series_years=TEN_YEAR_SERIES_YEARS,
        ).compute_series(listing_id, repo)
        if snapshot is None:
            return None

        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


@dataclass
class OperatingMarginSevenYearMinMetric:
    """Compute the minimum FY operating margin over strict latest 7 years."""

    id: str = "opm_7y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OperatingMarginSeriesCalculator(
            metric_context=self.id,
            series_years=SEVEN_YEAR_SERIES_YEARS,
        ).compute_series(listing_id, repo)
        if snapshot is None:
            return None

        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


__all__ = [
    "OperatingMarginSeriesSnapshot",
    "OperatingMarginSeriesCalculator",
    "OperatingMarginTenYearStdMetric",
    "OperatingMarginTenYearMinMetric",
    "OperatingMarginSevenYearMinMetric",
]
