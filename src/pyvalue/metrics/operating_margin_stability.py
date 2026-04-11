"""Operating-margin stability metrics implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import sqrt
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    normalize_metric_record,
    resolve_metric_ticker_currency,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

OPERATING_INCOME_CONCEPT = "OperatingIncomeLoss"
REVENUE_CONCEPT = "Revenues"

FY_PERIODS = {"FY"}
SERIES_YEARS = 10

REQUIRED_CONCEPTS = (
    OPERATING_INCOME_CONCEPT,
    REVENUE_CONCEPT,
)


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _OperatingMarginFYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class OperatingMarginTenYearSnapshot:
    points: tuple[_OperatingMarginFYPoint, ...]
    as_of: str
    currency: Optional[str]


class OperatingMarginTenYearCalculator:
    """Build a strict 10-year consecutive FY operating-margin series."""

    def compute_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OperatingMarginTenYearSnapshot]:
        operating_income_map = self._fy_map(symbol, repo, OPERATING_INCOME_CONCEPT)
        if not operating_income_map:
            LOGGER.warning(
                "opm_10y: missing FY operating income history for %s", symbol
            )
            return None

        revenue_map = self._fy_map(symbol, repo, REVENUE_CONCEPT)
        if not revenue_map:
            LOGGER.warning("opm_10y: missing FY revenues history for %s", symbol)
            return None

        margins_by_year: dict[int, _OperatingMarginFYPoint] = {}
        for year, revenue in revenue_map.items():
            if revenue.total <= 0:
                continue

            operating_income = operating_income_map.get(year)
            if operating_income is None:
                continue

            margins_by_year[year] = _OperatingMarginFYPoint(
                year=year,
                value=operating_income.total / revenue.total,
                as_of=max(revenue.as_of, operating_income.as_of),
                currency=None,
            )

        if not margins_by_year:
            LOGGER.warning("opm_10y: no FY operating-margin points for %s", symbol)
            return None

        latest_year = max(margins_by_year.keys())
        selected: list[_OperatingMarginFYPoint] = []
        for year in range(latest_year, latest_year - SERIES_YEARS, -1):
            point = margins_by_year.get(year)
            if point is None:
                LOGGER.warning(
                    "opm_10y: missing strict consecutive FY chain for %s", symbol
                )
                return None
            selected.append(point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("opm_10y: latest FY point too old for %s", symbol)
            return None

        return OperatingMarginTenYearSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=None,
        )

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
    ) -> dict[int, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _AmountResult] = {}
        for record in ordered:
            year = self._extract_year(record.end_date)
            if year is None or year in mapped:
                continue
            value, currency = self._normalize_currency(record, symbol, repo, concept)
            mapped[year] = _AmountResult(
                total=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

    def _filter_periods(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if (
                period not in periods
                or record.end_date in seen_end_dates
                or record.value is None
            ):
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_currency(
        self,
        record: FactRecord,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
    ) -> tuple[float, str]:
        return normalize_metric_record(
            record,
            metric_id="opm_10y",
            symbol=symbol,
            input_name=concept,
            expected_currency=resolve_metric_ticker_currency(
                symbol,
                repo,
                candidate_currencies=[record.currency],
            ),
            contexts=(repo,),
        )

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
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OperatingMarginTenYearCalculator().compute_series(symbol, repo)
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


@dataclass
class OperatingMarginTenYearMinMetric:
    """Compute the minimum FY operating margin over strict latest 10 years."""

    id: str = "opm_10y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OperatingMarginTenYearCalculator().compute_series(symbol, repo)
        if snapshot is None:
            return None

        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


__all__ = [
    "OperatingMarginTenYearSnapshot",
    "OperatingMarginTenYearCalculator",
    "OperatingMarginTenYearStdMetric",
    "OperatingMarginTenYearMinMetric",
]
