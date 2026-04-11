"""FY consistency metrics for owner-earnings and free-cash-flow screening.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
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

OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
CAPEX_CONCEPT = "CapitalExpenditures"
NET_INCOME_PRIMARY_CONCEPT = "NetIncomeLoss"
NET_INCOME_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"

OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
NET_INCOME_CONCEPTS = (
    NET_INCOME_PRIMARY_CONCEPT,
    NET_INCOME_FALLBACK_CONCEPT,
)

FIVE_YEAR_POINTS = 5
TEN_YEAR_POINTS = 10
FY_PERIODS = {"FY"}

FCF_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS)
)
NET_INCOME_REQUIRED_CONCEPTS = tuple(dict.fromkeys(NET_INCOME_CONCEPTS))


@dataclass(frozen=True)
class FundamentalConsistencySnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _FYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _FYSeriesSnapshot:
    points: tuple[_FYPoint, ...]
    as_of: str
    currency: Optional[str]


class FundamentalConsistencyCalculator:
    """Shared FY-series helpers for normalized FCF and net-income consistency metrics."""

    def compute_fcf_5y_median(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[FundamentalConsistencySnapshot]:
        latest_five = self._latest_available_five_points(
            self._build_fcf_points(symbol, repo, context="fcf_fy_median_5y"),
            symbol=symbol,
            context="fcf_fy_median_5y",
        )
        if latest_five is None:
            return None

        median = sorted(point.value for point in latest_five)[2]
        return FundamentalConsistencySnapshot(
            value=median,
            as_of=latest_five[0].as_of,
            currency=self._combine_currency([point.currency for point in latest_five]),
        )

    def compute_fcf_10y_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_FYSeriesSnapshot]:
        return self._strict_ten_year_series(
            self._build_fcf_points(symbol, repo, context="fcf_neg_years_10y"),
            symbol=symbol,
            context="fcf_neg_years_10y",
        )

    def compute_net_income_10y_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_FYSeriesSnapshot]:
        points = self._build_amount_points(
            symbol,
            repo,
            NET_INCOME_CONCEPTS,
            context="ni_loss_years_10y",
        )
        return self._strict_ten_year_series(
            points,
            symbol=symbol,
            context="ni_loss_years_10y",
        )

    def _build_fcf_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> list[_FYPoint]:
        ocf_map = self._build_fy_amount_map(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
        )
        capex_map = self._build_fy_amount_map(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            context=context,
        )

        points: list[_FYPoint] = []
        for year in sorted(ocf_map.keys(), reverse=True):
            operating = ocf_map[year]
            capex = capex_map.get(year)
            currency = self._combine_currency(
                [operating.currency, capex.currency if capex else None]
            )
            if currency is None and any(
                code is not None
                for code in (operating.currency, capex.currency if capex else None)
            ):
                LOGGER.warning(
                    "%s: currency mismatch on FY %s for %s",
                    context,
                    year,
                    symbol,
                )
                continue

            points.append(
                _FYPoint(
                    year=year,
                    value=operating.total - (capex.total if capex else 0.0),
                    as_of=max(
                        [
                            value
                            for value in (
                                operating.as_of,
                                capex.as_of if capex else None,
                            )
                            if value
                        ]
                    ),
                    currency=currency,
                )
            )
        return points

    def _build_amount_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
    ) -> list[_FYPoint]:
        amount_map = self._build_fy_amount_map(
            symbol,
            repo,
            concepts,
            context=context,
        )
        points: list[_FYPoint] = []
        for year in sorted(amount_map.keys(), reverse=True):
            amount = amount_map[year]
            points.append(
                _FYPoint(
                    year=year,
                    value=amount.total,
                    as_of=amount.as_of,
                    currency=amount.currency,
                )
            )
        if not points:
            LOGGER.warning("%s: missing FY history for %s", context, symbol)
        return points

    def _latest_available_five_points(
        self,
        points: list[_FYPoint],
        *,
        symbol: str,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        if len(points) < FIVE_YEAR_POINTS:
            LOGGER.warning(
                "%s: need 5 FY values for %s, found %s",
                context,
                symbol,
                len(points),
            )
            return None

        latest_five = points[:FIVE_YEAR_POINTS]
        if not self._is_recent_as_of(
            latest_five[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "%s: latest FY (%s) too old for %s",
                context,
                latest_five[0].as_of,
                symbol,
            )
            return None

        series_currency = self._combine_currency(
            [point.currency for point in latest_five]
        )
        if series_currency is None and any(
            point.currency is not None for point in latest_five
        ):
            LOGGER.warning(
                "%s: currency mismatch across selected FY series for %s",
                context,
                symbol,
            )
            return None
        return latest_five

    def _strict_ten_year_series(
        self,
        points: list[_FYPoint],
        *,
        symbol: str,
        context: str,
    ) -> Optional[_FYSeriesSnapshot]:
        if not points:
            LOGGER.warning("%s: missing FY history for %s", context, symbol)
            return None

        latest_by_year: dict[int, _FYPoint] = {}
        for point in points:
            latest_by_year.setdefault(point.year, point)

        latest_year = max(latest_by_year)
        selected: list[_FYPoint] = []
        for year in range(latest_year, latest_year - TEN_YEAR_POINTS, -1):
            selected_point = latest_by_year.get(year)
            if selected_point is None:
                LOGGER.warning(
                    "%s: missing strict consecutive FY chain for %s",
                    context,
                    symbol,
                )
                return None
            selected.append(selected_point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "%s: latest FY (%s) too old for %s",
                context,
                selected[0].as_of,
                symbol,
            )
            return None

        series_currency = self._combine_currency([point.currency for point in selected])
        if series_currency is None and any(
            point.currency is not None for point in selected
        ):
            LOGGER.warning(
                "%s: currency mismatch across selected FY series for %s",
                context,
                symbol,
            )
            return None

        return _FYSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=series_currency,
        )

    def _build_fy_amount_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
    ) -> dict[int, _AmountResult]:
        concept_maps = [
            self._fy_map(symbol, repo, concept, context=context) for concept in concepts
        ]
        merged: dict[int, _AmountResult] = {}
        candidate_years: set[int] = set()
        for mapped in concept_maps:
            candidate_years.update(mapped.keys())

        for year in sorted(candidate_years, reverse=True):
            for mapped in concept_maps:
                if year in mapped:
                    merged[year] = mapped[year]
                    break
        return merged

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        context: str,
    ) -> dict[int, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records)
        mapped: dict[int, _AmountResult] = {}
        for record in ordered:
            year = self._parse_year(record.end_date)
            if year is None or year in mapped:
                continue
            value, currency = self._normalize_currency(
                record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=concept,
            )
            mapped[year] = _AmountResult(
                total=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

    def _filter_periods(self, records: Sequence[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in FY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_currency(
        self,
        record: FactRecord,
        *,
        symbol: str,
        repo: FinancialFactsRepository,
        context: str,
        input_name: str,
    ) -> tuple[float, str]:
        return normalize_metric_record(
            record,
            metric_id=context,
            symbol=symbol,
            input_name=input_name,
            expected_currency=resolve_metric_ticker_currency(
                symbol,
                repo,
                candidate_currencies=[record.currency],
            ),
            contexts=(repo,),
        )

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))

    def _parse_year(self, as_of: str) -> Optional[int]:
        try:
            return date.fromisoformat(as_of).year
        except ValueError:
            return None

    def _combine_currency(self, values: Sequence[Optional[str]]) -> Optional[str]:
        merged: Optional[str] = None
        for value in values:
            if not value:
                continue
            if merged is None:
                merged = value
            elif merged != value:
                return None
        return merged


@dataclass
class FCFFiveYearMedianMetric:
    """Compute FY median free cash flow over the latest 5 available years."""

    id: str = "fcf_fy_median_5y"
    required_concepts = FCF_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_fcf_5y_median(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            currency=snapshot.currency,
        )


@dataclass
class NetIncomeLossYearsTenYearMetric:
    """Count FY loss years over the latest strict 10-year window."""

    id: str = "ni_loss_years_10y"
    required_concepts = NET_INCOME_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_net_income_10y_series(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=float(sum(1 for point in snapshot.points if point.value < 0)),
            as_of=snapshot.as_of,
        )


@dataclass
class FCFNegativeYearsTenYearMetric:
    """Count FY negative free-cash-flow years over the latest strict 10-year window."""

    id: str = "fcf_neg_years_10y"
    required_concepts = FCF_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_fcf_10y_series(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=float(sum(1 for point in snapshot.points if point.value < 0)),
            as_of=snapshot.as_of,
        )


__all__ = [
    "FundamentalConsistencyCalculator",
    "FundamentalConsistencySnapshot",
    "FCFFiveYearMedianMetric",
    "NetIncomeLossYearsTenYearMetric",
    "FCFNegativeYearsTenYearMetric",
]
