"""ROIC FY-series metrics implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.invested_capital import (
    REQUIRED_CONCEPTS as INVESTED_CAPITAL_REQUIRED_CONCEPTS,
    InvestedCapitalCalculator,
)
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS
from pyvalue.money import normalize_money_value
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
TAX_EXPENSE_CONCEPT = "IncomeTaxExpense"
PRETAX_INCOME_CONCEPT = "IncomeBeforeIncomeTaxes"

EBIT_CONCEPTS = (EBIT_CONCEPT,)
TAX_EXPENSE_CONCEPTS = (TAX_EXPENSE_CONCEPT,)
PRETAX_INCOME_CONCEPTS = (PRETAX_INCOME_CONCEPT,)
FY_PERIODS = {"FY"}

DEFAULT_TAX_RATE = 0.21
PRETAX_MIN_ABS = 1.0
ABOVE_THRESHOLD = 0.12
SERIES_YEARS = 10
IROIC_LOOKBACK_YEARS = 5
IROIC_MIN_RELATIVE_DELTA_IC = 0.01

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        EBIT_CONCEPTS
        + TAX_EXPENSE_CONCEPTS
        + PRETAX_INCOME_CONCEPTS
        + INVESTED_CAPITAL_REQUIRED_CONCEPTS
    )
)


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _TaxRateResult:
    rate: float
    as_of: Optional[str]


@dataclass(frozen=True)
class _ROICFYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class ROICFYSeriesSnapshot:
    points: tuple[_ROICFYPoint, ...]
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class IncrementalROICSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


class ROICFYSeriesCalculator:
    """Build a strict 10-year consecutive FY ROIC series."""

    def compute_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[ROICFYSeriesSnapshot]:
        ebit_map = self._fy_map(symbol, repo, EBIT_CONCEPT)
        if not ebit_map:
            LOGGER.warning("roic_10y: missing FY EBIT history for %s", symbol)
            return None

        ic_map = self._fy_invested_capital_map(symbol, repo)
        if not ic_map:
            LOGGER.warning(
                "roic_10y: missing FY invested capital history for %s", symbol
            )
            return None

        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT)
        latest_valid_tax_rate = self._latest_valid_fy_tax_rate(tax_map, pretax_map)

        roic_by_year: dict[int, _ROICFYPoint] = {}
        for year, ebit in ebit_map.items():
            ic_current = ic_map.get(year)
            ic_previous = ic_map.get(year - 1)
            if ic_current is None or ic_previous is None:
                continue

            ic_currency = self._combine_currency(
                [ic_current.currency, ic_previous.currency]
            )
            if ic_currency is None and any(
                code is not None for code in (ic_current.currency, ic_previous.currency)
            ):
                continue

            avg_ic = (ic_current.total + ic_previous.total) / 2.0
            if avg_ic == 0:
                continue

            if not self._currencies_match(ebit.currency, ic_currency):
                continue

            tax_rate = self._tax_rate_for_year(
                year=year,
                tax_map=tax_map,
                pretax_map=pretax_map,
                latest_valid_tax_rate=latest_valid_tax_rate,
            )
            nopat = ebit.total * (1.0 - tax_rate.rate)
            as_of_values = [ebit.as_of, ic_current.as_of, ic_previous.as_of]
            if tax_rate.as_of is not None:
                as_of_values.append(tax_rate.as_of)
            roic_by_year[year] = _ROICFYPoint(
                year=year,
                value=nopat / avg_ic,
                as_of=max(as_of_values),
                currency=ebit.currency or ic_currency,
            )

        if not roic_by_year:
            LOGGER.warning("roic_10y: no FY ROIC points for %s", symbol)
            return None

        latest_year = max(roic_by_year.keys())
        selected: list[_ROICFYPoint] = []
        for year in range(latest_year, latest_year - SERIES_YEARS, -1):
            point = roic_by_year.get(year)
            if point is None:
                LOGGER.warning(
                    "roic_10y: missing strict consecutive FY chain for %s", symbol
                )
                return None
            selected.append(point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roic_10y: latest FY point too old for %s", symbol)
            return None

        series_currency = self._combine_currency([point.currency for point in selected])
        if series_currency is None and any(
            point.currency is not None for point in selected
        ):
            LOGGER.warning(
                "roic_10y: currency mismatch across selected series for %s", symbol
            )
            return None

        return ROICFYSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=series_currency,
        )

    def compute_incremental_5y(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[IncrementalROICSnapshot]:
        ebit_map = self._fy_map(symbol, repo, EBIT_CONCEPT)
        if not ebit_map:
            LOGGER.warning("iroic_5y: missing FY EBIT history for %s", symbol)
            return None

        ic_map = self._fy_invested_capital_map(symbol, repo)
        if not ic_map:
            LOGGER.warning(
                "iroic_5y: missing FY invested capital history for %s", symbol
            )
            return None

        latest_year = self._latest_incremental_year(ebit_map, ic_map)
        if latest_year is None:
            LOGGER.warning("iroic_5y: missing strict t and t-5 FY pair for %s", symbol)
            return None

        prior_year = latest_year - IROIC_LOOKBACK_YEARS
        latest_ebit = ebit_map[latest_year]
        prior_ebit = ebit_map[prior_year]
        latest_ic = ic_map[latest_year]
        prior_ic = ic_map[prior_year]

        latest_pair_as_of = max(latest_ebit.as_of, latest_ic.as_of)
        if not self._is_recent_as_of(
            latest_pair_as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("iroic_5y: latest FY point too old for %s", symbol)
            return None

        pair_currency = self._combine_currency(
            [
                latest_ebit.currency,
                prior_ebit.currency,
                latest_ic.currency,
                prior_ic.currency,
            ]
        )
        if pair_currency is None and any(
            code is not None
            for code in (
                latest_ebit.currency,
                prior_ebit.currency,
                latest_ic.currency,
                prior_ic.currency,
            )
        ):
            LOGGER.warning("iroic_5y: currency mismatch across FY pair for %s", symbol)
            return None

        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT)
        latest_valid_tax_rate = self._latest_valid_fy_tax_rate(tax_map, pretax_map)
        latest_tax_rate = self._tax_rate_for_year(
            year=latest_year,
            tax_map=tax_map,
            pretax_map=pretax_map,
            latest_valid_tax_rate=latest_valid_tax_rate,
        )
        prior_tax_rate = self._tax_rate_for_year(
            year=prior_year,
            tax_map=tax_map,
            pretax_map=pretax_map,
            latest_valid_tax_rate=latest_valid_tax_rate,
        )

        latest_nopat = latest_ebit.total * (1.0 - latest_tax_rate.rate)
        prior_nopat = prior_ebit.total * (1.0 - prior_tax_rate.rate)
        delta_nopat = latest_nopat - prior_nopat

        delta_ic = latest_ic.total - prior_ic.total
        if delta_ic <= 0:
            LOGGER.warning(
                "iroic_5y: non-positive delta invested capital for %s", symbol
            )
            return None

        ic_scale = max(abs(latest_ic.total), abs(prior_ic.total), 1.0)
        relative_delta_ic = abs(delta_ic) / ic_scale
        if relative_delta_ic < IROIC_MIN_RELATIVE_DELTA_IC:
            LOGGER.warning("iroic_5y: tiny delta invested capital for %s", symbol)
            return None

        as_of_values = [latest_ebit.as_of, latest_ic.as_of]
        if latest_tax_rate.as_of is not None:
            as_of_values.append(latest_tax_rate.as_of)

        return IncrementalROICSnapshot(
            value=delta_nopat / delta_ic,
            as_of=max(as_of_values),
            currency=pair_currency,
        )

    def _fy_invested_capital_map(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> dict[int, _AmountResult]:
        snapshots = InvestedCapitalCalculator().compute_fy_series(symbol, repo)
        mapped: dict[int, _AmountResult] = {}
        for snapshot in snapshots:
            year = self._extract_year(snapshot.as_of)
            if year is None or year in mapped:
                continue
            mapped[year] = _AmountResult(
                total=snapshot.value,
                as_of=snapshot.as_of,
                currency=snapshot.currency,
            )
        return mapped

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
            value, currency = self._normalize_currency(record)
            mapped[year] = _AmountResult(
                total=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

    def _latest_valid_fy_tax_rate(
        self,
        tax_map: dict[int, _AmountResult],
        pretax_map: dict[int, _AmountResult],
    ) -> Optional[_TaxRateResult]:
        for year in sorted(set(tax_map).intersection(pretax_map), reverse=True):
            rate = self._rate_from_amounts(tax_map[year], pretax_map[year])
            if rate is not None:
                return rate
        return None

    def _tax_rate_for_year(
        self,
        *,
        year: int,
        tax_map: dict[int, _AmountResult],
        pretax_map: dict[int, _AmountResult],
        latest_valid_tax_rate: Optional[_TaxRateResult],
    ) -> _TaxRateResult:
        tax = tax_map.get(year)
        pretax = pretax_map.get(year)
        period_rate = self._rate_from_amounts(tax, pretax)
        if period_rate is not None:
            return period_rate
        if latest_valid_tax_rate is not None:
            return latest_valid_tax_rate
        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _latest_incremental_year(
        self,
        ebit_map: dict[int, _AmountResult],
        ic_map: dict[int, _AmountResult],
    ) -> Optional[int]:
        for year in sorted(set(ebit_map).intersection(ic_map), reverse=True):
            prior_year = year - IROIC_LOOKBACK_YEARS
            if prior_year in ebit_map and prior_year in ic_map:
                return year
        return None

    def _rate_from_amounts(
        self,
        tax: Optional[_AmountResult],
        pretax: Optional[_AmountResult],
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if not self._currencies_match(tax.currency, pretax.currency):
            return None
        if pretax.total <= PRETAX_MIN_ABS:
            return None
        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(rate=rate, as_of=max(tax.as_of, pretax.as_of))

    def _filter_periods(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        normalized_value, normalized_currency = normalize_money_value(
            record.value,
            record.currency,
        )
        return (
            record.value if normalized_value is None else normalized_value,
            normalized_currency,
        )

    def _combine_currency(self, values: Sequence[Optional[str]]) -> Optional[str]:
        merged = None
        for value in values:
            if not value:
                continue
            if merged is None:
                merged = value
            elif merged != value:
                return None
        return merged

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True

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
class ROIC10YMedianMetric:
    """Compute median FY ROIC over latest strict 10 consecutive years."""

    id: str = "roic_10y_median"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(symbol, repo)
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        median = (values[4] + values[5]) / 2.0
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
        )


@dataclass
class ROICYearsAbove12PctMetric:
    """Count FY ROIC years above 12% over latest strict 10 consecutive years."""

    id: str = "roic_years_above_12pct"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(symbol, repo)
        if snapshot is None:
            return None
        count = sum(1 for point in snapshot.points if point.value > ABOVE_THRESHOLD)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=float(count),
            as_of=snapshot.as_of,
        )


@dataclass
class ROIC10YMinMetric:
    """Compute minimum FY ROIC over latest strict 10 consecutive years."""

    id: str = "roic_10y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(symbol, repo)
        if snapshot is None:
            return None
        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
        )


@dataclass
class IncrementalROICFiveYearMetric:
    """Compute incremental ROIC using FY t versus strict FY t-5."""

    id: str = "iroic_5y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_incremental_5y(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "ROICFYSeriesSnapshot",
    "IncrementalROICSnapshot",
    "ROICFYSeriesCalculator",
    "ROIC10YMedianMetric",
    "ROICYearsAbove12PctMetric",
    "ROIC10YMinMetric",
    "IncrementalROICFiveYearMetric",
]
