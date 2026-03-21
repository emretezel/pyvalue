"""Owner earnings enterprise (unlevered) metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.nwc import DeltaNWCMaintMetric
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
TAX_EXPENSE_CONCEPT = "IncomeTaxExpense"
PRETAX_INCOME_CONCEPT = "IncomeBeforeIncomeTaxes"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
CAPEX_CONCEPT = "CapitalExpenditures"

EBIT_CONCEPTS = (EBIT_CONCEPT,)
TAX_EXPENSE_CONCEPTS = (TAX_EXPENSE_CONCEPT,)
PRETAX_INCOME_CONCEPTS = (PRETAX_INCOME_CONCEPT,)
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)

NWC_MAINT_REQUIRED_CONCEPTS = (
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "CashAndShortTermInvestments",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
    "ShortTermDebt",
)

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        EBIT_CONCEPTS
        + TAX_EXPENSE_CONCEPTS
        + PRETAX_INCOME_CONCEPTS
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + CAPEX_CONCEPTS
        + NWC_MAINT_REQUIRED_CONCEPTS
    )
)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DEFAULT_TAX_RATE = 0.21
PRETAX_MIN_ABS = 1.0
DA_MULTIPLIER = 1.1
FIVE_YEAR_POINTS = 5
TEN_YEAR_POINTS = 10


@dataclass(frozen=True)
class OwnerEarningsEnterpriseSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _TaxRateResult:
    rate: float
    as_of: Optional[str]


@dataclass
class _FYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class OwnerEarningsEnterpriseFYSeriesSnapshot:
    points: tuple[_FYPoint, ...]
    as_of: str
    currency: Optional[str]


class OwnerEarningsEnterpriseCalculator:
    """Shared calculator for enterprise owner-earnings numerators."""

    def compute_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        delta_nwc_maint = self._compute_delta_nwc_maint(symbol, repo)
        if delta_nwc_maint is None:
            LOGGER.warning("oe_ev_ttm: missing delta_nwc_maint for %s", symbol)
            return None

        ebit = self._compute_ttm_amount(
            symbol,
            repo,
            EBIT_CONCEPTS,
            context="oe_ev_ttm",
            absolute=False,
        )
        if ebit is None:
            LOGGER.warning("oe_ev_ttm: missing TTM EBIT for %s", symbol)
            return None

        tax_rate = self._effective_tax_rate_ttm(symbol, repo)
        nopat = ebit.total * (1.0 - tax_rate.rate)

        da = self._compute_ttm_amount(
            symbol,
            repo,
            DA_PRIMARY_CONCEPTS,
            context="oe_ev_ttm",
            absolute=False,
        )
        if da is None:
            da = self._compute_ttm_amount(
                symbol,
                repo,
                DA_FALLBACK_CONCEPTS,
                context="oe_ev_ttm",
                absolute=False,
            )

        mcapex = self._compute_mcapex_ttm(symbol, repo)
        if mcapex is None:
            LOGGER.warning("oe_ev_ttm: missing TTM mcapex inputs for %s", symbol)
            return None

        currency = self._combine_currency(
            [ebit.currency, da.currency if da else None, mcapex.currency]
        )
        if currency is None and any(
            code is not None
            for code in (ebit.currency, da.currency if da else None, mcapex.currency)
        ):
            LOGGER.warning("oe_ev_ttm: currency mismatch for %s", symbol)
            return None

        da_total = da.total if da is not None else 0.0
        as_of_dates = [ebit.as_of, mcapex.as_of, delta_nwc_maint.as_of]
        if da is not None:
            as_of_dates.append(da.as_of)
        if tax_rate.as_of is not None:
            as_of_dates.append(tax_rate.as_of)
        as_of = max(as_of_dates)

        value = nopat + da_total - mcapex.total - delta_nwc_maint.value
        return OwnerEarningsEnterpriseSnapshot(
            value=value, as_of=as_of, currency=currency
        )

    def compute_5y_average(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        latest_five = self._latest_available_five_points(
            symbol,
            repo,
            context="oe_ev_5y_avg",
        )
        if latest_five is None:
            return None

        average = sum(point.value for point in latest_five) / 5.0
        return OwnerEarningsEnterpriseSnapshot(
            value=average,
            as_of=latest_five[0].as_of,
            currency=self._combine_currency([point.currency for point in latest_five]),
        )

    def compute_5y_median(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        latest_five = self._latest_available_five_points(
            symbol,
            repo,
            context="oe_ev_fy_median_5y",
        )
        if latest_five is None:
            return None

        median = sorted(point.value for point in latest_five)[2]
        return OwnerEarningsEnterpriseSnapshot(
            value=median,
            as_of=latest_five[0].as_of,
            currency=self._combine_currency([point.currency for point in latest_five]),
        )

    def compute_10y_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseFYSeriesSnapshot]:
        points = self._build_fy_points(
            symbol,
            repo,
            context="worst_oe_ev_fy_10y",
        )
        if points is None:
            return None
        if not points:
            LOGGER.warning(
                "worst_oe_ev_fy_10y: no FY owner earnings points for %s", symbol
            )
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
                    "worst_oe_ev_fy_10y: missing strict consecutive FY chain for %s",
                    symbol,
                )
                return None
            selected.append(selected_point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "worst_oe_ev_fy_10y: latest FY (%s) too old for %s",
                selected[0].as_of,
                symbol,
            )
            return None

        series_currency = self._combine_currency([point.currency for point in selected])
        if series_currency is None and any(
            point.currency is not None for point in selected
        ):
            LOGGER.warning(
                "worst_oe_ev_fy_10y: currency mismatch across selected FY series for %s",
                symbol,
            )
            return None

        return OwnerEarningsEnterpriseFYSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=series_currency,
        )

    def _latest_available_five_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        points = self._build_fy_points(symbol, repo, context=context)
        if points is None:
            return None
        if len(points) < FIVE_YEAR_POINTS:
            LOGGER.warning(
                "%s: need 5 FY owner earnings values for %s, found %s",
                context,
                symbol,
                len(points),
            )
            return None

        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest FY (%s) too old for %s",
                context,
                latest.as_of,
                symbol,
            )
            return None

        latest_five = points[:FIVE_YEAR_POINTS]
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

    def _build_fy_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        delta_nwc_maint = self._compute_delta_nwc_maint(symbol, repo)
        if delta_nwc_maint is None:
            LOGGER.warning("%s: missing delta_nwc_maint for %s", context, symbol)
            return None

        ebit_map = self._build_fy_amount_map(
            symbol, repo, EBIT_CONCEPTS, absolute=False
        )
        da_map = self._build_fy_amount_map(
            symbol,
            repo,
            DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS,
            absolute=False,
        )
        mcapex_map = self._build_mcapex_fy_map(symbol, repo, context=context)
        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT, absolute=False)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT, absolute=False)

        latest_valid_fy_tax_rate = self._latest_valid_fy_tax_rate_from_maps(
            tax_map,
            pretax_map,
            symbol=symbol,
            context=context,
        )

        candidate_dates = sorted(
            set(ebit_map.keys()).intersection(mcapex_map.keys()),
            reverse=True,
        )
        points: list[_FYPoint] = []
        for end_date in candidate_dates:
            year = self._parse_year(end_date)
            if year is None:
                LOGGER.warning(
                    "%s: invalid FY end date %s for %s", context, end_date, symbol
                )
                continue

            ebit = ebit_map[end_date]
            da = da_map.get(end_date)
            mcapex = mcapex_map[end_date]
            tax_rate = self._effective_tax_rate_for_fy_date(
                end_date=end_date,
                tax_map=tax_map,
                pretax_map=pretax_map,
                latest_valid_fy_tax_rate=latest_valid_fy_tax_rate,
                symbol=symbol,
                context=context,
            )

            point_currency = self._combine_currency(
                [ebit.currency, da.currency if da else None, mcapex.currency]
            )
            if point_currency is None and any(
                code is not None
                for code in (
                    ebit.currency,
                    da.currency if da else None,
                    mcapex.currency,
                )
            ):
                LOGGER.warning(
                    "%s: currency mismatch on %s for %s",
                    context,
                    end_date,
                    symbol,
                )
                continue

            nopat = ebit.total * (1.0 - tax_rate.rate)
            da_total = da.total if da is not None else 0.0
            points.append(
                _FYPoint(
                    year=year,
                    value=nopat + da_total - mcapex.total - delta_nwc_maint.value,
                    as_of=end_date,
                    currency=point_currency,
                )
            )
        return points

    def _effective_tax_rate_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> _TaxRateResult:
        tax = self._compute_ttm_amount(
            symbol,
            repo,
            TAX_EXPENSE_CONCEPTS,
            context="oe_ev_ttm",
            absolute=False,
        )
        pretax = self._compute_ttm_amount(
            symbol,
            repo,
            PRETAX_INCOME_CONCEPTS,
            context="oe_ev_ttm",
            absolute=False,
        )

        period_rate = self._compute_tax_rate_from_amounts(
            tax,
            pretax,
            symbol=symbol,
            context="oe_ev_ttm",
            period_label="TTM",
        )
        if period_rate is not None:
            return period_rate

        fy_rate = self._latest_valid_fy_tax_rate(symbol, repo, context="oe_ev_ttm")
        if fy_rate is not None:
            return fy_rate

        LOGGER.warning("oe_ev_ttm: using default tax rate for %s", symbol)
        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _effective_tax_rate_for_fy_date(
        self,
        *,
        end_date: str,
        tax_map: dict[str, _AmountResult],
        pretax_map: dict[str, _AmountResult],
        latest_valid_fy_tax_rate: Optional[_TaxRateResult],
        symbol: str,
        context: str,
    ) -> _TaxRateResult:
        period_rate = self._compute_tax_rate_from_amounts(
            tax_map.get(end_date),
            pretax_map.get(end_date),
            symbol=symbol,
            context=context,
            period_label=end_date,
        )
        if period_rate is not None:
            return period_rate

        if latest_valid_fy_tax_rate is not None:
            return latest_valid_fy_tax_rate

        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _latest_valid_fy_tax_rate(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_TaxRateResult]:
        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT, absolute=False)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT, absolute=False)
        return self._latest_valid_fy_tax_rate_from_maps(
            tax_map,
            pretax_map,
            symbol=symbol,
            context=context,
        )

    def _latest_valid_fy_tax_rate_from_maps(
        self,
        tax_map: dict[str, _AmountResult],
        pretax_map: dict[str, _AmountResult],
        *,
        symbol: str,
        context: str,
    ) -> Optional[_TaxRateResult]:
        candidate_dates = sorted(
            set(tax_map.keys()).intersection(pretax_map.keys()), reverse=True
        )
        for end_date in candidate_dates:
            rate = self._compute_tax_rate_from_amounts(
                tax_map.get(end_date),
                pretax_map.get(end_date),
                symbol=symbol,
                context=context,
                period_label=end_date,
            )
            if rate is not None:
                return rate
        return None

    def _compute_tax_rate_from_amounts(
        self,
        tax: Optional[_AmountResult],
        pretax: Optional[_AmountResult],
        *,
        symbol: str,
        context: str,
        period_label: str,
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if not self._currencies_match(tax.currency, pretax.currency):
            LOGGER.warning(
                "%s: tax currency mismatch for %s on %s",
                context,
                symbol,
                period_label,
            )
            return None
        if pretax.total <= PRETAX_MIN_ABS:
            return None

        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(rate=rate, as_of=max(tax.as_of, pretax.as_of))

    def _compute_delta_nwc_maint(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        return DeltaNWCMaintMetric().compute(symbol, repo)

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> Optional[_AmountResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                continue
            normalized, currency = self._normalize_records(
                quarterly[:4], absolute=absolute
            )
            if normalized is None:
                LOGGER.warning(
                    "%s: currency conflict in %s quarterly values for %s",
                    context,
                    concept,
                    symbol,
                )
                continue
            return _AmountResult(
                total=sum(normalized),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _build_fy_amount_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        maps = [
            self._fy_map(symbol, repo, concept, absolute=absolute)
            for concept in concepts
        ]
        candidate_dates: set[str] = set()
        for mapped in maps:
            candidate_dates.update(mapped.keys())

        merged: dict[str, _AmountResult] = {}
        for end_date in sorted(candidate_dates, reverse=True):
            for mapped in maps:
                if end_date in mapped:
                    merged[end_date] = mapped[end_date]
                    break
        return merged

    def _build_mcapex_fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> dict[str, _AmountResult]:
        capex_map = self._fy_map(symbol, repo, CAPEX_CONCEPT, absolute=True)
        da_primary_map = self._fy_map(symbol, repo, DA_PRIMARY_CONCEPT, absolute=True)
        da_fallback_map = self._fy_map(symbol, repo, DA_FALLBACK_CONCEPT, absolute=True)

        candidate_dates = sorted(
            set(capex_map.keys())
            .union(da_primary_map.keys())
            .union(da_fallback_map.keys()),
            reverse=True,
        )
        mcapex_map: dict[str, _AmountResult] = {}
        for end_date in candidate_dates:
            capex = capex_map.get(end_date)
            da = da_primary_map.get(end_date) or da_fallback_map.get(end_date)
            value = self._compute_mcapex_value(
                capex,
                da,
                symbol=symbol,
                context=context,
            )
            if value is None:
                continue
            mcapex_map[end_date] = value
        return mcapex_map

    def _compute_mcapex_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountResult]:
        capex = self._compute_ttm_amount(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            context="oe_ev_ttm",
            absolute=True,
        )
        da = self._compute_ttm_amount(
            symbol,
            repo,
            DA_PRIMARY_CONCEPTS,
            context="oe_ev_ttm",
            absolute=True,
        )
        if da is None:
            da = self._compute_ttm_amount(
                symbol,
                repo,
                DA_FALLBACK_CONCEPTS,
                context="oe_ev_ttm",
                absolute=True,
            )
        return self._compute_mcapex_value(capex, da, symbol=symbol, context="oe_ev_ttm")

    def _compute_mcapex_value(
        self,
        capex: Optional[_AmountResult],
        da: Optional[_AmountResult],
        *,
        symbol: str,
        context: str,
    ) -> Optional[_AmountResult]:
        if capex is None and da is None:
            return None
        if capex is not None and da is not None:
            if not self._currencies_match(capex.currency, da.currency):
                LOGGER.warning("%s: mcapex currency mismatch for %s", context, symbol)
                return None
            return _AmountResult(
                total=min(capex.total, DA_MULTIPLIER * da.total),
                as_of=max(capex.as_of, da.as_of),
                currency=capex.currency or da.currency,
            )
        if capex is not None:
            return _AmountResult(
                total=capex.total,
                as_of=capex.as_of,
                currency=capex.currency,
            )
        assert da is not None
        return _AmountResult(
            total=DA_MULTIPLIER * da.total,
            as_of=da.as_of,
            currency=da.currency,
        )

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[str, _AmountResult] = {}
        for record in ordered:
            value, currency = self._normalize_currency(record, absolute=absolute)
            mapped[record.end_date] = _AmountResult(
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
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_records(
        self, records: Sequence[FactRecord], *, absolute: bool = False
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency = None
        normalized: list[float] = []
        for record in records:
            value, code = self._normalize_currency(record, absolute=absolute)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency

    def _normalize_currency(
        self, record: FactRecord, *, absolute: bool = False
    ) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            value = value / 100.0
            code = "GBP"
        if absolute:
            value = abs(value)
        return value, code

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

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True

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


@dataclass
class OwnerEarningsEnterpriseTTMMetric:
    """Compute TTM owner earnings enterprise (unlevered) for EODHD-oriented data."""

    id: str = "oe_ev_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_ttm(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class OwnerEarningsEnterpriseFiveYearAverageMetric:
    """Compute 5-year average FY owner earnings enterprise (unlevered)."""

    id: str = "oe_ev_5y_avg"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_5y_average(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class OwnerEarningsEnterpriseFiveYearMedianMetric:
    """Compute FY median owner earnings enterprise over the latest 5 available years."""

    id: str = "oe_ev_fy_median_5y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_5y_median(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class WorstOwnerEarningsEnterpriseTenYearMetric:
    """Compute the worst FY owner earnings enterprise value over a strict 10-year window."""

    id: str = "worst_oe_ev_fy_10y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_10y_series(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=min(point.value for point in snapshot.points),
            as_of=snapshot.as_of,
        )


__all__ = [
    "OwnerEarningsEnterpriseFYSeriesSnapshot",
    "OwnerEarningsEnterpriseSnapshot",
    "OwnerEarningsEnterpriseCalculator",
    "OwnerEarningsEnterpriseTTMMetric",
    "OwnerEarningsEnterpriseFiveYearAverageMetric",
    "OwnerEarningsEnterpriseFiveYearMedianMetric",
    "WorstOwnerEarningsEnterpriseTenYearMetric",
]
