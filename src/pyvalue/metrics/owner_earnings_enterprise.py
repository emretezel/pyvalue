"""Owner earnings enterprise (unlevered) metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.nwc import DeltaNWCMaintMetric
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_amount_money,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

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
AVG_WINDOW = 3
OE_CAGR_YEARS = 7


@dataclass(frozen=True)
class OwnerEarningsEnterpriseSnapshot:
    money: Money
    as_of: str


@dataclass(frozen=True)
class OwnerEarningsGrowthSnapshot:
    """A dimensionless owner-earnings growth rate (CAGR), not a money amount."""

    value: float
    as_of: str


@dataclass
class _AmountResult:
    money: Money
    as_of: str


@dataclass(frozen=True)
class _TaxRateResult:
    rate: float
    as_of: Optional[str]


@dataclass(frozen=True)
class _FYPoint:
    year: int
    money: Money
    as_of: str


@dataclass(frozen=True)
class OwnerEarningsEnterpriseFYSeriesSnapshot:
    points: tuple[_FYPoint, ...]
    as_of: str


class OwnerEarningsEnterpriseCalculator:
    """Shared calculator for enterprise owner-earnings numerators.

    NOPAT, D&A, maintenance capex and the maintenance NWC change are each aligned
    to the listing currency through the shared Money seam before any arithmetic,
    so owner earnings is single-currency by build.
    """

    def compute_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        context = "oe_ev_ttm"
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        delta_nwc_maint = self._delta_nwc_maint_money(
            listing_id, repo, target_currency=target_currency, context=context
        )
        if delta_nwc_maint is None:
            LOGGER.warning(
                "oe_ev_ttm: missing delta_nwc_maint for listing_id=%s", listing_id
            )
            return None

        ebit = self._compute_ttm_amount(
            listing_id,
            repo,
            EBIT_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )
        if ebit is None:
            LOGGER.warning("oe_ev_ttm: missing TTM EBIT for listing_id=%s", listing_id)
            return None

        tax_rate = self._effective_tax_rate_ttm(
            listing_id, repo, target_currency=target_currency, context=context
        )
        nopat = ebit.money * (1.0 - tax_rate.rate)

        da = self._compute_ttm_amount(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )
        if da is None:
            da = self._compute_ttm_amount(
                listing_id,
                repo,
                DA_FALLBACK_CONCEPTS,
                target_currency=target_currency,
                context=context,
            )

        mcapex = self._compute_mcapex_ttm(
            listing_id, repo, target_currency=target_currency, context=context
        )
        if mcapex is None:
            LOGGER.warning(
                "oe_ev_ttm: missing TTM mcapex inputs for listing_id=%s", listing_id
            )
            return None

        da_money = da.money if da is not None else Money.of(0.0, target_currency)
        as_of_dates = [ebit.as_of, mcapex.as_of, delta_nwc_maint.as_of]
        if da is not None:
            as_of_dates.append(da.as_of)
        if tax_rate.as_of is not None:
            as_of_dates.append(tax_rate.as_of)

        value = nopat + da_money - mcapex.money - delta_nwc_maint.money
        return OwnerEarningsEnterpriseSnapshot(money=value, as_of=max(as_of_dates))

    def compute_5y_average(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        latest_five = self._latest_available_five_points(
            listing_id,
            repo,
            context="oe_ev_5y_avg",
        )
        if latest_five is None:
            return None

        average = sum_money([point.money for point in latest_five]) / 5.0
        return OwnerEarningsEnterpriseSnapshot(
            money=average,
            as_of=latest_five[0].as_of,
        )

    def compute_5y_median(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseSnapshot]:
        latest_five = self._latest_available_five_points(
            listing_id,
            repo,
            context="oe_ev_fy_median_5y",
        )
        if latest_five is None:
            return None

        median = sorted((point.money for point in latest_five))[2]
        return OwnerEarningsEnterpriseSnapshot(
            money=median,
            as_of=latest_five[0].as_of,
        )

    def compute_10y_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEnterpriseFYSeriesSnapshot]:
        points = self._build_fy_points(
            listing_id,
            repo,
            context="worst_oe_ev_fy_10y",
        )
        if points is None:
            return None
        if not points:
            LOGGER.warning(
                "worst_oe_ev_fy_10y: no FY owner earnings points for listing_id=%s",
                listing_id,
            )
            return None

        selected = self._latest_consecutive_ten(
            points, context="worst_oe_ev_fy_10y", listing_id=listing_id
        )
        if selected is None:
            return None

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "worst_oe_ev_fy_10y: latest FY (%s) too old for listing_id=%s",
                selected[0].as_of,
                listing_id,
            )
            return None

        return OwnerEarningsEnterpriseFYSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
        )

    def compute_10y_cagr(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsGrowthSnapshot]:
        points = self._build_fy_points(
            listing_id,
            repo,
            context="owner_earnings_cagr_10y",
        )
        if points is None:
            return None
        if len(points) < TEN_YEAR_POINTS:
            LOGGER.warning(
                "owner_earnings_cagr_10y: need 10 FY owner earnings values "
                "for listing_id=%s, found %s",
                listing_id,
                len(points),
            )
            return None

        # The 1/OE_CAGR_YEARS exponent assumes the ten points span exactly ten
        # consecutive fiscal years; enforce the same strict chain as the
        # worst-of-10y metric instead of silently accepting gaps.
        latest_ten = self._latest_consecutive_ten(
            points, context="owner_earnings_cagr_10y", listing_id=listing_id
        )
        if latest_ten is None:
            return None
        if not self._is_recent_as_of(
            latest_ten[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "owner_earnings_cagr_10y: latest FY (%s) too old for listing_id=%s",
                latest_ten[0].as_of,
                listing_id,
            )
            return None

        ordered = list(reversed(latest_ten))
        start_monies = [point.money for point in ordered[:AVG_WINDOW]]
        end_monies = [point.money for point in ordered[-AVG_WINDOW:]]
        # A compound growth rate has no real solution from a non-positive base,
        # so both 3-year endpoint windows must be strictly positive; anything
        # else is reported as not-computable rather than a convention-based
        # number.
        if any(money.amount <= 0 for money in start_monies + end_monies):
            LOGGER.warning(
                "owner_earnings_cagr_10y: non-positive endpoint averages "
                "for listing_id=%s",
                listing_id,
            )
            return None

        start_avg = sum_money(start_monies) / AVG_WINDOW
        end_avg = sum_money(end_monies) / AVG_WINDOW
        return OwnerEarningsGrowthSnapshot(
            value=(end_avg / start_avg) ** (1.0 / OE_CAGR_YEARS) - 1.0,
            as_of=latest_ten[0].as_of,
        )

    def _latest_consecutive_ten(
        self,
        points: Sequence[_FYPoint],
        *,
        context: str,
        listing_id: int,
    ) -> Optional[list[_FYPoint]]:
        """Select the latest point per year across a strict 10-consecutive-year window.

        Both 10-year consumers (worst-of and CAGR) rely on the window spanning
        exactly ten fiscal years, so a gap or duplicate-year collapse must fail
        the selection rather than silently shift the window. Callers guarantee
        ``points`` is non-empty.
        """
        latest_by_year: dict[int, _FYPoint] = {}
        for point in points:
            latest_by_year.setdefault(point.year, point)

        latest_year = max(latest_by_year)
        selected: list[_FYPoint] = []
        for year in range(latest_year, latest_year - TEN_YEAR_POINTS, -1):
            selected_point = latest_by_year.get(year)
            if selected_point is None:
                LOGGER.warning(
                    "%s: missing strict consecutive FY chain for listing_id=%s",
                    context,
                    listing_id,
                )
                return None
            selected.append(selected_point)
        return selected

    def _latest_available_five_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        points = self._build_fy_points(listing_id, repo, context=context)
        if points is None:
            return None
        if len(points) < FIVE_YEAR_POINTS:
            LOGGER.warning(
                "%s: need 5 FY owner earnings values for listing_id=%s, found %s",
                context,
                listing_id,
                len(points),
            )
            return None

        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest FY (%s) too old for listing_id=%s",
                context,
                latest.as_of,
                listing_id,
            )
            return None

        return points[:FIVE_YEAR_POINTS]

    def _build_fy_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        # Each FY point subtracts *that year's own* maintenance NWC delta.
        # Reusing the current value for every year would scale today's
        # working-capital investment onto a decade-old (often far smaller)
        # business and can flip genuinely positive historical owner earnings
        # negative.
        maint_series = DeltaNWCMaintMetric().fy_series_by_year(listing_id, repo)
        if not maint_series:
            LOGGER.warning(
                "%s: missing delta_nwc_maint for listing_id=%s", context, listing_id
            )
            return None

        ebit_map = self._build_fy_amount_map(
            listing_id,
            repo,
            EBIT_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )
        da_map = self._build_fy_amount_map(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )
        mcapex_map = self._build_mcapex_fy_map(
            listing_id, repo, target_currency=target_currency, context=context
        )
        tax_map = self._fy_map(
            listing_id,
            repo,
            TAX_EXPENSE_CONCEPT,
            target_currency=target_currency,
            context=context,
        )
        pretax_map = self._fy_map(
            listing_id,
            repo,
            PRETAX_INCOME_CONCEPT,
            target_currency=target_currency,
            context=context,
        )

        latest_valid_fy_tax_rate = self._latest_valid_fy_tax_rate_from_maps(
            tax_map,
            pretax_map,
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
                    "%s: invalid FY end date %s for listing_id=%s",
                    context,
                    end_date,
                    listing_id,
                )
                continue

            maint = maint_series.get(year)
            if maint is None:
                # Expected at the series boundary: the oldest NWC years can
                # never carry a trailing 3-delta chain, so this fires for every
                # deep-history listing. Debug level keeps persisted failure
                # reasons pointing at the metric-level guards instead.
                LOGGER.debug(
                    "%s: no per-year delta_nwc_maint for FY %s (listing_id=%s)",
                    context,
                    end_date,
                    listing_id,
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
            )

            nopat = ebit.money * (1.0 - tax_rate.rate)
            da_money = da.money if da is not None else Money.of(0.0, target_currency)
            maint_money = require_metric_money(
                maint.money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name="delta_nwc_maint",
                as_of=maint.as_of,
            )
            points.append(
                _FYPoint(
                    year=year,
                    money=nopat + da_money - mcapex.money - maint_money,
                    as_of=end_date,
                )
            )
        return points

    def _effective_tax_rate_ttm(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
        context: str,
    ) -> _TaxRateResult:
        tax = self._compute_ttm_amount(
            listing_id,
            repo,
            TAX_EXPENSE_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )
        pretax = self._compute_ttm_amount(
            listing_id,
            repo,
            PRETAX_INCOME_CONCEPTS,
            target_currency=target_currency,
            context=context,
        )

        period_rate = self._compute_tax_rate_from_amounts(tax, pretax)
        if period_rate is not None:
            return period_rate

        fy_rate = self._latest_valid_fy_tax_rate(
            listing_id, repo, target_currency=target_currency, context=context
        )
        if fy_rate is not None:
            return fy_rate

        LOGGER.warning(
            "oe_ev_ttm: using default tax rate for listing_id=%s", listing_id
        )
        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _effective_tax_rate_for_fy_date(
        self,
        *,
        end_date: str,
        tax_map: dict[str, _AmountResult],
        pretax_map: dict[str, _AmountResult],
        latest_valid_fy_tax_rate: Optional[_TaxRateResult],
    ) -> _TaxRateResult:
        period_rate = self._compute_tax_rate_from_amounts(
            tax_map.get(end_date),
            pretax_map.get(end_date),
        )
        if period_rate is not None:
            return period_rate

        if latest_valid_fy_tax_rate is not None:
            return latest_valid_fy_tax_rate

        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _latest_valid_fy_tax_rate(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
        context: str,
    ) -> Optional[_TaxRateResult]:
        tax_map = self._fy_map(
            listing_id,
            repo,
            TAX_EXPENSE_CONCEPT,
            target_currency=target_currency,
            context=context,
        )
        pretax_map = self._fy_map(
            listing_id,
            repo,
            PRETAX_INCOME_CONCEPT,
            target_currency=target_currency,
            context=context,
        )
        return self._latest_valid_fy_tax_rate_from_maps(tax_map, pretax_map)

    def _latest_valid_fy_tax_rate_from_maps(
        self,
        tax_map: dict[str, _AmountResult],
        pretax_map: dict[str, _AmountResult],
    ) -> Optional[_TaxRateResult]:
        candidate_dates = sorted(
            set(tax_map.keys()).intersection(pretax_map.keys()), reverse=True
        )
        for end_date in candidate_dates:
            rate = self._compute_tax_rate_from_amounts(
                tax_map.get(end_date),
                pretax_map.get(end_date),
            )
            if rate is not None:
                return rate
        return None

    def _compute_tax_rate_from_amounts(
        self,
        tax: Optional[_AmountResult],
        pretax: Optional[_AmountResult],
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if pretax.money.amount <= PRETAX_MIN_ABS:
            return None

        rate = tax.money / pretax.money
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(rate=rate, as_of=max(tax.as_of, pretax.as_of))

    def _delta_nwc_maint_money(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
        context: str,
    ) -> Optional[_AmountResult]:
        result = DeltaNWCMaintMetric().compute(listing_id, repo)
        if result is None:
            return None
        money = require_metric_amount_money(
            result.value,
            result.currency,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name="delta_nwc_maint",
            as_of=result.as_of,
        )
        return _AmountResult(money=money, as_of=result.as_of)

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        target_currency: str,
        context: str,
        absolute: bool = False,
    ) -> Optional[_AmountResult]:
        for concept in concepts:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                continue
            monies = [
                self._money(
                    record,
                    target_currency=target_currency,
                    listing_id=listing_id,
                    context=context,
                    absolute=absolute,
                )
                for record in quarterly[:4]
            ]
            return _AmountResult(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _build_fy_amount_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        target_currency: str,
        context: str,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        maps = [
            self._fy_map(
                listing_id,
                repo,
                concept,
                target_currency=target_currency,
                context=context,
                absolute=absolute,
            )
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
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
        context: str,
    ) -> dict[str, _AmountResult]:
        capex_map = self._fy_map(
            listing_id,
            repo,
            CAPEX_CONCEPT,
            target_currency=target_currency,
            context=context,
            absolute=True,
        )
        da_primary_map = self._fy_map(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPT,
            target_currency=target_currency,
            context=context,
            absolute=True,
        )
        da_fallback_map = self._fy_map(
            listing_id,
            repo,
            DA_FALLBACK_CONCEPT,
            target_currency=target_currency,
            context=context,
            absolute=True,
        )

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
            value = self._compute_mcapex_value(capex, da)
            if value is None:
                continue
            mcapex_map[end_date] = value
        return mcapex_map

    def _compute_mcapex_ttm(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
        context: str,
    ) -> Optional[_AmountResult]:
        capex = self._compute_ttm_amount(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            target_currency=target_currency,
            context=context,
            absolute=True,
        )
        da = self._compute_ttm_amount(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS,
            target_currency=target_currency,
            context=context,
            absolute=True,
        )
        if da is None:
            da = self._compute_ttm_amount(
                listing_id,
                repo,
                DA_FALLBACK_CONCEPTS,
                target_currency=target_currency,
                context=context,
                absolute=True,
            )
        return self._compute_mcapex_value(capex, da)

    def _compute_mcapex_value(
        self,
        capex: Optional[_AmountResult],
        da: Optional[_AmountResult],
    ) -> Optional[_AmountResult]:
        if capex is None and da is None:
            return None
        if capex is not None and da is not None:
            # Maintenance capex is bounded by 1.1x D&A so a one-off growth-capex
            # spike does not understate owner earnings.
            return _AmountResult(
                money=min(capex.money, da.money * DA_MULTIPLIER),
                as_of=max(capex.as_of, da.as_of),
            )
        if capex is not None:
            return _AmountResult(money=capex.money, as_of=capex.as_of)
        assert da is not None
        return _AmountResult(money=da.money * DA_MULTIPLIER, as_of=da.as_of)

    def _fy_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        *,
        target_currency: str,
        context: str,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[str, _AmountResult] = {}
        for record in ordered:
            mapped[record.end_date] = _AmountResult(
                money=self._money(
                    record,
                    target_currency=target_currency,
                    listing_id=listing_id,
                    context=context,
                    absolute=absolute,
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
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _money(
        self,
        fact: MonetaryFact,
        *,
        target_currency: str,
        listing_id: int,
        context: str,
        absolute: bool = False,
    ) -> Money:
        money = require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )
        return abs(money) if absolute else money

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


@dataclass
class OwnerEarningsEnterpriseTTMMetric:
    """Compute TTM owner earnings enterprise (unlevered) for EODHD-oriented data."""

    id: str = "oe_ev_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_ttm(listing_id, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.money.amount,
            as_of=snapshot.as_of,
            currency=snapshot.money.currency,
        )


@dataclass
class OwnerEarningsEnterpriseFiveYearAverageMetric:
    """Compute 5-year average FY owner earnings enterprise (unlevered)."""

    id: str = "oe_ev_5y_avg"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_5y_average(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.money.amount,
            as_of=snapshot.as_of,
            currency=snapshot.money.currency,
        )


@dataclass
class OwnerEarningsEnterpriseFiveYearMedianMetric:
    """Compute FY median owner earnings enterprise over the latest 5 available years."""

    id: str = "oe_ev_fy_median_5y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_5y_median(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.money.amount,
            as_of=snapshot.as_of,
            currency=snapshot.money.currency,
        )


@dataclass
class WorstOwnerEarningsEnterpriseTenYearMetric:
    """Compute the worst FY owner earnings enterprise value over a strict 10-year window."""

    id: str = "worst_oe_ev_fy_10y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_10y_series(
            listing_id, repo
        )
        if snapshot is None:
            return None
        worst = min(point.money for point in snapshot.points)
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=worst.amount,
            as_of=snapshot.as_of,
            currency=worst.currency,
        )


__all__ = [
    "OwnerEarningsEnterpriseFYSeriesSnapshot",
    "OwnerEarningsEnterpriseSnapshot",
    "OwnerEarningsGrowthSnapshot",
    "OwnerEarningsEnterpriseCalculator",
    "OwnerEarningsEnterpriseTTMMetric",
    "OwnerEarningsEnterpriseFiveYearAverageMetric",
    "OwnerEarningsEnterpriseFiveYearMedianMetric",
    "WorstOwnerEarningsEnterpriseTenYearMetric",
]
