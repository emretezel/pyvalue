"""Owner earnings equity metrics.

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

TTM_CONTEXT = "oe_equity_ttm"
FIVE_YEAR_CONTEXT = "oe_equity_5y_avg"

NI_PRIMARY_CONCEPT = "NetIncomeLoss"
NI_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
CAPEX_CONCEPT = "CapitalExpenditures"

NI_CONCEPTS = (NI_PRIMARY_CONCEPT, NI_FALLBACK_CONCEPT)
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DA_MULTIPLIER = 1.1

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
        NI_CONCEPTS
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + CAPEX_CONCEPTS
        + NWC_MAINT_REQUIRED_CONCEPTS
    )
)


@dataclass(frozen=True)
class OwnerEarningsEquitySnapshot:
    money: Money
    as_of: str


@dataclass
class _AmountResult:
    money: Money
    as_of: str


@dataclass
class _FYPoint:
    money: Money
    as_of: str


class OwnerEarningsEquityCalculator:
    """Shared calculator for owner earnings equity numerators.

    Every monetary input -- net income, D&A, maintenance capex and the
    maintenance NWC change -- is aligned to the listing currency through the
    shared Money seam before any arithmetic, so owner earnings is single-currency
    by build and there is no per-input currency reconciliation to do.
    """

    def compute_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEquitySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=TTM_CONTEXT
        )
        delta_nwc_maint = self._delta_nwc_maint_money(
            listing_id, repo, target_currency=target_currency, context=TTM_CONTEXT
        )
        if delta_nwc_maint is None:
            LOGGER.warning(
                "oe_equity_ttm: missing delta_nwc_maint for listing_id=%s", listing_id
            )
            return None

        ni = self._compute_ttm_amount(
            listing_id,
            repo,
            NI_CONCEPTS,
            target_currency=target_currency,
            context=TTM_CONTEXT,
        )
        if ni is None:
            LOGGER.warning(
                "oe_equity_ttm: missing TTM net income for listing_id=%s", listing_id
            )
            return None

        da = self._compute_ttm_amount(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS,
            target_currency=target_currency,
            context=TTM_CONTEXT,
        )
        if da is None:
            da = self._compute_ttm_amount(
                listing_id,
                repo,
                DA_FALLBACK_CONCEPTS,
                target_currency=target_currency,
                context=TTM_CONTEXT,
            )

        mcapex = self._compute_mcapex_ttm(
            listing_id, repo, target_currency=target_currency
        )
        if mcapex is None:
            LOGGER.warning(
                "oe_equity_ttm: missing TTM mcapex inputs for listing_id=%s", listing_id
            )
            return None

        da_money = da.money if da is not None else Money.of(0.0, target_currency)
        as_of_dates = [ni.as_of, mcapex.as_of, delta_nwc_maint.as_of]
        if da is not None:
            as_of_dates.append(da.as_of)
        value = ni.money + da_money - mcapex.money - delta_nwc_maint.money
        return OwnerEarningsEquitySnapshot(money=value, as_of=max(as_of_dates))

    def compute_5y_average(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[OwnerEarningsEquitySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=FIVE_YEAR_CONTEXT
        )
        delta_nwc_maint = self._delta_nwc_maint_money(
            listing_id, repo, target_currency=target_currency, context=FIVE_YEAR_CONTEXT
        )
        if delta_nwc_maint is None:
            LOGGER.warning(
                "oe_equity_5y_avg: missing delta_nwc_maint for listing_id=%s",
                listing_id,
            )
            return None

        ni_map = self._build_fy_amount_map(
            listing_id,
            repo,
            NI_CONCEPTS,
            target_currency=target_currency,
            context=FIVE_YEAR_CONTEXT,
        )
        da_map = self._build_fy_amount_map(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS,
            target_currency=target_currency,
            context=FIVE_YEAR_CONTEXT,
        )
        mcapex_map = self._build_mcapex_fy_map(
            listing_id, repo, target_currency=target_currency
        )

        candidate_dates = sorted(
            set(ni_map.keys()).intersection(mcapex_map.keys()),
            reverse=True,
        )
        points: list[_FYPoint] = []
        for end_date in candidate_dates:
            ni = ni_map[end_date]
            mcapex = mcapex_map[end_date]
            da = da_map.get(end_date)
            da_money = da.money if da is not None else Money.of(0.0, target_currency)
            point_value = ni.money + da_money - mcapex.money - delta_nwc_maint.money
            points.append(_FYPoint(money=point_value, as_of=end_date))

        if len(points) < 5:
            LOGGER.warning(
                "oe_equity_5y_avg: need 5 FY owner earnings values "
                "for listing_id=%s, found %s",
                listing_id,
                len(points),
            )
            return None

        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "oe_equity_5y_avg: latest FY (%s) too old for listing_id=%s",
                latest.as_of,
                listing_id,
            )
            return None

        latest_five = points[:5]
        average = sum_money([point.money for point in latest_five]) / 5.0
        return OwnerEarningsEquitySnapshot(
            money=average,
            as_of=latest_five[0].as_of,
        )

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
                LOGGER.warning(
                    "%s: need 4 quarterly %s records for listing_id=%s, found %s",
                    context,
                    concept,
                    listing_id,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                LOGGER.warning(
                    "%s: latest %s (%s) too old for listing_id=%s",
                    context,
                    concept,
                    quarterly[0].end_date,
                    listing_id,
                )
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

    def _compute_mcapex_ttm(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        target_currency: str,
    ) -> Optional[_AmountResult]:
        capex = self._compute_ttm_amount(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            target_currency=target_currency,
            context=TTM_CONTEXT,
            absolute=True,
        )
        da = self._compute_ttm_amount(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS,
            target_currency=target_currency,
            context=TTM_CONTEXT,
            absolute=True,
        )
        if da is None:
            da = self._compute_ttm_amount(
                listing_id,
                repo,
                DA_FALLBACK_CONCEPTS,
                target_currency=target_currency,
                context=TTM_CONTEXT,
                absolute=True,
            )
        return self._compute_mcapex_value(capex, da, target_currency=target_currency)

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
    ) -> dict[str, _AmountResult]:
        capex_map = self._fy_map(
            listing_id,
            repo,
            CAPEX_CONCEPT,
            target_currency=target_currency,
            context=FIVE_YEAR_CONTEXT,
            absolute=True,
        )
        da_primary_map = self._fy_map(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPT,
            target_currency=target_currency,
            context=FIVE_YEAR_CONTEXT,
            absolute=True,
        )
        da_fallback_map = self._fy_map(
            listing_id,
            repo,
            DA_FALLBACK_CONCEPT,
            target_currency=target_currency,
            context=FIVE_YEAR_CONTEXT,
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
            value = self._compute_mcapex_value(
                capex, da, target_currency=target_currency
            )
            if value is None:
                continue
            mcapex_map[end_date] = value
        return mcapex_map

    def _compute_mcapex_value(
        self,
        capex: Optional[_AmountResult],
        da: Optional[_AmountResult],
        *,
        target_currency: str,
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


@dataclass
class OwnerEarningsEquityTTMMetric:
    """Compute TTM owner earnings equity for EODHD-oriented data."""

    id: str = "oe_equity_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEquityCalculator().compute_ttm(listing_id, repo)
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
class OwnerEarningsEquityFiveYearAverageMetric:
    """Compute 5-year average FY owner earnings equity for EODHD-oriented data."""

    id: str = "oe_equity_5y_avg"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEquityCalculator().compute_5y_average(listing_id, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.money.amount,
            as_of=snapshot.as_of,
            currency=snapshot.money.currency,
        )


__all__ = [
    "OwnerEarningsEquitySnapshot",
    "OwnerEarningsEquityCalculator",
    "OwnerEarningsEquityTTMMetric",
    "OwnerEarningsEquityFiveYearAverageMetric",
]
