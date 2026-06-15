"""Maintenance capex proxy metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

CAPEX_CONCEPT = "CapitalExpenditures"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)
ALL_CONCEPTS = CAPEX_CONCEPTS + DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DA_MULTIPLIER = 1.1


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


class _MCapexBase:
    def _compute_mcapex_value(
        self,
        capex: Optional[_MoneyResult],
        da: Optional[_MoneyResult],
        listing_id: int,
        *,
        context: str,
    ) -> Optional[_MoneyResult]:
        # All inputs are already aligned to the listing currency upstream, so the
        # min/scale below operate within one currency.
        if capex is None and da is None:
            return None
        if capex is not None and da is not None:
            return _MoneyResult(
                money=min(capex.money, da.money * DA_MULTIPLIER),
                as_of=max(capex.as_of, da.as_of),
            )
        if capex is not None:
            return capex
        assert da is not None
        return _MoneyResult(money=da.money * DA_MULTIPLIER, as_of=da.as_of)

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
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
                self._money(record, target_currency, listing_id, context)
                for record in quarterly[:4]
            ]
            return _MoneyResult(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _build_fy_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
        target_currency: str,
    ) -> list[_MoneyResult]:
        capex_map = self._fy_map(listing_id, repo, CAPEX_CONCEPT)
        da_primary_map = self._fy_map(listing_id, repo, DA_PRIMARY_CONCEPT)
        da_fallback_map = self._fy_map(listing_id, repo, DA_FALLBACK_CONCEPT)

        candidate_dates = sorted(
            set(capex_map.keys())
            .union(da_primary_map.keys())
            .union(da_fallback_map.keys()),
            reverse=True,
        )
        points: list[_MoneyResult] = []
        for end_date in candidate_dates:
            capex = self._amount_from_record(
                capex_map.get(end_date),
                listing_id=listing_id,
                context=context,
                target_currency=target_currency,
            )
            da_record = da_primary_map.get(end_date) or da_fallback_map.get(end_date)
            da = self._amount_from_record(
                da_record,
                listing_id=listing_id,
                context=context,
                target_currency=target_currency,
            )
            value = self._compute_mcapex_value(capex, da, listing_id, context=context)
            if value is None:
                continue
            points.append(value)
        return points

    def _amount_from_record(
        self,
        record: Optional[MonetaryFact],
        *,
        listing_id: int,
        context: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        if record is None:
            return None
        return _MoneyResult(
            money=self._money(record, target_currency, listing_id, context),
            as_of=record.end_date,
        )

    def _fy_map(
        self, listing_id: int, repo: RegionFactsRepository, concept: str
    ) -> dict[str, MonetaryFact]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records, FY_PERIODS)
        return {record.end_date: record for record in ordered}

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
        target_currency: str,
        listing_id: int,
        context: str,
    ) -> Money:
        # Capex is a negative cash outflow; the maintenance-capex proxy works in
        # absolute magnitudes, so align to the listing currency then take abs.
        return abs(
            require_metric_money(
                fact.money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name=fact.concept,
                as_of=fact.end_date,
            )
        )

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))


@dataclass
class MCapexFYMetric(_MCapexBase):
    """Compute latest fiscal-year maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_fy"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name="MaintenanceCapex"
        )
        points = self._build_fy_points(
            listing_id, repo, context=self.id, target_currency=target_currency
        )
        if not points:
            LOGGER.warning(
                "mcapex_fy: missing FY capex and D&A inputs for listing_id=%s",
                listing_id,
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "mcapex_fy: latest FY (%s) too old for listing_id=%s",
                latest.as_of,
                listing_id,
            )
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=latest.money.amount,
            as_of=latest.as_of,
            currency=latest.money.currency,
        )


@dataclass
class MCapexFiveYearMetric(_MCapexBase):
    """Compute 5-year average of FY maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_5y"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name="MaintenanceCapex"
        )
        points = self._build_fy_points(
            listing_id, repo, context=self.id, target_currency=target_currency
        )
        if len(points) < 5:
            LOGGER.warning(
                "mcapex_5y: need 5 FY maintenance capex values for listing_id=%s, found %s",
                listing_id,
                len(points),
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "mcapex_5y: latest FY (%s) too old for listing_id=%s",
                latest.as_of,
                listing_id,
            )
            return None
        latest_five = points[:5]
        average = sum_money([point.money for point in latest_five]) / 5.0
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=average.amount,
            as_of=latest_five[0].as_of,
            currency=average.currency,
        )


@dataclass
class MCapexTTMMetric(_MCapexBase):
    """Compute trailing-12-month maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_ttm"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name="MaintenanceCapex"
        )
        capex = self._compute_ttm_amount(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            context="mcapex_ttm",
            target_currency=target_currency,
        )
        da = self._compute_ttm_amount(
            listing_id,
            repo,
            DA_PRIMARY_CONCEPTS,
            context="mcapex_ttm",
            target_currency=target_currency,
        )
        if da is None:
            da = self._compute_ttm_amount(
                listing_id,
                repo,
                DA_FALLBACK_CONCEPTS,
                context="mcapex_ttm",
                target_currency=target_currency,
            )
        if capex is None and da is None:
            LOGGER.warning(
                "mcapex_ttm: missing TTM capex and D&A inputs for listing_id=%s",
                listing_id,
            )
            return None
        value = self._compute_mcapex_value(capex, da, listing_id, context="mcapex_ttm")
        if value is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=value.money.amount,
            as_of=value.as_of,
            currency=value.money.currency,
        )


__all__ = ["MCapexFYMetric", "MCapexFiveYearMetric", "MCapexTTMMetric"]
