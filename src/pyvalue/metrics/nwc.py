"""Net working capital and delta metrics.

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
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

ASSETS_CURRENT_CONCEPT = "AssetsCurrent"
LIABILITIES_CURRENT_CONCEPT = "LiabilitiesCurrent"
CASH_PRIMARY_CONCEPT = "CashAndShortTermInvestments"
CASH_EQUIVALENTS_CONCEPT = "CashAndCashEquivalents"
SHORT_TERM_INVESTMENTS_CONCEPT = "ShortTermInvestments"
SHORT_TERM_DEBT_CONCEPT = "ShortTermDebt"
REQUIRED_CONCEPTS = (
    ASSETS_CURRENT_CONCEPT,
    LIABILITIES_CURRENT_CONCEPT,
    CASH_PRIMARY_CONCEPT,
    CASH_EQUIVALENTS_CONCEPT,
    SHORT_TERM_INVESTMENTS_CONCEPT,
    SHORT_TERM_DEBT_CONCEPT,
)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}

# Shared metric id used when resolving the listing currency and aligning every
# NWC component to it; all five NWC metrics share the same currency invariant.
_NWC_METRIC_ID = "nwc"


@dataclass
class _NWCPoint:
    money: Money
    as_of: str
    fiscal_period: str


class _NWCBase:
    def _build_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
    ) -> list[_NWCPoint]:
        # Resolve the listing currency once; every component is aligned to it via
        # the shared Money seam, so each NWC point is single-currency by build.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=_NWC_METRIC_ID
        )
        assets_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, ASSETS_CURRENT_CONCEPT), periods
        )
        liabilities_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, LIABILITIES_CURRENT_CONCEPT),
            periods,
        )
        cash_primary_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_PRIMARY_CONCEPT), periods
        )
        cash_eq_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_EQUIVALENTS_CONCEPT),
            periods,
        )
        short_term_investments_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, SHORT_TERM_INVESTMENTS_CONCEPT),
            periods,
        )
        short_term_debt_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, SHORT_TERM_DEBT_CONCEPT),
            periods,
        )

        candidate_keys = sorted(
            set(assets_map.keys()).intersection(liabilities_map.keys()),
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )
        points: list[_NWCPoint] = []
        for key in candidate_keys:
            point = self._compute_point_for_key(
                listing_id=listing_id,
                target_currency=target_currency,
                key=key,
                assets=assets_map.get(key),
                liabilities=liabilities_map.get(key),
                cash_primary=cash_primary_map.get(key),
                cash_equivalents=cash_eq_map.get(key),
                short_term_investments=short_term_investments_map.get(key),
                short_term_debt=short_term_debt_map.get(key),
            )
            if point is not None:
                points.append(point)
        return points

    def _compute_point_for_key(
        self,
        *,
        listing_id: int,
        target_currency: str,
        key: tuple[str, str],
        assets: Optional[MonetaryFact],
        liabilities: Optional[MonetaryFact],
        cash_primary: Optional[MonetaryFact],
        cash_equivalents: Optional[MonetaryFact],
        short_term_investments: Optional[MonetaryFact],
        short_term_debt: Optional[MonetaryFact],
    ) -> Optional[_NWCPoint]:
        if assets is None or liabilities is None:
            return None

        assets_money = self._money(assets, target_currency, listing_id)
        liabilities_money = self._money(liabilities, target_currency, listing_id)

        cash_money = self._cash_amount(
            listing_id=listing_id,
            target_currency=target_currency,
            key=key,
            cash_primary=cash_primary,
            cash_equivalents=cash_equivalents,
            short_term_investments=short_term_investments,
        )
        if cash_money is None:
            return None

        zero = Money.of(0.0, target_currency)
        short_term_debt_money = (
            self._money(short_term_debt, target_currency, listing_id)
            if short_term_debt is not None
            else zero
        )

        # Operating liabilities exclude interest-bearing short-term debt, floored
        # at zero so an over-large debt figure cannot turn liabilities negative.
        adjusted_liabilities = max(liabilities_money - short_term_debt_money, zero)
        nwc_money = (assets_money - cash_money) - adjusted_liabilities
        return _NWCPoint(
            money=nwc_money,
            as_of=key[0],
            fiscal_period=key[1],
        )

    def _cash_amount(
        self,
        *,
        listing_id: int,
        target_currency: str,
        key: tuple[str, str],
        cash_primary: Optional[MonetaryFact],
        cash_equivalents: Optional[MonetaryFact],
        short_term_investments: Optional[MonetaryFact],
    ) -> Optional[Money]:
        if cash_primary is not None:
            return self._money(cash_primary, target_currency, listing_id)

        if cash_equivalents is None and short_term_investments is None:
            LOGGER.warning(
                "nwc: missing cash inputs for listing_id=%s on %s/%s",
                listing_id,
                key[0],
                key[1],
            )
            return None

        cash_money = Money.of(0.0, target_currency)
        if cash_equivalents is not None:
            cash_money = cash_money + self._money(
                cash_equivalents, target_currency, listing_id
            )
        if short_term_investments is not None:
            cash_money = cash_money + self._money(
                short_term_investments, target_currency, listing_id
            )
        return cash_money

    def _period_map(
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> dict[tuple[str, str], MonetaryFact]:
        mapped: dict[tuple[str, str], MonetaryFact] = {}
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _money(
        self, fact: MonetaryFact, target_currency: str, listing_id: int
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=_NWC_METRIC_ID,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        year = value[:4]
        if not year.isdigit():
            return None
        return int(year)

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))

    def _select_latest_point(
        self,
        points: Sequence[_NWCPoint],
        *,
        max_age_days: int,
        context: str,
        listing_id: int,
    ) -> Optional[_NWCPoint]:
        if not points:
            LOGGER.warning(
                "%s: missing NWC points for listing_id=%s", context, listing_id
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest point (%s) too old for listing_id=%s",
                context,
                latest.as_of,
                listing_id,
            )
            return None
        return latest


@dataclass
class NWCMostRecentQuarterMetric(_NWCBase):
    """Compute NWC for the most recent quarter (EODHD-oriented)."""

    id: str = "nwc_mqr"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(listing_id, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="nwc_mqr",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=latest.money.amount,
            as_of=latest.as_of,
            currency=latest.money.currency,
        )


@dataclass
class NWCFYMetric(_NWCBase):
    """Compute NWC for latest fiscal year end (EODHD-oriented)."""

    id: str = "nwc_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(listing_id, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="nwc_fy",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=latest.money.amount,
            as_of=latest.as_of,
            currency=latest.money.currency,
        )


@dataclass
class DeltaNWCTTMMetric(_NWCBase):
    """Compute quarter-over-quarter-year delta in NWC (EODHD-oriented)."""

    id: str = "delta_nwc_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(listing_id, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="delta_nwc_ttm",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning(
                "delta_nwc_ttm: invalid latest quarter date for listing_id=%s",
                listing_id,
            )
            return None

        prior: Optional[_NWCPoint] = None
        for point in points[1:]:
            point_year = self._extract_year(point.as_of)
            if (
                point_year is not None
                and point.fiscal_period == latest.fiscal_period
                and point_year == latest_year - 1
            ):
                prior = point
                break
        if prior is None:
            LOGGER.warning(
                "delta_nwc_ttm: missing prior-year %s for listing_id=%s",
                latest.fiscal_period,
                listing_id,
            )
            return None
        delta = latest.money - prior.money
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=delta.amount,
            as_of=latest.as_of,
            currency=delta.currency,
        )


@dataclass
class DeltaNWCFYMetric(_NWCBase):
    """Compute year-over-year fiscal-year NWC delta (EODHD-oriented)."""

    id: str = "delta_nwc_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(listing_id, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="delta_nwc_fy",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning(
                "delta_nwc_fy: invalid latest FY date for listing_id=%s", listing_id
            )
            return None

        prior: Optional[_NWCPoint] = None
        for point in points[1:]:
            point_year = self._extract_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                prior = point
                break
        if prior is None:
            LOGGER.warning(
                "delta_nwc_fy: missing strict prior FY for listing_id=%s", listing_id
            )
            return None
        delta = latest.money - prior.money
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=delta.amount,
            as_of=latest.as_of,
            currency=delta.currency,
        )


@dataclass
class DeltaNWCMaintMetric(_NWCBase):
    """Compute maintenance delta NWC as max(avg(last 3 FY deltas), 0)."""

    id: str = "delta_nwc_maint"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(listing_id, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="delta_nwc_maint",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning(
                "delta_nwc_maint: invalid latest FY date for listing_id=%s", listing_id
            )
            return None

        by_year: dict[int, _NWCPoint] = {}
        for point in points:
            year = self._extract_year(point.as_of)
            if year is None:
                continue
            if year not in by_year:
                by_year[year] = point

        required_years = [
            latest_year,
            latest_year - 1,
            latest_year - 2,
            latest_year - 3,
        ]
        if not all(year in by_year for year in required_years):
            LOGGER.warning(
                "delta_nwc_maint: need 4 consecutive FY NWC points for listing_id=%s",
                listing_id,
            )
            return None

        delta_latest = by_year[latest_year].money - by_year[latest_year - 1].money
        delta_prev_1 = by_year[latest_year - 1].money - by_year[latest_year - 2].money
        delta_prev_2 = by_year[latest_year - 2].money - by_year[latest_year - 3].money
        average_delta = (delta_latest + delta_prev_1 + delta_prev_2) / 3.0

        # Maintenance NWC investment is floored at zero: a shrinking NWC frees
        # cash rather than consuming it, so it does not reduce owner earnings.
        floored = max(average_delta, Money.of(0.0, average_delta.currency))
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=floored.amount,
            as_of=latest.as_of,
            currency=floored.currency,
        )


__all__ = [
    "NWCMostRecentQuarterMetric",
    "NWCFYMetric",
    "DeltaNWCTTMMetric",
    "DeltaNWCFYMetric",
    "DeltaNWCMaintMetric",
]
