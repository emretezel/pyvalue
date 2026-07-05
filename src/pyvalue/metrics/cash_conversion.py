"""Cash conversion metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_date,
    is_recent_fact,
    latest_consecutive_year_chain,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
NET_INCOME_PRIMARY_CONCEPT = "NetIncomeLoss"
NET_INCOME_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"

OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
NET_INCOME_CONCEPTS = (
    NET_INCOME_PRIMARY_CONCEPT,
    NET_INCOME_FALLBACK_CONCEPT,
)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
# Window cap: the freshest 10 fiscal years, the "full cycle" the metric name
# refers to. The window is adaptive below the cap — see MIN_VALID_POINTS.
SERIES_YEARS = 10
# Minimum positive-NI observations for a meaningful full-cycle median. The
# CFO/NI ratio is undefined for loss years (a negative denominator flips the
# sign of a *good* outcome), so loss years are skipped rather than fatal; six
# valid points keeps the median honest while tolerating up to four losses in a
# full 10-year window — deliberately equal to the DVG screen's old
# ``ni_loss_years_10y <= 4`` gate and to
# ``fundamental_consistency.MIN_CHAIN_YEARS`` (the screen pairs the two
# metrics). The old any-loss-is-fatal guard caused 87.6% of this metric's
# failures (see docs/research/screener-na-investigation.md).
MIN_VALID_POINTS = 6

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(OPERATING_CASH_FLOW_CONCEPTS + NET_INCOME_CONCEPTS)
)


@dataclass(frozen=True)
class CashConversionSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass(frozen=True)
class _CashConversionFYPoint:
    year: int
    value: float
    as_of: str


@dataclass(frozen=True)
class CashConversionTenYearSnapshot:
    """Adaptive FY cash-conversion series.

    ``points`` holds only the positive-NI ratio points (6..10, newest-first) of
    the latest consecutive joint CFO+NI chain; skipped loss years contribute no
    point. ``as_of`` is the chain anchor's date — the staleness clock — which
    may belong to a skipped loss year and therefore not match any point.
    """

    points: tuple[_CashConversionFYPoint, ...]
    as_of: str
    currency: Optional[str]


class CashConversionCalculator:
    """Shared calculator for TTM and FY-series cash conversion metrics.

    Each CFO / net-income amount is aligned to the listing currency before the
    ratio, so cash conversion (a dimensionless CFO/NI multiple) is currency-safe.
    """

    def compute_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[CashConversionSnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="cfo_to_ni_ttm"
        )
        cfo = self._compute_ttm_amount(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context="cfo_to_ni_ttm",
            target_currency=target_currency,
        )
        if cfo is None:
            LOGGER.warning(
                "cfo_to_ni_ttm: missing TTM CFO for listing_id=%s", listing_id
            )
            return None

        net_income = self._compute_ttm_amount(
            listing_id,
            repo,
            NET_INCOME_CONCEPTS,
            context="cfo_to_ni_ttm",
            target_currency=target_currency,
        )
        if net_income is None:
            LOGGER.warning(
                "cfo_to_ni_ttm: missing TTM net income for listing_id=%s", listing_id
            )
            return None
        if net_income.money.amount <= 0:
            LOGGER.warning(
                "cfo_to_ni_ttm: non-positive TTM net income for listing_id=%s",
                listing_id,
            )
            return None

        return CashConversionSnapshot(
            value=cfo.money / net_income.money,
            as_of=max(cfo.as_of, net_income.as_of),
            currency=None,
        )

    def compute_10y_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[CashConversionTenYearSnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="cfo_to_ni_10y_median"
        )
        cfo_map = self._build_fy_amount_map(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            target_currency=target_currency,
        )
        if not cfo_map:
            LOGGER.warning(
                "cfo_to_ni_10y: missing FY CFO history for listing_id=%s", listing_id
            )
            return None

        net_income_map = self._build_fy_amount_map(
            listing_id,
            repo,
            NET_INCOME_CONCEPTS,
            target_currency=target_currency,
        )
        if not net_income_map:
            LOGGER.warning(
                "cfo_to_ni_10y: missing FY net income history for listing_id=%s",
                listing_id,
            )
            return None

        joint: dict[int, tuple[_MoneyResult, _MoneyResult]] = {
            year: (cfo_map[year], net_income_map[year])
            for year in cfo_map.keys() & net_income_map.keys()
        }
        if not joint:
            LOGGER.warning(
                "cfo_to_ni_10y: missing overlapping FY history for listing_id=%s",
                listing_id,
            )
            return None

        # Adaptive window: the latest consecutive joint chain, capped at 10
        # years. Short histories (young listings, thin exchange coverage) shrink
        # the window instead of voiding the metric outright.
        chain = latest_consecutive_year_chain(joint, max_years=SERIES_YEARS)
        if len(chain) < MIN_VALID_POINTS:
            LOGGER.warning(
                "cfo_to_ni_10y: joint FY chain too short for listing_id=%s: %s of %s years",
                listing_id,
                len(chain),
                MIN_VALID_POINTS,
            )
            return None

        # The staleness clock is the chain anchor (latest joint year), captured
        # BEFORE loss filtering: whether the freshest year is profitable is a
        # stability question (ni_loss_years / eps_streak territory), not a
        # data-freshness one.
        anchor_year, (anchor_cfo, anchor_net_income) = chain[0]
        anchor_as_of = max(anchor_cfo.as_of, anchor_net_income.as_of)

        # Loss years are skipped, not fatal: CFO/NI is sign-ambiguous for
        # NI <= 0 (a cash-covered accounting loss would score *negative*), so
        # such years carry no usable conversion information. The loss itself is
        # policed by the stability metrics, not here.
        selected = [
            _CashConversionFYPoint(
                year=year,
                value=cfo.money / net_income.money,
                as_of=max(cfo.as_of, net_income.as_of),
            )
            for year, (cfo, net_income) in chain
            if net_income.money.amount > 0
        ]
        if len(selected) < MIN_VALID_POINTS:
            LOGGER.warning(
                "cfo_to_ni_10y: too few positive-NI FY years for listing_id=%s: %s of %s in chain",
                listing_id,
                len(selected),
                len(chain),
            )
            return None

        if not is_recent_date(anchor_as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "cfo_to_ni_10y: latest joint FY year %s (%s) too old for listing_id=%s",
                anchor_year,
                anchor_as_of,
                listing_id,
            )
            return None

        return CashConversionTenYearSnapshot(
            points=tuple(selected),
            as_of=anchor_as_of,
            currency=None,
        )

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
                self._money(record, concept, target_currency, listing_id, context)
                for record in quarterly[:4]
            ]
            return _MoneyResult(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _build_fy_amount_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        target_currency: str,
    ) -> dict[int, _MoneyResult]:
        concept_maps = [
            self._fy_map(listing_id, repo, concept, target_currency=target_currency)
            for concept in concepts
        ]
        merged: dict[int, _MoneyResult] = {}
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
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        *,
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
                money=self._money(
                    record, concept, target_currency, listing_id, "cfo_to_ni_10y_median"
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
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _money(
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        listing_id: int,
        metric_id: str,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=concept,
            as_of=fact.end_date,
        )

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        prefix = value[:4]
        if not prefix.isdigit():
            return None
        return int(prefix)


@dataclass
class CFOToNITTMMetric:
    """Compute trailing 12-month operating cash flow to net income."""

    id: str = "cfo_to_ni_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = CashConversionCalculator().compute_ttm(listing_id, repo)
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            unit_kind="ratio",
        )


@dataclass
class CFOToNITenYearMedianMetric:
    """Compute the median FY cash conversion over an adaptive full-cycle window.

    The window is the latest consecutive joint CFO+NI chain capped at 10 fiscal
    years; loss years are skipped and at least six positive-NI points are
    required (see ``MIN_VALID_POINTS``). For a full loss-free 10-year window
    ``statistics.median`` reproduces the previous hardcoded even-count formula
    bit-for-bit, so strict-history listings keep identical values.
    """

    id: str = "cfo_to_ni_10y_median"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = CashConversionCalculator().compute_10y_series(listing_id, repo)
        if snapshot is None:
            return None
        median = statistics.median(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
            unit_kind="ratio",
        )


__all__ = [
    "CashConversionSnapshot",
    "CashConversionTenYearSnapshot",
    "CashConversionCalculator",
    "CFOToNITTMMetric",
    "CFOToNITenYearMedianMetric",
]
