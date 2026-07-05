"""FY consistency metrics for owner-earnings and free-cash-flow screening.

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
    MAX_FY_FACT_AGE_DAYS,
    is_recent_date,
    latest_consecutive_year_chain,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

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
# Minimum length of the adaptive FY chain behind ``ni_loss_year_share``: a
# loss-year *share* over fewer than six observed years says little. Equal to
# ``cash_conversion.MIN_CHAIN_YEARS`` on purpose — the DVG screen pairs the
# two adaptive metrics, so their chain-evidence floors must match (and
# ``cash_conversion.POSITIVE_NI_SHARE_FLOOR`` mirrors the ``<= 0.40``
# loss-share tolerance itself).
MIN_CHAIN_YEARS = 6
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
class _MoneyResult:
    money: Money
    as_of: str


@dataclass(frozen=True)
class _FYPoint:
    year: int
    money: Money
    as_of: str


@dataclass(frozen=True)
class _FYSeriesSnapshot:
    points: tuple[_FYPoint, ...]
    as_of: str
    currency: Optional[str]


class FundamentalConsistencyCalculator:
    """Shared FY-series helpers for normalized FCF and net-income consistency metrics.

    Every FY amount is aligned to the listing currency, so the FCF (= OCF -
    capex) and net-income series are single-currency by construction.
    """

    def compute_fcf_5y_median(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[FundamentalConsistencySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="fcf_fy_median_5y"
        )
        latest_five = self._latest_available_five_points(
            self._build_fcf_points(
                listing_id,
                repo,
                context="fcf_fy_median_5y",
                target_currency=target_currency,
            ),
            listing_id=listing_id,
            context="fcf_fy_median_5y",
        )
        if latest_five is None:
            return None

        median = sorted(point.money.amount for point in latest_five)[2]
        return FundamentalConsistencySnapshot(
            value=median,
            as_of=latest_five[0].as_of,
            currency=target_currency,
        )

    def compute_fcf_10y_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_FYSeriesSnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="fcf_neg_years_10y"
        )
        return self._strict_ten_year_series(
            self._build_fcf_points(
                listing_id,
                repo,
                context="fcf_neg_years_10y",
                target_currency=target_currency,
            ),
            listing_id=listing_id,
            context="fcf_neg_years_10y",
            target_currency=target_currency,
        )

    def compute_net_income_10y_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_FYSeriesSnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="ni_loss_years_10y"
        )
        points = self._build_amount_points(
            listing_id,
            repo,
            NET_INCOME_CONCEPTS,
            context="ni_loss_years_10y",
            target_currency=target_currency,
        )
        return self._strict_ten_year_series(
            points,
            listing_id=listing_id,
            context="ni_loss_years_10y",
            target_currency=target_currency,
        )

    def compute_net_income_loss_year_share(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[FundamentalConsistencySnapshot]:
        """Share of FY loss years within the latest adaptive (<= 10y) chain.

        Unlike the strict 10-year count behind ``ni_loss_years_10y``, the
        denominator here is the actual consecutive-chain length (6..10 years),
        so short histories stay screenable while the tolerance a screen
        expresses (e.g. ``<= 0.40``) scales with the observed window.
        """

        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id="ni_loss_year_share"
        )
        amount_map = self._build_fy_amount_map(
            listing_id,
            repo,
            NET_INCOME_CONCEPTS,
            context="ni_loss_year_share",
            target_currency=target_currency,
        )
        if not amount_map:
            LOGGER.warning(
                "ni_loss_year_share: missing FY net income history for listing_id=%s",
                listing_id,
            )
            return None

        chain = latest_consecutive_year_chain(amount_map, max_years=TEN_YEAR_POINTS)
        if len(chain) < MIN_CHAIN_YEARS:
            LOGGER.warning(
                "ni_loss_year_share: FY net income chain too short for listing_id=%s: %s of %s years",
                listing_id,
                len(chain),
                MIN_CHAIN_YEARS,
            )
            return None

        anchor_year, anchor_point = chain[0]
        if not is_recent_date(anchor_point.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "ni_loss_year_share: latest FY year %s (%s) too old for listing_id=%s",
                anchor_year,
                anchor_point.as_of,
                listing_id,
            )
            return None

        # NI == 0 counts as a loss: a zero-profit year is not a profitable one.
        # This aligns the loss partition with the years cfo_to_ni_10y_median
        # skips (NI <= 0) and deliberately differs from ``ni_loss_years_10y``'s
        # strict ``< 0`` count, which stays unchanged for QARP-primary's gate.
        loss_years = sum(1 for _, point in chain if point.money.amount <= 0)
        return FundamentalConsistencySnapshot(
            value=loss_years / len(chain),
            as_of=anchor_point.as_of,
            currency=None,
        )

    def _build_fcf_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
        target_currency: str,
    ) -> list[_FYPoint]:
        ocf_map = self._build_fy_amount_map(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
            target_currency=target_currency,
        )
        capex_map = self._build_fy_amount_map(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            context=context,
            target_currency=target_currency,
        )

        points: list[_FYPoint] = []
        for year in sorted(ocf_map.keys(), reverse=True):
            operating = ocf_map[year]
            capex = capex_map.get(year)
            fcf = operating.money - capex.money if capex else operating.money
            as_of = max(
                value
                for value in (operating.as_of, capex.as_of if capex else None)
                if value
            )
            points.append(_FYPoint(year=year, money=fcf, as_of=as_of))
        return points

    def _build_amount_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        target_currency: str,
    ) -> list[_FYPoint]:
        amount_map = self._build_fy_amount_map(
            listing_id,
            repo,
            concepts,
            context=context,
            target_currency=target_currency,
        )
        points: list[_FYPoint] = []
        for year in sorted(amount_map.keys(), reverse=True):
            amount = amount_map[year]
            points.append(_FYPoint(year=year, money=amount.money, as_of=amount.as_of))
        if not points:
            LOGGER.warning(
                "%s: missing FY history for listing_id=%s", context, listing_id
            )
        return points

    def _latest_available_five_points(
        self,
        points: list[_FYPoint],
        *,
        listing_id: int,
        context: str,
    ) -> Optional[list[_FYPoint]]:
        if len(points) < FIVE_YEAR_POINTS:
            LOGGER.warning(
                "%s: need 5 FY values for listing_id=%s, found %s",
                context,
                listing_id,
                len(points),
            )
            return None

        latest_five = points[:FIVE_YEAR_POINTS]
        if not self._is_recent_as_of(
            latest_five[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "%s: latest FY (%s) too old for listing_id=%s",
                context,
                latest_five[0].as_of,
                listing_id,
            )
            return None
        return latest_five

    def _strict_ten_year_series(
        self,
        points: list[_FYPoint],
        *,
        listing_id: int,
        context: str,
        target_currency: str,
    ) -> Optional[_FYSeriesSnapshot]:
        if not points:
            LOGGER.warning(
                "%s: missing FY history for listing_id=%s", context, listing_id
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
                    "%s: missing strict consecutive FY chain for listing_id=%s",
                    context,
                    listing_id,
                )
                return None
            selected.append(selected_point)

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "%s: latest FY (%s) too old for listing_id=%s",
                context,
                selected[0].as_of,
                listing_id,
            )
            return None

        return _FYSeriesSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=target_currency,
        )

    def _build_fy_amount_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        target_currency: str,
    ) -> dict[int, _MoneyResult]:
        concept_maps = [
            self._fy_map(
                listing_id,
                repo,
                concept,
                context=context,
                target_currency=target_currency,
            )
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
        context: str,
        target_currency: str,
    ) -> dict[int, _MoneyResult]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records)
        mapped: dict[int, _MoneyResult] = {}
        for record in ordered:
            year = self._parse_year(record.end_date)
            if year is None or year in mapped:
                continue
            mapped[year] = _MoneyResult(
                money=require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id=context,
                    listing_id=listing_id,
                    input_name=concept,
                    as_of=record.end_date,
                ),
                as_of=record.end_date,
            )
        return mapped

    def _filter_periods(self, records: Sequence[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in FY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

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
class FCFFiveYearMedianMetric:
    """Compute FY median free cash flow over the latest 5 available years."""

    id: str = "fcf_fy_median_5y"
    required_concepts = FCF_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_fcf_5y_median(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult.monetary(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_net_income_10y_series(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=float(sum(1 for point in snapshot.points if point.money.amount < 0)),
            as_of=snapshot.as_of,
        )


@dataclass
class NetIncomeLossYearShareMetric:
    """Share of FY loss years within the latest adaptive (<= 10y) chain.

    The length-normalized sibling of ``ni_loss_years_10y``: the denominator is
    the observed consecutive-chain length (6..10 FY years), so 6-9-year
    histories stay screenable and a share threshold keeps one meaning across
    window lengths. Stored as a 0..1 fraction with ``unit_kind="percent"``,
    mirroring ``short_term_debt_share``.
    """

    id: str = "ni_loss_year_share"
    required_concepts = NET_INCOME_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            FundamentalConsistencyCalculator().compute_net_income_loss_year_share(
                listing_id, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            unit_kind="percent",
        )


@dataclass
class FCFNegativeYearsTenYearMetric:
    """Count FY negative free-cash-flow years over the latest strict 10-year window."""

    id: str = "fcf_neg_years_10y"
    required_concepts = FCF_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = FundamentalConsistencyCalculator().compute_fcf_10y_series(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=float(sum(1 for point in snapshot.points if point.money.amount < 0)),
            as_of=snapshot.as_of,
        )


__all__ = [
    "FundamentalConsistencyCalculator",
    "FundamentalConsistencySnapshot",
    "FCFFiveYearMedianMetric",
    "NetIncomeLossYearsTenYearMetric",
    "NetIncomeLossYearShareMetric",
    "FCFNegativeYearsTenYearMetric",
]
