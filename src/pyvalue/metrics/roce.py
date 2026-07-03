"""Return on capital employed (ROCE) metric.

Terry Smith's / Fundsmith's headline quality metric: trailing EBIT over
capital employed, where capital employed = total assets minus current
liabilities. It differs from Greenblatt's ROC (tangible capital only) and
from roic_ttm (after-tax NOPAT over debt+equity-cash invested capital), so
all three can coexist in screens.

The denominator is *averaged* -- latest quarter paired with the same fiscal
quarter one year prior, with a strict prior-FY fallback -- matching the
convention every other TTM return metric here uses (roic_ttm, roa_ttm), so
ROCE and ROIC stay comparable within one screen.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value_ratios import (
    EBIT_CONCEPT,
    EnterpriseValueRatioCalculator,
)
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    extract_year,
    is_recent_date,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

ASSETS_CONCEPT = "Assets"
CURRENT_LIABILITIES_CONCEPT = "LiabilitiesCurrent"

REQUIRED_CONCEPTS = (EBIT_CONCEPT, ASSETS_CONCEPT, CURRENT_LIABILITIES_CONCEPT)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}


@dataclass(frozen=True)
class _CEPoint:
    money: Money
    as_of: str
    fiscal_period: str


@dataclass(frozen=True)
class CapitalEmployedSnapshot:
    money: Money
    as_of: str


class CapitalEmployedCalculator:
    """Averaged capital-employed (Assets - LiabilitiesCurrent) denominators.

    Mirrors the point-building/averaging shape of
    :class:`~pyvalue.metrics.invested_capital.InvestedCapitalCalculator.compute_avg`:
    same-quarter-prior-year averaging first, strict prior-FY pair as fallback.
    """

    def compute_avg(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[CapitalEmployedSnapshot]:
        quarterly_points = self._build_points(
            listing_id, repo, QUARTERLY_PERIODS, context=context
        )
        latest_quarter = self._select_latest(
            quarterly_points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context=context,
            listing_id=listing_id,
        )
        if latest_quarter is not None:
            latest_year = extract_year(latest_quarter.as_of)
            if latest_year is not None:
                for point in quarterly_points[1:]:
                    point_year = extract_year(point.as_of)
                    if (
                        point_year == latest_year - 1
                        and point.fiscal_period == latest_quarter.fiscal_period
                    ):
                        return CapitalEmployedSnapshot(
                            money=(latest_quarter.money + point.money) / 2.0,
                            as_of=latest_quarter.as_of,
                        )

        fy_points = self._build_points(listing_id, repo, FY_PERIODS, context=context)
        latest_fy = self._select_latest(
            fy_points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context=context,
            listing_id=listing_id,
        )
        if latest_fy is None:
            return None
        latest_year = extract_year(latest_fy.as_of)
        if latest_year is None:
            return None
        for point in fy_points[1:]:
            if extract_year(point.as_of) == latest_year - 1:
                return CapitalEmployedSnapshot(
                    money=(latest_fy.money + point.money) / 2.0,
                    as_of=latest_fy.as_of,
                )
        LOGGER.warning(
            "%s: missing strict prior FY capital employed for listing_id=%s",
            context,
            listing_id,
        )
        return None

    def _build_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
        *,
        context: str,
    ) -> list[_CEPoint]:
        # Resolve the listing currency once; both components are aligned to it
        # so capital employed is single-currency by construction.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        assets_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, ASSETS_CONCEPT), periods
        )
        liabilities_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, CURRENT_LIABILITIES_CONCEPT),
            periods,
        )

        # Unlike invested capital there is no fallback chain: capital employed
        # needs both sides on the same balance-sheet date or the point is void.
        points: list[_CEPoint] = []
        for key in sorted(set(assets_map) & set(liabilities_map), reverse=True):
            assets_money = require_metric_money(
                assets_map[key].money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name=ASSETS_CONCEPT,
                as_of=key[0],
            )
            liabilities_money = require_metric_money(
                liabilities_map[key].money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name=CURRENT_LIABILITIES_CONCEPT,
                as_of=key[0],
            )
            points.append(
                _CEPoint(
                    money=assets_money - liabilities_money,
                    as_of=key[0],
                    fiscal_period=key[1],
                )
            )
        return points

    def _period_map(
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> dict[tuple[str, str], MonetaryFact]:
        mapped: dict[tuple[str, str], MonetaryFact] = {}
        for record in sorted(records, key=lambda item: item.end_date, reverse=True):
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _select_latest(
        self,
        points: Sequence[_CEPoint],
        *,
        max_age_days: int,
        context: str,
        listing_id: int,
    ) -> Optional[_CEPoint]:
        if not points:
            LOGGER.warning(
                "%s: missing capital employed points for listing_id=%s",
                context,
                listing_id,
            )
            return None
        latest = points[0]
        if not is_recent_date(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest capital employed (%s) too old for listing_id=%s",
                context,
                latest.as_of,
                listing_id,
            )
            return None
        return latest


@dataclass
class ROCEMetric:
    """Compute return on capital employed: TTM EBIT / avg capital employed."""

    id: str = "roce"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        ebit = EnterpriseValueRatioCalculator().compute_ttm_ebit(
            listing_id, repo, context=self.id
        )
        if ebit is None:
            LOGGER.warning(
                "%s: missing TTM EBIT for listing_id=%s", self.id, listing_id
            )
            return None

        avg_ce = CapitalEmployedCalculator().compute_avg(
            listing_id, repo, context=self.id
        )
        if avg_ce is None:
            return None
        if avg_ce.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive capital employed for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        # Negative EBIT flows through as a negative return. roic_ttm suppresses
        # non-positive NOPAT because its effective-tax-rate model is meaningless
        # there; ROCE has no tax modeling, so a loss period is real information.
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ebit.money / avg_ce.money,
            as_of=max(ebit.as_of, avg_ce.as_of),
            unit_kind="percent",
        )


__all__ = ["CapitalEmployedCalculator", "CapitalEmployedSnapshot", "ROCEMetric"]
