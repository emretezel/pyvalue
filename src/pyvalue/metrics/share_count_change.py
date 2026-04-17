"""Share-count change metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

SHARE_COUNT_CONCEPT = "CommonStockSharesOutstanding"

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
FIVE_YEAR_EXACT_YEARS = 5
TEN_YEAR_EXACT_YEARS = 10

REQUIRED_CONCEPTS = (SHARE_COUNT_CONCEPT,)


@dataclass(frozen=True)
class _ShareCountPoint:
    year: int
    fiscal_period: str
    shares: float
    as_of: str


@dataclass(frozen=True)
class ShareCountSnapshot:
    latest: _ShareCountPoint
    prior: _ShareCountPoint
    as_of: str


# Backward-compatible alias for any external imports using the old name.
ShareCountTenYearSnapshot = ShareCountSnapshot


class ShareCountChangeCalculator:
    """Resolve the latest exact-year share-count comparison pair."""

    def compute_pair(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[ShareCountSnapshot]:
        return self.compute_pair_for_years(
            symbol,
            repo,
            exact_years=TEN_YEAR_EXACT_YEARS,
            context="share_count_change",
        )

    def compute_pair_for_years(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        exact_years: int,
        context: str,
    ) -> Optional[ShareCountSnapshot]:
        records = repo.facts_for_concept(symbol, SHARE_COUNT_CONCEPT)
        if not records:
            LOGGER.warning(
                "%s: missing outstanding share history for %s", context, symbol
            )
            return None

        quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
        quarterly_snapshot = self._resolve_period_pair(
            symbol,
            quarterly,
            max_age_days=MAX_FACT_AGE_DAYS,
            exact_years=exact_years,
            context="quarterly",
            logger_context=context,
        )
        if quarterly_snapshot is not None:
            return quarterly_snapshot

        fy = self._filter_periods(records, FY_PERIODS)
        fy_snapshot = self._resolve_period_pair(
            symbol,
            fy,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            exact_years=exact_years,
            context="fy",
            logger_context=context,
        )
        if fy_snapshot is not None:
            return fy_snapshot

        LOGGER.warning(
            "%s: no valid exact %s-year share-count pair for %s",
            context,
            exact_years,
            symbol,
        )
        return None

    def _resolve_period_pair(
        self,
        symbol: str,
        records: Sequence[FactRecord],
        *,
        max_age_days: int,
        exact_years: int,
        context: str,
        logger_context: str,
    ) -> Optional[ShareCountSnapshot]:
        if not records:
            return None

        latest = self._to_point(records[0])
        if latest is None:
            return None
        if not is_recent_fact(records[0], max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest %s share-count point (%s) too old for %s",
                logger_context,
                context,
                records[0].end_date,
                symbol,
            )
            return None
        if latest.shares <= 0:
            LOGGER.warning(
                "%s: non-positive latest %s share count for %s",
                logger_context,
                context,
                symbol,
            )
            return None

        target_year = latest.year - exact_years
        prior: Optional[_ShareCountPoint] = None
        for record in records[1:]:
            point = self._to_point(record)
            if point is None:
                continue
            if (
                point.fiscal_period == latest.fiscal_period
                and point.year == target_year
            ):
                prior = point
                break

        if prior is None:
            LOGGER.warning(
                "%s: missing exact %s-year %s match for %s",
                logger_context,
                exact_years,
                context,
                symbol,
            )
            return None
        if prior.shares <= 0:
            LOGGER.warning(
                "%s: non-positive prior %s share count for %s",
                logger_context,
                context,
                symbol,
            )
            return None

        return ShareCountSnapshot(
            latest=latest,
            prior=prior,
            as_of=latest.as_of,
        )

    def _filter_periods(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen: set[tuple[str, str]] = set()
        for record in records:
            fiscal_period = (record.fiscal_period or "").upper()
            key = (record.end_date, fiscal_period)
            if fiscal_period not in periods:
                continue
            if record.value is None:
                continue
            if key in seen:
                continue
            filtered.append(record)
            seen.add(key)
        return filtered

    def _to_point(self, record: FactRecord) -> Optional[_ShareCountPoint]:
        year = self._extract_year(record.end_date)
        fiscal_period = (record.fiscal_period or "").upper()
        if year is None or not fiscal_period or record.value is None:
            return None
        return _ShareCountPoint(
            year=year,
            fiscal_period=fiscal_period,
            shares=record.value,
            as_of=record.end_date,
        )

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        prefix = value[:4]
        if not prefix.isdigit():
            return None
        return int(prefix)


def _compute_share_count_cagr(
    *,
    metric_id: str,
    symbol: str,
    repo: FinancialFactsRepository,
    exact_years: int,
) -> Optional[MetricResult]:
    snapshot = ShareCountChangeCalculator().compute_pair_for_years(
        symbol,
        repo,
        exact_years=exact_years,
        context=metric_id,
    )
    if snapshot is None:
        return None

    ratio = snapshot.latest.shares / snapshot.prior.shares
    value = ratio ** (1.0 / exact_years) - 1.0
    return MetricResult(
        symbol=symbol,
        metric_id=metric_id,
        value=value,
        as_of=snapshot.as_of,
    )


@dataclass
class ShareCountCAGR5YMetric:
    """Compute 5-year CAGR of outstanding shares."""

    id: str = "share_count_cagr_5y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        return _compute_share_count_cagr(
            metric_id=self.id,
            symbol=symbol,
            repo=repo,
            exact_years=FIVE_YEAR_EXACT_YEARS,
        )


@dataclass
class ShareCountCAGR10YMetric:
    """Compute 10-year CAGR of outstanding shares."""

    id: str = "share_count_cagr_10y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        return _compute_share_count_cagr(
            metric_id=self.id,
            symbol=symbol,
            repo=repo,
            exact_years=TEN_YEAR_EXACT_YEARS,
        )


@dataclass
class Shares10YPctChangeMetric:
    """Compute exact 10-year percent change in outstanding shares."""

    id: str = "shares_10y_pct_change"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ShareCountChangeCalculator().compute_pair(symbol, repo)
        if snapshot is None:
            return None

        value = snapshot.latest.shares / snapshot.prior.shares - 1.0
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "ShareCountSnapshot",
    "ShareCountTenYearSnapshot",
    "ShareCountChangeCalculator",
    "ShareCountCAGR5YMetric",
    "ShareCountCAGR10YMetric",
    "Shares10YPctChangeMetric",
]
