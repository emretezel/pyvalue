"""Accruals ratio metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

_METRIC_ID = "accruals_ratio"

ASSETS_CONCEPT = "Assets"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
NET_INCOME_PRIMARY_CONCEPT = "NetIncomeLoss"
NET_INCOME_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"

OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
NET_INCOME_CONCEPTS = (
    NET_INCOME_PRIMARY_CONCEPT,
    NET_INCOME_FALLBACK_CONCEPT,
)
REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (ASSETS_CONCEPT,) + OPERATING_CASH_FLOW_CONCEPTS + NET_INCOME_CONCEPTS
    )
)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}


@dataclass(frozen=True)
class AccrualsRatioSnapshot:
    value: float
    as_of: str


@dataclass
class _AmountResult:
    money: Money
    as_of: str


@dataclass
class _AssetPoint:
    money: Money
    as_of: str
    fiscal_period: str


class AccrualsRatioCalculator:
    """Shared calculator for accruals ratio inputs."""

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[AccrualsRatioSnapshot]:
        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_CONCEPTS,
            context=_METRIC_ID,
        )
        if net_income is None:
            LOGGER.warning("accruals_ratio: missing TTM net income for %s", symbol)
            return None

        cfo = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=_METRIC_ID,
        )
        if cfo is None:
            LOGGER.warning("accruals_ratio: missing TTM CFO for %s", symbol)
            return None

        average_assets = self._compute_avg_total_assets(symbol, repo)
        if average_assets is None:
            return None
        if average_assets.money.amount <= 0:
            LOGGER.warning("accruals_ratio: non-positive average assets for %s", symbol)
            return None

        # NI, CFO and average assets are each aligned to the listing currency by
        # the builders below, so the accrual numerator and its asset denominator
        # are single-currency and ``Money`` division yields a dimensionless ratio.
        accruals = (net_income.money - cfo.money) / average_assets.money
        return AccrualsRatioSnapshot(
            value=accruals,
            as_of=max(net_income.as_of, cfo.as_of, average_assets.as_of),
        )

    def compute_avg_total_assets(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[_AmountResult]:
        """Return the average-assets denominator used by accrual-based metrics."""

        return self._compute_avg_total_assets(symbol, repo)

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
    ) -> Optional[_AmountResult]:
        target_currency = require_metric_ticker_currency(
            symbol, repo, metric_id=context
        )
        for concept in concepts:
            records = repo.monetary_facts_for_concept(symbol, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "%s: need 4 quarterly %s records for %s, found %s",
                    context,
                    concept,
                    symbol,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                LOGGER.warning(
                    "%s: latest %s (%s) too old for %s",
                    context,
                    concept,
                    quarterly[0].end_date,
                    symbol,
                )
                continue
            monies = [
                self._money(record, target_currency, symbol, context)
                for record in quarterly[:4]
            ]
            return _AmountResult(
                money=sum_money(monies),
                as_of=quarterly[0].end_date,
            )
        return None

    def _compute_avg_total_assets(
        self,
        symbol: str,
        repo: RegionFactsRepository,
    ) -> Optional[_AmountResult]:
        target_currency = require_metric_ticker_currency(
            symbol, repo, metric_id=_METRIC_ID
        )
        records = repo.monetary_facts_for_concept(symbol, ASSETS_CONCEPT)
        quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
        if not quarterly:
            LOGGER.warning("accruals_ratio: missing quarterly assets for %s", symbol)
            return None

        latest = quarterly[0]
        if not is_recent_fact(latest, max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "accruals_ratio: latest assets quarter (%s) too old for %s",
                latest.end_date,
                symbol,
            )
            return None

        latest_point = _AssetPoint(
            money=self._money(latest, target_currency, symbol, _METRIC_ID),
            as_of=latest.end_date,
            fiscal_period=(latest.fiscal_period or "").upper(),
        )

        latest_year = self._extract_year(latest.end_date)
        if latest_year is None:
            LOGGER.warning("accruals_ratio: invalid latest assets date for %s", symbol)
            return None

        prior_point: Optional[_AssetPoint] = None
        for record in quarterly[1:]:
            point_year = self._extract_year(record.end_date)
            if (
                point_year is not None
                and (record.fiscal_period or "").upper() == latest_point.fiscal_period
                and point_year == latest_year - 1
            ):
                prior_point = _AssetPoint(
                    money=self._money(record, target_currency, symbol, _METRIC_ID),
                    as_of=record.end_date,
                    fiscal_period=(record.fiscal_period or "").upper(),
                )
                break

        if prior_point is None:
            LOGGER.warning(
                "accruals_ratio: missing same-quarter prior-year assets for %s", symbol
            )
            return None

        return _AmountResult(
            money=(latest_point.money + prior_point.money) / 2.0,
            as_of=latest_point.as_of,
        )

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
        self, fact: MonetaryFact, target_currency: str, symbol: str, context: str
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=context,
            symbol=symbol,
            input_name=fact.concept,
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
class AccrualsRatioMetric:
    """Compute accruals ratio using TTM net income/CFO over average total assets."""

    id: str = "accruals_ratio"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = AccrualsRatioCalculator().compute(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            unit_kind="ratio",
        )


__all__ = [
    "AccrualsRatioSnapshot",
    "AccrualsRatioCalculator",
    "AccrualsRatioMetric",
]
