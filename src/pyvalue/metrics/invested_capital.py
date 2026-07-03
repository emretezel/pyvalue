"""Invested capital base metrics implementation.

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
    MAX_FY_FACT_AGE_DAYS,
    extract_year,
    is_recent_date,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

SHORT_TERM_DEBT_CONCEPT = "ShortTermDebt"
LONG_TERM_DEBT_CONCEPT = "LongTermDebt"
TOTAL_DEBT_CONCEPT = "TotalDebtFromBalanceSheet"
EQUITY_PRIMARY_CONCEPT = "StockholdersEquity"
EQUITY_FALLBACK_CONCEPT = "CommonStockholdersEquity"
CASH_PRIMARY_CONCEPT = "CashAndCashEquivalents"
CASH_FALLBACK_CONCEPT = "CashAndShortTermInvestments"

# Shared metric id used when resolving the listing currency for the calculator.
_INVESTED_CAPITAL_METRIC_ID = "invested_capital"

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}

REQUIRED_CONCEPTS = (
    SHORT_TERM_DEBT_CONCEPT,
    LONG_TERM_DEBT_CONCEPT,
    TOTAL_DEBT_CONCEPT,
    EQUITY_PRIMARY_CONCEPT,
    EQUITY_FALLBACK_CONCEPT,
    CASH_PRIMARY_CONCEPT,
    CASH_FALLBACK_CONCEPT,
)


@dataclass
class _Amount:
    money: Money
    as_of: str


@dataclass
class _ICPoint:
    money: Money
    as_of: str
    fiscal_period: str


@dataclass(frozen=True)
class InvestedCapitalSnapshot:
    money: Money
    as_of: str


class _InvestedCapitalBase:
    def _build_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
    ) -> list[_ICPoint]:
        # Resolve the listing currency once; every component is aligned to it, so
        # invested capital (debt + equity - cash) is single-currency by build.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=_INVESTED_CAPITAL_METRIC_ID
        )
        short_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, SHORT_TERM_DEBT_CONCEPT),
            periods,
        )
        long_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, LONG_TERM_DEBT_CONCEPT), periods
        )
        total_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, TOTAL_DEBT_CONCEPT), periods
        )
        equity_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, EQUITY_PRIMARY_CONCEPT), periods
        )
        common_equity_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, EQUITY_FALLBACK_CONCEPT),
            periods,
        )
        cash_primary_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_PRIMARY_CONCEPT), periods
        )
        cash_fallback_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_FALLBACK_CONCEPT), periods
        )

        candidate_keys = sorted(
            set(short_map.keys())
            | set(long_map.keys())
            | set(total_map.keys())
            | set(equity_map.keys())
            | set(common_equity_map.keys())
            | set(cash_primary_map.keys())
            | set(cash_fallback_map.keys()),
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )

        points: list[_ICPoint] = []
        for key in candidate_keys:
            debt = self._resolve_debt(
                listing_id=listing_id,
                target_currency=target_currency,
                key=key,
                short_debt=short_map.get(key),
                long_debt=long_map.get(key),
                total_debt=total_map.get(key),
            )
            if debt is None:
                continue

            equity = self._resolve_equity(
                listing_id=listing_id,
                target_currency=target_currency,
                key=key,
                primary=equity_map.get(key),
                fallback=common_equity_map.get(key),
            )
            if equity is None:
                continue

            cash = self._resolve_cash(
                listing_id=listing_id,
                target_currency=target_currency,
                key=key,
                primary=cash_primary_map.get(key),
                fallback=cash_fallback_map.get(key),
            )
            if cash is None:
                continue

            points.append(
                _ICPoint(
                    money=debt.money + equity.money - cash.money,
                    as_of=max(debt.as_of, equity.as_of, cash.as_of),
                    fiscal_period=key[1],
                )
            )
        return points

    def _resolve_debt(
        self,
        *,
        listing_id: int,
        target_currency: str,
        key: tuple[str, str],
        short_debt: Optional[MonetaryFact],
        long_debt: Optional[MonetaryFact],
        total_debt: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        if short_debt is not None and long_debt is not None:
            return _Amount(
                money=self._money(short_debt, target_currency, listing_id)
                + self._money(long_debt, target_currency, listing_id),
                as_of=max(short_debt.end_date, long_debt.end_date),
            )

        if total_debt is not None:
            return _Amount(
                money=self._money(total_debt, target_currency, listing_id),
                as_of=total_debt.end_date,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            LOGGER.warning(
                "invested_capital: missing debt inputs for listing_id=%s on %s/%s",
                listing_id,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(one_side, target_currency, listing_id),
            as_of=one_side.end_date,
        )

    def _resolve_equity(
        self,
        *,
        listing_id: int,
        target_currency: str,
        key: tuple[str, str],
        primary: Optional[MonetaryFact],
        fallback: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        record = primary or fallback
        if record is None:
            LOGGER.warning(
                "invested_capital: missing equity for listing_id=%s on %s/%s",
                listing_id,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(record, target_currency, listing_id),
            as_of=record.end_date,
        )

    def _resolve_cash(
        self,
        *,
        listing_id: int,
        target_currency: str,
        key: tuple[str, str],
        primary: Optional[MonetaryFact],
        fallback: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        record = primary or fallback
        if record is None:
            LOGGER.warning(
                "invested_capital: missing cash for listing_id=%s on %s/%s",
                listing_id,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(record, target_currency, listing_id),
            as_of=record.end_date,
        )

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

    def _money(
        self, fact: MonetaryFact, target_currency: str, listing_id: int
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=_INVESTED_CAPITAL_METRIC_ID,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        return is_recent_date(as_of, max_age_days=max_age_days)

    def _select_latest_point(
        self,
        points: Sequence[_ICPoint],
        *,
        max_age_days: int,
        context: str,
        listing_id: int,
    ) -> Optional[_ICPoint]:
        if not points:
            LOGGER.warning(
                "%s: missing invested capital points for listing_id=%s",
                context,
                listing_id,
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

    def _extract_year(self, value: str) -> Optional[int]:
        return extract_year(value)


class InvestedCapitalCalculator(_InvestedCapitalBase):
    """Shared calculator for invested-capital snapshots."""

    def compute_mqr(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(listing_id, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="ic_mqr",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(money=latest.money, as_of=latest.as_of)

    def compute_fy(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(listing_id, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="ic_fy",
            listing_id=listing_id,
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(money=latest.money, as_of=latest.as_of)

    def compute_fy_series(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> list[InvestedCapitalSnapshot]:
        """Return FY invested-capital points (latest first) without freshness gating."""

        points = self._build_points(listing_id, repo, FY_PERIODS)
        return [
            InvestedCapitalSnapshot(money=point.money, as_of=point.as_of)
            for point in points
        ]

    def compute_avg(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        quarterly_points = self._build_points(listing_id, repo, QUARTERLY_PERIODS)
        latest_quarter = self._select_latest_point(
            quarterly_points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="avg_ic",
            listing_id=listing_id,
        )
        if latest_quarter is not None:
            latest_year = self._extract_year(latest_quarter.as_of)
            if latest_year is not None:
                for point in quarterly_points[1:]:
                    point_year = self._extract_year(point.as_of)
                    if (
                        point_year is not None
                        and point.fiscal_period == latest_quarter.fiscal_period
                        and point_year == latest_year - 1
                    ):
                        return InvestedCapitalSnapshot(
                            money=(latest_quarter.money + point.money) / 2.0,
                            as_of=latest_quarter.as_of,
                        )

        fy_points = self._build_points(listing_id, repo, FY_PERIODS)
        latest_fy = self._select_latest_point(
            fy_points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="avg_ic",
            listing_id=listing_id,
        )
        if latest_fy is None:
            return None

        latest_year = self._extract_year(latest_fy.as_of)
        if latest_year is None:
            LOGGER.warning(
                "avg_ic: invalid latest FY date for listing_id=%s", listing_id
            )
            return None

        prior_fy: Optional[_ICPoint] = None
        for point in fy_points[1:]:
            point_year = self._extract_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                prior_fy = point
                break

        if prior_fy is None:
            LOGGER.warning(
                "avg_ic: missing strict prior FY for listing_id=%s", listing_id
            )
            return None

        return InvestedCapitalSnapshot(
            money=(latest_fy.money + prior_fy.money) / 2.0,
            as_of=latest_fy.as_of,
        )


@dataclass
class ICMostRecentQuarterMetric(_InvestedCapitalBase):
    """Compute invested capital for the latest quarter (EODHD-oriented)."""

    id: str = "ic_mqr"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_mqr(listing_id, repo)
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
class ICFYMetric(_InvestedCapitalBase):
    """Compute invested capital for the latest FY point (EODHD-oriented)."""

    id: str = "ic_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_fy(listing_id, repo)
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
class AvgICMetric(_InvestedCapitalBase):
    """Compute averaged invested capital from quarterly or FY pairs (EODHD-oriented)."""

    id: str = "avg_ic"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_avg(listing_id, repo)
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
    "InvestedCapitalSnapshot",
    "InvestedCapitalCalculator",
    "ICMostRecentQuarterMetric",
    "ICFYMetric",
    "AvgICMetric",
]
