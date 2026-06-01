"""Invested capital base metrics implementation.

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
        symbol: str,
        repo: RegionFactsRepository,
        periods: set[str],
    ) -> list[_ICPoint]:
        # Resolve the listing currency once; every component is aligned to it, so
        # invested capital (debt + equity - cash) is single-currency by build.
        target_currency = require_metric_ticker_currency(
            symbol, repo, metric_id=_INVESTED_CAPITAL_METRIC_ID
        )
        short_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, SHORT_TERM_DEBT_CONCEPT), periods
        )
        long_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, LONG_TERM_DEBT_CONCEPT), periods
        )
        total_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, TOTAL_DEBT_CONCEPT), periods
        )
        equity_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, EQUITY_PRIMARY_CONCEPT), periods
        )
        common_equity_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, EQUITY_FALLBACK_CONCEPT), periods
        )
        cash_primary_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, CASH_PRIMARY_CONCEPT), periods
        )
        cash_fallback_map = self._period_map(
            repo.monetary_facts_for_concept(symbol, CASH_FALLBACK_CONCEPT), periods
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
                symbol=symbol,
                target_currency=target_currency,
                key=key,
                short_debt=short_map.get(key),
                long_debt=long_map.get(key),
                total_debt=total_map.get(key),
            )
            if debt is None:
                continue

            equity = self._resolve_equity(
                symbol=symbol,
                target_currency=target_currency,
                key=key,
                primary=equity_map.get(key),
                fallback=common_equity_map.get(key),
            )
            if equity is None:
                continue

            cash = self._resolve_cash(
                symbol=symbol,
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
        symbol: str,
        target_currency: str,
        key: tuple[str, str],
        short_debt: Optional[MonetaryFact],
        long_debt: Optional[MonetaryFact],
        total_debt: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        if short_debt is not None and long_debt is not None:
            return _Amount(
                money=self._money(short_debt, target_currency, symbol)
                + self._money(long_debt, target_currency, symbol),
                as_of=max(short_debt.end_date, long_debt.end_date),
            )

        if total_debt is not None:
            return _Amount(
                money=self._money(total_debt, target_currency, symbol),
                as_of=total_debt.end_date,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            LOGGER.warning(
                "invested_capital: missing debt inputs for %s on %s/%s",
                symbol,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(one_side, target_currency, symbol),
            as_of=one_side.end_date,
        )

    def _resolve_equity(
        self,
        *,
        symbol: str,
        target_currency: str,
        key: tuple[str, str],
        primary: Optional[MonetaryFact],
        fallback: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        record = primary or fallback
        if record is None:
            LOGGER.warning(
                "invested_capital: missing equity for %s on %s/%s",
                symbol,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(record, target_currency, symbol),
            as_of=record.end_date,
        )

    def _resolve_cash(
        self,
        *,
        symbol: str,
        target_currency: str,
        key: tuple[str, str],
        primary: Optional[MonetaryFact],
        fallback: Optional[MonetaryFact],
    ) -> Optional[_Amount]:
        record = primary or fallback
        if record is None:
            LOGGER.warning(
                "invested_capital: missing cash for %s on %s/%s",
                symbol,
                key[0],
                key[1],
            )
            return None
        return _Amount(
            money=self._money(record, target_currency, symbol),
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

    def _money(self, fact: MonetaryFact, target_currency: str, symbol: str) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=_INVESTED_CAPITAL_METRIC_ID,
            symbol=symbol,
            input_name=fact.concept,
            as_of=fact.end_date,
        )

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))

    def _select_latest_point(
        self,
        points: Sequence[_ICPoint],
        *,
        max_age_days: int,
        context: str,
        symbol: str,
    ) -> Optional[_ICPoint]:
        if not points:
            LOGGER.warning(
                "%s: missing invested capital points for %s", context, symbol
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest point (%s) too old for %s",
                context,
                latest.as_of,
                symbol,
            )
            return None
        return latest

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        year = value[:4]
        if not year.isdigit():
            return None
        return int(year)


class InvestedCapitalCalculator(_InvestedCapitalBase):
    """Shared calculator for invested-capital snapshots."""

    def compute_mqr(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(symbol, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FACT_AGE_DAYS, context="ic_mqr", symbol=symbol
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(money=latest.money, as_of=latest.as_of)

    def compute_fy(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(symbol, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FY_FACT_AGE_DAYS, context="ic_fy", symbol=symbol
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(money=latest.money, as_of=latest.as_of)

    def compute_fy_series(
        self, symbol: str, repo: RegionFactsRepository
    ) -> list[InvestedCapitalSnapshot]:
        """Return FY invested-capital points (latest first) without freshness gating."""

        points = self._build_points(symbol, repo, FY_PERIODS)
        return [
            InvestedCapitalSnapshot(money=point.money, as_of=point.as_of)
            for point in points
        ]

    def compute_avg(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        quarterly_points = self._build_points(symbol, repo, QUARTERLY_PERIODS)
        latest_quarter = self._select_latest_point(
            quarterly_points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="avg_ic",
            symbol=symbol,
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

        fy_points = self._build_points(symbol, repo, FY_PERIODS)
        latest_fy = self._select_latest_point(
            fy_points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="avg_ic",
            symbol=symbol,
        )
        if latest_fy is None:
            return None

        latest_year = self._extract_year(latest_fy.as_of)
        if latest_year is None:
            LOGGER.warning("avg_ic: invalid latest FY date for %s", symbol)
            return None

        prior_fy: Optional[_ICPoint] = None
        for point in fy_points[1:]:
            point_year = self._extract_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                prior_fy = point
                break

        if prior_fy is None:
            LOGGER.warning("avg_ic: missing strict prior FY for %s", symbol)
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
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_mqr(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
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
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_fy(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
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
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_avg(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
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
