"""Invested capital base metrics implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, MAX_FY_FACT_AGE_DAYS
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

SHORT_TERM_DEBT_CONCEPT = "ShortTermDebt"
LONG_TERM_DEBT_CONCEPT = "LongTermDebt"
TOTAL_DEBT_CONCEPT = "TotalDebtFromBalanceSheet"
EQUITY_PRIMARY_CONCEPT = "StockholdersEquity"
EQUITY_FALLBACK_CONCEPT = "CommonStockholdersEquity"
CASH_PRIMARY_CONCEPT = "CashAndCashEquivalents"
CASH_FALLBACK_CONCEPT = "CashAndShortTermInvestments"

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
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _ICPoint:
    value: float
    as_of: str
    fiscal_period: str
    currency: Optional[str]


@dataclass(frozen=True)
class InvestedCapitalSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


class _InvestedCapitalBase:
    def _build_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
    ) -> list[_ICPoint]:
        short_map = self._period_map(
            repo.facts_for_concept(symbol, SHORT_TERM_DEBT_CONCEPT), periods
        )
        long_map = self._period_map(
            repo.facts_for_concept(symbol, LONG_TERM_DEBT_CONCEPT), periods
        )
        total_map = self._period_map(
            repo.facts_for_concept(symbol, TOTAL_DEBT_CONCEPT), periods
        )
        equity_map = self._period_map(
            repo.facts_for_concept(symbol, EQUITY_PRIMARY_CONCEPT), periods
        )
        common_equity_map = self._period_map(
            repo.facts_for_concept(symbol, EQUITY_FALLBACK_CONCEPT), periods
        )
        cash_primary_map = self._period_map(
            repo.facts_for_concept(symbol, CASH_PRIMARY_CONCEPT), periods
        )
        cash_fallback_map = self._period_map(
            repo.facts_for_concept(symbol, CASH_FALLBACK_CONCEPT), periods
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
                key=key,
                short_debt=short_map.get(key),
                long_debt=long_map.get(key),
                total_debt=total_map.get(key),
            )
            if debt is None:
                continue

            equity = self._resolve_equity(
                symbol=symbol,
                key=key,
                primary=equity_map.get(key),
                fallback=common_equity_map.get(key),
            )
            if equity is None:
                continue

            cash = self._resolve_cash(
                symbol=symbol,
                key=key,
                primary=cash_primary_map.get(key),
                fallback=cash_fallback_map.get(key),
            )
            if cash is None:
                continue

            currency = self._merge_currency(
                [debt.currency, equity.currency, cash.currency]
            )
            if currency is None and any(
                code is not None
                for code in (debt.currency, equity.currency, cash.currency)
            ):
                LOGGER.warning(
                    "invested_capital: currency mismatch for %s on %s/%s",
                    symbol,
                    key[0],
                    key[1],
                )
                continue

            points.append(
                _ICPoint(
                    value=debt.value + equity.value - cash.value,
                    as_of=max(debt.as_of, equity.as_of, cash.as_of),
                    fiscal_period=key[1],
                    currency=currency,
                )
            )
        return points

    def _resolve_debt(
        self,
        *,
        symbol: str,
        key: tuple[str, str],
        short_debt: Optional[FactRecord],
        long_debt: Optional[FactRecord],
        total_debt: Optional[FactRecord],
    ) -> Optional[_Amount]:
        if short_debt is not None and long_debt is not None:
            short_value, short_currency = self._normalize_currency(short_debt)
            long_value, long_currency = self._normalize_currency(long_debt)
            currency = self._merge_currency([short_currency, long_currency])
            if currency is None and any(
                code is not None for code in (short_currency, long_currency)
            ):
                LOGGER.warning(
                    "invested_capital: debt currency mismatch for %s on %s/%s",
                    symbol,
                    key[0],
                    key[1],
                )
                return None
            return _Amount(
                value=short_value + long_value,
                as_of=max(short_debt.end_date, long_debt.end_date),
                currency=currency,
            )

        if total_debt is not None:
            total_value, total_currency = self._normalize_currency(total_debt)
            return _Amount(
                value=total_value,
                as_of=total_debt.end_date,
                currency=total_currency,
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

        value, currency = self._normalize_currency(one_side)
        return _Amount(value=value, as_of=one_side.end_date, currency=currency)

    def _resolve_equity(
        self,
        *,
        symbol: str,
        key: tuple[str, str],
        primary: Optional[FactRecord],
        fallback: Optional[FactRecord],
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
        value, currency = self._normalize_currency(record)
        return _Amount(value=value, as_of=record.end_date, currency=currency)

    def _resolve_cash(
        self,
        *,
        symbol: str,
        key: tuple[str, str],
        primary: Optional[FactRecord],
        fallback: Optional[FactRecord],
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
        value, currency = self._normalize_currency(record)
        return _Amount(value=value, as_of=record.end_date, currency=currency)

    def _period_map(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> dict[tuple[str, str], FactRecord]:
        mapped: dict[tuple[str, str], FactRecord] = {}
        for record in sorted(records, key=lambda item: item.end_date, reverse=True):
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.value is None:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _merge_currency(self, codes: Sequence[Optional[str]]) -> Optional[str]:
        merged = None
        for code in codes:
            if not code:
                continue
            if merged is None:
                merged = code
            elif merged != code:
                return None
        return merged

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

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


class InvestedCapitalCalculator(_InvestedCapitalBase):
    """Shared calculator for invested-capital snapshots."""

    def compute_mqr(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(symbol, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FACT_AGE_DAYS, context="ic_mqr", symbol=symbol
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(
            value=latest.value, as_of=latest.as_of, currency=latest.currency
        )

    def compute_fy(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[InvestedCapitalSnapshot]:
        points = self._build_points(symbol, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FY_FACT_AGE_DAYS, context="ic_fy", symbol=symbol
        )
        if latest is None:
            return None
        return InvestedCapitalSnapshot(
            value=latest.value, as_of=latest.as_of, currency=latest.currency
        )

    def compute_fy_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> list[InvestedCapitalSnapshot]:
        """Return FY invested-capital points (latest first) without freshness gating."""

        points = self._build_points(symbol, repo, FY_PERIODS)
        return [
            InvestedCapitalSnapshot(
                value=point.value,
                as_of=point.as_of,
                currency=point.currency,
            )
            for point in points
        ]

    def compute_avg(
        self, symbol: str, repo: FinancialFactsRepository
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
                        if not self._currencies_match(
                            latest_quarter.currency, point.currency
                        ):
                            LOGGER.warning(
                                "avg_ic: quarterly currency mismatch for %s", symbol
                            )
                            break
                        value = (latest_quarter.value + point.value) / 2.0
                        return InvestedCapitalSnapshot(
                            value=value,
                            as_of=latest_quarter.as_of,
                            currency=latest_quarter.currency or point.currency,
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

        if not self._currencies_match(latest_fy.currency, prior_fy.currency):
            LOGGER.warning("avg_ic: FY currency mismatch for %s", symbol)
            return None

        value = (latest_fy.value + prior_fy.value) / 2.0
        return InvestedCapitalSnapshot(
            value=value,
            as_of=latest_fy.as_of,
            currency=latest_fy.currency or prior_fy.currency,
        )


@dataclass
class ICMostRecentQuarterMetric(_InvestedCapitalBase):
    """Compute invested capital for the latest quarter (EODHD-oriented)."""

    id: str = "ic_mqr"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_mqr(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            currency=snapshot.currency,
        )


@dataclass
class ICFYMetric(_InvestedCapitalBase):
    """Compute invested capital for the latest FY point (EODHD-oriented)."""

    id: str = "ic_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_fy(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            currency=snapshot.currency,
        )


@dataclass
class AvgICMetric(_InvestedCapitalBase):
    """Compute averaged invested capital from quarterly or FY pairs (EODHD-oriented)."""

    id: str = "avg_ic"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = InvestedCapitalCalculator().compute_avg(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            currency=snapshot.currency,
        )


__all__ = [
    "InvestedCapitalSnapshot",
    "InvestedCapitalCalculator",
    "ICMostRecentQuarterMetric",
    "ICFYMetric",
    "AvgICMetric",
]
