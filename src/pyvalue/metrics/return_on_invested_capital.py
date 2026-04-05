"""Return on invested capital (ROIC) metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.money import align_money_values, fx_service_for_context
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPTS = ("OperatingIncomeLoss",)
TAX_EXPENSE_CONCEPTS = ("IncomeTaxExpense",)
PRETAX_INCOME_CONCEPTS = ("IncomeBeforeIncomeTaxes",)

DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
EQUITY_CONCEPTS = ("StockholdersEquity",)
CASH_CONCEPTS = ("CashAndShortTermInvestments",)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DEFAULT_TAX_RATE = 0.21


@dataclass
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _CapitalPoint:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class ReturnOnInvestedCapitalMetric:
    """Compute ROIC using TTM EBIT and average invested capital (EODHD-only)."""

    id: str = "return_on_invested_capital"
    required_concepts = tuple(
        EBIT_CONCEPTS
        + TAX_EXPENSE_CONCEPTS
        + PRETAX_INCOME_CONCEPTS
        + DEBT_CONCEPTS
        + EQUITY_CONCEPTS
        + CASH_CONCEPTS
    )

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        fx_service = fx_service_for_context(repo)
        ebit = self._ttm_sum(symbol, repo, EBIT_CONCEPTS, fx_service=fx_service)
        if ebit is None:
            LOGGER.warning("roic: missing TTM EBIT for %s", symbol)
            return None

        tax_rate = self._effective_tax_rate(symbol, repo, fx_service=fx_service)
        nopat = ebit.total * (1.0 - tax_rate)
        if nopat <= 0:
            LOGGER.warning("roic: non-positive NOPAT for %s", symbol)
            return None

        capital_points = self._invested_capital_points(
            symbol, repo, fx_service=fx_service
        )
        if len(capital_points) < 2:
            LOGGER.warning("roic: insufficient invested capital history for %s", symbol)
            return None
        latest = capital_points[0]
        previous = capital_points[1]
        avg_capital = (latest.value + previous.value) / 2.0
        if avg_capital <= 0:
            LOGGER.warning("roic: non-positive invested capital for %s", symbol)
            return None

        aligned, _ = align_money_values(
            values=[
                (nopat, ebit.currency, ebit.as_of, EBIT_CONCEPTS[0]),
                (avg_capital, latest.currency, latest.as_of, "avg_invested_capital"),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:return_on_invested_capital",
            symbol=symbol,
            target_currency=ebit.currency or latest.currency,
        )
        if aligned is None:
            LOGGER.warning("roic: currency mismatch for %s", symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=aligned[0] / aligned[1],
            as_of=max(ebit.as_of, latest.as_of),
            unit_kind="percent",
        )

    def _effective_tax_rate(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        fx_service: FXService,
    ) -> float:
        tax = self._ttm_sum(symbol, repo, TAX_EXPENSE_CONCEPTS, fx_service=fx_service)
        pretax = self._ttm_sum(
            symbol,
            repo,
            PRETAX_INCOME_CONCEPTS,
            fx_service=fx_service,
        )
        if tax is None or pretax is None or pretax.total <= 0:
            return DEFAULT_TAX_RATE
        aligned, _ = align_money_values(
            values=[
                (tax.total, tax.currency, tax.as_of, TAX_EXPENSE_CONCEPTS[0]),
                (
                    pretax.total,
                    pretax.currency,
                    pretax.as_of,
                    PRETAX_INCOME_CONCEPTS[0],
                ),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:return_on_invested_capital:tax_rate",
            symbol=symbol,
            target_currency=tax.currency or pretax.currency,
        )
        if aligned is None or aligned[1] <= 0:
            return DEFAULT_TAX_RATE
        rate = aligned[0] / aligned[1]
        if rate < 0 or rate > 1:
            return DEFAULT_TAX_RATE
        return rate

    def _invested_capital_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        fx_service: FXService,
    ) -> list[_CapitalPoint]:
        points = self._build_capital_points(
            symbol,
            repo,
            QUARTERLY_PERIODS,
            max_age_days=MAX_FACT_AGE_DAYS,
            fx_service=fx_service,
        )
        if len(points) < 2:
            points = self._build_capital_points(
                symbol,
                repo,
                FY_PERIODS,
                max_age_days=MAX_FY_FACT_AGE_DAYS,
                fx_service=fx_service,
            )
        return points

    def _build_capital_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
        *,
        max_age_days: int,
        fx_service: FXService,
    ) -> list[_CapitalPoint]:
        short_map = self._period_map(
            repo.facts_for_concept(symbol, "ShortTermDebt"), periods
        )
        long_map = self._period_map(
            repo.facts_for_concept(symbol, "LongTermDebt"), periods
        )
        equity_map = self._period_map(
            repo.facts_for_concept(symbol, "StockholdersEquity"), periods
        )
        cash_map = self._period_map(
            repo.facts_for_concept(symbol, "CashAndShortTermInvestments"), periods
        )

        common_dates = [
            date_str
            for date_str in short_map
            if date_str in long_map and date_str in equity_map and date_str in cash_map
        ]
        points: list[_CapitalPoint] = []
        for date_str in sorted(common_dates, reverse=True):
            short = short_map[date_str]
            long = long_map[date_str]
            equity = equity_map[date_str]
            cash = cash_map[date_str]
            if not all(
                is_recent_fact(record, max_age_days=max_age_days)
                for record in (short, long, equity, cash)
            ):
                continue
            aligned, currency = align_money_values(
                values=[
                    (short.value, short.currency, short.end_date, "ShortTermDebt"),
                    (long.value, long.currency, long.end_date, "LongTermDebt"),
                    (
                        equity.value,
                        equity.currency,
                        equity.end_date,
                        "StockholdersEquity",
                    ),
                    (
                        cash.value,
                        cash.currency,
                        cash.end_date,
                        "CashAndShortTermInvestments",
                    ),
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation="metric:return_on_invested_capital:capital",
                symbol=symbol,
                target_currency=short.currency
                or long.currency
                or equity.currency
                or cash.currency,
            )
            if aligned is None or currency is None:
                continue
            invested_capital = aligned[0] + aligned[1] + aligned[2] - aligned[3]
            points.append(
                _CapitalPoint(value=invested_capital, as_of=date_str, currency=currency)
            )
        return points

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        fx_service: FXService,
    ) -> Optional[_TTMResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4 or not is_recent_fact(quarterly[0]):
                continue
            normalized, currency = align_money_values(
                values=[
                    (record.value, record.currency, record.end_date, concept)
                    for record in quarterly[:4]
                    if record.value is not None
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation=f"metric:return_on_invested_capital:{concept}",
                symbol=symbol,
                target_currency=quarterly[0].currency,
            )
            if normalized is None:
                continue
            return _TTMResult(
                total=sum(normalized),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _filter_quarterly(self, records: Sequence[FactRecord]) -> list[FactRecord]:
        return self._filter_periods(records, QUARTERLY_PERIODS)

    def _period_map(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> dict[str, FactRecord]:
        ordered = self._filter_periods(records, periods)
        return {record.end_date: record for record in ordered}

    def _filter_periods(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if (
                period not in periods
                or record.end_date in seen_end_dates
                or record.value is None
            ):
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered


__all__ = ["ReturnOnInvestedCapitalMetric"]
