"""Return on invested capital (ROIC) metric implementation.

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
    normalize_metric_record,
    require_metric_ticker_currency,
)
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
        ebit = self._ttm_sum(symbol, repo, EBIT_CONCEPTS)
        if ebit is None:
            LOGGER.warning("roic: missing TTM EBIT for %s", symbol)
            return None

        tax_rate = self._effective_tax_rate(symbol, repo)
        nopat = ebit.total * (1.0 - tax_rate)
        if nopat <= 0:
            LOGGER.warning("roic: non-positive NOPAT for %s", symbol)
            return None

        capital_points = self._invested_capital_points(symbol, repo)
        if len(capital_points) < 2:
            LOGGER.warning("roic: insufficient invested capital history for %s", symbol)
            return None
        latest = capital_points[0]
        previous = capital_points[1]
        avg_capital = (latest.value + previous.value) / 2.0
        if avg_capital <= 0:
            LOGGER.warning("roic: non-positive invested capital for %s", symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=nopat / avg_capital,
            as_of=max(ebit.as_of, latest.as_of),
            unit_kind="percent",
        )

    def _effective_tax_rate(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> float:
        tax = self._ttm_sum(symbol, repo, TAX_EXPENSE_CONCEPTS)
        pretax = self._ttm_sum(symbol, repo, PRETAX_INCOME_CONCEPTS)
        if tax is None or pretax is None or pretax.total <= 0:
            return DEFAULT_TAX_RATE
        if pretax.total <= 0:
            return DEFAULT_TAX_RATE
        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return DEFAULT_TAX_RATE
        return rate

    def _invested_capital_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> list[_CapitalPoint]:
        points = self._build_capital_points(
            symbol,
            repo,
            QUARTERLY_PERIODS,
            max_age_days=MAX_FACT_AGE_DAYS,
        )
        if len(points) < 2:
            points = self._build_capital_points(
                symbol,
                repo,
                FY_PERIODS,
                max_age_days=MAX_FY_FACT_AGE_DAYS,
            )
        return points

    def _build_capital_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
        *,
        max_age_days: int,
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
            currency = (
                short.currency or long.currency or equity.currency or cash.currency
            )
            invested_capital = short.value + long.value + equity.value - cash.value
            points.append(
                _CapitalPoint(value=invested_capital, as_of=date_str, currency=currency)
            )
        return points

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
    ) -> Optional[_TTMResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4 or not is_recent_fact(quarterly[0]):
                continue
            target_currency = require_metric_ticker_currency(
                symbol,
                repo,
                metric_id=self.id,
                input_name=concept,
                as_of=quarterly[0].end_date if quarterly else None,
                candidate_currencies=[record.currency for record in quarterly[:4]],
            )
            normalized: list[float] = []
            for record in quarterly[:4]:
                value, _ = normalize_metric_record(
                    record,
                    metric_id=self.id,
                    symbol=symbol,
                    input_name=concept,
                    expected_currency=target_currency,
                    contexts=(repo,),
                )
                normalized.append(value)
            return _TTMResult(
                total=sum(normalized),
                as_of=quarterly[0].end_date,
                currency=target_currency,
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
