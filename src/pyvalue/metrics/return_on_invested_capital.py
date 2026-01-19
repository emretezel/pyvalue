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

        if not self._currencies_match(ebit.currency, latest.currency):
            LOGGER.warning("roic: currency mismatch for %s", symbol)
            return None

        ratio = nopat / avg_capital
        as_of = max(ebit.as_of, latest.as_of)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)

    def _effective_tax_rate(self, symbol: str, repo: FinancialFactsRepository) -> float:
        tax = self._ttm_sum(symbol, repo, TAX_EXPENSE_CONCEPTS)
        pretax = self._ttm_sum(symbol, repo, PRETAX_INCOME_CONCEPTS)
        if tax is None or pretax is None:
            return DEFAULT_TAX_RATE
        if pretax.total <= 0:
            return DEFAULT_TAX_RATE
        if not self._currencies_match(tax.currency, pretax.currency):
            return DEFAULT_TAX_RATE
        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return DEFAULT_TAX_RATE
        return rate

    def _invested_capital_points(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> list[_CapitalPoint]:
        points = self._build_capital_points(
            symbol, repo, QUARTERLY_PERIODS, max_age_days=MAX_FACT_AGE_DAYS
        )
        if len(points) < 2:
            # Fall back to FY balance sheet values when quarterly points are sparse.
            points = self._build_capital_points(
                symbol, repo, FY_PERIODS, max_age_days=MAX_FY_FACT_AGE_DAYS
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
            currency = self._merge_currency(
                [short.currency, long.currency, equity.currency, cash.currency]
            )
            if currency is None and any(
                code is not None
                for code in (
                    short.currency,
                    long.currency,
                    equity.currency,
                    cash.currency,
                )
            ):
                continue
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
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0]):
                continue
            normalized, currency = self._normalize_quarterly(quarterly[:4])
            if normalized is None:
                continue
            total = sum(record.value for record in normalized)
            return _TTMResult(
                total=total, as_of=normalized[0].end_date, currency=currency or None
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
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_quarterly(
        self, records: Sequence[FactRecord]
    ) -> tuple[Optional[list[FactRecord]], Optional[str]]:
        currency = None
        normalized: list[FactRecord] = []
        for record in records:
            value, code = self._normalize_currency(record)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(
                FactRecord(
                    symbol=record.symbol,
                    cik=record.cik,
                    concept=record.concept,
                    fiscal_period=record.fiscal_period,
                    end_date=record.end_date,
                    unit=record.unit,
                    value=value,
                    accn=record.accn,
                    filed=record.filed,
                    frame=record.frame,
                    start_date=getattr(record, "start_date", None),
                    accounting_standard=getattr(record, "accounting_standard", None),
                    currency=code,
                )
            )
        return normalized, currency

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _merge_currency(self, codes: Sequence[Optional[str]]) -> Optional[str]:
        currency = None
        for code in codes:
            if not code:
                continue
            if currency is None:
                currency = code
            elif code != currency:
                return None
        return currency

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


__all__ = ["ReturnOnInvestedCapitalMetric"]
