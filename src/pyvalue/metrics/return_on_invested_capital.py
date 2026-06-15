"""Return on invested capital (ROIC) metric implementation.

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
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

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
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class _CapitalPoint:
    money: Money
    as_of: str


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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; EBIT and every invested-capital
        # component are aligned to it before any Money arithmetic.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id
        )

        ebit = self._ttm_sum(listing_id, repo, EBIT_CONCEPTS, target_currency)
        if ebit is None:
            LOGGER.warning("roic: missing TTM EBIT for listing_id=%s", listing_id)
            return None

        tax_rate = self._effective_tax_rate(listing_id, repo, target_currency)
        nopat = ebit.money * (1.0 - tax_rate)
        if nopat.amount <= 0:
            LOGGER.warning("roic: non-positive NOPAT for listing_id=%s", listing_id)
            return None

        capital_points = self._invested_capital_points(
            listing_id, repo, target_currency
        )
        if len(capital_points) < 2:
            LOGGER.warning(
                "roic: insufficient invested capital history for listing_id=%s",
                listing_id,
            )
            return None
        latest = capital_points[0]
        previous = capital_points[1]
        avg_capital = (latest.money + previous.money) / 2.0
        if avg_capital.amount <= 0:
            LOGGER.warning(
                "roic: non-positive invested capital for listing_id=%s", listing_id
            )
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=nopat / avg_capital,
            as_of=max(ebit.as_of, latest.as_of),
            unit_kind="percent",
        )

    def _effective_tax_rate(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> float:
        tax = self._ttm_sum(listing_id, repo, TAX_EXPENSE_CONCEPTS, target_currency)
        pretax = self._ttm_sum(
            listing_id, repo, PRETAX_INCOME_CONCEPTS, target_currency
        )
        if tax is None or pretax is None or pretax.money.amount <= 0:
            return DEFAULT_TAX_RATE
        rate = tax.money / pretax.money
        if rate < 0 or rate > 1:
            return DEFAULT_TAX_RATE
        return rate

    def _invested_capital_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> list[_CapitalPoint]:
        points = self._build_capital_points(
            listing_id,
            repo,
            QUARTERLY_PERIODS,
            max_age_days=MAX_FACT_AGE_DAYS,
            target_currency=target_currency,
        )
        if len(points) < 2:
            points = self._build_capital_points(
                listing_id,
                repo,
                FY_PERIODS,
                max_age_days=MAX_FY_FACT_AGE_DAYS,
                target_currency=target_currency,
            )
        return points

    def _build_capital_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
        *,
        max_age_days: int,
        target_currency: str,
    ) -> list[_CapitalPoint]:
        short_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, "ShortTermDebt"), periods
        )
        long_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, "LongTermDebt"), periods
        )
        equity_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, "StockholdersEquity"), periods
        )
        cash_map = self._period_map(
            repo.monetary_facts_for_concept(listing_id, "CashAndShortTermInvestments"),
            periods,
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
            invested_capital = (
                self._money(short, target_currency, listing_id)
                + self._money(long, target_currency, listing_id)
                + self._money(equity, target_currency, listing_id)
                - self._money(cash, target_currency, listing_id)
            )
            points.append(_CapitalPoint(money=invested_capital, as_of=date_str))
        return points

    def _ttm_sum(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        for concept in concepts:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4 or not is_recent_fact(quarterly[0]):
                continue
            monies = [
                self._money(record, target_currency, listing_id)
                for record in quarterly[:4]
            ]
            return _MoneyResult(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _filter_quarterly(self, records: Sequence[MonetaryFact]) -> list[MonetaryFact]:
        return self._filter_periods(records, QUARTERLY_PERIODS)

    def _period_map(
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> dict[str, MonetaryFact]:
        ordered = self._filter_periods(records, periods)
        return {record.end_date: record for record in ordered}

    def _filter_periods(
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods or record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _money(
        self, fact: MonetaryFact, target_currency: str, listing_id: int
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )


__all__ = ["ReturnOnInvestedCapitalMetric"]
