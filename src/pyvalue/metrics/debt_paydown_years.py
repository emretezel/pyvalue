"""Debt paydown and FCF-to-debt metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

OPERATING_CASH_FLOW_CONCEPTS = ("NetCashProvidedByUsedInOperatingActivities",)
CAPEX_CONCEPTS = ("CapitalExpenditures",)
DEBT_COMPONENT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
TOTAL_DEBT_FALLBACK_CONCEPTS = ("TotalDebtFromBalanceSheet",)
REQUIRED_CONCEPTS = tuple(
    OPERATING_CASH_FLOW_CONCEPTS
    + CAPEX_CONCEPTS
    + DEBT_COMPONENT_CONCEPTS
    + TOTAL_DEBT_FALLBACK_CONCEPTS
)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class _FCFDebtInputs:
    fcf: _MoneyResult
    debt: _MoneyResult
    as_of: str


class _FCFDebtCalculator:
    def compute_inputs(
        self, listing_id: int, repo: RegionFactsRepository, *, metric_id: str
    ) -> Optional[_FCFDebtInputs]:
        # Resolve the listing currency once; debt and FCF are both aligned to it,
        # so the debt/FCF (and FCF/debt) ratios are currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=metric_id
        )

        debt = self._compute_total_debt(
            listing_id, repo, metric_id=metric_id, target_currency=target_currency
        )
        if debt is None:
            LOGGER.warning(
                "%s: missing debt inputs for listing_id=%s", metric_id, listing_id
            )
            return None
        if debt.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive debt for listing_id=%s", metric_id, listing_id
            )
            return None

        fcf = self._compute_ttm_fcf(
            listing_id, repo, metric_id=metric_id, target_currency=target_currency
        )
        if fcf is None:
            LOGGER.warning(
                "%s: missing TTM FCF for listing_id=%s", metric_id, listing_id
            )
            return None
        if fcf.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive FCF for listing_id=%s", metric_id, listing_id
            )
            return None

        return _FCFDebtInputs(
            fcf=fcf,
            debt=debt,
            as_of=max(debt.as_of, fcf.as_of),
        )

    def _compute_total_debt(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        short_debt = self._latest_recent_fact(repo, listing_id, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, listing_id, "LongTermDebt")
        total_debt = self._latest_recent_fact(
            repo, listing_id, "TotalDebtFromBalanceSheet"
        )

        if short_debt is not None and long_debt is not None:
            short_money = self._money(
                short_debt, "ShortTermDebt", target_currency, listing_id, metric_id
            )
            long_money = self._money(
                long_debt, "LongTermDebt", target_currency, listing_id, metric_id
            )
            return _MoneyResult(
                money=short_money + long_money,
                as_of=max(short_debt.end_date, long_debt.end_date),
            )

        if total_debt is not None:
            return _MoneyResult(
                money=self._money(
                    total_debt, "TotalDebt", target_currency, listing_id, metric_id
                ),
                as_of=total_debt.end_date,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            return None
        concept = "ShortTermDebt" if short_debt is not None else "LongTermDebt"
        return _MoneyResult(
            money=self._money(
                one_side, concept, target_currency, listing_id, metric_id
            ),
            as_of=one_side.end_date,
        )

    def _compute_ttm_fcf(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        operating = self._ttm_sum(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            metric_id=metric_id,
            target_currency=target_currency,
        )
        if operating is None:
            return None

        capex = self._ttm_sum(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            metric_id=metric_id,
            target_currency=target_currency,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for listing_id=%s; assuming zero",
                metric_id,
                listing_id,
            )
            return operating

        return _MoneyResult(
            money=operating.money - capex.money,
            as_of=max(operating.as_of, capex.as_of),
        )

    def _ttm_sum(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        for concept in concepts:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "%s: need 4 quarterly %s records for listing_id=%s, found %s",
                    metric_id,
                    concept,
                    listing_id,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0]):
                LOGGER.warning(
                    "%s: latest %s (%s) too old for listing_id=%s",
                    metric_id,
                    concept,
                    quarterly[0].end_date,
                    listing_id,
                )
                continue

            monies = [
                self._money(record, concept, target_currency, listing_id, metric_id)
                for record in quarterly[:4]
            ]
            return _MoneyResult(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _filter_quarterly(self, records: Sequence[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _latest_recent_fact(
        self, repo: RegionFactsRepository, listing_id: int, concept: str
    ) -> Optional[MonetaryFact]:
        record = repo.latest_monetary_fact(listing_id, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _money(
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        listing_id: int,
        metric_id: str,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=concept,
            as_of=fact.end_date,
        )


@dataclass
class DebtPaydownYearsMetric:
    """Compute total debt divided by TTM free cash flow (EODHD-only)."""

    id: str = "debt_paydown_years"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(
            listing_id, repo, metric_id=self.id
        )
        if inputs is None:
            return None
        ratio = inputs.debt.money / inputs.fcf.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="multiple",
        )


@dataclass
class FCFToDebtMetric:
    """Compute TTM free cash flow divided by total debt (EODHD-only)."""

    id: str = "fcf_to_debt"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(
            listing_id, repo, metric_id=self.id
        )
        if inputs is None:
            return None
        ratio = inputs.fcf.money / inputs.debt.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="ratio",
        )


__all__ = ["DebtPaydownYearsMetric", "FCFToDebtMetric"]
