"""ROIC TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.invested_capital import (
    REQUIRED_CONCEPTS as INVESTED_CAPITAL_REQUIRED_CONCEPTS,
    InvestedCapitalCalculator,
)
from pyvalue.metrics.ttm import resolve_ttm_window
from pyvalue.metrics.utils import (
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
TAX_EXPENSE_CONCEPT = "IncomeTaxExpense"
PRETAX_INCOME_CONCEPT = "IncomeBeforeIncomeTaxes"

EBIT_CONCEPTS = (EBIT_CONCEPT,)
TAX_EXPENSE_CONCEPTS = (TAX_EXPENSE_CONCEPT,)
PRETAX_INCOME_CONCEPTS = (PRETAX_INCOME_CONCEPT,)

FY_PERIODS = {"FY"}
DEFAULT_TAX_RATE = 0.21
PRETAX_MIN_ABS = 1.0

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        EBIT_CONCEPTS
        + TAX_EXPENSE_CONCEPTS
        + PRETAX_INCOME_CONCEPTS
        + INVESTED_CAPITAL_REQUIRED_CONCEPTS
    )
)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class _TaxRateResult:
    rate: float
    as_of: Optional[str]


@dataclass
class RoicTTMMetric:
    """Compute ROIC using TTM NOPAT over AvgIC (EODHD-oriented)."""

    id: str = "roic_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id
        )

        ebit = self._compute_ttm_amount(
            listing_id,
            repo,
            EBIT_CONCEPTS,
            context=self.id,
            target_currency=target_currency,
        )
        if ebit is None:
            LOGGER.warning("roic_ttm: missing TTM EBIT for listing_id=%s", listing_id)
            return None

        tax_rate = self._effective_tax_rate(listing_id, repo, target_currency)
        numerator_as_of = ebit.as_of
        if tax_rate.as_of is not None:
            numerator_as_of = max(numerator_as_of, tax_rate.as_of)

        nopat = ebit.money * (1.0 - tax_rate.rate)
        if nopat.amount <= 0:
            LOGGER.warning("roic_ttm: non-positive NOPAT for listing_id=%s", listing_id)
            return None

        avg_ic = InvestedCapitalCalculator().compute_avg(listing_id, repo)
        if avg_ic is None:
            LOGGER.warning("roic_ttm: missing avg_ic for listing_id=%s", listing_id)
            return None
        if avg_ic.money.amount <= 0:
            LOGGER.warning(
                "roic_ttm: non-positive avg_ic for listing_id=%s", listing_id
            )
            return None

        ratio = nopat / avg_ic.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=max(numerator_as_of, avg_ic.as_of),
            unit_kind="percent",
        )

    def _effective_tax_rate(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> _TaxRateResult:
        ttm_tax = self._compute_ttm_amount(
            listing_id,
            repo,
            TAX_EXPENSE_CONCEPTS,
            context=self.id,
            target_currency=target_currency,
        )
        ttm_pretax = self._compute_ttm_amount(
            listing_id,
            repo,
            PRETAX_INCOME_CONCEPTS,
            context=self.id,
            target_currency=target_currency,
        )
        ttm_rate = self._rate_from_amounts(
            ttm_tax,
            ttm_pretax,
            listing_id=listing_id,
            period_label="TTM",
        )
        if ttm_rate is not None:
            return ttm_rate

        fy_rate = self._latest_valid_fy_tax_rate(listing_id, repo, target_currency)
        if fy_rate is not None:
            return fy_rate

        LOGGER.warning("roic_ttm: using default tax rate for listing_id=%s", listing_id)
        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _latest_valid_fy_tax_rate(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> Optional[_TaxRateResult]:
        tax_map = self._fy_map(listing_id, repo, TAX_EXPENSE_CONCEPT, target_currency)
        pretax_map = self._fy_map(
            listing_id, repo, PRETAX_INCOME_CONCEPT, target_currency
        )
        for end_date in sorted(set(tax_map).intersection(pretax_map), reverse=True):
            rate = self._rate_from_amounts(
                tax_map[end_date],
                pretax_map[end_date],
                listing_id=listing_id,
                period_label=end_date,
            )
            if rate is not None:
                return rate
        return None

    def _rate_from_amounts(
        self,
        tax: Optional[_MoneyResult],
        pretax: Optional[_MoneyResult],
        *,
        listing_id: int,
        period_label: str,
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if pretax.money.amount <= PRETAX_MIN_ABS:
            return None
        rate = tax.money / pretax.money
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(rate=rate, as_of=max(tax.as_of, pretax.as_of))

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        # EBIT, tax and pretax each resolve their own window independently, as
        # before the refactor: the tax-rate guards (sign/bounds) already reject
        # a rate built from mismatched windows.
        for concept in concepts:
            resolution = resolve_ttm_window(
                repo.monetary_facts_for_concept(listing_id, concept)
            )
            window = resolution.window
            if window is None:
                LOGGER.warning(
                    "%s: %s (concept=%s, listing_id=%s)",
                    context,
                    resolution.failure,
                    concept,
                    listing_id,
                )
                continue
            monies = [
                self._money(record, concept, target_currency, listing_id, context)
                for record in window.records
            ]
            return _MoneyResult(money=sum_money(monies), as_of=window.as_of)
        return None

    def _fy_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        target_currency: str,
    ) -> dict[str, _MoneyResult]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[str, _MoneyResult] = {}
        for record in ordered:
            mapped[record.end_date] = _MoneyResult(
                money=self._money(
                    record, concept, target_currency, listing_id, self.id
                ),
                as_of=record.end_date,
            )
        return mapped

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
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        listing_id: int,
        context: str,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name=concept,
            as_of=fact.end_date,
        )


__all__ = ["RoicTTMMetric"]
