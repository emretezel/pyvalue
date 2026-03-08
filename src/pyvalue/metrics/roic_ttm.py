"""ROIC TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.invested_capital import (
    REQUIRED_CONCEPTS as INVESTED_CAPITAL_REQUIRED_CONCEPTS,
    InvestedCapitalCalculator,
)
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
TAX_EXPENSE_CONCEPT = "IncomeTaxExpense"
PRETAX_INCOME_CONCEPT = "IncomeBeforeIncomeTaxes"

EBIT_CONCEPTS = (EBIT_CONCEPT,)
TAX_EXPENSE_CONCEPTS = (TAX_EXPENSE_CONCEPT,)
PRETAX_INCOME_CONCEPTS = (PRETAX_INCOME_CONCEPT,)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
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
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


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
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        ebit = self._compute_ttm_amount(symbol, repo, EBIT_CONCEPTS, context=self.id)
        if ebit is None:
            LOGGER.warning("roic_ttm: missing TTM EBIT for %s", symbol)
            return None

        tax_rate = self._effective_tax_rate(symbol, repo)
        numerator_as_of = ebit.as_of
        if tax_rate.as_of is not None:
            numerator_as_of = max(numerator_as_of, tax_rate.as_of)

        nopat = ebit.total * (1.0 - tax_rate.rate)
        if nopat <= 0:
            LOGGER.warning("roic_ttm: non-positive NOPAT for %s", symbol)
            return None

        avg_ic = InvestedCapitalCalculator().compute_avg(symbol, repo)
        if avg_ic is None:
            LOGGER.warning("roic_ttm: missing avg_ic for %s", symbol)
            return None
        if avg_ic.value <= 0:
            LOGGER.warning("roic_ttm: non-positive avg_ic for %s", symbol)
            return None

        if not self._currencies_match(ebit.currency, avg_ic.currency):
            LOGGER.warning(
                "roic_ttm: numerator/denominator currency mismatch for %s", symbol
            )
            return None

        ratio = nopat / avg_ic.value
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=max(numerator_as_of, avg_ic.as_of),
        )

    def _effective_tax_rate(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> _TaxRateResult:
        ttm_tax = self._compute_ttm_amount(
            symbol, repo, TAX_EXPENSE_CONCEPTS, context=self.id
        )
        ttm_pretax = self._compute_ttm_amount(
            symbol, repo, PRETAX_INCOME_CONCEPTS, context=self.id
        )
        ttm_rate = self._rate_from_amounts(
            ttm_tax, ttm_pretax, symbol=symbol, period_label="TTM"
        )
        if ttm_rate is not None:
            return ttm_rate

        fy_rate = self._latest_valid_fy_tax_rate(symbol, repo)
        if fy_rate is not None:
            return fy_rate

        LOGGER.warning("roic_ttm: using default tax rate for %s", symbol)
        return _TaxRateResult(rate=DEFAULT_TAX_RATE, as_of=None)

    def _latest_valid_fy_tax_rate(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_TaxRateResult]:
        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT)
        for end_date in sorted(set(tax_map).intersection(pretax_map), reverse=True):
            rate = self._rate_from_amounts(
                tax_map[end_date],
                pretax_map[end_date],
                symbol=symbol,
                period_label=end_date,
            )
            if rate is not None:
                return rate
        return None

    def _rate_from_amounts(
        self,
        tax: Optional[_AmountResult],
        pretax: Optional[_AmountResult],
        *,
        symbol: str,
        period_label: str,
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if not self._currencies_match(tax.currency, pretax.currency):
            LOGGER.warning(
                "roic_ttm: tax currency mismatch for %s on %s", symbol, period_label
            )
            return None
        if pretax.total <= PRETAX_MIN_ABS:
            return None
        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(rate=rate, as_of=max(tax.as_of, pretax.as_of))

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
    ) -> Optional[_AmountResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                continue
            normalized_values, currency = self._normalize_records(quarterly[:4])
            if normalized_values is None:
                LOGGER.warning(
                    "%s: currency conflict in %s quarterly values for %s",
                    context,
                    concept,
                    symbol,
                )
                continue
            return _AmountResult(
                total=sum(normalized_values),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
    ) -> dict[str, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[str, _AmountResult] = {}
        for record in ordered:
            value, currency = self._normalize_currency(record)
            mapped[record.end_date] = _AmountResult(
                total=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

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

    def _normalize_records(
        self, records: Sequence[FactRecord]
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency = None
        normalized: list[float] = []
        for record in records:
            value, code = self._normalize_currency(record)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


__all__ = ["RoicTTMMetric"]
