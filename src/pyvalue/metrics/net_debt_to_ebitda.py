"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    normalize_metric_record,
    resolve_metric_ticker_currency,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
DA_PRIMARY_CONCEPTS = ("DepreciationDepletionAndAmortization",)
DA_FALLBACK_CONCEPTS = ("DepreciationFromCashFlow",)
DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
CASH_CONCEPTS = (
    "CashAndShortTermInvestments",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
)


@dataclass
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _NetDebtResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _CashResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class NetDebtToEBITDAMetric:
    """Compute net debt to TTM EBITDA for EODHD-normalized facts."""

    id: str = "net_debt_to_ebitda"
    required_concepts = tuple(
        EBIT_CONCEPTS
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + DEBT_CONCEPTS
        + CASH_CONCEPTS
    )

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        ttm_ebitda = self._compute_ttm_ebitda(symbol, repo)
        if ttm_ebitda is None:
            LOGGER.warning("net_debt_to_ebitda: missing TTM EBITDA for %s", symbol)
            return None
        if ttm_ebitda.total <= 0:
            LOGGER.warning("net_debt_to_ebitda: non-positive EBITDA for %s", symbol)
            return None

        net_debt = self._compute_net_debt(symbol, repo)
        if net_debt is None:
            LOGGER.warning("net_debt_to_ebitda: missing net debt inputs for %s", symbol)
            return None

        ratio = net_debt.total / ttm_ebitda.total
        as_of = max(ttm_ebitda.as_of, net_debt.as_of)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
            unit_kind="multiple",
        )

    def _compute_ttm_ebitda(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[_TTMResult]:
        ebit_records = self._filter_quarterly(
            repo.facts_for_concept(symbol, EBIT_CONCEPTS[0])
        )
        if len(ebit_records) < 4:
            LOGGER.warning(
                "net_debt_to_ebitda: need 4 quarterly EBIT records for %s", symbol
            )
            return None
        if not is_recent_fact(ebit_records[0]):
            LOGGER.warning(
                "net_debt_to_ebitda: latest EBIT (%s) too old for %s",
                ebit_records[0].end_date,
                symbol,
            )
            return None

        da_primary = self._quarterly_map(
            repo.facts_for_concept(symbol, DA_PRIMARY_CONCEPTS[0])
        )
        da_fallback = self._quarterly_map(
            repo.facts_for_concept(symbol, DA_FALLBACK_CONCEPTS[0])
        )

        total = 0.0
        currency: Optional[str] = None
        for ebit_record in ebit_records[:4]:
            da_record = da_primary.get(ebit_record.end_date) or da_fallback.get(
                ebit_record.end_date
            )
            if da_record is None:
                LOGGER.warning(
                    "net_debt_to_ebitda: missing D&A for quarter %s (%s)",
                    ebit_record.end_date,
                    symbol,
                )
                return None

            ebit_value, ebit_currency = self._normalize_currency(
                ebit_record,
                symbol,
                repo,
                EBIT_CONCEPTS[0],
            )
            da_value, da_currency = self._normalize_currency(
                da_record,
                symbol,
                repo,
                DA_PRIMARY_CONCEPTS[0],
            )
            currency = currency or ebit_currency or da_currency
            total += ebit_value + da_value

        return _TTMResult(
            total=total, as_of=ebit_records[0].end_date, currency=currency
        )

    def _compute_net_debt(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[_NetDebtResult]:
        short_debt = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        if short_debt is None and long_debt is None:
            return None

        cash = self._compute_cash(symbol, repo)
        if cash is None:
            return None

        short_value, short_currency = (
            self._normalize_currency(short_debt, symbol, repo, "ShortTermDebt")
            if short_debt is not None
            else (0.0, None)
        )
        long_value, long_currency = (
            self._normalize_currency(long_debt, symbol, repo, "LongTermDebt")
            if long_debt is not None
            else (0.0, None)
        )
        currency = short_currency or long_currency or cash.currency
        total_debt = short_value + long_value
        net_debt = total_debt - cash.total
        as_of_candidates = [cash.as_of]
        if short_debt is not None:
            as_of_candidates.append(short_debt.end_date)
        if long_debt is not None:
            as_of_candidates.append(long_debt.end_date)
        return _NetDebtResult(
            total=net_debt, as_of=max(as_of_candidates), currency=currency
        )

    def _filter_quarterly(self, records: Sequence[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if (
                period not in QUARTERLY_PERIODS
                or record.end_date in seen_end_dates
                or record.value is None
            ):
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _quarterly_map(self, records: Sequence[FactRecord]) -> dict[str, FactRecord]:
        return {record.end_date: record for record in self._filter_quarterly(records)}

    def _latest_recent_fact(
        self,
        repo: FinancialFactsRepository,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        record = repo.latest_fact(symbol, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _compute_cash(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[_CashResult]:
        primary = self._latest_recent_fact(repo, symbol, "CashAndShortTermInvestments")
        if primary is not None:
            value, currency = self._normalize_currency(
                primary,
                symbol,
                repo,
                "CashAndShortTermInvestments",
            )
            return _CashResult(total=value, as_of=primary.end_date, currency=currency)

        cash_eq = self._latest_recent_fact(repo, symbol, "CashAndCashEquivalents")
        if cash_eq is None:
            return None
        short_term_investments = self._latest_recent_fact(
            repo, symbol, "ShortTermInvestments"
        )

        cash_value, cash_currency = self._normalize_currency(
            cash_eq,
            symbol,
            repo,
            "CashAndCashEquivalents",
        )
        as_of_candidates = [cash_eq.end_date]
        short_term_currency: Optional[str] = None
        short_term_value = 0.0
        if short_term_investments is not None:
            short_term_value, short_term_currency = self._normalize_currency(
                short_term_investments,
                symbol,
                repo,
                "ShortTermInvestments",
            )
            as_of_candidates.append(short_term_investments.end_date)
        return _CashResult(
            total=cash_value + short_term_value,
            as_of=max(as_of_candidates),
            currency=cash_currency or short_term_currency,
        )

    def _normalize_currency(
        self,
        record: FactRecord,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
    ) -> tuple[float, str]:
        return normalize_metric_record(
            record,
            metric_id=self.id,
            symbol=symbol,
            input_name=concept,
            expected_currency=resolve_metric_ticker_currency(
                symbol,
                repo,
                candidate_currencies=[record.currency],
            ),
            contexts=(repo,),
        )


__all__ = ["NetDebtToEBITDAMetric"]
