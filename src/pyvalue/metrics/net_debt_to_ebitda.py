"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.money import (
    align_money_values,
    fx_service_for_context,
    normalize_money_value,
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
        fx_service = fx_service_for_context(repo)
        ttm_ebitda = self._compute_ttm_ebitda(symbol, repo, fx_service=fx_service)
        if ttm_ebitda is None:
            LOGGER.warning("net_debt_to_ebitda: missing TTM EBITDA for %s", symbol)
            return None
        if ttm_ebitda.total <= 0:
            LOGGER.warning("net_debt_to_ebitda: non-positive EBITDA for %s", symbol)
            return None

        net_debt = self._compute_net_debt(symbol, repo, fx_service=fx_service)
        if net_debt is None:
            LOGGER.warning("net_debt_to_ebitda: missing net debt inputs for %s", symbol)
            return None

        aligned_pair, _ = align_money_values(
            values=[
                (net_debt.total, net_debt.currency, net_debt.as_of, "NetDebt"),
                (ttm_ebitda.total, ttm_ebitda.currency, ttm_ebitda.as_of, "EBITDA"),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:net_debt_to_ebitda",
            symbol=symbol,
            target_currency=net_debt.currency or ttm_ebitda.currency,
        )
        if aligned_pair is None:
            LOGGER.warning("net_debt_to_ebitda: currency mismatch for %s", symbol)
            return None

        ratio = aligned_pair[0] / aligned_pair[1]
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
        *,
        fx_service: FXService,
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

            ebit_value, ebit_currency = self._normalize_currency(ebit_record)
            da_value, da_currency = self._normalize_currency(da_record)
            aligned, aligned_currency = align_money_values(
                values=[
                    (ebit_value, ebit_currency, ebit_record.end_date, EBIT_CONCEPTS[0]),
                    (da_value, da_currency, da_record.end_date, DA_PRIMARY_CONCEPTS[0]),
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation="metric:net_debt_to_ebitda:quarter",
                symbol=symbol,
                target_currency=currency or ebit_currency or da_currency,
            )
            if aligned is None or aligned_currency is None:
                LOGGER.warning(
                    "net_debt_to_ebitda: currency conflict in EBIT/D&A for %s",
                    symbol,
                )
                return None
            currency = aligned_currency
            total += aligned[0] + aligned[1]

        return _TTMResult(
            total=total, as_of=ebit_records[0].end_date, currency=currency
        )

    def _compute_net_debt(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        fx_service: FXService,
    ) -> Optional[_NetDebtResult]:
        short_debt = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        if short_debt is None and long_debt is None:
            return None

        cash = self._compute_cash(symbol, repo, fx_service=fx_service)
        if cash is None:
            return None

        short_value, short_currency = (
            self._normalize_currency(short_debt)
            if short_debt is not None
            else (0.0, None)
        )
        long_value, long_currency = (
            self._normalize_currency(long_debt)
            if long_debt is not None
            else (0.0, None)
        )
        values: list[tuple[float, Optional[str], str, str]] = []
        if short_debt is not None:
            values.append(
                (
                    short_value,
                    short_currency,
                    short_debt.end_date,
                    "ShortTermDebt",
                )
            )
        if long_debt is not None:
            values.append(
                (
                    long_value,
                    long_currency,
                    long_debt.end_date,
                    "LongTermDebt",
                )
            )
        values.append(
            (cash.total, cash.currency, cash.as_of, "CashAndShortTermInvestments")
        )
        aligned, currency = align_money_values(
            values=values,
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:net_debt_to_ebitda:net_debt",
            symbol=symbol,
            target_currency=short_currency or long_currency or cash.currency,
        )
        if aligned is None or currency is None:
            return None

        cash_index = len(aligned) - 1
        total_debt = sum(aligned[:cash_index])
        net_debt = total_debt - aligned[cash_index]
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
        *,
        fx_service: FXService,
    ) -> Optional[_CashResult]:
        primary = self._latest_recent_fact(repo, symbol, "CashAndShortTermInvestments")
        if primary is not None:
            value, currency = self._normalize_currency(primary)
            return _CashResult(total=value, as_of=primary.end_date, currency=currency)

        cash_eq = self._latest_recent_fact(repo, symbol, "CashAndCashEquivalents")
        if cash_eq is None:
            return None
        short_term_investments = self._latest_recent_fact(
            repo, symbol, "ShortTermInvestments"
        )

        cash_value, cash_currency = self._normalize_currency(cash_eq)
        as_of_candidates = [cash_eq.end_date]
        values: list[tuple[float, Optional[str], str, str]] = [
            (cash_value, cash_currency, cash_eq.end_date, "CashAndCashEquivalents")
        ]
        short_term_currency: Optional[str] = None
        if short_term_investments is not None:
            short_term_value: float
            short_term_value, short_term_currency = self._normalize_currency(
                short_term_investments
            )
            as_of_candidates.append(short_term_investments.end_date)
            values.append(
                (
                    short_term_value,
                    short_term_currency,
                    short_term_investments.end_date,
                    "ShortTermInvestments",
                )
            )

        aligned, currency = align_money_values(
            values=values,
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:net_debt_to_ebitda:cash",
            symbol=symbol,
            target_currency=cash_currency or short_term_currency,
        )
        if aligned is None or currency is None:
            return None
        return _CashResult(
            total=sum(aligned),
            as_of=max(as_of_candidates),
            currency=currency,
        )

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        normalized_value, normalized_currency = normalize_money_value(
            record.value,
            getattr(record, "currency", None),
        )
        return (
            record.value if normalized_value is None else normalized_value,
            normalized_currency,
        )


__all__ = ["NetDebtToEBITDAMetric"]
