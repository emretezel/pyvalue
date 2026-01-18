"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
EBITDA_CONCEPTS = ("EBITDA",)
DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
CASH_CONCEPTS = ("CashAndShortTermInvestments",)


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
class NetDebtToEBITDAMetric:
    """Compute net debt to TTM EBITDA for EODHD-normalized facts."""

    id: str = "net_debt_to_ebitda"
    required_concepts = tuple(EBITDA_CONCEPTS + DEBT_CONCEPTS + CASH_CONCEPTS)

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

        if not self._currencies_match(ttm_ebitda.currency, net_debt.currency):
            LOGGER.warning("net_debt_to_ebitda: currency mismatch for %s", symbol)
            return None

        ratio = net_debt.total / ttm_ebitda.total
        as_of = max(ttm_ebitda.as_of, net_debt.as_of)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)

    def _compute_ttm_ebitda(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_TTMResult]:
        records = repo.facts_for_concept(symbol, EBITDA_CONCEPTS[0])
        quarterly = self._filter_quarterly(records)
        if len(quarterly) < 4:
            LOGGER.warning(
                "net_debt_to_ebitda: need 4 quarterly EBITDA records for %s", symbol
            )
            return None
        if not is_recent_fact(quarterly[0]):
            LOGGER.warning(
                "net_debt_to_ebitda: latest EBITDA (%s) too old for %s",
                quarterly[0].end_date,
                symbol,
            )
            return None

        # Normalize currency and GBX/GBP0.01 quirks before summing.
        normalized, currency = self._normalize_quarterly(quarterly[:4])
        if normalized is None:
            LOGGER.warning(
                "net_debt_to_ebitda: currency conflict in EBITDA for %s", symbol
            )
            return None

        total = sum(record.value for record in normalized)
        return _TTMResult(total=total, as_of=quarterly[0].end_date, currency=currency)

    def _compute_net_debt(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_NetDebtResult]:
        short_debt = repo.latest_fact(symbol, "ShortTermDebt")
        long_debt = repo.latest_fact(symbol, "LongTermDebt")
        cash = repo.latest_fact(symbol, "CashAndShortTermInvestments")
        if short_debt is None or long_debt is None or cash is None:
            return None
        if not all(is_recent_fact(record) for record in (short_debt, long_debt, cash)):
            return None

        # Normalize currencies for balance sheet facts; we require a single currency.
        short_value, short_currency = self._normalize_currency(short_debt)
        long_value, long_currency = self._normalize_currency(long_debt)
        cash_value, cash_currency = self._normalize_currency(cash)
        currency = self._merge_currency([short_currency, long_currency, cash_currency])
        if currency is None and any(
            code is not None for code in (short_currency, long_currency, cash_currency)
        ):
            return None

        total_debt = short_value + long_value
        net_debt = total_debt - cash_value
        as_of = max(short_debt.end_date, long_debt.end_date, cash.end_date)
        return _NetDebtResult(total=net_debt, as_of=as_of, currency=currency)

    def _filter_quarterly(self, records: Sequence[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
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
        code = getattr(record, "currency", None)
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


__all__ = ["NetDebtToEBITDAMetric"]
