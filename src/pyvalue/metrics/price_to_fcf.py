"""Price to Free Cash Flow metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.fx import FXRateStore
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

OPERATING_CASH_FLOW_CONCEPTS = ["NetCashProvidedByUsedInOperatingActivities"]
CAPEX_CONCEPTS = ["CapitalExpenditures"]
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}

LOGGER = logging.getLogger(__name__)


@dataclass
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class PriceToFCFMetric:
    id: str = "price_to_fcf"
    required_concepts = tuple(OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS)
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        fcf_result = self._compute_ttm_fcf(symbol, repo)
        if fcf_result is None:
            LOGGER.warning("price_to_fcf: missing TTM FCF for %s", symbol)
            return None
        if fcf_result.total <= 0:
            LOGGER.warning("price_to_fcf: non-positive TTM FCF for %s", symbol)
            return None
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None or snapshot.market_cap <= 0:
            LOGGER.warning("price_to_fcf: missing market cap for %s", symbol)
            return None

        market_cap = snapshot.market_cap
        if fcf_result.currency and getattr(snapshot, "currency", None):
            converted = FXRateStore().convert(market_cap, snapshot.currency, fcf_result.currency, snapshot.as_of)
            if converted is None:
                LOGGER.warning(
                    "price_to_fcf: FX conversion failed %s -> %s for %s",
                    snapshot.currency,
                    fcf_result.currency,
                    symbol,
                )
                return None
            market_cap = converted

        ratio = market_cap / fcf_result.total
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=fcf_result.as_of)

    def _compute_ttm_fcf(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[_TTMResult]:
        operating = self._ttm_sum(symbol, repo, OPERATING_CASH_FLOW_CONCEPTS)
        capex = self._ttm_sum(symbol, repo, CAPEX_CONCEPTS)
        if operating is None:
            return None
        if capex is None:
            LOGGER.warning("price_to_fcf: missing/stale capex for %s; assuming zero", symbol)
            capex_total = 0.0
            capex_as_of = operating.as_of
            capex_currency = None
        else:
            capex_total = capex.total
            capex_as_of = capex.as_of
            capex_currency = capex.currency
        fcf_total = operating.total - capex_total
        as_of = operating.as_of if operating.as_of >= capex_as_of else capex_as_of
        currency = operating.currency or capex_currency
        return _TTMResult(total=fcf_total, as_of=as_of, currency=currency)

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
                LOGGER.warning("price_to_fcf: need 4 quarterly %s records for %s, found %s", concept, symbol, len(quarterly))
                continue
            values = quarterly[:4]
            if not is_recent_fact(values[0]):
                LOGGER.warning(
                    "price_to_fcf: latest %s (%s) too old for %s",
                    concept,
                    values[0].end_date,
                    symbol,
                )
                continue
            normalized, currency = self._normalize_quarterly(values)
            if normalized is None:
                LOGGER.warning("price_to_fcf: currency conflict in %s quarterly values for %s", concept, symbol)
                continue
            total = sum(record.value for record in normalized)
            return _TTMResult(total=total, as_of=normalized[0].end_date, currency=currency or None)
        return None

    def _filter_quarterly(self, records: Iterable[FactRecord]) -> list[FactRecord]:
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

    def _normalize_quarterly(self, records: list[FactRecord]) -> tuple[Optional[list[FactRecord]], Optional[str]]:
        """Normalize GBX/GBP0.01 records to GBP and ensure consistent currency."""

        currency = None
        normalized: list[FactRecord] = []
        for record in records:
            code = getattr(record, "currency", None)
            value = record.value
            if code in {"GBX", "GBP0.01"}:
                code = "GBP"
                value = value / 100.0 if value is not None else None
            if value is None:
                continue
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(
                FactRecord(
                    symbol=record.symbol,
                    provider=record.provider,
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
        return (normalized, currency)


__all__ = ["PriceToFCFMetric"]
