"""Price to Free Cash Flow metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

OPERATING_CASH_FLOW_CONCEPTS = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
]
CAPEX_CONCEPTS = [
    "CapitalExpenditures",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PurchaseOfPropertyPlantAndEquipment",
    "PropertyPlantAndEquipmentAdditions",
    "PaymentsToAcquireProductiveAssets",
]
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}


@dataclass
class _TTMResult:
    total: float
    as_of: str


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
            return None
        if fcf_result.total <= 0:
            return None
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None or snapshot.market_cap <= 0:
            return None

        ratio = snapshot.market_cap / fcf_result.total
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=fcf_result.as_of)

    def _compute_ttm_fcf(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[_TTMResult]:
        operating = self._ttm_sum(symbol, repo, OPERATING_CASH_FLOW_CONCEPTS)
        capex = self._ttm_sum(symbol, repo, CAPEX_CONCEPTS)
        if operating is None or capex is None:
            return None
        fcf_total = operating.total - capex.total
        as_of = operating.as_of if operating.as_of >= capex.as_of else capex.as_of
        return _TTMResult(total=fcf_total, as_of=as_of)

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
            values = quarterly[:4]
            total = sum(record.value for record in values)
            return _TTMResult(total=total, as_of=values[0].end_date)
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


__all__ = ["PriceToFCFMetric"]
