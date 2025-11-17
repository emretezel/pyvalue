"""ROC% Greenblatt 5y average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FactRecord, FinancialFactsRepository

EBIT_CONCEPTS = [
    "OperatingIncomeLoss",
    "IncomeFromOperations",
    "OperatingProfitLoss",
]


@dataclass
class ROCGreenblattMetric:
    id: str = "roc_greenblatt_5y_avg"
    required_concepts = (
        "OperatingIncomeLoss",
        "IncomeFromOperations",
        "OperatingProfitLoss",
        "PropertyPlantAndEquipmentNet",
        "NetPropertyPlantAndEquipment",
        "AssetsCurrent",
        "LiabilitiesCurrent",
    )

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        ebit_records = self._fetch_ebit_history(symbol, repo)
        if not ebit_records:
            return None

        tangible_capital_records = self._fetch_tangible_capital_history(symbol, repo)
        if not tangible_capital_records:
            return None

        # merge by end_date
        tc_map = {record.end_date: record for record in tangible_capital_records}
        values: List[float] = []
        years_considered = 0
        for record in sorted(ebit_records, key=lambda r: r.end_date, reverse=True):
            tc = tc_map.get(record.end_date)
            if tc is None or tc.value is None or tc.value <= 0:
                continue
            if record.value is None:
                continue
            values.append(record.value / tc.value)
            years_considered += 1
            if years_considered == 5:
                break
        if not values:
            return None
        avg = sum(values) / len(values)
        latest = ebit_records[0].end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg, as_of=latest)

    def _fetch_ebit_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        for concept in EBIT_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                return records
        return []

    def _fetch_tangible_capital_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        ppe_records = repo.facts_for_concept(symbol, "PropertyPlantAndEquipmentNet", fiscal_period="FY")
        if not ppe_records:
            ppe_records = repo.facts_for_concept(symbol, "NetPropertyPlantAndEquipment", fiscal_period="FY")
        if not ppe_records:
            return []
        assets_records = repo.facts_for_concept(symbol, "AssetsCurrent", fiscal_period="FY")
        liabilities_records = repo.facts_for_concept(symbol, "LiabilitiesCurrent", fiscal_period="FY")
        assets_map = {r.end_date: r for r in assets_records}
        liabilities_map = {r.end_date: r for r in liabilities_records}
        combined: List[FactRecord] = []
        for ppe in ppe_records:
            assets = assets_map.get(ppe.end_date)
            liabilities = liabilities_map.get(ppe.end_date)
            if assets is None or liabilities is None:
                continue
            value = (ppe.value or 0) + (assets.value or 0) - (liabilities.value or 0)
            combined.append(
                FactRecord(
                    symbol=ppe.symbol,
                    cik=ppe.cik,
                    concept="TangibleCapital",
                    fiscal_year=ppe.fiscal_year,
                    fiscal_period=ppe.fiscal_period,
                    end_date=ppe.end_date,
                    unit=ppe.unit,
                    value=value,
                    accn=ppe.accn,
                    filed=ppe.filed,
                    frame=ppe.frame,
                )
            )
        return combined
