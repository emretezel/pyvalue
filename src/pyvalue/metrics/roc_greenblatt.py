"""ROC% Greenblatt 5y average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS, has_recent_fact, is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

EBIT_CONCEPTS = ["OperatingIncomeLoss"]

LOGGER = logging.getLogger(__name__)


@dataclass
class ROCGreenblattMetric:
    id: str = "roc_greenblatt_5y_avg"
    required_concepts = (
        "OperatingIncomeLoss",
        "PropertyPlantAndEquipmentNet",
        "AssetsCurrent",
        "LiabilitiesCurrent",
    )

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        ebit_records = self._fetch_ebit_history(symbol, repo)
        if not ebit_records:
            LOGGER.warning("roc_greenblatt: no FY EBIT records for %s", symbol)
            return None
        if not has_recent_fact(repo, symbol, EBIT_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("roc_greenblatt: no recent FY EBIT fact for %s", symbol)
            return None

        tangible_capital_records = self._fetch_tangible_capital_history(symbol, repo)
        if not tangible_capital_records:
            LOGGER.warning("roc_greenblatt: missing tangible capital components for %s", symbol)
            return None
        assets_check = repo.latest_fact(symbol, "AssetsCurrent")
        liabilities_check = repo.latest_fact(symbol, "LiabilitiesCurrent")
        if assets_check is None or not is_recent_fact(assets_check, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("roc_greenblatt: no recent assets current for %s", symbol)
            return None
        if liabilities_check is None or not is_recent_fact(liabilities_check, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("roc_greenblatt: liabilities current too old for %s", symbol)
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
            LOGGER.warning("roc_greenblatt: insufficient overlapping years for %s", symbol)
            return None
        avg = sum(values) / len(values)
        latest = ebit_records[0].end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg, as_of=latest)

    def _fetch_ebit_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        return repo.facts_for_concept(symbol, "OperatingIncomeLoss", fiscal_period="FY")

    def _fetch_tangible_capital_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        ppe_records = repo.facts_for_concept(symbol, "PropertyPlantAndEquipmentNet", fiscal_period="FY")
        if not ppe_records:
            return []
        assets_records = repo.facts_for_concept(symbol, "AssetsCurrent", fiscal_period="FY")
        liabilities_records = repo.facts_for_concept(symbol, "LiabilitiesCurrent", fiscal_period="FY")
        assets_by_period = self._index_by_period(assets_records)
        liabilities_by_period = self._index_by_period(liabilities_records)
        combined: List[FactRecord] = []
        for ppe in ppe_records:
            assets = assets_by_period.get((ppe.end_date, ppe.fiscal_period))
            liabilities = liabilities_by_period.get((ppe.end_date, ppe.fiscal_period))
            if assets is None or liabilities is None:
                continue
            value = (ppe.value or 0) + (assets.value or 0) - (liabilities.value or 0)
            combined.append(
                FactRecord(
                    symbol=ppe.symbol,
                    cik=ppe.cik,
                    concept="TangibleCapital",
                    fiscal_period=ppe.fiscal_period,
                    end_date=ppe.end_date,
                    unit=ppe.unit,
                    value=value,
                    accn=ppe.accn,
                    filed=ppe.filed,
                    frame=ppe.frame,
                    start_date=ppe.start_date,
                )
            )
        return combined

    def _index_by_period(self, records: List[FactRecord]) -> dict[tuple[str, str], FactRecord]:
        indexed: dict[tuple[str, str], FactRecord] = {}
        for record in records:
            key = (record.end_date, record.fiscal_period)
            if key not in indexed:
                indexed[key] = record
        return indexed
