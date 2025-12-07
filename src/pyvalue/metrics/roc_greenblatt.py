"""ROC% Greenblatt 5y average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    has_recent_fact,
    is_recent_fact,
    resolve_assets_current,
    resolve_liabilities_current,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

EBIT_CONCEPTS = [
    "OperatingIncomeLoss",
    "IncomeFromOperations",
    "OperatingProfitLoss",
]

LOGGER = logging.getLogger(__name__)


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
            LOGGER.warning("roc_greenblatt: no FY EBIT records for %s", symbol)
            return None
        if not has_recent_fact(repo, symbol, EBIT_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("roc_greenblatt: no recent FY EBIT fact for %s", symbol)
            return None

        tangible_capital_records = self._fetch_tangible_capital_history(symbol, repo)
        if not tangible_capital_records:
            LOGGER.warning("roc_greenblatt: missing tangible capital components for %s", symbol)
            return None
        assets_check = resolve_assets_current(repo, symbol, max_age_days=MAX_FY_FACT_AGE_DAYS)
        liabilities_check = resolve_liabilities_current(repo, symbol, max_age_days=MAX_FY_FACT_AGE_DAYS)
        if assets_check is None:
            LOGGER.warning("roc_greenblatt: no recent assets current for %s", symbol)
            return None
        if liabilities_check is None:
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
        for concept in EBIT_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept)
            if records:
                return records
        return []

    def _fetch_tangible_capital_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        ppe_records = repo.facts_for_concept(symbol, "PropertyPlantAndEquipmentNet")
        if not ppe_records:
            ppe_records = repo.facts_for_concept(symbol, "NetPropertyPlantAndEquipment")
        if not ppe_records:
            return []
        combined: List[FactRecord] = []
        for ppe in ppe_records:
            if not is_recent_fact(ppe, max_age_days=MAX_FY_FACT_AGE_DAYS):
                continue
            assets = resolve_assets_current(repo, symbol, end_date=ppe.end_date, fiscal_period=ppe.fiscal_period, max_age_days=MAX_FY_FACT_AGE_DAYS)
            liabilities = resolve_liabilities_current(repo, symbol, end_date=ppe.end_date, fiscal_period=ppe.fiscal_period, max_age_days=MAX_FY_FACT_AGE_DAYS)
            if assets is None or liabilities is None:
                continue
            if not is_recent_fact(assets, max_age_days=MAX_FY_FACT_AGE_DAYS):
                continue
            if not is_recent_fact(liabilities, max_age_days=MAX_FY_FACT_AGE_DAYS):
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
