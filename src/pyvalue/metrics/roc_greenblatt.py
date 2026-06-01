"""ROC% Greenblatt 5y average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    has_recent_fact,
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

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

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        ebit_records = self._fetch_ebit_history(symbol, repo)
        if not ebit_records:
            LOGGER.warning("roc_greenblatt: no FY EBIT records for %s", symbol)
            return None
        if not has_recent_fact(
            repo, symbol, EBIT_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roc_greenblatt: no recent FY EBIT fact for %s", symbol)
            return None

        target_currency = require_metric_ticker_currency(
            symbol, repo, metric_id=self.id
        )
        tc_map = self._fetch_tangible_capital(symbol, repo, target_currency)
        if not tc_map:
            LOGGER.warning(
                "roc_greenblatt: missing tangible capital components for %s", symbol
            )
            return None
        assets_check = repo.latest_monetary_fact(symbol, "AssetsCurrent")
        liabilities_check = repo.latest_monetary_fact(symbol, "LiabilitiesCurrent")
        if assets_check is None or not is_recent_fact(
            assets_check, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roc_greenblatt: no recent assets current for %s", symbol)
            return None
        if liabilities_check is None or not is_recent_fact(
            liabilities_check, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roc_greenblatt: liabilities current too old for %s", symbol)
            return None

        values: List[float] = []
        years_considered = 0
        for record in sorted(ebit_records, key=lambda r: r.end_date, reverse=True):
            tc = tc_map.get(record.end_date)
            if tc is None or tc.amount <= 0:
                continue
            ebit_money = require_metric_money(
                record.money,
                target_currency=target_currency,
                metric_id=self.id,
                symbol=symbol,
                input_name="OperatingIncomeLoss",
                as_of=record.end_date,
            )
            values.append(ebit_money / tc)
            years_considered += 1
            if years_considered == 5:
                break
        if not values:
            LOGGER.warning(
                "roc_greenblatt: insufficient overlapping years for %s", symbol
            )
            return None
        avg = sum(values) / len(values)
        latest = ebit_records[0].end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg, as_of=latest)

    def _fetch_ebit_history(
        self, symbol: str, repo: RegionFactsRepository
    ) -> List[MonetaryFact]:
        return repo.monetary_facts_for_concept(
            symbol, "OperatingIncomeLoss", fiscal_period="FY"
        )

    def _fetch_tangible_capital(
        self, symbol: str, repo: RegionFactsRepository, target_currency: str
    ) -> dict[str, Money]:
        """Map FY end_date -> tangible capital (PP&E + current assets - current liab)."""

        ppe_records = repo.monetary_facts_for_concept(
            symbol, "PropertyPlantAndEquipmentNet", fiscal_period="FY"
        )
        if not ppe_records:
            return {}
        assets_by_period = self._index_by_period(
            repo.monetary_facts_for_concept(symbol, "AssetsCurrent", fiscal_period="FY")
        )
        liabilities_by_period = self._index_by_period(
            repo.monetary_facts_for_concept(
                symbol, "LiabilitiesCurrent", fiscal_period="FY"
            )
        )
        combined: dict[str, Money] = {}
        for ppe in ppe_records:
            assets = assets_by_period.get((ppe.end_date, ppe.fiscal_period))
            liabilities = liabilities_by_period.get((ppe.end_date, ppe.fiscal_period))
            if assets is None or liabilities is None:
                continue
            tangible_capital = (
                self._money(ppe, target_currency, symbol)
                + self._money(assets, target_currency, symbol)
                - self._money(liabilities, target_currency, symbol)
            )
            combined[ppe.end_date] = tangible_capital
        return combined

    def _index_by_period(
        self, records: List[MonetaryFact]
    ) -> dict[tuple[str, str], MonetaryFact]:
        indexed: dict[tuple[str, str], MonetaryFact] = {}
        for record in records:
            key = (record.end_date, record.fiscal_period)
            if key not in indexed:
                indexed[key] = record
        return indexed

    def _money(self, fact: MonetaryFact, target_currency: str, symbol: str) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name=fact.concept,
            as_of=fact.end_date,
        )
