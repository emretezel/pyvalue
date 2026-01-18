"""ROE% Greenblatt 5-year average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS, has_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

NET_INCOME_CONCEPTS = ["NetIncomeLossAvailableToCommonStockholdersBasic"]
EQUITY_CONCEPTS = ["CommonStockholdersEquity"]

LOGGER = logging.getLogger(__name__)


@dataclass
class ROEGreenblattMetric:
    id: str = "roe_greenblatt_5y_avg"
    required_concepts = tuple(NET_INCOME_CONCEPTS + EQUITY_CONCEPTS)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        income_records = self._net_income_history(symbol, repo)
        if len(income_records) < 2:
            LOGGER.warning("roe_greenblatt: need >=2 FY income records for %s", symbol)
            return None
        if not has_recent_fact(
            repo, symbol, NET_INCOME_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roe_greenblatt: no recent FY income fact for %s", symbol)
            return None
        equity_records = self._equity_history(symbol, repo)
        if len(equity_records) < 2:
            LOGGER.warning("roe_greenblatt: need >=2 FY equity records for %s", symbol)
            return None
        if not has_recent_fact(
            repo, symbol, EQUITY_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("roe_greenblatt: no recent FY equity fact for %s", symbol)
            return None
        equity_map = {}
        for rec in equity_records:
            year = self._year_from_record(rec)
            if year is None:
                continue
            equity_map[year] = rec
        income_map = {}
        for rec in income_records:
            year = self._year_from_record(rec)
            if year is None:
                continue
            income_map[year] = rec
        years = sorted(income_map.keys(), reverse=True)
        roe_values: List[float] = []
        for year in years:
            income = income_map[year]
            equity_now = equity_map.get(year)
            equity_prev = equity_map.get(year - 1)
            if equity_now is None or equity_prev is None:
                continue
            if (
                income.value is None
                or equity_now.value is None
                or equity_prev.value is None
            ):
                continue
            avg_equity = (equity_now.value + equity_prev.value) / 2
            if avg_equity == 0:
                continue
            roe_values.append(income.value / avg_equity)
            if len(roe_values) == 5:
                break
        if not roe_values:
            LOGGER.warning(
                "roe_greenblatt: insufficient overlapping years for %s", symbol
            )
            return None
        avg_roe = sum(roe_values) / len(roe_values)
        latest = income_records[0].end_date
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=avg_roe, as_of=latest
        )

    def _net_income_history(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> List[FactRecord]:
        return repo.facts_for_concept(
            symbol,
            "NetIncomeLossAvailableToCommonStockholdersBasic",
            fiscal_period="FY",
        )

    def _equity_history(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> List[FactRecord]:
        return repo.facts_for_concept(
            symbol, "CommonStockholdersEquity", fiscal_period="FY"
        )

    def _year_from_record(self, record: FactRecord) -> Optional[int]:
        try:
            return int(record.end_date[:4])
        except (TypeError, ValueError):
            return None
