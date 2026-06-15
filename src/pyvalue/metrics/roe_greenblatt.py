"""ROE% Greenblatt 5-year average metric.

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
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

NET_INCOME_CONCEPTS = ["NetIncomeLossAvailableToCommonStockholdersBasic"]
EQUITY_CONCEPTS = ["CommonStockholdersEquity"]

LOGGER = logging.getLogger(__name__)


@dataclass
class ROEGreenblattMetric:
    id: str = "roe_greenblatt_5y_avg"
    required_concepts = tuple(NET_INCOME_CONCEPTS + EQUITY_CONCEPTS)

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        income_records = self._net_income_history(listing_id, repo)
        if len(income_records) < 2:
            LOGGER.warning(
                "roe_greenblatt: need >=2 FY income records for listing_id=%s",
                listing_id,
            )
            return None
        if not has_recent_fact(
            repo, listing_id, NET_INCOME_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "roe_greenblatt: no recent FY income fact for listing_id=%s", listing_id
            )
            return None
        equity_records = self._equity_history(listing_id, repo)
        if len(equity_records) < 2:
            LOGGER.warning(
                "roe_greenblatt: need >=2 FY equity records for listing_id=%s",
                listing_id,
            )
            return None
        if not has_recent_fact(
            repo, listing_id, EQUITY_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "roe_greenblatt: no recent FY equity fact for listing_id=%s", listing_id
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id
        )

        equity_map: dict[int, MonetaryFact] = {}
        for rec in equity_records:
            year = self._year_from_record(rec)
            if year is None:
                continue
            equity_map[year] = rec
        income_map: dict[int, MonetaryFact] = {}
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
            avg_equity = (
                self._money(equity_now, target_currency, listing_id)
                + self._money(equity_prev, target_currency, listing_id)
            ) / 2
            if avg_equity.amount == 0:
                continue
            income_money = self._money(income, target_currency, listing_id)
            roe_values.append(income_money / avg_equity)
            if len(roe_values) == 5:
                break
        if not roe_values:
            LOGGER.warning(
                "roe_greenblatt: insufficient overlapping years for listing_id=%s",
                listing_id,
            )
            return None
        avg_roe = sum(roe_values) / len(roe_values)
        latest = income_records[0].end_date
        return MetricResult.ratio(
            listing_id=listing_id,
            metric_id=self.id,
            value=avg_roe,
            as_of=latest,
            unit_kind="percent",
        )

    def _net_income_history(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> List[MonetaryFact]:
        return repo.monetary_facts_for_concept(
            listing_id,
            "NetIncomeLossAvailableToCommonStockholdersBasic",
            fiscal_period="FY",
        )

    def _equity_history(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> List[MonetaryFact]:
        return repo.monetary_facts_for_concept(
            listing_id, "CommonStockholdersEquity", fiscal_period="FY"
        )

    def _money(
        self, fact: MonetaryFact, target_currency: str, listing_id: int
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )

    def _year_from_record(self, record: MonetaryFact) -> Optional[int]:
        try:
            return int(record.end_date[:4])
        except (TypeError, ValueError):
            return None
