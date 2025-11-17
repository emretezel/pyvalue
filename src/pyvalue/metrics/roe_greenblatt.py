"""ROE% Greenblatt 5-year average metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FactRecord, FinancialFactsRepository

NET_INCOME_CONCEPTS = [
    "NetIncomeLossAvailableToCommonStockholdersBasic",
    "NetIncomeLoss",
]
PREFERRED_DIVIDEND_CONCEPTS = [
    "PreferredStockDividendsAndOtherAdjustments",
    "PreferredStockDividends",
]
EQUITY_CONCEPTS = [
    "CommonStockholdersEquity",
    "StockholdersEquity",
]


@dataclass
class ROEGreenblattMetric:
    id: str = "roe_greenblatt_5y_avg"
    required_concepts = tuple(NET_INCOME_CONCEPTS + PREFERRED_DIVIDEND_CONCEPTS + EQUITY_CONCEPTS + ["PreferredStock"])

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        income_records = self._net_income_history(symbol, repo)
        if len(income_records) < 2:
            return None
        equity_records = self._equity_history(symbol, repo)
        if len(equity_records) < 2:
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
            if income.value is None or equity_now.value is None or equity_prev.value is None:
                continue
            avg_equity = (equity_now.value + equity_prev.value) / 2
            if avg_equity == 0:
                continue
            roe_values.append(income.value / avg_equity)
            if len(roe_values) == 5:
                break
        if not roe_values:
            return None
        avg_roe = sum(roe_values) / len(roe_values)
        latest = income_records[0].end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg_roe, as_of=latest)

    def _net_income_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        for concept in NET_INCOME_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                if concept == "NetIncomeLoss":
                    pref = self._preferred_dividends(symbol, repo)
                    if pref is not None:
                        adjusted = []
                        for record in records:
                            adjusted.append(
                                FactRecord(
                                    symbol=record.symbol,
                                    cik=record.cik,
                                    concept=record.concept,
                                    fiscal_period=record.fiscal_period,
                                    end_date=record.end_date,
                                    unit=record.unit,
                                    value=(record.value or 0) - pref,
                                    accn=record.accn,
                                    filed=record.filed,
                                    frame=record.frame,
                                )
                            )
                        return adjusted
                return records
        return []

    def _preferred_dividends(self, symbol: str, repo: FinancialFactsRepository) -> Optional[float]:
        for concept in PREFERRED_DIVIDEND_CONCEPTS:
            fact = repo.latest_fact(symbol, concept)
            if fact is not None and fact.value is not None:
                return fact.value
        return None

    def _equity_history(self, symbol: str, repo: FinancialFactsRepository) -> List[FactRecord]:
        records = repo.facts_for_concept(symbol, "CommonStockholdersEquity", fiscal_period="FY")
        if not records:
            records = repo.facts_for_concept(symbol, "StockholdersEquity", fiscal_period="FY")
        if not records:
            return []
        preferred = repo.latest_fact(symbol, "PreferredStock")
        if preferred is not None and preferred.value is not None:
            adjusted = []
            for record in records:
                value = (record.value or 0) - preferred.value
                adjusted.append(
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
                    )
                )
            return adjusted
        return records

    def _year_from_record(self, record: FactRecord) -> Optional[int]:
        try:
            return int(record.end_date[:4])
        except (TypeError, ValueError):
            return None
