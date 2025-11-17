"""Normalize SEC company facts payloads into relational records.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pyvalue.storage import FactRecord

# Concepts needed to compute the initial set of metrics. Include the most common
# GAAP tags plus frequent synonyms across industries so normalization yields data
# even when companies use slightly different taxonomy labels.
TARGET_CONCEPTS = {
    # Balance sheet / leverage
    "LongTermDebtNoncurrent",
    "LongTermDebt",
    "LongTermDebtCurrent",
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    "CommonStockholdersEquity",
    "PreferredStock",
    # Income statement
    "NetIncomeLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
    "PreferredStockDividendsAndOtherAdjustments",
    "PreferredStockDividends",
    "OperatingIncomeLoss",
    "IncomeFromOperations",
    "OperatingProfitLoss",
    "IncomeBeforeIncomeTaxes",
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "EarningsPerShareDiluted",
    "DilutedEPS",
    "EarningsPerShareBasic",
    "EarningsPerShareBasicAndDiluted",
    # Cash flow / FCF
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    "CapitalExpenditures",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
    "PurchaseOfFixedAssets",
    # Dividends
    "DividendsPerShareCommonStockDeclared",
    "CommonStockDividendsPerShareCashPaid",
    "CommonStockDividendsPerShareDeclared",
    # Shares / valuation
    "CommonStockSharesOutstanding",
    "CommonStockDividendsPaid",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    # Operating assets
    "PropertyPlantAndEquipmentNet",
    "NetPropertyPlantAndEquipment",
    "GrossPropertyPlantAndEquipment",
    "Goodwill",
    "IntangibleAssetsNetExcludingGoodwill",
    "IntangibleAssetsNet",
}


class SECFactsNormalizer:
    """Flatten SEC fact payloads into FactRecord entries."""

    def __init__(self, concepts: Optional[Iterable[str]] = None) -> None:
        self.concepts = set(concepts or TARGET_CONCEPTS)

    def normalize(self, payload: Dict, symbol: str, cik: str) -> List[FactRecord]:
        """Return FactRecords for the provided SEC payload."""

        if not payload:
            return []

        facts = payload.get("facts", {})
        records: List[FactRecord] = []
        for taxonomy, concept_map in facts.items():
            if taxonomy not in {"us-gaap", "dei"}:
                continue
            for concept, detail in concept_map.items():
                if concept not in self.concepts:
                    continue
                entries = self._collect_entries(detail)
                if not entries:
                    continue
                fy_records, _ = self._build_fy_records(entries, symbol, cik, concept)
                records.extend(fy_records)
        return records

    def _collect_entries(self, detail: Dict) -> List[Dict]:
        entries: List[Dict] = []
        units = detail.get("units", {})
        for unit, items in units.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                data = item.copy()
                data["__unit"] = unit
                entries.append(data)
        return entries

    def _build_fy_records(
        self,
        entries: List[Dict],
        symbol: str,
        cik: str,
        concept: str,
    ) -> tuple[List[FactRecord], Dict[int, FactRecord]]:
        filtered = [
            entry
            for entry in entries
            if (entry.get("form") or "").startswith("10-K") and (entry.get("fp") or "").upper() == "FY"
        ]
        if not filtered:
            return [], {}

        by_end: Dict[str, Dict] = {}
        for entry in filtered:
            end = entry.get("end")
            if not end:
                continue
            filed_key = self._filed_key(entry)
            existing = by_end.get(end)
            if existing is None or filed_key > self._filed_key(existing):
                by_end[end] = entry

        by_year: Dict[int, List[Dict]] = {}
        for entry in by_end.values():
            year = self._year_from_end(entry.get("end"))
            if year is None:
                continue
            by_year.setdefault(year, []).append(entry)

        records: List[FactRecord] = []
        fy_map: Dict[int, FactRecord] = {}
        for year, group in by_year.items():
            selected = self._select_fy_entry(group)
            record = self._entry_to_record(selected, symbol, cik, concept, selected["__unit"])
            if record is None:
                continue
            records.append(record)
            fy_map[year] = record
        return records, fy_map

    def _select_fy_entry(self, entries: List[Dict]) -> Dict:
        with_start = [entry for entry in entries if entry.get("start")]
        if with_start:
            return min(
                with_start,
                key=lambda e: (
                    self._days_from_start(e),
                    -self._filed_key_value(e),
                ),
            )
        return max(entries, key=self._filed_key)

    def _days_from_start(self, entry: Dict) -> float:
        start = entry.get("start")
        end = entry.get("end")
        if not start or not end:
            return float("inf")
        try:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
        except ValueError:
            return float("inf")
        return abs((end_dt - start_dt).days - 365)

    def _filed_key(self, entry: Dict) -> datetime:
        filed = entry.get("filed")
        try:
            return datetime.fromisoformat(filed)
        except (TypeError, ValueError):
            return datetime.min

    def _filed_key_value(self, entry: Dict) -> float:
        filed = entry.get("filed")
        try:
            return datetime.fromisoformat(filed).timestamp()
        except (TypeError, ValueError):
            return float("-inf")

    def _year_from_end(self, end: Optional[str]) -> Optional[int]:
        if not end:
            return None
        try:
            return int(end[:4])
        except ValueError:
            return None

    def _to_float(self, value: Optional[object]) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _entry_to_record(
        self,
        entry: Dict,
        symbol: str,
        cik: str,
        concept: str,
        unit: str,
    ) -> Optional[FactRecord]:
        value = entry.get("val")
        if value is None:
            return None
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None

        fiscal_period = entry.get("fp") or ""
        end_date = entry.get("end")
        if end_date is None:
            return None

        return FactRecord(
            symbol=symbol,
            cik=cik,
            concept=concept,
            fiscal_period=fiscal_period,
            end_date=end_date,
            unit=unit,
            value=numeric_value,
            accn=entry.get("accn"),
            filed=entry.get("filed"),
            frame=entry.get("frame"),
            start_date=entry.get("start"),
        )


__all__ = ["SECFactsNormalizer", "TARGET_CONCEPTS"]
