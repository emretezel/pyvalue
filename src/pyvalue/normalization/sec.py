"""Normalize SEC company facts payloads into relational records.

Author: Emre Tezel
"""

from __future__ import annotations

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
    # Income statement
    "NetIncomeLoss",
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
                units = detail.get("units", {})
                for unit, entries in units.items():
                    if not isinstance(entries, list):
                        continue
                    for entry in entries:
                        maybe_record = self._entry_to_record(entry, symbol, cik, concept, unit)
                        if maybe_record is None:
                            continue
                        records.append(maybe_record)
        return records

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

        fiscal_year = entry.get("fy")
        try:
            fiscal_year_int = int(fiscal_year) if fiscal_year is not None else None
        except (TypeError, ValueError):
            fiscal_year_int = None

        fiscal_period = entry.get("fp") or ""
        end_date = entry.get("end")
        if end_date is None:
            return None

        return FactRecord(
            symbol=symbol,
            cik=cik,
            concept=concept,
            fiscal_year=fiscal_year_int,
            fiscal_period=fiscal_period,
            end_date=end_date,
            unit=unit,
            value=numeric_value,
            accn=entry.get("accn"),
            filed=entry.get("filed"),
            frame=entry.get("frame"),
        )


__all__ = ["SECFactsNormalizer", "TARGET_CONCEPTS"]
