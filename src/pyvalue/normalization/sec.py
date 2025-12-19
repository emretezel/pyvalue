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
    "LongTermLineOfCredit",
    "CommercialPaperNoncurrent",
    "ConstructionLoanNoncurrent",
    "SecuredLongTermDebt",
    "UnsecuredLongTermDebt",
    "SubordinatedLongTermDebt",
    "ConvertibleDebtNoncurrent",
    "ConvertibleSubordinatedDebtNoncurrent",
    "LongTermNotesAndLoans",
    "LongtermFederalHomeLoanBankAdvancesNoncurrent",
    "OtherLongTermDebtNoncurrent",
    "LongTermNotesPayable",
    "NotesPayable",
    "LongTermDebtAndCapitalLeaseObligations",
    "LongTermDebtAndCapitalLeaseObligationsCurrent",
    "LongTermDebtAndCapitalLeaseObligationsNoncurrent",
    "AssetsCurrent",
    "AccountsPayableCurrent",
    "AccruedLiabilitiesCurrent",
    "EmployeeRelatedLiabilitiesCurrent",
    "TaxesPayableCurrent",
    "InterestPayableCurrent",
    "DeferredRevenueCurrent",
    "ShortTermBorrowings",
    "CommercialPaper",
    "FinanceLeaseLiabilityCurrent",
    "OperatingLeaseLiabilityCurrent",
    "OtherLiabilitiesCurrent",
    "TradeAndOtherCurrentPayables",
    "CurrentTradePayables",
    "OtherCurrentPayables",
    "CurrentTaxLiabilities",
    "CurrentProvisions",
    "CurrentFinancialLiabilities",
    "CurrentBorrowings",
    "CurrentPortionOfNoncurrentBorrowings",
    "OtherCurrentFinancialLiabilities",
    "OtherCurrentNonfinancialLiabilities",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
    "MarketableSecuritiesCurrent",
    "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
    "HeldToMaturitySecuritiesCurrent",
    "AccountsReceivableNetCurrent",
    "LoansAndLeasesReceivableNetCurrent",
    "InventoryNet",
    "Inventories",
    "PrepaidExpenseAndOtherAssetsCurrent",
    "PrepaidExpenseCurrent",
    "DeferredTaxAssetsNetCurrent",
    "OtherAssetsCurrent",
    "OtherShortTermFinancialAssets",
    "CurrentFinancialAssetsOtherThanCashAndCashEquivalents",
    "TradeAndOtherCurrentReceivables",
    "CurrentTradeReceivables",
    "OtherCurrentReceivables",
    "CurrentTaxAssets",
    "OtherCurrentNonfinancialAssets",
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
    "EntityCommonStockSharesOutstanding",
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
                fy_records, fy_map = self._build_fy_records(entries, symbol, cik, concept)
                quarter_records = self._build_quarter_records(entries, fy_map, symbol, cik, concept)
                records.extend(fy_records)
                records.extend(quarter_records)
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
    ) -> tuple[List[FactRecord], Dict[str, FactRecord]]:
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
        fy_map: Dict[str, FactRecord] = {}
        for year, group in by_year.items():
            selected = self._select_fy_entry(group)
            record = self._entry_to_record(selected, symbol, cik, concept, selected["__unit"])
            if record is None:
                continue
            records.append(record)
            fy_map[record.end_date] = record
        return records, fy_map

    def _build_quarter_records(
        self,
        entries: List[Dict],
        fy_map: Dict[str, FactRecord],
        symbol: str,
        cik: str,
        concept: str,
    ) -> List[FactRecord]:
        filtered = [
            entry
            for entry in entries
            if (entry.get("form") or "").startswith("10-Q") and (entry.get("fp") or "").upper() in {"Q1", "Q2", "Q3"}
        ]
        if not filtered:
            return []

        filtered = self._dedup_quarter_filings(filtered)

        dedup: Dict[tuple, Dict] = {}
        for entry in filtered:
            end = entry.get("end")
            fp = (entry.get("fp") or "").upper()
            if not end:
                continue
            key = (entry["__unit"], end, fp)
            existing = dedup.get(key)
            if existing is None or self._filed_key(entry) > self._filed_key(existing):
                dedup[key] = entry

        entries_sorted = sorted(dedup.values(), key=lambda e: e.get("end") or "")
        flow_cumulative: Dict[tuple[str, str], float] = {}
        fy_lookup = self._build_fy_lookup(fy_map)
        cycle_keys: Dict[str, str] = {}
        cycle_counters: Dict[str, int] = {}
        records: List[FactRecord] = []

        for entry in entries_sorted:
            fp = (entry.get("fp") or "").upper()
            if fp not in {"Q1", "Q2", "Q3"}:
                continue
            value = self._to_float(entry.get("val"))
            if value is None:
                continue
            is_flow = entry.get("start") is not None
            fy_key = self._resolve_fy_key(entry, fy_lookup, cycle_keys, cycle_counters)
            if fy_key is None:
                fy_key = f"{entry['__unit']}-unknown"
            if is_flow:
                if fp == "Q1":
                    incremental = value
                else:
                    prev_fp = "Q1" if fp == "Q2" else "Q2"
                    prev = flow_cumulative.get((fy_key, prev_fp))
                    if prev is None:
                        continue
                    incremental = value - prev
                flow_cumulative[(fy_key, fp)] = value
            else:
                incremental = value
            new_entry = entry.copy()
            new_entry["val"] = incremental
            record = self._entry_to_record(new_entry, symbol, cik, concept, entry["__unit"])
            if record:
                records.append(record)

        for fy_key, fy_record in fy_map.items():
            if fy_record.value is None:
                continue
            is_flow = fy_record.start_date is not None
            if is_flow:
                q3_cumulative = flow_cumulative.get((fy_key, "Q3"))
                if q3_cumulative is None:
                    continue
                q4_value = fy_record.value - q3_cumulative
                start = fy_record.start_date
            else:
                q4_value = fy_record.value
                start = None
            new_entry = {
                "__unit": fy_record.unit,
                "val": q4_value,
                "end": fy_record.end_date,
                "fp": "Q4",
                "filed": fy_record.filed,
                "accn": fy_record.accn,
                "frame": fy_record.frame,
                "start": start,
            }
            record = self._entry_to_record(new_entry, symbol, cik, concept, fy_record.unit)
            if record:
                records.append(record)

        return records

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

        currency = self._currency_from_unit(unit)

        return FactRecord(
            symbol=symbol,
            provider="SEC",
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
            accounting_standard="US-GAAP",
            currency=currency,
        )

    def _build_fy_lookup(self, fy_map: Dict[str, FactRecord]) -> List[tuple[datetime, str]]:
        lookup: List[tuple[datetime, str]] = []
        for end_date, record in fy_map.items():
            dt = self._parse_date(end_date)
            if dt is None:
                continue
            lookup.append((dt, end_date))
        lookup.sort(key=lambda item: item[0])
        return lookup

    def _resolve_fy_key(
        self,
        entry: Dict,
        fy_lookup: List[tuple[datetime, str]],
        cycle_keys: Dict[str, str],
        cycle_counters: Dict[str, int],
    ) -> Optional[str]:
        end = entry.get("end")
        unit = entry.get("__unit")
        if not unit:
            return None
        dt = self._parse_date(end)
        if dt is not None:
            for fy_dt, key in fy_lookup:
                if dt <= fy_dt:
                    return key
        fp = (entry.get("fp") or "").upper()
        if fp == "Q1" or unit not in cycle_keys:
            idx = cycle_counters.get(unit, 0)
            cycle_counters[unit] = idx + 1
            cycle_key = f"{unit}-cycle-{idx}"
            cycle_keys[unit] = cycle_key
            return cycle_key
        return cycle_keys[unit]

    def _parse_date(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _dedup_quarter_filings(self, entries: List[Dict]) -> List[Dict]:
        grouped: Dict[tuple, Dict] = {}
        for entry in entries:
            fp = (entry.get("fp") or "").upper()
            unit = entry.get("__unit")
            if not unit:
                continue
            key = (unit, fp, entry.get("accn") or entry.get("filed") or "")
            existing = grouped.get(key)
            if existing is None or self._prefer_quarter_entry(entry, existing):
                grouped[key] = entry
        return list(grouped.values())

    def _prefer_quarter_entry(self, new_entry: Dict, existing: Dict) -> bool:
        new_end = self._parse_date(new_entry.get("end"))
        old_end = self._parse_date(existing.get("end"))
        if new_end and old_end:
            if new_end > old_end:
                return True
            if new_end < old_end:
                return False
        elif new_end or old_end:
            return new_end is not None

        new_start = self._parse_date(new_entry.get("start"))
        old_start = self._parse_date(existing.get("start"))
        if new_start and old_start:
            if new_start < old_start:
                return True
            if new_start > old_start:
                return False
        elif new_start or old_start:
            return old_start is None

        return self._filed_key(new_entry) >= self._filed_key(existing)

    def _currency_from_unit(self, unit: str) -> Optional[str]:
        """Extract currency code from a unit string (e.g., USD)."""

        if not unit:
            return None
        cleaned = unit.strip().upper()
        if len(cleaned) == 3 and cleaned.isalpha():
            return cleaned
        return None


__all__ = ["SECFactsNormalizer", "TARGET_CONCEPTS"]
