"""Normalize EODHD fundamentals into FactRecord entries.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pyvalue.storage import FactRecord
from .sec import TARGET_CONCEPTS


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class EODHDFactsNormalizer:
    """Flatten EODHD fundamentals payloads into FactRecord entries."""

    STATEMENT_FIELDS = {
        "Balance_Sheet": {
            "AssetsCurrent": ["totalCurrentAssets"],
            "LiabilitiesCurrent": ["totalCurrentLiabilities"],
            "Assets": ["totalAssets"],
            "Liabilities": ["totalLiabilities", "totalLiab"],
            "StockholdersEquity": ["totalStockholderEquity", "totalShareholderEquity"],
            "PreferredStock": ["preferredStock", "capitalStock"],
            "Goodwill": ["goodWill", "goodwill"],
            "IntangibleAssetsNet": ["intangibleAssets"],
            "LongTermDebtNoncurrent": [
                "longTermDebt",
                "longTermDebtNoncurrent",
                "longTermDebtTotal",
                "shortLongTermDebtTotal",
                "totalNonCurrentLiabilitiesNetMinorityInterest",
            ],
            "LongTermDebt": [
                "longTermDebt",
                "longTermDebtNoncurrent",
                "longTermDebtTotal",
                "shortLongTermDebtTotal",
                "totalNonCurrentLiabilitiesNetMinorityInterest",
            ],
            "CommonStockSharesOutstanding": [
                "shareIssued",
                "commonStockSharesOutstanding",
            ],
            "EntityCommonStockSharesOutstanding": [
                "shareIssued",
                "commonStockSharesOutstanding",
            ],
        },
        "Income_Statement": {
            "NetIncomeLoss": ["netIncome"],
            "OperatingIncomeLoss": ["operatingIncome"],
            "IncomeBeforeIncomeTaxes": ["incomeBeforeTax"],
            "Revenues": ["totalRevenue", "revenue"],
            "EarningsPerShareDiluted": ["epsDiluted", "epsdiluted", "epsDilluted"],
            "EarningsPerShareBasic": ["eps", "epsBasic"],
            "WeightedAverageNumberOfDilutedSharesOutstanding": ["weightedAverageShsOutDil", "weightedAverageShsOutDiluted"],
            "WeightedAverageNumberOfSharesOutstandingBasic": ["weightedAverageShsOut", "weightedAverageShsOutBasic"],
        },
        "Cash_Flow": {
            "NetCashProvidedByUsedInOperatingActivities": [
                "totalCashFromOperatingActivities",
                "netCashProvidedByOperatingActivities",
            ],
            "CapitalExpenditures": ["capitalExpenditures", "capex"],
        },
    }

    def __init__(self, concepts: Optional[Iterable[str]] = None) -> None:
        self.concepts = set(concepts or TARGET_CONCEPTS)

    def normalize(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str] = None,
    ) -> List[FactRecord]:
        if not payload:
            return []

        general = payload.get("General") or {}
        accounting_standard = accounting_standard or general.get("AccountingStandard")
        currency_code = general.get("CurrencyCode")
        records: List[FactRecord] = []

        financials = payload.get("Financials") or {}
        for statement, field_map in self.STATEMENT_FIELDS.items():
            statement_payload = financials.get(statement) or {}
            records.extend(
                self._normalize_statement(
                    statement_payload,
                    field_map,
                    symbol=symbol,
                    accounting_standard=accounting_standard,
                    default_currency=currency_code,
                )
            )

        records.extend(self._normalize_share_counts(payload, symbol, accounting_standard, currency_code))
        records.extend(self._normalize_earnings_eps(payload, symbol, accounting_standard))
        return records

    def _normalize_statement(
        self,
        statement_payload: Dict,
        field_map: Dict[str, List[str]],
        symbol: str,
        accounting_standard: Optional[str],
        default_currency: Optional[str],
    ) -> List[FactRecord]:
        records: List[FactRecord] = []
        for frequency, fiscal_period in (("yearly", "FY"), ("quarterly", None)):
            entries = self._iter_entries(statement_payload.get(frequency))
            for entry in entries:
                end_date = self._extract_date(entry)
                if not end_date:
                    continue
                currency = entry.get("currency_symbol") or default_currency or entry.get("CurrencyCode")
                period_code = fiscal_period or self._infer_quarter(entry)
                frame = self._build_frame(end_date, period_code)
                total_liab = self._extract_value(entry, ["totalLiabilities", "totalLiab"])
                current_liab = self._extract_value(entry, ["totalCurrentLiabilities"])
                derived_debt = None
                if total_liab is not None and current_liab is not None:
                    derived_debt = total_liab - current_liab
                for concept, keys in field_map.items():
                    if concept not in self.concepts:
                        continue
                    value = self._extract_value(entry, keys)
                    if value is None and concept in {"LongTermDebt", "LongTermDebtNoncurrent"}:
                        value = derived_debt
                    if value is None:
                        continue
                    records.append(
                        FactRecord(
                            symbol=symbol.upper(),
                            provider="EODHD",
                            concept=concept,
                            fiscal_period=period_code or "",
                            end_date=end_date,
                            unit=currency or "",
                            value=value,
                            accn=None,
                            filed=entry.get("filing_date"),
                            frame=frame,
                            start_date=None,
                            accounting_standard=accounting_standard,
                            currency=currency,
                        )
                    )
        return records

    def _normalize_share_counts(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str],
        default_currency: Optional[str],
    ) -> List[FactRecord]:
        """Map share stats to outstanding share count facts."""

        stats = payload.get("SharesStats") or {}
        value = stats.get("SharesOutstanding") or stats.get("SharesFloat")
        shares = _to_float(value)
        if shares is None:
            return []
        general = payload.get("General") or {}
        end_date = general.get("LatestQuarter") or general.get("LatestReportDate")
        if not end_date:
            return []
        currency = stats.get("CurrencyCode") or default_currency
        record = FactRecord(
            symbol=symbol.upper(),
            provider="EODHD",
            concept="CommonStockSharesOutstanding",
            fiscal_period="",
            end_date=end_date,
            unit=currency or "",
            value=shares,
            accn=None,
            filed=None,
            frame=None,
            start_date=None,
            accounting_standard=accounting_standard,
            currency=currency,
        )
        return [record] if record else []

    def _normalize_earnings_eps(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str],
    ) -> List[FactRecord]:
        earnings = payload.get("Earnings") or {}
        history = earnings.get("History") or {}
        annual = earnings.get("Annual") or {}
        records: List[FactRecord] = []

        def add_record(date_str: str, value: float, period: str) -> None:
            records.append(
                FactRecord(
                    symbol=symbol.upper(),
                    provider="EODHD",
                    concept="EarningsPerShareDiluted",
                    fiscal_period=period,
                    end_date=date_str,
                    unit="EPS",
                    value=value,
                    accn=None,
                    filed=None,
                    frame=self._build_frame(date_str, period or "FY"),
                    start_date=None,
                    accounting_standard=accounting_standard,
                    currency=None,
                )
            )

        for date_str, entry in history.items():
            val = _to_float(entry.get("epsActual"))
            if val is None:
                continue
            period = self._infer_quarter({"date": date_str}) or ""
            add_record(date_str[:10], val, period)

        for date_str, entry in annual.items():
            val = _to_float(entry.get("epsActual"))
            if val is None:
                continue
            add_record(date_str[:10], val, "FY")

        return records

    def _extract_value(self, entry: Dict, keys: List[str]) -> Optional[float]:
        lowered = {k.lower(): entry[k] for k in entry.keys() if isinstance(k, str)}
        for key in keys:
            if key in entry:
                value = _to_float(entry.get(key))
            elif key.lower() in lowered:
                value = _to_float(lowered[key.lower()])
            else:
                value = None
            if value is not None:
                return value
        return None

    def _build_frame(self, end_date: Optional[str], period: Optional[str]) -> Optional[str]:
        if not end_date:
            return None
        year = end_date[:4]
        if not year.isdigit():
            return None
        period = (period or "").upper()
        if period in {"Q1", "Q2", "Q3", "Q4"}:
            return f"CY{year}{period}"
        return f"CY{year}"

    def _extract_date(self, entry: Dict) -> Optional[str]:
        date = entry.get("date") or entry.get("Date") or entry.get("period")
        if not date:
            return None
        try:
            datetime.fromisoformat(str(date)[:10])
        except ValueError:
            return None
        return str(date)[:10]

    def _infer_quarter(self, entry: Dict) -> Optional[str]:
        explicit = (entry.get("period") or "").upper()
        if explicit in {"Q1", "Q2", "Q3", "Q4"}:
            return explicit
        date = self._extract_date(entry)
        if not date:
            return None
        try:
            month = int(date.split("-")[1])
        except (IndexError, ValueError):
            return None
        if month <= 3:
            return "Q1"
        if month <= 6:
            return "Q2"
        if month <= 9:
            return "Q3"
        return "Q4"

    def _iter_entries(self, container) -> Iterable[Dict]:
        if container is None:
            return []
        if isinstance(container, dict):
            values = container.values()
        elif isinstance(container, list):
            values = container
        else:
            return []
        return [entry for entry in values if isinstance(entry, dict)]


__all__ = ["EODHDFactsNormalizer"]
