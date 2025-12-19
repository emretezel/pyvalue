"""Normalize EODHD fundamentals into FactRecord entries.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pyvalue.storage import FactRecord

EPS_PREFERRED_CONCEPTS = (
    "EarningsPerShareDiluted",
    "EarningsPerShareBasic",
)
INTANGIBLE_EXCL_GOODWILL_FALLBACK = ("IntangibleAssetsNet",)
EQUITY_FALLBACK_CONCEPTS = ("CommonStockholdersEquity",)
SHARES_FALLBACK_CONCEPTS = ("EntityCommonStockSharesOutstanding",)
OPERATING_CASH_FLOW_FALLBACK: tuple[str, ...] = ()
CAPEX_FALLBACK_CONCEPTS: tuple[str, ...] = ()
EBIT_FALLBACK_CONCEPTS: tuple[str, ...] = ()
PPE_FALLBACK_CONCEPTS: tuple[str, ...] = ()
INCOME_AVAILABLE_TO_COMMON_FALLBACK = ("NetIncomeLoss",)
PREFERRED_DIVIDEND_FALLBACK = ("PreferredStockDividendsAndOtherAdjustments",)
COMMON_EQUITY_FALLBACK = ("StockholdersEquity",)

EODHD_STATEMENT_FIELDS = {
    "Balance_Sheet": {
        "AssetsCurrent": ["totalCurrentAssets"],
        "LiabilitiesCurrent": ["totalCurrentLiabilities"],
        "Assets": ["totalAssets"],
        "Liabilities": ["totalLiabilities", "totalLiab"],
        "StockholdersEquity": ["totalStockholderEquity", "totalShareholderEquity"],
        "CommonStockholdersEquity": ["commonStockTotalEquity"],
        "PreferredStock": ["preferredStockTotalEquity", "preferredStockRedeemable", "preferredStock", "capitalStock"],
        "Goodwill": ["goodWill", "goodwill"],
        "IntangibleAssetsNet": ["intangibleAssets"],
        "LongTermDebtNoncurrent": [
            "longTermDebt",
            "longTermDebtNoncurrent",
            "longTermDebtTotal",
        ],
        "LongTermDebt": [
            "shortLongTermDebtTotal",
            "longTermDebtTotal",
            "longTermDebt",
            "shortLongTermDebt",
        ],
        "PropertyPlantAndEquipmentNet": [
            "propertyPlantAndEquipmentNet",
            "propertyPlantEquipment",
            "netPropertyPlantAndEquipment",
            "propertyPlantAndEquipment",
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
        "NetIncomeLoss": ["netIncome", "netIncomeFromContinuingOps"],
        "NetIncomeLossAvailableToCommonStockholdersBasic": ["netIncomeApplicableToCommonShares"],
        "PreferredStockDividendsAndOtherAdjustments": ["preferredStockAndOtherAdjustments"],
        "OperatingIncomeLoss": ["operatingIncome", "ebit"],
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
        ],
        "CapitalExpenditures": ["capitalExpenditures", "capex"],
    },
}

EODHD_TARGET_CONCEPTS = {
    concept for statement in EODHD_STATEMENT_FIELDS.values() for concept in statement
}


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_currency_code(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        code = str(value).strip()
    except Exception:
        return None
    return code.upper() or None


class EODHDFactsNormalizer:
    """Flatten EODHD fundamentals payloads into FactRecord entries."""

    STATEMENT_FIELDS = EODHD_STATEMENT_FIELDS

    def __init__(self, concepts: Optional[Iterable[str]] = None) -> None:
        self.concepts = set(concepts or EODHD_TARGET_CONCEPTS)

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
                    default_currency=self._normalize_statement_currency(statement_payload, currency_code),
                )
            )

        records.extend(self._normalize_share_counts(payload, symbol, accounting_standard, currency_code))
        records.extend(self._normalize_earnings_eps(payload, symbol, accounting_standard))
        records.extend(self._derive_eps_alias(records))
        records.extend(self._derive_intangibles_excluding_goodwill(records))
        records.extend(self._derive_equity_alias(records))
        records.extend(self._derive_shares_alias(records))
        records.extend(self._derive_operating_cash_flow_alias(records))
        records.extend(self._derive_capex_alias(records))
        records.extend(self._derive_ebit_alias(records))
        records.extend(self._derive_ppe_alias(records))
        records.extend(self._derive_net_income_available_to_common(records))
        records.extend(self._derive_common_stockholders_equity(records))
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
                currency = (
                    _normalize_currency_code(entry.get("currency_symbol"))
                    or _normalize_currency_code(default_currency)
                    or _normalize_currency_code(entry.get("CurrencyCode"))
                )
                period_code = fiscal_period or self._infer_quarter(entry)
                frame = self._build_frame(end_date, period_code)
                total_liab = self._extract_value(entry, ["totalLiabilities", "totalLiab"])
                current_liab = self._extract_value(entry, ["totalCurrentLiabilities"])
                derived_debt = None
                if total_liab is not None and current_liab is not None:
                    derived_debt = total_liab - current_liab
                derived_debt, currency = self._normalize_value_currency(derived_debt, currency)
                for concept, keys in field_map.items():
                    if concept not in self.concepts:
                        continue
                    value = self._extract_value(entry, keys)
                    if value is None and concept in {"LongTermDebt", "LongTermDebtNoncurrent"}:
                        value = derived_debt
                    if value is None:
                        continue
                    value, normalized_currency = self._normalize_value_currency(value, currency)
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
                            currency=normalized_currency,
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
        currency = _normalize_currency_code(stats.get("CurrencyCode") or default_currency)
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
        earnings_currency = self._latest_earnings_currency(history, annual)
        records: List[FactRecord] = []

        def add_record(date_str: str, value: float, period: str, currency_hint: Optional[str]) -> None:
            currency = _normalize_currency_code(currency_hint) or earnings_currency
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
                    currency=currency,
                )
            )

        for date_str, entry in history.items():
            val = _to_float(entry.get("epsActual"))
            if val is None:
                continue
            period = self._infer_quarter({"date": date_str}) or ""
            add_record(date_str[:10], val, period, entry.get("currency"))

        for date_str, entry in annual.items():
            val = _to_float(entry.get("epsActual"))
            if val is None:
                continue
            add_record(date_str[:10], val, "FY", entry.get("currency"))

        return records

    def _index_records(self, records: List[FactRecord]) -> Dict[str, Dict[tuple[str, str, str], FactRecord]]:
        indexed: Dict[str, Dict[tuple[str, str, str], FactRecord]] = {}
        for record in records:
            key = (record.end_date, record.fiscal_period or "", record.unit)
            bucket = indexed.setdefault(record.concept, {})
            if key not in bucket:
                bucket[key] = record
        return indexed

    def _derive_eps_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("EarningsPerShare", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in EPS_PREFERRED_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in EPS_PREFERRED_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "EarningsPerShare"))
        return derived

    def _derive_intangibles_excluding_goodwill(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("IntangibleAssetsNetExcludingGoodwill", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in INTANGIBLE_EXCL_GOODWILL_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in INTANGIBLE_EXCL_GOODWILL_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "IntangibleAssetsNetExcludingGoodwill"))
        return derived

    def _derive_equity_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("StockholdersEquity", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in EQUITY_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in EQUITY_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "StockholdersEquity"))
        return derived

    def _derive_shares_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CommonStockSharesOutstanding", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in SHARES_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in SHARES_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "CommonStockSharesOutstanding"))
        return derived

    def _derive_operating_cash_flow_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("NetCashProvidedByUsedInOperatingActivities", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in OPERATING_CASH_FLOW_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in OPERATING_CASH_FLOW_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "NetCashProvidedByUsedInOperatingActivities"))
        return derived

    def _derive_capex_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CapitalExpenditures", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in CAPEX_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in CAPEX_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "CapitalExpenditures"))
        return derived

    def _derive_ebit_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("OperatingIncomeLoss", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in EBIT_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in EBIT_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "OperatingIncomeLoss"))
        return derived

    def _derive_ppe_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("PropertyPlantAndEquipmentNet", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in PPE_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in PPE_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "PropertyPlantAndEquipmentNet"))
        return derived

    def _derive_net_income_available_to_common(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("NetIncomeLossAvailableToCommonStockholdersBasic", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in INCOME_AVAILABLE_TO_COMMON_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in INCOME_AVAILABLE_TO_COMMON_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            preferred_value = None
            for concept in PREFERRED_DIVIDEND_FALLBACK:
                pref = indexed.get(concept, {}).get(key)
                if pref and pref.value is not None:
                    preferred_value = pref.value
                    break
            adjusted = base.value - (preferred_value or 0.0)
            derived.append(
                FactRecord(
                    symbol=base.symbol,
                    provider=base.provider,
                    cik=base.cik,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    fiscal_period=base.fiscal_period,
                    end_date=base.end_date,
                    unit=base.unit,
                    value=adjusted,
                    accn=base.accn,
                    filed=base.filed,
                    frame=base.frame,
                    start_date=base.start_date,
                    accounting_standard=base.accounting_standard,
                    currency=base.currency,
                )
            )
        return derived

    def _derive_common_stockholders_equity(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CommonStockholdersEquity", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in COMMON_EQUITY_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        for key in candidate_keys:
            if key in existing:
                continue
            base = None
            for concept in COMMON_EQUITY_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            preferred = indexed.get("PreferredStock", {}).get(key)
            preferred_value = preferred.value if preferred and preferred.value is not None else 0.0
            adjusted = base.value - preferred_value
            derived.append(
                FactRecord(
                symbol=base.symbol,
                provider=base.provider,
                cik=base.cik,
                concept="CommonStockholdersEquity",
                fiscal_period=base.fiscal_period,
                end_date=base.end_date,
                unit=base.unit,
                value=adjusted,
                accn=base.accn,
                filed=base.filed,
                frame=base.frame,
                start_date=base.start_date,
                accounting_standard=base.accounting_standard,
                currency=base.currency,
                )
            )
        return derived

    def _alias_record(self, base: FactRecord, concept: str) -> FactRecord:
        return FactRecord(
            symbol=base.symbol,
            provider=base.provider,
            cik=base.cik,
            concept=concept,
            fiscal_period=base.fiscal_period,
            end_date=base.end_date,
            unit=base.unit,
            value=base.value,
            accn=base.accn,
            filed=base.filed,
            frame=base.frame,
            start_date=base.start_date,
            accounting_standard=base.accounting_standard,
            currency=base.currency,
        )

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

    def _normalize_value_currency(
        self, value: Optional[float], currency: Optional[str]
    ) -> tuple[Optional[float], Optional[str]]:
        """Normalize GBX/GBP0.01 to GBP and scale values accordingly."""

        if currency in {"GBX", "GBP0.01"}:
            return (value / 100.0) if value is not None else None, "GBP"
        return value, currency

    def _normalize_statement_currency(self, statement_payload: Dict, default: Optional[str]) -> Optional[str]:
        """Prefer an explicit currency_symbol in the statement over General currency."""

        for key in ("yearly", "quarterly"):
            entries = self._iter_entries(statement_payload.get(key))
            for entry in entries:
                code = _normalize_currency_code(entry.get("currency_symbol"))
                if code:
                    return code
        return _normalize_currency_code(default)

    def _latest_earnings_currency(self, history: Dict, annual: Dict) -> Optional[str]:
        """Return the most recent non-null earnings currency."""

        candidates: List[tuple[str, str]] = []
        for date_str, entry in {**history, **annual}.items():
            currency = _normalize_currency_code((entry or {}).get("currency"))
            if not currency:
                continue
            normalized_date = self._extract_date({"date": date_str}) or date_str
            candidates.append((normalized_date, currency))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

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


__all__ = ["EODHDFactsNormalizer", "EODHD_TARGET_CONCEPTS"]
