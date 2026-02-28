"""Normalize EODHD fundamentals into FactRecord entries.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

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
EPS_UNIT_FLIP_RATIO_MIN = 40.0
EPS_UNIT_FLIP_RATIO_MAX = 140.0
EPS_MIN_ABS_FOR_UNIT_CHECK = 0.05
EPS_IMPLIED_MIN_MATCHES = 2
EPS_IMPLIED_RATIO_NEAR_ONE = 2.0
EPS_IMPLIED_MAX_GAP_DAYS_Q = 120
EPS_IMPLIED_MAX_GAP_DAYS_FY = 370
EPS_STATEMENT_KEYS = (
    "epsDiluted",
    "epsdiluted",
    "epsDilluted",
    "eps",
    "epsBasic",
)
NET_INCOME_KEYS = (
    "netIncomeApplicableToCommonShares",
    "netIncome",
    "netIncomeFromContinuingOps",
)
INCOME_STATEMENT_SHARES_KEYS = (
    "weightedAverageShsOutDil",
    "weightedAverageShsOutDiluted",
    "weightedAverageShsOut",
    "weightedAverageShsOutBasic",
)
BALANCE_SHEET_SHARES_KEYS = ("commonStockSharesOutstanding", "shareIssued")

EODHD_STATEMENT_FIELDS = {
    "Balance_Sheet": {
        "AssetsCurrent": ["totalCurrentAssets"],
        "LiabilitiesCurrent": ["totalCurrentLiabilities"],
        "Assets": ["totalAssets"],
        "Liabilities": ["totalLiabilities", "totalLiab"],
        "StockholdersEquity": ["totalStockholderEquity", "totalShareholderEquity"],
        "CommonStockholdersEquity": ["commonStockTotalEquity"],
        "PreferredStock": [
            "preferredStockTotalEquity",
            "preferredStockRedeemable",
            "preferredStock",
            "capitalStock",
        ],
        "Goodwill": ["goodWill", "goodwill"],
        "IntangibleAssetsNet": ["intangibleAssets"],
        "NetTangibleAssets": ["netTangibleAssets"],
        "NoncontrollingInterestInConsolidatedEntity": [
            "noncontrollingInterestInConsolidatedEntity"
        ],
        "CashAndShortTermInvestments": ["cashAndShortTermInvestments"],
        "ShortTermDebt": ["shortTermDebt", "shortLongTermDebt"],
        "LongTermDebtNoncurrent": [
            "longTermDebtNoncurrent",
            "longTermDebtTotal",
            "longTermDebt",
        ],
        "LongTermDebt": [
            "longTermDebtTotal",
            "longTermDebt",
            "longTermDebtNoncurrent",
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
        "EBITDA": ["ebitda", "EBITDA"],
        "DepreciationDepletionAndAmortization": [
            "depreciationAndAmortization",
            "reconciledDepreciation",
        ],
        "IncomeTaxExpense": ["incomeTaxExpense"],
        "InterestExpense": ["interestExpense"],
        "NetIncomeLoss": ["netIncome", "netIncomeFromContinuingOps"],
        "NetIncomeLossAvailableToCommonStockholdersBasic": [
            "netIncomeApplicableToCommonShares"
        ],
        "PreferredStockDividendsAndOtherAdjustments": [
            "preferredStockAndOtherAdjustments"
        ],
        "OperatingIncomeLoss": ["operatingIncome", "ebit"],
        "IncomeBeforeIncomeTaxes": ["incomeBeforeTax"],
        "Revenues": ["totalRevenue", "revenue"],
        "EarningsPerShareDiluted": ["epsDiluted", "epsdiluted", "epsDilluted"],
        "EarningsPerShareBasic": ["eps", "epsBasic"],
        "WeightedAverageNumberOfDilutedSharesOutstanding": [
            "weightedAverageShsOutDil",
            "weightedAverageShsOutDiluted",
        ],
        "WeightedAverageNumberOfSharesOutstandingBasic": [
            "weightedAverageShsOut",
            "weightedAverageShsOutBasic",
        ],
    },
    "Cash_Flow": {
        "NetCashProvidedByUsedInOperatingActivities": [
            "totalCashFromOperatingActivities",
        ],
        "CapitalExpenditures": ["capitalExpenditures", "capex"],
        "DepreciationFromCashFlow": ["depreciation"],
    },
}

EODHD_TARGET_CONCEPTS = {
    concept for statement in EODHD_STATEMENT_FIELDS.values() for concept in statement
}
EODHD_DERIVED_OVERRIDE_CONCEPTS = ("CommonStockholdersEquity",)


def _to_float(value: Any) -> Optional[float]:
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

    def __init__(
        self,
        concepts: Optional[Iterable[str]] = None,
        derived_overrides: Optional[Iterable[str]] = None,
    ) -> None:
        self.concepts = set(concepts or EODHD_TARGET_CONCEPTS)
        if derived_overrides is None:
            self.derived_overrides = set(EODHD_DERIVED_OVERRIDE_CONCEPTS)
        else:
            self.derived_overrides = set(derived_overrides)

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
                    default_currency=self._normalize_statement_currency(
                        statement_payload, currency_code
                    ),
                )
            )

        records.extend(
            self._normalize_share_counts(
                payload, symbol, accounting_standard, currency_code
            )
        )
        records.extend(
            self._normalize_outstanding_shares(
                payload, symbol, accounting_standard, currency_code
            )
        )
        records.extend(
            self._normalize_earnings_eps(payload, symbol, accounting_standard)
        )
        records = self._extend_with_override(
            records,
            self._derive_eps_alias(records),
            "EarningsPerShare",
        )
        records = self._extend_with_override(
            records,
            self._derive_intangibles_excluding_goodwill(records),
            "IntangibleAssetsNetExcludingGoodwill",
        )
        records = self._extend_with_override(
            records,
            self._derive_equity_alias(records),
            "StockholdersEquity",
        )
        records = self._extend_with_override(
            records,
            self._derive_shares_alias(records),
            "CommonStockSharesOutstanding",
        )
        records = self._extend_with_override(
            records,
            self._derive_operating_cash_flow_alias(records),
            "NetCashProvidedByUsedInOperatingActivities",
        )
        records = self._extend_with_override(
            records,
            self._derive_capex_alias(records),
            "CapitalExpenditures",
        )
        records = self._extend_with_override(
            records,
            self._derive_ebit_alias(records),
            "OperatingIncomeLoss",
        )
        records = self._extend_with_override(
            records,
            self._derive_ppe_alias(records),
            "PropertyPlantAndEquipmentNet",
        )
        records = self._extend_with_override(
            records,
            self._derive_net_income_available_to_common(records),
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        )
        records = self._extend_with_override(
            records,
            self._derive_common_stockholders_equity(records),
            "CommonStockholdersEquity",
        )
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
                total_liab = self._extract_value(
                    entry, ["totalLiabilities", "totalLiab"]
                )
                current_liab = self._extract_value(entry, ["totalCurrentLiabilities"])
                derived_debt = None
                if total_liab is not None and current_liab is not None:
                    candidate = total_liab - current_liab
                    if candidate >= 0:
                        derived_debt = candidate

                derived_current_assets = None
                if "AssetsCurrent" in field_map:
                    total_assets = self._extract_value(entry, ["totalAssets"])
                    noncurrent_assets = self._extract_value(
                        entry, ["nonCurrentAssetsTotal"]
                    )
                    if total_assets is not None and noncurrent_assets is not None:
                        candidate = total_assets - noncurrent_assets
                        if candidate >= 0:
                            derived_current_assets = candidate
                    if derived_current_assets is None:
                        cash_bucket = self._extract_value(
                            entry, ["cashAndShortTermInvestments"]
                        )
                        short_term_investments = None
                        if cash_bucket is None:
                            short_term_investments = self._extract_value(
                                entry, ["shortTermInvestments"]
                            )
                            cash_bucket = self._extract_value(
                                entry, ["cashAndEquivalents", "cash"]
                            )
                        receivables = self._extract_value(entry, ["netReceivables"])
                        inventory = self._extract_value(entry, ["inventory"])
                        other_current = self._extract_value(
                            entry, ["otherCurrentAssets"]
                        )
                        components = [
                            cash_bucket,
                            short_term_investments,
                            receivables,
                            inventory,
                            other_current,
                        ]
                        if any(item is not None for item in components):
                            derived_current_assets = sum(
                                item or 0.0 for item in components
                            )

                derived_current_liab = None
                if "LiabilitiesCurrent" in field_map:
                    noncurrent_liab = self._extract_value(
                        entry, ["nonCurrentLiabilitiesTotal"]
                    )
                    if total_liab is not None and noncurrent_liab is not None:
                        candidate = total_liab - noncurrent_liab
                        if candidate >= 0:
                            derived_current_liab = candidate
                    if derived_current_liab is None:
                        accounts_payable = self._extract_value(
                            entry, ["accountsPayable"]
                        )
                        other_current = self._extract_value(entry, ["otherCurrentLiab"])
                        deferred_revenue = self._extract_value(
                            entry, ["currentDeferredRevenue"]
                        )
                        short_term_debt = self._extract_value(entry, ["shortTermDebt"])
                        short_long_term_debt = None
                        if short_term_debt is None:
                            short_long_term_debt = self._extract_value(
                                entry, ["shortLongTermDebt"]
                            )
                        components = [
                            accounts_payable,
                            other_current,
                            deferred_revenue,
                            short_term_debt,
                            short_long_term_debt,
                        ]
                        if any(item is not None for item in components):
                            derived_current_liab = sum(
                                item or 0.0 for item in components
                            )

                derived_ppe = None
                if "PropertyPlantAndEquipmentNet" in field_map:
                    gross = self._extract_value(
                        entry, ["propertyPlantAndEquipmentGross"]
                    )
                    accumulated = self._extract_value(
                        entry, ["accumulatedDepreciation"]
                    )
                    if gross is not None and accumulated is not None:
                        candidate = gross - accumulated
                        if candidate >= 0:
                            derived_ppe = candidate

                derived_operating_income = None
                if "OperatingIncomeLoss" in field_map:
                    income_before_tax = self._extract_value(entry, ["incomeBeforeTax"])
                    interest_expense = self._extract_value(entry, ["interestExpense"])
                    interest_income = self._extract_value(entry, ["interestIncome"])
                    if income_before_tax is not None and interest_expense is not None:
                        derived_operating_income = (
                            income_before_tax
                            + interest_expense
                            - (interest_income or 0.0)
                        )
                    if derived_operating_income is None:
                        total_revenue = self._extract_value(entry, ["totalRevenue"])
                        total_operating_expenses = self._extract_value(
                            entry, ["totalOperatingExpenses"]
                        )
                        if (
                            total_revenue is not None
                            and total_operating_expenses is not None
                        ):
                            derived_operating_income = (
                                total_revenue - total_operating_expenses
                            )

                derived_capex = None
                if "CapitalExpenditures" in field_map:
                    operating_cash = self._extract_value(
                        entry, ["totalCashFromOperatingActivities"]
                    )
                    free_cash_flow = self._extract_value(entry, ["freeCashFlow"])
                    if operating_cash is not None and free_cash_flow is not None:
                        derived_capex = operating_cash - free_cash_flow

                derived_operating_cash = None
                if "NetCashProvidedByUsedInOperatingActivities" in field_map:
                    free_cash_flow = self._extract_value(entry, ["freeCashFlow"])
                    capex_value = self._extract_value(
                        entry, ["capitalExpenditures", "capex"]
                    )
                    if free_cash_flow is not None and capex_value is not None:
                        derived_operating_cash = free_cash_flow + capex_value
                for concept, keys in field_map.items():
                    if concept not in self.concepts:
                        continue
                    value = self._extract_value(entry, keys)
                    if value is None and concept == "AssetsCurrent":
                        value = derived_current_assets
                    if value is None and concept == "LiabilitiesCurrent":
                        value = derived_current_liab
                    if value is None and concept == "LongTermDebt":
                        value = derived_debt
                    if value is None and concept == "PropertyPlantAndEquipmentNet":
                        value = derived_ppe
                    if value is None and concept == "OperatingIncomeLoss":
                        value = derived_operating_income
                    if value is None and concept == "CapitalExpenditures":
                        value = derived_capex
                    if (
                        value is None
                        and concept == "NetCashProvidedByUsedInOperatingActivities"
                    ):
                        value = derived_operating_cash
                    if value is None:
                        continue
                    normalized_value, normalized_currency = (
                        self._normalize_value_currency(value, currency)
                    )
                    if normalized_value is None:
                        continue
                    records.append(
                        FactRecord(
                            symbol=symbol.upper(),
                            concept=concept,
                            fiscal_period=period_code or "",
                            end_date=end_date,
                            unit=currency or "",
                            value=normalized_value,
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
        currency = _normalize_currency_code(
            stats.get("CurrencyCode") or default_currency
        )
        record = FactRecord(
            symbol=symbol.upper(),
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

    def _normalize_outstanding_shares(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str],
        default_currency: Optional[str],
    ) -> List[FactRecord]:
        shares_payload = payload.get("outstandingShares") or {}
        if not shares_payload:
            return []

        currency = _normalize_currency_code(default_currency)
        records: List[FactRecord] = []
        for bucket, fiscal_period in (("annual", "FY"), ("quarterly", None)):
            entries = self._iter_entries(shares_payload.get(bucket))
            for entry in entries:
                date_value = entry.get("dateFormatted") or entry.get("date")
                end_date = (
                    self._extract_date({"date": date_value}) if date_value else None
                )
                if (
                    not end_date
                    and isinstance(date_value, str)
                    and date_value.isdigit()
                    and len(date_value) == 4
                ):
                    end_date = f"{date_value}-12-31"
                if not end_date:
                    continue
                shares = _to_float(entry.get("shares"))
                if shares is None:
                    shares_mln = _to_float(entry.get("sharesMln"))
                    if shares_mln is not None:
                        shares = shares_mln * 1_000_000
                if shares is None:
                    continue
                period = fiscal_period or self._infer_quarter({"date": end_date}) or ""
                frame = self._build_frame(end_date, period or "FY")
                records.append(
                    FactRecord(
                        symbol=symbol.upper(),
                        concept="CommonStockSharesOutstanding",
                        fiscal_period=period,
                        end_date=end_date,
                        unit=currency or "",
                        value=shares,
                        accn=None,
                        filed=None,
                        frame=frame,
                        start_date=None,
                        accounting_standard=accounting_standard,
                        currency=currency,
                    )
                )
        return records

    def _build_implied_eps_maps(
        self, payload: Dict
    ) -> tuple[Dict[str, float], Dict[str, float]]:
        financials = payload.get("Financials") or {}
        income = financials.get("Income_Statement") or {}
        balance = financials.get("Balance_Sheet") or {}

        net_income_quarterly = self._build_net_income_map(income.get("quarterly"))
        net_income_annual = self._build_net_income_map(income.get("yearly"))

        shares_quarterly = self._build_income_statement_shares_map(
            income.get("quarterly")
        )
        shares_annual = self._build_income_statement_shares_map(income.get("yearly"))

        shares_quarterly = self._merge_missing(
            shares_quarterly,
            self._build_outstanding_shares_map(
                payload.get("outstandingShares"), "quarterly"
            ),
        )
        shares_annual = self._merge_missing(
            shares_annual,
            self._build_outstanding_shares_map(
                payload.get("outstandingShares"), "annual"
            ),
        )
        shares_quarterly = self._merge_missing(
            shares_quarterly,
            self._build_balance_sheet_shares_map(balance.get("quarterly")),
        )
        shares_annual = self._merge_missing(
            shares_annual,
            self._build_balance_sheet_shares_map(balance.get("yearly")),
        )

        return (
            self._build_implied_eps_map(
                net_income_quarterly,
                shares_quarterly,
                max_gap_days=EPS_IMPLIED_MAX_GAP_DAYS_Q,
            ),
            self._build_implied_eps_map(
                net_income_annual,
                shares_annual,
                max_gap_days=EPS_IMPLIED_MAX_GAP_DAYS_FY,
            ),
        )

    def _merge_missing(
        self, target: Dict[str, float], fallback: Dict[str, float]
    ) -> Dict[str, float]:
        for date_str, value in fallback.items():
            target.setdefault(date_str, value)
        return target

    def _build_net_income_map(self, entries) -> Dict[str, float]:
        net_income: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, list(NET_INCOME_KEYS))
            if value is None:
                continue
            net_income[date_str] = value
        return net_income

    def _build_income_statement_shares_map(self, entries) -> Dict[str, float]:
        shares: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, list(INCOME_STATEMENT_SHARES_KEYS))
            if value is None:
                continue
            shares[date_str] = value
        return shares

    def _collect_statement_eps_dates(self, entries) -> set[str]:
        dates: set[str] = set()
        for key, entry in self._iter_entries_with_keys(entries):
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, list(EPS_STATEMENT_KEYS))
            if value is None:
                continue
            dates.add(date_str)
        return dates

    def _build_balance_sheet_shares_map(self, entries) -> Dict[str, float]:
        shares: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, list(BALANCE_SHEET_SHARES_KEYS))
            if value is None:
                continue
            shares[date_str] = value
        return shares

    def _build_outstanding_shares_map(
        self, shares_payload: Optional[Dict], bucket: str
    ) -> Dict[str, float]:
        if not shares_payload:
            return {}
        entries = shares_payload.get(bucket)
        shares: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            date_value = entry.get("dateFormatted") or entry.get("date") or key
            end_date = self._extract_date({"date": date_value}) if date_value else None
            if (
                not end_date
                and isinstance(date_value, str)
                and date_value.isdigit()
                and len(date_value) == 4
            ):
                end_date = f"{date_value}-12-31"
            if not end_date:
                continue
            value = _to_float(entry.get("shares"))
            if value is None:
                shares_mln = _to_float(entry.get("sharesMln"))
                if shares_mln is not None:
                    value = shares_mln * 1_000_000
            if value is None:
                continue
            shares[end_date] = value
        return shares

    def _build_implied_eps_map(
        self,
        net_income: Dict[str, float],
        shares: Dict[str, float],
        max_gap_days: Optional[int] = None,
    ) -> Dict[str, float]:
        implied: Dict[str, float] = {}
        share_dates: List[tuple[datetime, float]] = []
        if max_gap_days is not None:
            for date_str, share_value in shares.items():
                share_date = self._parse_date_value(date_str)
                if share_date:
                    share_dates.append((share_date, share_value))
        for date_str, income in net_income.items():
            share_count = shares.get(date_str)
            if share_count is None and max_gap_days is not None and share_dates:
                income_date = self._parse_date_value(date_str)
                if income_date:
                    nearest_date, nearest_value = min(
                        share_dates,
                        key=lambda item: abs((item[0] - income_date).days),
                    )
                    if abs((nearest_date - income_date).days) <= max_gap_days:
                        share_count = nearest_value
            if share_count is None or share_count == 0:
                continue
            implied[date_str] = income / share_count
        return implied

    def _normalize_earnings_eps(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str],
    ) -> List[FactRecord]:
        earnings = payload.get("Earnings") or {}
        history = earnings.get("History") or {}
        annual = earnings.get("Annual") or {}
        general = payload.get("General") or {}
        general_currency = _normalize_currency_code(general.get("CurrencyCode"))
        earnings_currency = self._latest_earnings_currency(history, annual)
        income_statement = (payload.get("Financials") or {}).get(
            "Income_Statement"
        ) or {}
        statement_currency = (
            _normalize_currency_code(income_statement.get("currency_symbol"))
            or general_currency
        )
        statement_eps_quarterly = self._collect_statement_eps_dates(
            income_statement.get("quarterly")
        )
        statement_eps_annual = self._collect_statement_eps_dates(
            income_statement.get("yearly")
        )
        history_eps_dates: set[str] = set()
        for date_str, entry in history.items():
            if _to_float((entry or {}).get("epsActual")) is None:
                continue
            if isinstance(entry, dict):
                normalized = self._extract_date(entry)
            else:
                normalized = None
            normalized = (
                normalized
                or self._extract_date({"date": date_str})
                or str(date_str)[:10]
            )
            history_eps_dates.add(normalized)

        annual_eps_dates: set[str] = set()
        for date_str, entry in annual.items():
            if _to_float((entry or {}).get("epsActual")) is None:
                continue
            if isinstance(entry, dict):
                normalized = self._extract_date(entry)
            else:
                normalized = None
            normalized = (
                normalized
                or self._extract_date({"date": date_str})
                or str(date_str)[:10]
            )
            annual_eps_dates.add(normalized)
        implied_quarterly, implied_annual = self._build_implied_eps_maps(payload)
        records: List[FactRecord] = []

        def add_record(
            date_str: str, value: float, period: str, currency_hint: Optional[str]
        ) -> None:
            currency = (
                _normalize_currency_code(currency_hint)
                or earnings_currency
                or general_currency
            )
            normalized_value, normalized_currency = self._normalize_value_currency(
                value, currency
            )
            if normalized_value is None:
                return
            records.append(
                FactRecord(
                    symbol=symbol.upper(),
                    concept="EarningsPerShareDiluted",
                    fiscal_period=period,
                    end_date=date_str,
                    unit="EPS",
                    value=normalized_value,
                    accn=None,
                    filed=None,
                    frame=self._build_frame(date_str, period or "FY"),
                    start_date=None,
                    accounting_standard=accounting_standard,
                    currency=normalized_currency,
                )
            )

        def add_fallback(
            implied_map: Dict[str, float],
            existing_dates: set[str],
            statement_dates: set[str],
            period_hint: Optional[str],
        ) -> None:
            for date_str, value in implied_map.items():
                if date_str in existing_dates or date_str in statement_dates:
                    continue
                period = period_hint or (self._infer_quarter({"date": date_str}) or "")
                if not period:
                    continue
                add_record(date_str, value, period, statement_currency)

        if general_currency in {"GBP", "GBX", "GBP0.01"}:
            for date_str, value, currency in self._normalize_eps_series(
                history, general_currency, implied_quarterly
            ):
                period = self._infer_quarter({"date": date_str}) or ""
                add_record(date_str, value, period, currency)
            for date_str, value, currency in self._normalize_eps_series(
                annual, general_currency, implied_annual
            ):
                add_record(date_str, value, "FY", currency)
        else:
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

        add_fallback(
            implied_quarterly, history_eps_dates, statement_eps_quarterly, None
        )
        add_fallback(implied_annual, annual_eps_dates, statement_eps_annual, "FY")

        return records

    def _normalize_eps_series(
        self,
        entries: Dict,
        base_currency: Optional[str],
        implied_eps: Optional[Dict[str, float]] = None,
    ) -> List[tuple[str, float, Optional[str]]]:
        ordered: List[tuple[str, Dict]] = []
        if isinstance(entries, dict):
            items = entries.items()
        elif isinstance(entries, list):
            items = [
                (entry.get("date") or entry.get("Date") or entry.get("period"), entry)
                for entry in entries
            ]
        else:
            items = []
        for key, entry in items:
            if not isinstance(entry, dict):
                continue
            date_str = (
                self._extract_date(entry)
                or self._extract_date({"date": key})
                or str(key or "")
            )
            if not date_str:
                continue
            ordered.append((date_str[:10], entry))
        ordered.sort(key=lambda item: item[0])

        normalized_base = _normalize_currency_code(base_currency)
        target_currency = normalized_base or "GBP"
        scale = 1.0
        if normalized_base in {"GBX", "GBP0.01"}:
            scale = 0.01
        elif normalized_base == "GBP":
            scale = 1.0

        if target_currency != "GBP":
            normalized_entries: List[tuple[str, float, Optional[str]]] = []
            for date_str, entry in ordered:
                value = _to_float(entry.get("epsActual"))
                if value is None:
                    continue
                currency = (
                    _normalize_currency_code(entry.get("currency")) or normalized_base
                )
                normalized_value, normalized_currency = self._normalize_value_currency(
                    value, currency
                )
                if normalized_value is None:
                    continue
                normalized_entries.append(
                    (date_str, normalized_value, normalized_currency)
                )
            return normalized_entries

        normalized_scaled: List[tuple[str, float, Optional[str]]] = []
        values: List[float] = []
        dates: List[str] = []
        for date_str, entry in ordered:
            value = _to_float(entry.get("epsActual"))
            if value is None:
                continue
            values.append(value)
            dates.append(date_str)

        default_scale = scale
        implied_scale = self._infer_eps_scale_from_implied(
            values, dates, implied_eps, default_scale
        )
        if implied_scale is not None:
            default_scale = implied_scale

        boundaries: List[int] = []
        for idx in range(1, len(values)):
            prev = values[idx - 1]
            curr = values[idx]
            if (
                abs(prev) < EPS_MIN_ABS_FOR_UNIT_CHECK
                or abs(curr) < EPS_MIN_ABS_FOR_UNIT_CHECK
            ):
                continue
            ratio = max(abs(curr) / abs(prev), abs(prev) / abs(curr))
            if EPS_UNIT_FLIP_RATIO_MIN <= ratio <= EPS_UNIT_FLIP_RATIO_MAX:
                boundaries.append(idx)

        segment_starts = [0] + boundaries
        segment_ends = boundaries + [len(values)]
        segment_medians: List[Optional[float]] = []
        for start, end in zip(segment_starts, segment_ends):
            segment = [
                abs(values[i])
                for i in range(start, end)
                if abs(values[i]) >= EPS_MIN_ABS_FOR_UNIT_CHECK
            ]
            if not segment:
                segment_medians.append(None)
                continue
            segment.sort()
            mid = len(segment) // 2
            if len(segment) % 2 == 0:
                median = (segment[mid - 1] + segment[mid]) / 2
            else:
                median = segment[mid]
            segment_medians.append(median)

        min_median = min((m for m in segment_medians if m is not None), default=None)
        max_median = max((m for m in segment_medians if m is not None), default=None)
        use_clusters = False
        if min_median is not None and max_median is not None and min_median > 0:
            ratio = max_median / min_median
            if EPS_UNIT_FLIP_RATIO_MIN <= ratio <= EPS_UNIT_FLIP_RATIO_MAX:
                use_clusters = True
                threshold = (min_median * max_median) ** 0.5

        segment_scales: List[float] = []
        for segment_median in segment_medians:
            if not use_clusters or segment_median is None:
                segment_scales.append(default_scale)
            else:
                # Smaller cluster is treated as GBP, larger cluster as GBX (pence).
                segment_scales.append(1.0 if segment_median <= threshold else 0.01)

        for seg_index, (start, end) in enumerate(zip(segment_starts, segment_ends)):
            seg_scale = segment_scales[seg_index]
            for idx in range(start, end):
                scaled = values[idx] * seg_scale
                normalized_scaled.append((dates[idx], scaled, "GBP"))
        return normalized_scaled

    def _infer_eps_scale_from_implied(
        self,
        values: List[float],
        dates: List[str],
        implied_eps: Optional[Dict[str, float]],
        base_scale: float,
    ) -> Optional[float]:
        if not implied_eps:
            return None
        ratios: List[float] = []
        for date_str, value in zip(dates, values):
            implied = implied_eps.get(date_str)
            if implied is None:
                continue
            if (
                abs(value) < EPS_MIN_ABS_FOR_UNIT_CHECK
                or abs(implied) < EPS_MIN_ABS_FOR_UNIT_CHECK
            ):
                continue
            ratios.append(abs(value) / abs(implied))
        if len(ratios) < EPS_IMPLIED_MIN_MATCHES:
            return None
        ratios.sort()
        mid = len(ratios) // 2
        if len(ratios) % 2 == 0:
            median = (ratios[mid - 1] + ratios[mid]) / 2
        else:
            median = ratios[mid]
        if EPS_UNIT_FLIP_RATIO_MIN <= median <= EPS_UNIT_FLIP_RATIO_MAX:
            return 0.01
        if (
            base_scale == 0.01
            and (1.0 / EPS_IMPLIED_RATIO_NEAR_ONE)
            <= median
            <= EPS_IMPLIED_RATIO_NEAR_ONE
        ):
            return 1.0
        return None

    def _index_records(
        self, records: List[FactRecord]
    ) -> Dict[str, Dict[tuple[str, str, str], FactRecord]]:
        indexed: Dict[str, Dict[tuple[str, str, str], FactRecord]] = {}
        for record in records:
            key = (record.end_date, record.fiscal_period or "", record.unit)
            bucket = indexed.setdefault(record.concept, {})
            if key not in bucket:
                bucket[key] = record
        return indexed

    def _should_override(self, concept: str) -> bool:
        return concept in self.derived_overrides

    def _extend_with_override(
        self,
        records: List[FactRecord],
        derived: List[FactRecord],
        concept: str,
    ) -> List[FactRecord]:
        if not derived:
            return records
        if self._should_override(concept):
            derived_keys = {
                (rec.end_date, rec.fiscal_period or "", rec.unit) for rec in derived
            }
            records = [
                rec
                for rec in records
                if not (
                    rec.concept == concept
                    and (rec.end_date, rec.fiscal_period or "", rec.unit)
                    in derived_keys
                )
            ]
        records.extend(derived)
        return records

    def _derive_eps_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("EarningsPerShare", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in EPS_PREFERRED_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override("EarningsPerShare")
        for key in candidate_keys:
            if key in existing and not override:
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

    def _derive_intangibles_excluding_goodwill(
        self, records: List[FactRecord]
    ) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("IntangibleAssetsNetExcludingGoodwill", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in INTANGIBLE_EXCL_GOODWILL_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())
        net_tangible = indexed.get("NetTangibleAssets", {})
        assets = indexed.get("Assets", {})
        liabilities = indexed.get("Liabilities", {})
        goodwill = indexed.get("Goodwill", {})
        candidate_keys.update(net_tangible.keys())

        derived: List[FactRecord] = []
        override = self._should_override("IntangibleAssetsNetExcludingGoodwill")
        for key in candidate_keys:
            if key in existing and not override:
                continue
            base = None
            for concept in INTANGIBLE_EXCL_GOODWILL_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is not None and base.value is not None:
                derived.append(
                    self._alias_record(base, "IntangibleAssetsNetExcludingGoodwill")
                )
                continue
            net_tangible_rec = net_tangible.get(key)
            assets_rec = assets.get(key)
            liabilities_rec = liabilities.get(key)
            if (
                net_tangible_rec
                and assets_rec
                and liabilities_rec
                and net_tangible_rec.value is not None
                and assets_rec.value is not None
                and liabilities_rec.value is not None
            ):
                goodwill_rec = goodwill.get(key)
                goodwill_value = (
                    goodwill_rec.value
                    if goodwill_rec and goodwill_rec.value is not None
                    else 0.0
                )
                equity_value = assets_rec.value - liabilities_rec.value
                candidate = equity_value - net_tangible_rec.value - goodwill_value
                if candidate >= 0:
                    derived.append(
                        FactRecord(
                            symbol=net_tangible_rec.symbol,
                            cik=net_tangible_rec.cik,
                            concept="IntangibleAssetsNetExcludingGoodwill",
                            fiscal_period=net_tangible_rec.fiscal_period,
                            end_date=net_tangible_rec.end_date,
                            unit=net_tangible_rec.unit,
                            value=candidate,
                            accn=net_tangible_rec.accn,
                            filed=net_tangible_rec.filed,
                            frame=net_tangible_rec.frame,
                            start_date=net_tangible_rec.start_date,
                            accounting_standard=net_tangible_rec.accounting_standard,
                            currency=net_tangible_rec.currency,
                        )
                    )
        return derived

    def _derive_equity_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("StockholdersEquity", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        assets = indexed.get("Assets", {})
        liabilities = indexed.get("Liabilities", {})
        candidate_keys.update(assets.keys())
        candidate_keys.update(liabilities.keys())
        for concept in EQUITY_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override("StockholdersEquity")
        derived_keys = set() if override else set(existing.keys())
        for key in candidate_keys:
            if key in derived_keys:
                continue
            assets_rec = assets.get(key)
            liabilities_rec = liabilities.get(key)
            if (
                assets_rec
                and liabilities_rec
                and assets_rec.value is not None
                and liabilities_rec.value is not None
            ):
                value = assets_rec.value - liabilities_rec.value
                if value >= 0:
                    derived.append(
                        FactRecord(
                            symbol=assets_rec.symbol,
                            cik=assets_rec.cik,
                            concept="StockholdersEquity",
                            fiscal_period=assets_rec.fiscal_period,
                            end_date=assets_rec.end_date,
                            unit=assets_rec.unit,
                            value=value,
                            accn=assets_rec.accn,
                            filed=assets_rec.filed,
                            frame=assets_rec.frame,
                            start_date=assets_rec.start_date,
                            accounting_standard=assets_rec.accounting_standard,
                            currency=assets_rec.currency,
                        )
                    )
                    derived_keys.add(key)
                    continue
            base = None
            for concept in EQUITY_FALLBACK_CONCEPTS:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "StockholdersEquity"))
            derived_keys.add(key)
        return derived

    def _derive_shares_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CommonStockSharesOutstanding", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in SHARES_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override("CommonStockSharesOutstanding")
        for key in candidate_keys:
            if key in existing and not override:
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

    def _derive_operating_cash_flow_alias(
        self, records: List[FactRecord]
    ) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("NetCashProvidedByUsedInOperatingActivities", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in OPERATING_CASH_FLOW_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override("NetCashProvidedByUsedInOperatingActivities")
        for key in candidate_keys:
            if key in existing and not override:
                continue
            base = None
            for concept in OPERATING_CASH_FLOW_FALLBACK:
                base = indexed.get(concept, {}).get(key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(
                self._alias_record(base, "NetCashProvidedByUsedInOperatingActivities")
            )
        return derived

    def _derive_capex_alias(self, records: List[FactRecord]) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CapitalExpenditures", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in CAPEX_FALLBACK_CONCEPTS:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override("CapitalExpenditures")
        for key in candidate_keys:
            if key in existing and not override:
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
        override = self._should_override("OperatingIncomeLoss")
        for key in candidate_keys:
            if key in existing and not override:
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
        override = self._should_override("PropertyPlantAndEquipmentNet")
        for key in candidate_keys:
            if key in existing and not override:
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

    def _derive_net_income_available_to_common(
        self, records: List[FactRecord]
    ) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("NetIncomeLossAvailableToCommonStockholdersBasic", {})
        candidate_keys: set[tuple[str, str, str]] = set(existing.keys())
        for concept in INCOME_AVAILABLE_TO_COMMON_FALLBACK:
            candidate_keys.update(indexed.get(concept, {}).keys())

        derived: List[FactRecord] = []
        override = self._should_override(
            "NetIncomeLossAvailableToCommonStockholdersBasic"
        )
        for key in candidate_keys:
            if key in existing and not override:
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

    def _derive_common_stockholders_equity(
        self, records: List[FactRecord]
    ) -> List[FactRecord]:
        indexed = self._index_records(records)
        existing = indexed.get("CommonStockholdersEquity", {})
        stockholders_equity = indexed.get("StockholdersEquity", {})
        noncontrolling = indexed.get("NoncontrollingInterestInConsolidatedEntity", {})

        derived: List[FactRecord] = []
        override = self._should_override("CommonStockholdersEquity")
        for key, base in stockholders_equity.items():
            if key in existing and not override:
                continue
            if base.value is None:
                continue
            preferred = indexed.get("PreferredStock", {}).get(key)
            preferred_value = (
                preferred.value if preferred and preferred.value is not None else 0.0
            )
            noncontrolling_rec = noncontrolling.get(key)
            noncontrolling_value = (
                noncontrolling_rec.value
                if noncontrolling_rec and noncontrolling_rec.value is not None
                else 0.0
            )
            adjusted = base.value - preferred_value - noncontrolling_value
            derived.append(
                FactRecord(
                    symbol=base.symbol,
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

    def _build_frame(
        self, end_date: Optional[str], period: Optional[str]
    ) -> Optional[str]:
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

    def _parse_date_value(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value)[:10])
        except ValueError:
            return None

    def _extract_entry_date_keyed(
        self, key: Optional[str], entry: Dict
    ) -> Optional[str]:
        date_str = self._extract_date(entry)
        if date_str:
            return date_str
        if isinstance(key, str):
            return self._extract_date({"date": key})
        return None

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

    def _normalize_statement_currency(
        self, statement_payload: Dict, default: Optional[str]
    ) -> Optional[str]:
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

    def _iter_entries(self, container) -> List[Dict]:
        if container is None:
            return []
        if isinstance(container, dict):
            values = list(container.values())
        elif isinstance(container, list):
            values = container
        else:
            return []
        return [entry for entry in values if isinstance(entry, dict)]

    def _iter_entries_with_keys(self, container) -> List[tuple[Optional[str], Dict]]:
        if container is None:
            return []
        if isinstance(container, dict):
            items = list(container.items())
        elif isinstance(container, list):
            items = [(None, entry) for entry in container]
        else:
            return []
        return [(key, entry) for key, entry in items if isinstance(entry, dict)]


__all__ = [
    "EODHDFactsNormalizer",
    "EODHD_TARGET_CONCEPTS",
    "EODHD_DERIVED_OVERRIDE_CONCEPTS",
]
