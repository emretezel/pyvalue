"""Normalize EODHD fundamentals into FactRecord entries.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
import logging

from pyvalue.currency import (
    SHARES_UNIT,
    is_subunit_base_currency,
    normalize_currency_code as shared_normalize_currency_code,
    normalize_monetary_amount,
    raw_currency_code,
    resolve_eodhd_currency,
    subunit_divisor,
    warn_missing_monetary_currency,
)
from pyvalue.fx import FXService
from pyvalue.money import choose_target_currency, convert_money_value
from pyvalue.storage import FactRecord

FactKey = tuple[str, str, str]
FactIndex = Dict[str, Dict[FactKey, FactRecord]]
FactPeriodKey = tuple[str, str]

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
        "CashAndCashEquivalents": ["cashAndEquivalents", "cash"],
        "ShortTermInvestments": ["shortTermInvestments"],
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
        "TotalDebtFromBalanceSheet": ["shortLongTermDebtTotal"],
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
        "GrossProfit": ["grossProfit"],
        "CostOfRevenue": ["costOfRevenue"],
        "DepreciationDepletionAndAmortization": [
            "depreciationAndAmortization",
            "reconciledDepreciation",
        ],
        "IncomeTaxExpense": ["incomeTaxExpense", "taxProvision"],
        "InterestExpense": ["interestExpense"],
        "InterestExpenseFromNetInterestIncome": [],
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
        "CommonStockDividendsPaid": ["dividendsPaid"],
        "StockBasedCompensation": ["stockBasedCompensation"],
        "SalePurchaseOfStock": ["salePurchaseOfStock"],
        "IssuanceOfCapitalStock": ["issuanceOfCapitalStock"],
    },
}

EODHD_EXTRA_CONCEPTS = {"EnterpriseValue", "CommonStockDividendsPerShareCashPaid"}
EODHD_TARGET_CONCEPTS = {
    concept for statement in EODHD_STATEMENT_FIELDS.values() for concept in statement
} | EODHD_EXTRA_CONCEPTS
EODHD_DERIVED_OVERRIDE_CONCEPTS = ("CommonStockholdersEquity",)
LOGGER = logging.getLogger(__name__)


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_currency_code(value: object) -> Optional[str]:
    return shared_normalize_currency_code(value)


class EODHDFactsNormalizer:
    """Flatten EODHD fundamentals payloads into FactRecord entries."""

    STATEMENT_FIELDS = EODHD_STATEMENT_FIELDS

    def __init__(
        self,
        concepts: Optional[Iterable[str]] = None,
        derived_overrides: Optional[Iterable[str]] = None,
        fx_service: Optional[FXService] = None,
    ) -> None:
        self.concepts = set(concepts or EODHD_TARGET_CONCEPTS)
        if derived_overrides is None:
            self.derived_overrides = set(EODHD_DERIVED_OVERRIDE_CONCEPTS)
        else:
            self.derived_overrides = set(derived_overrides)
        self.fx_service = fx_service

    def normalize(
        self,
        payload: Dict,
        symbol: str,
        accounting_standard: Optional[str] = None,
        target_currency: Optional[str] = None,
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
            self._normalize_enterprise_value(
                payload,
                symbol=symbol,
                accounting_standard=accounting_standard,
                default_currency=currency_code,
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
            self._normalize_dividends_per_share(
                payload,
                symbol=symbol,
                accounting_standard=accounting_standard,
                default_currency=currency_code,
            )
        )
        records.extend(
            self._normalize_earnings_eps(payload, symbol, accounting_standard)
        )
        indexed = self._index_records(records)
        self._merge_derived_records(
            records,
            indexed,
            self._derive_eps_alias(indexed),
            "EarningsPerShare",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_intangibles_excluding_goodwill(indexed),
            "IntangibleAssetsNetExcludingGoodwill",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_equity_alias(indexed),
            "StockholdersEquity",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_shares_alias(indexed),
            "CommonStockSharesOutstanding",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_operating_cash_flow_alias(indexed),
            "NetCashProvidedByUsedInOperatingActivities",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_capex_alias(indexed),
            "CapitalExpenditures",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_ebit_alias(indexed),
            "OperatingIncomeLoss",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_ppe_alias(indexed),
            "PropertyPlantAndEquipmentNet",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_net_income_available_to_common(indexed),
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        )
        self._merge_derived_records(
            records,
            indexed,
            self._derive_common_stockholders_equity(indexed),
            "CommonStockholdersEquity",
        )
        if target_currency is not None:
            records = self._convert_facts_to_target_currency(
                records, target_currency, symbol
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
                lowered = self._build_case_insensitive_entry(entry)
                end_date = self._extract_date(entry)
                if not end_date:
                    continue
                currency_resolution = resolve_eodhd_currency(
                    entry,
                    statement_currency=default_currency,
                    payload_currency=None,
                )
                currency = currency_resolution.currency_code
                period_code = fiscal_period or self._infer_quarter(entry)
                frame = self._build_frame(end_date, period_code)
                total_liab = self._extract_value(
                    entry, ["totalLiabilities", "totalLiab"], lowered
                )
                current_liab = self._extract_value(
                    entry, ["totalCurrentLiabilities"], lowered
                )
                derived_debt = None
                if total_liab is not None and current_liab is not None:
                    candidate = total_liab - current_liab
                    if candidate >= 0:
                        derived_debt = candidate

                derived_current_assets = None
                if "AssetsCurrent" in field_map:
                    total_assets = self._extract_value(entry, ["totalAssets"], lowered)
                    noncurrent_assets = self._extract_value(
                        entry, ["nonCurrentAssetsTotal"], lowered
                    )
                    if total_assets is not None and noncurrent_assets is not None:
                        candidate = total_assets - noncurrent_assets
                        if candidate >= 0:
                            derived_current_assets = candidate
                    if derived_current_assets is None:
                        cash_bucket = self._extract_value(
                            entry, ["cashAndShortTermInvestments"], lowered
                        )
                        short_term_investments = None
                        if cash_bucket is None:
                            short_term_investments = self._extract_value(
                                entry, ["shortTermInvestments"], lowered
                            )
                            cash_bucket = self._extract_value(
                                entry, ["cashAndEquivalents", "cash"], lowered
                            )
                        receivables = self._extract_value(
                            entry, ["netReceivables"], lowered
                        )
                        inventory = self._extract_value(entry, ["inventory"], lowered)
                        other_current = self._extract_value(
                            entry, ["otherCurrentAssets"], lowered
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
                        entry, ["nonCurrentLiabilitiesTotal"], lowered
                    )
                    if total_liab is not None and noncurrent_liab is not None:
                        candidate = total_liab - noncurrent_liab
                        if candidate >= 0:
                            derived_current_liab = candidate
                    if derived_current_liab is None:
                        accounts_payable = self._extract_value(
                            entry, ["accountsPayable"], lowered
                        )
                        other_current = self._extract_value(
                            entry, ["otherCurrentLiab"], lowered
                        )
                        deferred_revenue = self._extract_value(
                            entry, ["currentDeferredRevenue"], lowered
                        )
                        short_term_debt = self._extract_value(
                            entry, ["shortTermDebt"], lowered
                        )
                        short_long_term_debt = None
                        if short_term_debt is None:
                            short_long_term_debt = self._extract_value(
                                entry, ["shortLongTermDebt"], lowered
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
                        entry, ["propertyPlantAndEquipmentGross"], lowered
                    )
                    accumulated = self._extract_value(
                        entry, ["accumulatedDepreciation"], lowered
                    )
                    if gross is not None and accumulated is not None:
                        candidate = gross - accumulated
                        if candidate >= 0:
                            derived_ppe = candidate

                derived_operating_income = None
                if "OperatingIncomeLoss" in field_map:
                    income_before_tax = self._extract_value(
                        entry, ["incomeBeforeTax"], lowered
                    )
                    interest_expense = self._extract_value(
                        entry, ["interestExpense"], lowered
                    )
                    interest_income = self._extract_value(
                        entry, ["interestIncome"], lowered
                    )
                    if income_before_tax is not None and interest_expense is not None:
                        derived_operating_income = (
                            income_before_tax
                            + interest_expense
                            - (interest_income or 0.0)
                        )
                    if derived_operating_income is None:
                        total_revenue = self._extract_value(
                            entry, ["totalRevenue"], lowered
                        )
                        total_operating_expenses = self._extract_value(
                            entry, ["totalOperatingExpenses"], lowered
                        )
                        if (
                            total_revenue is not None
                            and total_operating_expenses is not None
                        ):
                            derived_operating_income = (
                                total_revenue - total_operating_expenses
                            )

                derived_interest_expense_from_net_interest_income = None
                if "InterestExpenseFromNetInterestIncome" in field_map:
                    interest_income = self._extract_value(
                        entry, ["interestIncome"], lowered
                    )
                    net_interest_income = self._extract_value(
                        entry, ["netInterestIncome"], lowered
                    )
                    if interest_income is not None and net_interest_income is not None:
                        candidate = interest_income - net_interest_income
                        if candidate > 0:
                            derived_interest_expense_from_net_interest_income = (
                                candidate
                            )

                derived_capex = None
                if "CapitalExpenditures" in field_map:
                    operating_cash = self._extract_value(
                        entry, ["totalCashFromOperatingActivities"], lowered
                    )
                    free_cash_flow = self._extract_value(
                        entry, ["freeCashFlow"], lowered
                    )
                    if operating_cash is not None and free_cash_flow is not None:
                        derived_capex = operating_cash - free_cash_flow

                derived_operating_cash = None
                if "NetCashProvidedByUsedInOperatingActivities" in field_map:
                    free_cash_flow = self._extract_value(
                        entry, ["freeCashFlow"], lowered
                    )
                    capex_value = self._extract_value(
                        entry, ["capitalExpenditures", "capex"], lowered
                    )
                    if free_cash_flow is not None and capex_value is not None:
                        derived_operating_cash = free_cash_flow + capex_value
                for concept, keys in field_map.items():
                    if concept not in self.concepts:
                        continue
                    value = self._extract_value(entry, keys, lowered)
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
                    if (
                        value is None
                        and concept == "InterestExpenseFromNetInterestIncome"
                    ):
                        value = derived_interest_expense_from_net_interest_income
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
                    if normalized_currency is None:
                        warn_missing_monetary_currency(
                            symbol=symbol.upper(),
                            field_name=concept,
                            statement_name=statement_payload.get("__statement_name")
                            if isinstance(statement_payload, dict)
                            else None,
                            end_date=end_date,
                            logger=LOGGER,
                        )
                        continue
                    records.append(
                        FactRecord(
                            symbol=symbol.upper(),
                            concept=concept,
                            fiscal_period=period_code or "",
                            end_date=end_date,
                            unit=normalized_currency,
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

    def _normalize_enterprise_value(
        self,
        payload: Dict,
        *,
        symbol: str,
        accounting_standard: Optional[str],
        default_currency: Optional[str],
    ) -> List[FactRecord]:
        if "EnterpriseValue" not in self.concepts:
            return []

        valuation = payload.get("Valuation") or {}
        raw_value = _to_float(valuation.get("EnterpriseValue"))
        if raw_value is None:
            return []

        highlights = payload.get("Highlights") or {}
        end_date = self._extract_date({"date": highlights.get("MostRecentQuarter")})
        if not end_date:
            end_date = self._latest_financials_end_date(payload.get("Financials") or {})
        if not end_date:
            return []

        currency = _normalize_currency_code(default_currency)
        normalized_value, normalized_currency = self._normalize_value_currency(
            raw_value, currency
        )
        if normalized_value is None:
            return []

        return [
            FactRecord(
                symbol=symbol.upper(),
                concept="EnterpriseValue",
                fiscal_period="",
                end_date=end_date,
                unit=normalized_currency or "",
                value=normalized_value,
                accn=None,
                filed=None,
                frame=None,
                start_date=None,
                accounting_standard=accounting_standard,
                currency=normalized_currency,
            )
        ]

    def _latest_financials_end_date(self, financials: Dict) -> Optional[str]:
        latest: Optional[str] = None
        for statement_payload in financials.values():
            if not isinstance(statement_payload, dict):
                continue
            for frequency in ("yearly", "quarterly"):
                for key, entry in self._iter_entries_with_keys(
                    statement_payload.get(frequency)
                ):
                    end_date = self._extract_entry_date_keyed(key, entry)
                    if not end_date:
                        continue
                    if latest is None or end_date > latest:
                        latest = end_date
        return latest

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
        record = FactRecord(
            symbol=symbol.upper(),
            concept="CommonStockSharesOutstanding",
            fiscal_period="",
            end_date=end_date,
            unit=SHARES_UNIT,
            value=shares,
            accn=None,
            filed=None,
            frame=None,
            start_date=None,
            accounting_standard=accounting_standard,
            currency=None,
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
                        unit=SHARES_UNIT,
                        value=shares,
                        accn=None,
                        filed=None,
                        frame=frame,
                        start_date=None,
                        accounting_standard=accounting_standard,
                        currency=None,
                    )
                )
        return records

    def _normalize_dividends_per_share(
        self,
        payload: Dict,
        *,
        symbol: str,
        accounting_standard: Optional[str],
        default_currency: Optional[str],
    ) -> List[FactRecord]:
        if "CommonStockDividendsPerShareCashPaid" not in self.concepts:
            return []

        highlights = payload.get("Highlights") or {}
        raw_value = _to_float(highlights.get("DividendShare"))
        if raw_value is None:
            return []

        end_date = self._extract_date({"date": highlights.get("MostRecentQuarter")})
        if not end_date:
            end_date = self._latest_financials_end_date(payload.get("Financials") or {})
        if not end_date:
            return []

        currency = _normalize_currency_code(default_currency)
        normalized_value, normalized_currency = self._normalize_value_currency(
            raw_value, currency
        )
        if normalized_value is None:
            return []

        return [
            FactRecord(
                symbol=symbol.upper(),
                concept="CommonStockDividendsPerShareCashPaid",
                fiscal_period="",
                end_date=end_date,
                unit=normalized_currency or "",
                value=normalized_value,
                accn=None,
                filed=None,
                frame=None,
                start_date=None,
                accounting_standard=accounting_standard,
                currency=normalized_currency,
            )
        ]

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
            lowered = self._build_case_insensitive_entry(entry)
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, NET_INCOME_KEYS, lowered)
            if value is None:
                continue
            net_income[date_str] = value
        return net_income

    def _build_income_statement_shares_map(self, entries) -> Dict[str, float]:
        shares: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            lowered = self._build_case_insensitive_entry(entry)
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, INCOME_STATEMENT_SHARES_KEYS, lowered)
            if value is None:
                continue
            shares[date_str] = value
        return shares

    def _collect_statement_eps_dates(self, entries) -> set[str]:
        dates: set[str] = set()
        for key, entry in self._iter_entries_with_keys(entries):
            lowered = self._build_case_insensitive_entry(entry)
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, EPS_STATEMENT_KEYS, lowered)
            if value is None:
                continue
            dates.add(date_str)
        return dates

    def _build_balance_sheet_shares_map(self, entries) -> Dict[str, float]:
        shares: Dict[str, float] = {}
        for key, entry in self._iter_entries_with_keys(entries):
            lowered = self._build_case_insensitive_entry(entry)
            date_str = self._extract_entry_date_keyed(key, entry)
            if not date_str:
                continue
            value = self._extract_value(entry, BALANCE_SHEET_SHARES_KEYS, lowered)
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
        raw_general_currency = raw_currency_code(general.get("CurrencyCode"))
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

        if is_subunit_base_currency(general_currency):
            for date_str, value, currency in self._normalize_eps_series(
                history,
                raw_general_currency or general_currency,
                implied_quarterly,
            ):
                period = self._infer_quarter({"date": date_str}) or ""
                add_record(date_str, value, period, currency)
            for date_str, value, currency in self._normalize_eps_series(
                annual,
                raw_general_currency or general_currency,
                implied_annual,
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

        raw_base = raw_currency_code(base_currency)
        normalized_base = _normalize_currency_code(base_currency)
        target_currency = normalized_base
        if target_currency is None or not is_subunit_base_currency(target_currency):
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

        scale = 1.0
        divisor = subunit_divisor(raw_base)
        if divisor is not None:
            scale = 1.0 / float(divisor)
        elif is_subunit_base_currency(normalized_base):
            scale = 1.0

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
                # Smaller cluster is treated as the base currency, larger cluster as
                # the exchange subunit denomination (for example GBX, ILA, or ZAC).
                segment_scales.append(1.0 if segment_median <= threshold else 0.01)

        for seg_index, (start, end) in enumerate(zip(segment_starts, segment_ends)):
            seg_scale = segment_scales[seg_index]
            for idx in range(start, end):
                scaled = values[idx] * seg_scale
                normalized_scaled.append((dates[idx], scaled, target_currency))
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

    def _record_key(self, record: FactRecord) -> FactKey:
        return (record.end_date, record.fiscal_period or "", record.unit)

    def _period_key(self, record: FactRecord) -> FactPeriodKey:
        return (record.end_date, record.fiscal_period or "")

    def _index_records(self, records: List[FactRecord]) -> FactIndex:
        indexed: FactIndex = {}
        for record in records:
            key = self._record_key(record)
            bucket = indexed.setdefault(record.concept, {})
            if key not in bucket:
                bucket[key] = record
        return indexed

    def _candidate_period_keys(
        self, indexed: FactIndex, concepts: Iterable[str]
    ) -> set[FactPeriodKey]:
        keys: set[FactPeriodKey] = set()
        for concept in concepts:
            keys.update(
                self._period_key(record) for record in indexed.get(concept, {}).values()
            )
        return keys

    def _records_for_period(
        self,
        indexed: FactIndex,
        concept: str,
        period_key: FactPeriodKey,
    ) -> List[FactRecord]:
        end_date, fiscal_period = period_key
        return [
            record
            for record in indexed.get(concept, {}).values()
            if record.end_date == end_date
            and (record.fiscal_period or "") == fiscal_period
            and record.value is not None
        ]

    def _pick_period_record(
        self,
        indexed: FactIndex,
        concept: str,
        period_key: FactPeriodKey,
    ) -> Optional[FactRecord]:
        records = self._records_for_period(indexed, concept, period_key)
        return records[0] if records else None

    def _convert_record_value(
        self,
        record: FactRecord,
        target_currency: Optional[str],
        *,
        derived_concept: str,
        symbol: str,
    ) -> Optional[float]:
        if record.value is None:
            return None
        return convert_money_value(
            amount=record.value,
            source_currency=record.currency,
            target_currency=target_currency,
            as_of=record.end_date,
            fx_service=self.fx_service,
            logger=LOGGER,
            operation=f"eodhd:{derived_concept}",
            symbol=symbol.upper(),
            field_name=record.concept,
            raise_on_missing_fx=False,
        )

    def _build_monetary_derived_record(
        self,
        base: FactRecord,
        *,
        concept: str,
        value: float,
        currency: str,
    ) -> FactRecord:
        return FactRecord(
            symbol=base.symbol,
            cik=base.cik,
            concept=concept,
            fiscal_period=base.fiscal_period,
            end_date=base.end_date,
            unit=currency,
            value=value,
            accn=base.accn,
            filed=base.filed,
            frame=base.frame,
            start_date=base.start_date,
            accounting_standard=base.accounting_standard,
            currency=currency,
        )

    def _should_override(self, concept: str) -> bool:
        return concept in self.derived_overrides

    def _merge_derived_records(
        self,
        records: List[FactRecord],
        indexed: FactIndex,
        derived: List[FactRecord],
        concept: str,
    ) -> None:
        if not derived:
            return
        if self._should_override(concept):
            derived_keys = {self._record_key(rec) for rec in derived}
            records[:] = [
                rec
                for rec in records
                if not (
                    rec.concept == concept and self._record_key(rec) in derived_keys
                )
            ]
            bucket = indexed.get(concept)
            if bucket is not None:
                for key in derived_keys:
                    bucket.pop(key, None)
                if not bucket:
                    indexed.pop(concept, None)
        records.extend(derived)
        bucket = indexed.setdefault(concept, {})
        for record in derived:
            bucket.setdefault(self._record_key(record), record)

    def _derive_eps_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("EarningsPerShare", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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
        self, indexed: FactIndex
    ) -> List[FactRecord]:
        existing = indexed.get("IntangibleAssetsNetExcludingGoodwill", {})
        candidate_periods = self._candidate_period_keys(
            indexed,
            (
                "IntangibleAssetsNetExcludingGoodwill",
                *INTANGIBLE_EXCL_GOODWILL_FALLBACK,
                "NetTangibleAssets",
                "Assets",
                "Liabilities",
                "Goodwill",
            ),
        )

        derived: List[FactRecord] = []
        override = self._should_override("IntangibleAssetsNetExcludingGoodwill")
        for period_key in candidate_periods:
            if (
                any(
                    self._period_key(record) == period_key
                    for record in existing.values()
                )
                and not override
            ):
                continue
            base = None
            for concept in INTANGIBLE_EXCL_GOODWILL_FALLBACK:
                base = self._pick_period_record(indexed, concept, period_key)
                if base and base.value is not None:
                    break
            if base is not None and base.value is not None:
                derived.append(
                    self._alias_record(base, "IntangibleAssetsNetExcludingGoodwill")
                )
                continue
            net_tangible_rec = self._pick_period_record(
                indexed, "NetTangibleAssets", period_key
            )
            assets_rec = self._pick_period_record(indexed, "Assets", period_key)
            liabilities_rec = self._pick_period_record(
                indexed, "Liabilities", period_key
            )
            if (
                net_tangible_rec
                and assets_rec
                and liabilities_rec
                and net_tangible_rec.value is not None
                and assets_rec.value is not None
                and liabilities_rec.value is not None
            ):
                goodwill_rec = self._pick_period_record(indexed, "Goodwill", period_key)
                target_currency = choose_target_currency(
                    [
                        assets_rec.currency,
                        liabilities_rec.currency,
                        net_tangible_rec.currency,
                        goodwill_rec.currency if goodwill_rec else None,
                    ]
                )
                assets_value = self._convert_record_value(
                    assets_rec,
                    target_currency,
                    derived_concept="IntangibleAssetsNetExcludingGoodwill",
                    symbol=net_tangible_rec.symbol,
                )
                liabilities_value = self._convert_record_value(
                    liabilities_rec,
                    target_currency,
                    derived_concept="IntangibleAssetsNetExcludingGoodwill",
                    symbol=net_tangible_rec.symbol,
                )
                net_tangible_value = self._convert_record_value(
                    net_tangible_rec,
                    target_currency,
                    derived_concept="IntangibleAssetsNetExcludingGoodwill",
                    symbol=net_tangible_rec.symbol,
                )
                goodwill_value = 0.0
                if goodwill_rec and goodwill_rec.value is not None:
                    converted_goodwill = self._convert_record_value(
                        goodwill_rec,
                        target_currency,
                        derived_concept="IntangibleAssetsNetExcludingGoodwill",
                        symbol=net_tangible_rec.symbol,
                    )
                    if converted_goodwill is None:
                        continue
                    goodwill_value = converted_goodwill
                if (
                    assets_value is None
                    or liabilities_value is None
                    or net_tangible_value is None
                    or target_currency is None
                ):
                    continue
                equity_value = assets_value - liabilities_value
                candidate = equity_value - net_tangible_value - goodwill_value
                if candidate >= 0:
                    derived.append(
                        self._build_monetary_derived_record(
                            net_tangible_rec,
                            concept="IntangibleAssetsNetExcludingGoodwill",
                            value=candidate,
                            currency=target_currency,
                        )
                    )
        return derived

    def _derive_equity_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("StockholdersEquity", {})
        candidate_periods = self._candidate_period_keys(
            indexed,
            ("StockholdersEquity", "Assets", "Liabilities", *EQUITY_FALLBACK_CONCEPTS),
        )

        derived: List[FactRecord] = []
        override = self._should_override("StockholdersEquity")
        derived_periods = (
            set()
            if override
            else {self._period_key(record) for record in existing.values()}
        )
        for period_key in candidate_periods:
            if period_key in derived_periods:
                continue
            assets_rec = self._pick_period_record(indexed, "Assets", period_key)
            liabilities_rec = self._pick_period_record(
                indexed, "Liabilities", period_key
            )
            if (
                assets_rec
                and liabilities_rec
                and assets_rec.value is not None
                and liabilities_rec.value is not None
            ):
                target_currency = choose_target_currency(
                    [assets_rec.currency, liabilities_rec.currency]
                )
                assets_value = self._convert_record_value(
                    assets_rec,
                    target_currency,
                    derived_concept="StockholdersEquity",
                    symbol=assets_rec.symbol,
                )
                liabilities_value = self._convert_record_value(
                    liabilities_rec,
                    target_currency,
                    derived_concept="StockholdersEquity",
                    symbol=assets_rec.symbol,
                )
                if (
                    assets_value is None
                    or liabilities_value is None
                    or target_currency is None
                ):
                    continue
                value = assets_value - liabilities_value
                if value >= 0:
                    derived.append(
                        self._build_monetary_derived_record(
                            assets_rec,
                            concept="StockholdersEquity",
                            value=value,
                            currency=target_currency,
                        )
                    )
                    derived_periods.add(period_key)
                    continue
            base = None
            for concept in EQUITY_FALLBACK_CONCEPTS:
                base = self._pick_period_record(indexed, concept, period_key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            derived.append(self._alias_record(base, "StockholdersEquity"))
            derived_periods.add(period_key)
        return derived

    def _derive_shares_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("CommonStockSharesOutstanding", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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

    def _derive_operating_cash_flow_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("NetCashProvidedByUsedInOperatingActivities", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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

    def _derive_capex_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("CapitalExpenditures", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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

    def _derive_ebit_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("OperatingIncomeLoss", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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

    def _derive_ppe_alias(self, indexed: FactIndex) -> List[FactRecord]:
        existing = indexed.get("PropertyPlantAndEquipmentNet", {})
        candidate_keys: set[FactKey] = set(existing.keys())
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
        self, indexed: FactIndex
    ) -> List[FactRecord]:
        existing = indexed.get("NetIncomeLossAvailableToCommonStockholdersBasic", {})
        candidate_periods = self._candidate_period_keys(
            indexed,
            (
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                *INCOME_AVAILABLE_TO_COMMON_FALLBACK,
                *PREFERRED_DIVIDEND_FALLBACK,
            ),
        )

        derived: List[FactRecord] = []
        override = self._should_override(
            "NetIncomeLossAvailableToCommonStockholdersBasic"
        )
        for period_key in candidate_periods:
            if (
                any(
                    self._period_key(record) == period_key
                    for record in existing.values()
                )
                and not override
            ):
                continue
            base = None
            for concept in INCOME_AVAILABLE_TO_COMMON_FALLBACK:
                base = self._pick_period_record(indexed, concept, period_key)
                if base and base.value is not None:
                    break
            if base is None or base.value is None:
                continue
            preferred_value = 0.0
            preferred_currency = None
            for concept in PREFERRED_DIVIDEND_FALLBACK:
                pref = self._pick_period_record(indexed, concept, period_key)
                if pref and pref.value is not None:
                    preferred_value = pref.value
                    preferred_currency = pref.currency
                    break
            target_currency = choose_target_currency(
                [base.currency, preferred_currency]
            )
            base_value = self._convert_record_value(
                base,
                target_currency,
                derived_concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                symbol=base.symbol,
            )
            if base_value is None or target_currency is None:
                continue
            adjusted = base_value
            if preferred_currency is not None:
                converted_preferred = convert_money_value(
                    amount=preferred_value,
                    source_currency=preferred_currency,
                    target_currency=target_currency,
                    as_of=base.end_date,
                    fx_service=self.fx_service,
                    logger=LOGGER,
                    operation="eodhd:NetIncomeLossAvailableToCommonStockholdersBasic",
                    symbol=base.symbol,
                    field_name="PreferredStockDividendsAndOtherAdjustments",
                    raise_on_missing_fx=False,
                )
                if converted_preferred is None:
                    continue
                adjusted -= converted_preferred
            derived.append(
                self._build_monetary_derived_record(
                    base,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    value=adjusted,
                    currency=target_currency,
                )
            )
        return derived

    def _derive_common_stockholders_equity(
        self, indexed: FactIndex
    ) -> List[FactRecord]:
        existing = indexed.get("CommonStockholdersEquity", {})
        stockholders_equity = indexed.get("StockholdersEquity", {})

        derived: List[FactRecord] = []
        override = self._should_override("CommonStockholdersEquity")
        existing_periods = {self._period_key(record) for record in existing.values()}
        for base in stockholders_equity.values():
            period_key = self._period_key(base)
            if period_key in existing_periods and not override:
                continue
            if base.value is None:
                continue
            preferred = self._pick_period_record(indexed, "PreferredStock", period_key)
            noncontrolling_rec = self._pick_period_record(
                indexed,
                "NoncontrollingInterestInConsolidatedEntity",
                period_key,
            )
            target_currency = choose_target_currency(
                [
                    base.currency,
                    preferred.currency if preferred else None,
                    noncontrolling_rec.currency if noncontrolling_rec else None,
                ]
            )
            base_value = self._convert_record_value(
                base,
                target_currency,
                derived_concept="CommonStockholdersEquity",
                symbol=base.symbol,
            )
            if base_value is None or target_currency is None:
                continue
            adjusted: Optional[float] = base_value
            for optional_record in (preferred, noncontrolling_rec):
                if optional_record is None or optional_record.value is None:
                    continue
                converted = self._convert_record_value(
                    optional_record,
                    target_currency,
                    derived_concept="CommonStockholdersEquity",
                    symbol=base.symbol,
                )
                if converted is None:
                    adjusted = None
                    break
                if adjusted is None:
                    break
                adjusted -= converted
            if adjusted is None:
                continue
            derived.append(
                self._build_monetary_derived_record(
                    base,
                    concept="CommonStockholdersEquity",
                    value=adjusted,
                    currency=target_currency,
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

    def _build_case_insensitive_entry(self, entry: Dict) -> Dict[str, Any]:
        return {
            key.lower(): value for key, value in entry.items() if isinstance(key, str)
        }

    def _extract_value(
        self,
        entry: Dict,
        keys: Sequence[str],
        lowered: Optional[Mapping[str, Any]] = None,
    ) -> Optional[float]:
        if lowered is None:
            lowered = self._build_case_insensitive_entry(entry)
        for key in keys:
            if key in entry:
                value = _to_float(entry.get(key))
            else:
                lowered_value = lowered.get(key.lower())
                value = _to_float(lowered_value)
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
        """Normalize configured subunit currencies into their base currencies."""

        normalized_value, normalized_currency = normalize_monetary_amount(
            value,
            currency,
        )
        return (
            float(normalized_value) if normalized_value is not None else None,
            normalized_currency,
        )

    def _convert_facts_to_target_currency(
        self,
        records: List[FactRecord],
        target_currency: str,
        symbol: str,
    ) -> List[FactRecord]:
        """Convert all monetary facts to the ticker's canonical trading currency.

        Non-monetary facts (shares, unitless) pass through unchanged.
        Facts already in the target currency pass through unchanged.
        Facts where FX conversion fails are skipped for that period only.
        """

        if self.fx_service is None:
            LOGGER.warning(
                "FX service unavailable for ticker-centric conversion | "
                "symbol=%s target=%s",
                symbol,
                target_currency,
            )
            return records

        converted: List[FactRecord] = []
        for record in records:
            if not self._is_monetary_fact(record):
                converted.append(record)
                continue
            if record.currency == target_currency:
                converted.append(record)
                continue
            new_value = convert_money_value(
                amount=record.value,
                source_currency=record.currency,
                target_currency=target_currency,
                as_of=record.end_date,
                fx_service=self.fx_service,
                logger=LOGGER,
                operation="ticker_currency_alignment",
                symbol=symbol,
                field_name=record.concept,
                raise_on_missing_fx=False,
            )
            if new_value is None:
                continue
            converted.append(
                FactRecord(
                    symbol=record.symbol,
                    cik=record.cik,
                    concept=record.concept,
                    fiscal_period=record.fiscal_period,
                    end_date=record.end_date,
                    unit=target_currency,
                    value=new_value,
                    accn=record.accn,
                    filed=record.filed,
                    frame=record.frame,
                    start_date=record.start_date,
                    accounting_standard=record.accounting_standard,
                    currency=target_currency,
                )
            )
        return converted

    @staticmethod
    def _is_monetary_fact(record: FactRecord) -> bool:
        """Return True when the fact carries a monetary value needing currency alignment."""

        if record.currency is None:
            return False
        if record.unit == SHARES_UNIT:
            return False
        return True

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
