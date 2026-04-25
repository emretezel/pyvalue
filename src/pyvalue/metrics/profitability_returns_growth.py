"""TTM profitability, return, dividend, and 10Y growth metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.accruals_ratio import AccrualsRatioCalculator
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.buyback_yield import NetBuybackYieldMetric
from pyvalue.metrics.enterprise_value import validate_denominator_amount
from pyvalue.metrics.owner_earnings_enterprise import (
    REQUIRED_CONCEPTS as OE_ENTERPRISE_REQUIRED_CONCEPTS,
    OwnerEarningsEnterpriseCalculator,
)
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
    normalize_market_cap_amount,
    normalize_metric_record,
    require_metric_ticker_currency,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

REVENUE_CONCEPT = "Revenues"
COST_OF_REVENUE_CONCEPT = "CostOfRevenue"
GROSS_PROFIT_CONCEPT = "GrossProfit"
EBIT_CONCEPT = "OperatingIncomeLoss"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
CAPEX_CONCEPT = "CapitalExpenditures"
NET_INCOME_CONCEPT = "NetIncomeLoss"
NET_INCOME_COMMON_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"
ASSETS_CONCEPT = "Assets"
COMMON_EQUITY_CONCEPT = "CommonStockholdersEquity"
GOODWILL_CONCEPT = "Goodwill"
INTANGIBLE_PRIMARY_CONCEPT = "IntangibleAssetsNetExcludingGoodwill"
INTANGIBLE_FALLBACK_CONCEPT = "IntangibleAssetsNet"
DIVIDENDS_PAID_CONCEPT = "CommonStockDividendsPaid"
DIVIDENDS_PER_SHARE_CONCEPT = "CommonStockDividendsPerShareCashPaid"
DILUTED_SHARES_CONCEPT = "WeightedAverageNumberOfDilutedSharesOutstanding"

REVENUE_CONCEPTS = (REVENUE_CONCEPT,)
COST_OF_REVENUE_CONCEPTS = (COST_OF_REVENUE_CONCEPT,)
GROSS_PROFIT_CONCEPTS = (GROSS_PROFIT_CONCEPT,)
EBIT_CONCEPTS = (EBIT_CONCEPT,)
OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
NET_INCOME_CONCEPTS = (NET_INCOME_CONCEPT,)
NET_INCOME_COMMON_CONCEPTS = (NET_INCOME_COMMON_CONCEPT,)
COMMON_EQUITY_CONCEPTS = (COMMON_EQUITY_CONCEPT,)
GOODWILL_CONCEPTS = (GOODWILL_CONCEPT,)
INTANGIBLE_CONCEPTS = (INTANGIBLE_PRIMARY_CONCEPT, INTANGIBLE_FALLBACK_CONCEPT)
DIVIDENDS_PAID_CONCEPTS = (DIVIDENDS_PAID_CONCEPT,)
DIVIDENDS_PER_SHARE_CONCEPTS = (DIVIDENDS_PER_SHARE_CONCEPT,)
DILUTED_SHARES_CONCEPTS = (DILUTED_SHARES_CONCEPT,)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
TEN_YEARS = 10

GROSS_MARGIN_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(REVENUE_CONCEPTS + COST_OF_REVENUE_CONCEPTS + GROSS_PROFIT_CONCEPTS)
)
OPERATING_MARGIN_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(REVENUE_CONCEPTS + EBIT_CONCEPTS)
)
FCF_MARGIN_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(REVENUE_CONCEPTS + OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS)
)
ROE_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS + COMMON_EQUITY_CONCEPTS
    )
)
ROA_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(NET_INCOME_CONCEPTS + NET_INCOME_COMMON_CONCEPTS + (ASSETS_CONCEPT,))
)
ROETCE_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        NET_INCOME_COMMON_CONCEPTS
        + NET_INCOME_CONCEPTS
        + COMMON_EQUITY_CONCEPTS
        + GOODWILL_CONCEPTS
        + INTANGIBLE_CONCEPTS
    )
)
DIVIDEND_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(DIVIDENDS_PAID_CONCEPTS + DIVIDENDS_PER_SHARE_CONCEPTS)
)
DIVIDEND_PAYOUT_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        DIVIDENDS_PAID_CONCEPTS + NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS
    )
)
REVENUE_CAGR_REQUIRED_CONCEPTS = REVENUE_CONCEPTS
FCF_PER_SHARE_CAGR_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS + DILUTED_SHARES_CONCEPTS
    )
)
GROSS_PROFIT_TO_ASSETS_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        REVENUE_CONCEPTS
        + COST_OF_REVENUE_CONCEPTS
        + GROSS_PROFIT_CONCEPTS
        + (ASSETS_CONCEPT,)
    )
)
SHAREHOLDER_YIELD_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(DIVIDEND_REQUIRED_CONCEPTS + NetBuybackYieldMetric.required_concepts)
)


@dataclass(frozen=True)
class _AmountSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _BalancePoint:
    value: float
    as_of: str
    fiscal_period: str
    currency: Optional[str]


@dataclass(frozen=True)
class _FYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


class ProfitabilityReturnsGrowthCalculator:
    """Shared calculator for profitability, return, dividend, and growth metrics."""

    def compute_gross_margin_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        revenue = self._compute_ttm_amount(
            symbol,
            repo,
            REVENUE_CONCEPTS,
            context="gross_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning("gross_margin_ttm: missing TTM revenue for %s", symbol)
            return None
        if revenue.value <= 0:
            LOGGER.warning("gross_margin_ttm: non-positive TTM revenue for %s", symbol)
            return None

        cogs = self._compute_ttm_cogs(symbol, repo, context="gross_margin_ttm")
        if cogs is None:
            LOGGER.warning("gross_margin_ttm: missing TTM COGS for %s", symbol)
            return None
        if not self._currencies_match(revenue.currency, cogs.currency):
            LOGGER.warning(
                "gross_margin_ttm: revenue/COGS currency mismatch for %s", symbol
            )
            return None

        gross_margin = (revenue.value - cogs.value) / revenue.value
        return _AmountSnapshot(
            value=max(-1.0, min(1.0, gross_margin)),
            as_of=max(revenue.as_of, cogs.as_of),
            currency=revenue.currency or cogs.currency,
        )

    def compute_operating_margin_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        revenue = self._compute_ttm_amount(
            symbol,
            repo,
            REVENUE_CONCEPTS,
            context="operating_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning("operating_margin_ttm: missing TTM revenue for %s", symbol)
            return None
        if revenue.value <= 0:
            LOGGER.warning(
                "operating_margin_ttm: non-positive TTM revenue for %s", symbol
            )
            return None

        ebit = self._compute_ttm_amount(
            symbol,
            repo,
            EBIT_CONCEPTS,
            context="operating_margin_ttm",
        )
        if ebit is None:
            LOGGER.warning("operating_margin_ttm: missing TTM EBIT for %s", symbol)
            return None
        if not self._currencies_match(revenue.currency, ebit.currency):
            LOGGER.warning(
                "operating_margin_ttm: revenue/EBIT currency mismatch for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=ebit.value / revenue.value,
            as_of=max(revenue.as_of, ebit.as_of),
            currency=revenue.currency or ebit.currency,
        )

    def compute_fcf_margin_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        revenue = self._compute_ttm_amount(
            symbol,
            repo,
            REVENUE_CONCEPTS,
            context="fcf_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning("fcf_margin_ttm: missing TTM revenue for %s", symbol)
            return None
        if revenue.value <= 0:
            LOGGER.warning("fcf_margin_ttm: non-positive TTM revenue for %s", symbol)
            return None

        fcf = self._compute_ttm_fcf(symbol, repo, context="fcf_margin_ttm")
        if fcf is None:
            LOGGER.warning("fcf_margin_ttm: missing TTM FCF for %s", symbol)
            return None
        if not self._currencies_match(revenue.currency, fcf.currency):
            LOGGER.warning(
                "fcf_margin_ttm: revenue/FCF currency mismatch for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=fcf.value / revenue.value,
            as_of=max(revenue.as_of, fcf.as_of),
            currency=revenue.currency or fcf.currency,
        )

    def compute_gross_profit_to_assets_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        gross_profit = self._compute_ttm_gross_profit(
            symbol,
            repo,
            context="gross_profit_to_assets_ttm",
        )
        if gross_profit is None:
            LOGGER.warning(
                "gross_profit_to_assets_ttm: missing TTM gross profit for %s", symbol
            )
            return None

        avg_assets = AccrualsRatioCalculator().compute_avg_total_assets(symbol, repo)
        if avg_assets is None:
            return None
        if avg_assets.total <= 0:
            LOGGER.warning(
                "gross_profit_to_assets_ttm: non-positive average assets for %s",
                symbol,
            )
            return None
        if not self._currencies_match(gross_profit.currency, avg_assets.currency):
            LOGGER.warning(
                "gross_profit_to_assets_ttm: numerator/denominator currency mismatch for %s",
                symbol,
            )
            return None

        return _AmountSnapshot(
            value=gross_profit.value / avg_assets.total,
            as_of=max(gross_profit.as_of, avg_assets.as_of),
            currency=gross_profit.currency or avg_assets.currency,
        )

    def compute_roe_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="roe_ttm",
        )
        if net_income is None:
            LOGGER.warning("roe_ttm: missing TTM net income for %s", symbol)
            return None

        avg_equity = self._compute_avg_common_equity(symbol, repo, context="roe_ttm")
        if avg_equity is None:
            return None
        if avg_equity.value <= 0:
            LOGGER.warning("roe_ttm: non-positive average equity for %s", symbol)
            return None
        if not self._currencies_match(net_income.currency, avg_equity.currency):
            LOGGER.warning(
                "roe_ttm: numerator/denominator currency mismatch for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=net_income.value / avg_equity.value,
            as_of=max(net_income.as_of, avg_equity.as_of),
            currency=net_income.currency or avg_equity.currency,
        )

    def compute_roa_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_CONCEPTS + NET_INCOME_COMMON_CONCEPTS,
            context="roa_ttm",
        )
        if net_income is None:
            LOGGER.warning("roa_ttm: missing TTM net income for %s", symbol)
            return None

        avg_assets = AccrualsRatioCalculator().compute_avg_total_assets(symbol, repo)
        if avg_assets is None:
            return None
        if avg_assets.total <= 0:
            LOGGER.warning("roa_ttm: non-positive average assets for %s", symbol)
            return None
        if not self._currencies_match(net_income.currency, avg_assets.currency):
            LOGGER.warning(
                "roa_ttm: numerator/denominator currency mismatch for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=net_income.value / avg_assets.total,
            as_of=max(net_income.as_of, avg_assets.as_of),
            currency=net_income.currency or avg_assets.currency,
        )

    def compute_roetce_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="roetce_ttm",
        )
        if net_income is None:
            LOGGER.warning("roetce_ttm: missing TTM net income for %s", symbol)
            return None

        avg_tce = self._compute_avg_tangible_common_equity(
            symbol,
            repo,
            context="roetce_ttm",
        )
        if avg_tce is None:
            return None
        if avg_tce.value <= 0:
            LOGGER.warning("roetce_ttm: non-positive average TCE for %s", symbol)
            return None
        if not self._currencies_match(net_income.currency, avg_tce.currency):
            LOGGER.warning(
                "roetce_ttm: numerator/denominator currency mismatch for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=net_income.value / avg_tce.value,
            as_of=max(net_income.as_of, avg_tce.as_of),
            currency=net_income.currency or avg_tce.currency,
        )

    def compute_dividend_yield_ttm(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[_AmountSnapshot]:
        dividends_paid = self._compute_ttm_amount(
            symbol,
            repo,
            DIVIDENDS_PAID_CONCEPTS,
            context="dividend_yield_ttm",
            absolute=True,
        )
        if dividends_paid is not None:
            cash_yield = self._compute_cash_dividend_yield(
                symbol,
                dividends_paid,
                repo,
                market_repo,
                context="dividend_yield_ttm",
            )
            if cash_yield is not None:
                return cash_yield

        dividend_per_share = self._latest_amount(
            symbol,
            repo,
            DIVIDENDS_PER_SHARE_CONCEPTS,
            context="dividend_yield_ttm",
            max_age_days=MAX_FACT_AGE_DAYS,
            absolute=True,
        )
        if dividend_per_share is None:
            LOGGER.warning(
                "dividend_yield_ttm: missing cash dividends and DPS fallback for %s",
                symbol,
            )
            return None

        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.price is None or snapshot.price <= 0:
            LOGGER.warning("dividend_yield_ttm: missing price snapshot for %s", symbol)
            return None

        price = validate_denominator_amount(
            symbol=symbol,
            amount=snapshot.price,
            source_currency=getattr(snapshot, "currency", None),
            target_currency=dividend_per_share.currency,
            as_of=snapshot.as_of,
            context="dividend_yield_ttm",
            contexts=(market_repo, repo),
        )
        if price is None or price <= 0:
            if price is not None:
                LOGGER.warning(
                    "dividend_yield_ttm: non-positive price for %s",
                    symbol,
                )
            return None

        return _AmountSnapshot(
            value=dividend_per_share.value / price,
            as_of=max(dividend_per_share.as_of, snapshot.as_of),
            currency=dividend_per_share.currency,
        )

    def compute_dividend_payout_ratio_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        dividends_paid = self._compute_ttm_amount(
            symbol,
            repo,
            DIVIDENDS_PAID_CONCEPTS,
            context="dividend_payout_ratio_ttm",
            absolute=True,
        )
        if dividends_paid is None:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: missing TTM cash dividends for %s", symbol
            )
            return None

        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="dividend_payout_ratio_ttm",
        )
        if net_income is None:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: missing TTM net income for %s", symbol
            )
            return None
        if net_income.value <= 0:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: non-positive TTM net income for %s", symbol
            )
            return None
        if not self._currencies_match(dividends_paid.currency, net_income.currency):
            LOGGER.warning(
                "dividend_payout_ratio_ttm: numerator/denominator currency mismatch for %s",
                symbol,
            )
            return None

        return _AmountSnapshot(
            value=dividends_paid.value / net_income.value,
            as_of=max(dividends_paid.as_of, net_income.as_of),
            currency=dividends_paid.currency or net_income.currency,
        )

    def compute_revenue_cagr_10y(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        points = self._build_fy_amount_points(
            symbol,
            repo,
            REVENUE_CONCEPTS,
            context="revenue_cagr_10y",
        )
        pair = self._select_exact_year_pair(
            points,
            years_back=TEN_YEARS,
            context="revenue_cagr_10y",
            symbol=symbol,
        )
        if pair is None:
            return None
        latest, prior = pair
        if latest.value <= 0 or prior.value <= 0:
            LOGGER.warning(
                "revenue_cagr_10y: non-positive revenue endpoints for %s", symbol
            )
            return None

        return _AmountSnapshot(
            value=(latest.value / prior.value) ** (1.0 / TEN_YEARS) - 1.0,
            as_of=latest.as_of,
            currency=latest.currency or prior.currency,
        )

    def compute_fcf_per_share_cagr_10y(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountSnapshot]:
        fcf_points = self._build_fcf_fy_points(
            symbol,
            repo,
            context="fcf_per_share_cagr_10y",
        )
        share_points = self._build_fy_amount_points(
            symbol,
            repo,
            DILUTED_SHARES_CONCEPTS,
            context="fcf_per_share_cagr_10y",
        )
        if not share_points:
            LOGGER.warning(
                "fcf_per_share_cagr_10y: missing diluted share history for %s", symbol
            )
            return None

        share_map = {point.year: point for point in share_points}
        per_share_points: list[_FYPoint] = []
        for fcf_point in fcf_points:
            share_point = share_map.get(fcf_point.year)
            if share_point is None:
                continue
            if share_point.value <= 0:
                continue
            per_share_points.append(
                _FYPoint(
                    year=fcf_point.year,
                    value=fcf_point.value / share_point.value,
                    as_of=max(fcf_point.as_of, share_point.as_of),
                    currency=fcf_point.currency,
                )
            )

        pair = self._select_exact_year_pair(
            per_share_points,
            years_back=TEN_YEARS,
            context="fcf_per_share_cagr_10y",
            symbol=symbol,
        )
        if pair is None:
            return None
        latest, prior = pair
        if latest.value <= 0 or prior.value <= 0:
            LOGGER.warning(
                "fcf_per_share_cagr_10y: non-positive FCF/share endpoints for %s",
                symbol,
            )
            return None

        return _AmountSnapshot(
            value=(latest.value / prior.value) ** (1.0 / TEN_YEARS) - 1.0,
            as_of=latest.as_of,
            currency=latest.currency or prior.currency,
        )

    def _compute_cash_dividend_yield(
        self,
        symbol: str,
        dividends_paid: _AmountSnapshot,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None or snapshot.market_cap <= 0:
            LOGGER.warning("%s: missing market cap snapshot for %s", context, symbol)
            return None

        market_cap = normalize_market_cap_amount(
            snapshot.market_cap,
            metric_id=context,
            symbol=symbol,
            as_of=snapshot.as_of,
            expected_currency=dividends_paid.currency,
            contexts=(market_repo, repo),
        )[0]
        if market_cap is None or market_cap <= 0:
            if market_cap is not None:
                LOGGER.warning(
                    "%s: non-positive market cap for %s",
                    context,
                    symbol,
                )
            return None

        return _AmountSnapshot(
            value=dividends_paid.value / market_cap,
            as_of=max(dividends_paid.as_of, snapshot.as_of),
            currency=dividends_paid.currency,
        )

    def _compute_ttm_gross_profit(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        revenue = self._compute_ttm_amount(
            symbol, repo, REVENUE_CONCEPTS, context=context
        )
        if revenue is None:
            return None
        cogs = self._compute_ttm_cogs(symbol, repo, context=context)
        if cogs is None:
            return None
        if not self._currencies_match(revenue.currency, cogs.currency):
            LOGGER.warning("%s: revenue/COGS currency mismatch for %s", context, symbol)
            return None
        return _AmountSnapshot(
            value=revenue.value - cogs.value,
            as_of=max(revenue.as_of, cogs.as_of),
            currency=revenue.currency or cogs.currency,
        )

    def _compute_ttm_cogs(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        revenue_records = self._filter_periods(
            repo.facts_for_concept(symbol, REVENUE_CONCEPT), QUARTERLY_PERIODS
        )
        if len(revenue_records) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly revenue records for %s, found %s",
                context,
                symbol,
                len(revenue_records),
            )
            return None
        latest_revenue = revenue_records[0]
        if not is_recent_fact(latest_revenue, max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest revenue quarter (%s) too old for %s",
                context,
                latest_revenue.end_date,
                symbol,
            )
            return None

        cogs_map = self._period_record_map(
            repo.facts_for_concept(symbol, COST_OF_REVENUE_CONCEPT), QUARTERLY_PERIODS
        )
        gross_profit_map = self._period_record_map(
            repo.facts_for_concept(symbol, GROSS_PROFIT_CONCEPT), QUARTERLY_PERIODS
        )

        totals: list[float] = []
        currencies: list[Optional[str]] = []
        as_of_dates: list[str] = []
        for revenue_record in revenue_records[:4]:
            revenue_value, revenue_currency = self._normalize_record(
                revenue_record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=REVENUE_CONCEPT,
            )
            key = (
                revenue_record.end_date,
                (revenue_record.fiscal_period or "").upper(),
            )
            cogs_record = cogs_map.get(key)
            if cogs_record is not None:
                cogs_value, cogs_currency = self._normalize_record(
                    cogs_record,
                    symbol=symbol,
                    repo=repo,
                    context=context,
                    input_name=COST_OF_REVENUE_CONCEPT,
                )
                if not self._currencies_match(revenue_currency, cogs_currency):
                    LOGGER.warning(
                        "%s: revenue/COGS currency mismatch on %s for %s",
                        context,
                        revenue_record.end_date,
                        symbol,
                    )
                    return None
                totals.append(cogs_value)
                currencies.extend([revenue_currency, cogs_currency])
                as_of_dates.extend([revenue_record.end_date, cogs_record.end_date])
                continue

            gross_profit_record = gross_profit_map.get(key)
            if gross_profit_record is None:
                LOGGER.warning(
                    "%s: missing quarterly COGS/gross profit on %s for %s",
                    context,
                    revenue_record.end_date,
                    symbol,
                )
                return None
            gross_profit_value, gross_profit_currency = self._normalize_record(
                gross_profit_record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=GROSS_PROFIT_CONCEPT,
            )
            if not self._currencies_match(revenue_currency, gross_profit_currency):
                LOGGER.warning(
                    "%s: revenue/gross profit currency mismatch on %s for %s",
                    context,
                    revenue_record.end_date,
                    symbol,
                )
                return None
            totals.append(revenue_value - gross_profit_value)
            currencies.extend([revenue_currency, gross_profit_currency])
            as_of_dates.extend([revenue_record.end_date, gross_profit_record.end_date])

        currency = self._combine_currency(currencies)
        if currency is None and any(code is not None for code in currencies):
            LOGGER.warning(
                "%s: quarterly COGS currency mismatch for %s", context, symbol
            )
            return None

        return _AmountSnapshot(
            value=sum(totals),
            as_of=max(as_of_dates),
            currency=currency,
        )

    def _compute_ttm_fcf(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        operating_cash = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
        )
        if operating_cash is None:
            return None

        capex = self._compute_ttm_amount(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            context=context,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", context, symbol
            )
            return _AmountSnapshot(
                value=operating_cash.value,
                as_of=operating_cash.as_of,
                currency=operating_cash.currency,
            )
        if not self._currencies_match(operating_cash.currency, capex.currency):
            LOGGER.warning("%s: OCF/capex currency mismatch for %s", context, symbol)
            return None
        return _AmountSnapshot(
            value=operating_cash.value - capex.value,
            as_of=max(operating_cash.as_of, capex.as_of),
            currency=operating_cash.currency or capex.currency,
        )

    def _compute_avg_common_equity(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        quarterly_points = self._build_common_equity_points(
            symbol, repo, QUARTERLY_PERIODS, context=context
        )
        fy_points = self._build_common_equity_points(
            symbol,
            repo,
            FY_PERIODS,
            context=context,
        )
        return self._compute_average_balance_points(
            quarterly_points,
            fy_points,
            symbol=symbol,
            context=context,
            allow_fy_fallback=True,
        )

    def _compute_avg_tangible_common_equity(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> Optional[_AmountSnapshot]:
        quarterly_points = self._build_tangible_common_equity_points(
            symbol,
            repo,
            QUARTERLY_PERIODS,
            context=context,
        )
        fy_points = self._build_tangible_common_equity_points(
            symbol,
            repo,
            FY_PERIODS,
            context=context,
        )
        return self._compute_average_balance_points(
            quarterly_points,
            fy_points,
            symbol=symbol,
            context=context,
            allow_fy_fallback=True,
        )

    def _build_common_equity_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
        *,
        context: str,
    ) -> list[_BalancePoint]:
        equity_map = self._period_record_map(
            repo.facts_for_concept(symbol, COMMON_EQUITY_CONCEPT), periods
        )
        points: list[_BalancePoint] = []
        for (_, fiscal_period), record in sorted(
            equity_map.items(),
            key=lambda item: (item[0][0], item[0][1]),
            reverse=True,
        ):
            value, currency = self._normalize_record(
                record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=COMMON_EQUITY_CONCEPT,
            )
            points.append(
                _BalancePoint(
                    value=value,
                    as_of=record.end_date,
                    fiscal_period=fiscal_period,
                    currency=currency,
                )
            )
        return points

    def _build_tangible_common_equity_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
        *,
        context: str,
    ) -> list[_BalancePoint]:
        equity_map = self._period_record_map(
            repo.facts_for_concept(symbol, COMMON_EQUITY_CONCEPT), periods
        )
        goodwill_map = self._period_record_map(
            repo.facts_for_concept(symbol, GOODWILL_CONCEPT), periods
        )
        intangibles_primary_map = self._period_record_map(
            repo.facts_for_concept(symbol, INTANGIBLE_PRIMARY_CONCEPT), periods
        )
        intangibles_fallback_map = self._period_record_map(
            repo.facts_for_concept(symbol, INTANGIBLE_FALLBACK_CONCEPT), periods
        )

        points: list[_BalancePoint] = []
        for key, equity_record in sorted(
            equity_map.items(),
            key=lambda item: (item[0][0], item[0][1]),
            reverse=True,
        ):
            equity_value, equity_currency = self._normalize_record(
                equity_record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=COMMON_EQUITY_CONCEPT,
            )
            goodwill_record = goodwill_map.get(key)
            goodwill_value = 0.0
            goodwill_currency = None
            if goodwill_record is not None:
                goodwill_value, goodwill_currency = self._normalize_record(
                    goodwill_record,
                    symbol=symbol,
                    repo=repo,
                    context=context,
                    input_name=GOODWILL_CONCEPT,
                )

            intangibles_record = intangibles_primary_map.get(key)
            if intangibles_record is None:
                intangibles_record = intangibles_fallback_map.get(key)
            intangible_value = 0.0
            intangible_currency = None
            if intangibles_record is not None:
                intangible_value, intangible_currency = self._normalize_record(
                    intangibles_record,
                    symbol=symbol,
                    repo=repo,
                    context=context,
                    input_name=intangibles_record.concept,
                )

            currency = self._combine_currency(
                [equity_currency, goodwill_currency, intangible_currency]
            )
            if currency is None and any(
                code is not None
                for code in (equity_currency, goodwill_currency, intangible_currency)
            ):
                LOGGER.warning(
                    "%s: TCE currency mismatch on %s for %s",
                    context,
                    equity_record.end_date,
                    symbol,
                )
                continue

            points.append(
                _BalancePoint(
                    value=equity_value - goodwill_value - intangible_value,
                    as_of=equity_record.end_date,
                    fiscal_period=key[1],
                    currency=currency,
                )
            )
        return points

    def _compute_average_balance_points(
        self,
        quarterly_points: list[_BalancePoint],
        fy_points: list[_BalancePoint],
        *,
        symbol: str,
        context: str,
        allow_fy_fallback: bool,
    ) -> Optional[_AmountSnapshot]:
        latest_quarter = self._select_latest_balance_point(
            quarterly_points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context=context,
            symbol=symbol,
        )
        if latest_quarter is not None:
            latest_year = self._parse_year(latest_quarter.as_of)
            if latest_year is not None:
                for point in quarterly_points[1:]:
                    point_year = self._parse_year(point.as_of)
                    if (
                        point_year is not None
                        and point.fiscal_period == latest_quarter.fiscal_period
                        and point_year == latest_year - 1
                    ):
                        if not self._currencies_match(
                            latest_quarter.currency, point.currency
                        ):
                            LOGGER.warning(
                                "%s: quarterly denominator currency mismatch for %s",
                                context,
                                symbol,
                            )
                            return None
                        return _AmountSnapshot(
                            value=(latest_quarter.value + point.value) / 2.0,
                            as_of=latest_quarter.as_of,
                            currency=latest_quarter.currency or point.currency,
                        )

        if not allow_fy_fallback:
            LOGGER.warning(
                "%s: missing same-quarter prior-year pair for %s", context, symbol
            )
            return None

        latest_fy = self._select_latest_balance_point(
            fy_points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context=context,
            symbol=symbol,
        )
        if latest_fy is None:
            return None

        latest_year = self._parse_year(latest_fy.as_of)
        if latest_year is None:
            LOGGER.warning("%s: invalid latest FY date for %s", context, symbol)
            return None
        for point in fy_points[1:]:
            point_year = self._parse_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                if not self._currencies_match(latest_fy.currency, point.currency):
                    LOGGER.warning(
                        "%s: FY denominator currency mismatch for %s", context, symbol
                    )
                    return None
                return _AmountSnapshot(
                    value=(latest_fy.value + point.value) / 2.0,
                    as_of=latest_fy.as_of,
                    currency=latest_fy.currency or point.currency,
                )

        LOGGER.warning("%s: missing strict prior FY pair for %s", context, symbol)
        return None

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> Optional[_AmountSnapshot]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                continue
            normalized, currency = self._normalize_records(
                quarterly[:4],
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=concept,
                absolute=absolute,
            )
            return _AmountSnapshot(
                value=sum(normalized),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _latest_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        max_age_days: int,
        absolute: bool = False,
    ) -> Optional[_AmountSnapshot]:
        for concept in concepts:
            record = repo.latest_fact(symbol, concept)
            if record is None:
                continue
            if not is_recent_fact(record, max_age_days=max_age_days):
                continue
            value, currency = self._normalize_record(
                record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=concept,
                absolute=absolute,
            )
            return _AmountSnapshot(
                value=value, as_of=record.end_date, currency=currency
            )
        LOGGER.warning(
            "%s: missing recent %s for %s", context, ",".join(concepts), symbol
        )
        return None

    def _build_fcf_fy_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        context: str,
    ) -> list[_FYPoint]:
        operating_map = self._build_fy_amount_map(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
        )
        capex_map = self._build_fy_amount_map(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            context=context,
        )
        points: list[_FYPoint] = []
        for year in sorted(operating_map.keys(), reverse=True):
            operating = operating_map[year]
            capex = capex_map.get(year)
            currency = self._combine_currency(
                [operating.currency, capex.currency if capex is not None else None]
            )
            if currency is None and any(
                code is not None
                for code in (
                    operating.currency,
                    capex.currency if capex is not None else None,
                )
            ):
                LOGGER.warning(
                    "%s: FY FCF currency mismatch on %s for %s",
                    context,
                    year,
                    symbol,
                )
                continue
            points.append(
                _FYPoint(
                    year=year,
                    value=operating.value - (capex.value if capex is not None else 0.0),
                    as_of=max(
                        [
                            value
                            for value in (
                                operating.as_of,
                                capex.as_of if capex is not None else None,
                            )
                            if value is not None
                        ]
                    ),
                    currency=currency,
                )
            )
        return points

    def _build_fy_amount_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> list[_FYPoint]:
        amount_map = self._build_fy_amount_map(
            symbol,
            repo,
            concepts,
            context=context,
            absolute=absolute,
        )
        points = [
            _FYPoint(
                year=year,
                value=amount.value,
                as_of=amount.as_of,
                currency=amount.currency,
            )
            for year, amount in sorted(amount_map.items(), reverse=True)
        ]
        if not points:
            LOGGER.warning("%s: missing FY history for %s", context, symbol)
        return points

    def _build_fy_amount_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> dict[int, _AmountSnapshot]:
        concept_maps = [
            self._fy_map(symbol, repo, concept, context=context, absolute=absolute)
            for concept in concepts
        ]
        merged: dict[int, _AmountSnapshot] = {}
        candidate_years: set[int] = set()
        for mapped in concept_maps:
            candidate_years.update(mapped.keys())
        for year in sorted(candidate_years, reverse=True):
            for mapped in concept_maps:
                if year in mapped:
                    merged[year] = mapped[year]
                    break
        return merged

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        context: str,
        absolute: bool = False,
    ) -> dict[int, _AmountSnapshot]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _AmountSnapshot] = {}
        for record in ordered:
            year = self._parse_year(record.end_date)
            if year is None or year in mapped:
                continue
            value, currency = self._normalize_record(
                record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=concept,
                absolute=absolute,
            )
            mapped[year] = _AmountSnapshot(
                value=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

    def _select_exact_year_pair(
        self,
        points: Sequence[_FYPoint],
        *,
        years_back: int,
        context: str,
        symbol: str,
    ) -> Optional[tuple[_FYPoint, _FYPoint]]:
        if not points:
            LOGGER.warning("%s: missing FY history for %s", context, symbol)
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest FY (%s) too old for %s", context, latest.as_of, symbol
            )
            return None
        prior = next(
            (point for point in points[1:] if point.year == latest.year - years_back),
            None,
        )
        if prior is None:
            LOGGER.warning(
                "%s: missing strict FY-%s pair for %s",
                context,
                years_back,
                symbol,
            )
            return None
        if not self._currencies_match(latest.currency, prior.currency):
            LOGGER.warning("%s: endpoint currency mismatch for %s", context, symbol)
            return None
        return latest, prior

    def _period_record_map(
        self,
        records: Sequence[FactRecord],
        periods: set[str],
    ) -> dict[tuple[str, str], FactRecord]:
        mapped: dict[tuple[str, str], FactRecord] = {}
        for record in self._filter_periods(records, periods):
            key = (record.end_date, (record.fiscal_period or "").upper())
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _filter_periods(
        self,
        records: Sequence[FactRecord],
        periods: set[str],
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _select_latest_balance_point(
        self,
        points: Sequence[_BalancePoint],
        *,
        max_age_days: int,
        context: str,
        symbol: str,
    ) -> Optional[_BalancePoint]:
        if not points:
            LOGGER.warning("%s: missing denominator history for %s", context, symbol)
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest point (%s) too old for %s", context, latest.as_of, symbol
            )
            return None
        return latest

    def _normalize_records(
        self,
        records: Sequence[FactRecord],
        *,
        symbol: str,
        repo: FinancialFactsRepository,
        context: str,
        input_name: str,
        absolute: bool = False,
    ) -> tuple[list[float], str]:
        normalized: list[float] = []
        currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=context,
            input_name=input_name,
            as_of=records[0].end_date if records else None,
            candidate_currencies=[record.currency for record in records],
        )
        for record in records:
            value, code = self._normalize_record(
                record,
                symbol=symbol,
                repo=repo,
                context=context,
                input_name=input_name,
                absolute=absolute,
            )
            normalized.append(value)
            currency = code
        return normalized, currency

    def _normalize_record(
        self,
        record: FactRecord,
        *,
        symbol: str,
        repo: FinancialFactsRepository,
        context: str,
        input_name: str,
        absolute: bool = False,
    ) -> tuple[float, str]:
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=context,
            input_name=input_name,
            as_of=record.end_date,
            candidate_currencies=[record.currency],
        )
        value, currency = normalize_metric_record(
            record,
            metric_id=context,
            symbol=symbol,
            input_name=input_name,
            expected_currency=target_currency,
            contexts=(repo,),
        )
        if absolute:
            value = abs(value)
        return value, currency

    def _combine_currency(self, values: Sequence[Optional[str]]) -> Optional[str]:
        merged: Optional[str] = None
        for value in values:
            if not value:
                continue
            if merged is None:
                merged = value
            elif merged != value:
                return None
        return merged

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True

    def _parse_year(self, as_of: str) -> Optional[int]:
        try:
            return date.fromisoformat(as_of).year
        except ValueError:
            return None

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))


@dataclass
class GrossMarginTTMMetric:
    """Compute trailing twelve-month gross margin."""

    id: str = "gross_margin_ttm"
    required_concepts = GROSS_MARGIN_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_gross_margin_ttm(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class OperatingMarginTTMMetric:
    """Compute trailing twelve-month operating margin."""

    id: str = "operating_margin_ttm"
    required_concepts = OPERATING_MARGIN_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_operating_margin_ttm(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class FCFMarginTTMMetric:
    """Compute trailing twelve-month free-cash-flow margin."""

    id: str = "fcf_margin_ttm"
    required_concepts = FCF_MARGIN_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_fcf_margin_ttm(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class ROETTMMetric:
    """Compute trailing twelve-month ROE using averaged common equity."""

    id: str = "roe_ttm"
    required_concepts = ROE_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roe_ttm(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class ROATTMMetric:
    """Compute trailing twelve-month ROA using average total assets."""

    id: str = "roa_ttm"
    required_concepts = ROA_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roa_ttm(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class ROETangibleCommonEquityTTMMetric:
    """Compute trailing twelve-month return on tangible common equity."""

    id: str = "roetce_ttm"
    required_concepts = ROETCE_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roetce_ttm(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class DividendYieldTTMMetric:
    """Compute trailing twelve-month dividend yield."""

    id: str = "dividend_yield_ttm"
    required_concepts = DIVIDEND_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_dividend_yield_ttm(
            symbol,
            repo,
            market_repo,
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class ShareholderYieldTTMMetric:
    """Compute trailing twelve-month shareholder yield."""

    id: str = "shareholder_yield_ttm"
    required_concepts = SHAREHOLDER_YIELD_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        dividend_yield = DividendYieldTTMMetric().compute(symbol, repo, market_repo)
        if dividend_yield is None:
            LOGGER.warning(
                "shareholder_yield_ttm: missing dividend yield for %s", symbol
            )
            return None
        buyback_yield = NetBuybackYieldMetric().compute(symbol, repo, market_repo)
        if buyback_yield is None:
            LOGGER.warning(
                "shareholder_yield_ttm: missing net buyback yield for %s", symbol
            )
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=dividend_yield.value + buyback_yield.value,
            as_of=max(dividend_yield.as_of, buyback_yield.as_of),
        )


@dataclass
class DividendPayoutRatioTTMMetric:
    """Compute trailing twelve-month dividend payout ratio."""

    id: str = "dividend_payout_ratio_ttm"
    required_concepts = DIVIDEND_PAYOUT_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_dividend_payout_ratio_ttm(
                symbol, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class RevenueCAGR10YMetric:
    """Compute 10-year revenue CAGR from strict FY endpoints."""

    id: str = "revenue_cagr_10y"
    required_concepts = REVENUE_CAGR_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_revenue_cagr_10y(
            symbol, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class FCFPerShareCAGR10YMetric:
    """Compute 10-year FCF-per-share CAGR from strict FY endpoints."""

    id: str = "fcf_per_share_cagr_10y"
    required_concepts = FCF_PER_SHARE_CAGR_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_fcf_per_share_cagr_10y(
                symbol, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class OwnerEarningsCAGR10YMetric:
    """Compute 10-year enterprise owner-earnings CAGR using 3-year average endpoints."""

    id: str = "owner_earnings_cagr_10y"
    required_concepts = OE_ENTERPRISE_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_10y_cagr(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


@dataclass
class GrossProfitToAssetsTTMMetric:
    """Compute trailing twelve-month gross profit relative to average assets."""

    id: str = "gross_profit_to_assets_ttm"
    required_concepts = GROSS_PROFIT_TO_ASSETS_REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_gross_profit_to_assets_ttm(
                symbol, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=snapshot.value, as_of=snapshot.as_of
        )


__all__ = [
    "GrossMarginTTMMetric",
    "OperatingMarginTTMMetric",
    "FCFMarginTTMMetric",
    "ROETTMMetric",
    "ROATTMMetric",
    "ROETangibleCommonEquityTTMMetric",
    "DividendYieldTTMMetric",
    "ShareholderYieldTTMMetric",
    "DividendPayoutRatioTTMMetric",
    "RevenueCAGR10YMetric",
    "FCFPerShareCAGR10YMetric",
    "OwnerEarningsCAGR10YMetric",
    "GrossProfitToAssetsTTMMetric",
    "ProfitabilityReturnsGrowthCalculator",
]
