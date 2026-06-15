"""TTM profitability, return, dividend, and 10Y growth metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.accruals_ratio import AccrualsRatioCalculator
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.buyback_yield import NetBuybackYieldMetric
from pyvalue.metrics.owner_earnings_enterprise import (
    REQUIRED_CONCEPTS as OE_ENTERPRISE_REQUIRED_CONCEPTS,
    OwnerEarningsEnterpriseCalculator,
)
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    SHARE_COUNT_CONCEPTS,
    is_recent_fact,
    market_cap_money,
    require_metric_amount_money,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

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
    # The cash dividend yield divides by market cap (shares x price), so preload
    # the share-count concepts market_cap_money resolves.
    dict.fromkeys(
        DIVIDENDS_PAID_CONCEPTS + DIVIDENDS_PER_SHARE_CONCEPTS + SHARE_COUNT_CONCEPTS
    )
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
class _RatioSnapshot:
    """A dimensionless output (margin / return / yield / CAGR), not a money amount."""

    value: float
    as_of: str


@dataclass(frozen=True)
class _MoneySnapshot:
    """A monetary building block aligned to the listing currency."""

    money: Money
    as_of: str


@dataclass(frozen=True)
class _BalancePoint:
    money: Money
    as_of: str
    fiscal_period: str


@dataclass(frozen=True)
class _MoneyFYPoint:
    year: int
    money: Money
    as_of: str


class ProfitabilityReturnsGrowthCalculator:
    """Shared calculator for profitability, return, dividend, and growth metrics.

    Every monetary building block is aligned to the listing currency at read time
    through the shared Money seam, so each margin/return/yield is a ratio of
    same-currency ``Money`` (``Money / Money`` -> ``float``); there is no
    per-input currency reconciliation left to do.
    """

    def compute_gross_margin_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        revenue = self._compute_ttm_amount(
            listing_id,
            repo,
            REVENUE_CONCEPTS,
            context="gross_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning(
                "gross_margin_ttm: missing TTM revenue for listing_id=%s", listing_id
            )
            return None
        if revenue.money.amount <= 0:
            LOGGER.warning(
                "gross_margin_ttm: non-positive TTM revenue for listing_id=%s",
                listing_id,
            )
            return None

        cogs = self._compute_ttm_cogs(listing_id, repo, context="gross_margin_ttm")
        if cogs is None:
            LOGGER.warning(
                "gross_margin_ttm: missing TTM COGS for listing_id=%s", listing_id
            )
            return None

        gross_margin = (revenue.money - cogs.money) / revenue.money
        return _RatioSnapshot(
            value=max(-1.0, min(1.0, gross_margin)),
            as_of=max(revenue.as_of, cogs.as_of),
        )

    def compute_operating_margin_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        revenue = self._compute_ttm_amount(
            listing_id,
            repo,
            REVENUE_CONCEPTS,
            context="operating_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning(
                "operating_margin_ttm: missing TTM revenue for listing_id=%s",
                listing_id,
            )
            return None
        if revenue.money.amount <= 0:
            LOGGER.warning(
                "operating_margin_ttm: non-positive TTM revenue for listing_id=%s",
                listing_id,
            )
            return None

        ebit = self._compute_ttm_amount(
            listing_id,
            repo,
            EBIT_CONCEPTS,
            context="operating_margin_ttm",
        )
        if ebit is None:
            LOGGER.warning(
                "operating_margin_ttm: missing TTM EBIT for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=ebit.money / revenue.money,
            as_of=max(revenue.as_of, ebit.as_of),
        )

    def compute_fcf_margin_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        revenue = self._compute_ttm_amount(
            listing_id,
            repo,
            REVENUE_CONCEPTS,
            context="fcf_margin_ttm",
        )
        if revenue is None:
            LOGGER.warning(
                "fcf_margin_ttm: missing TTM revenue for listing_id=%s", listing_id
            )
            return None
        if revenue.money.amount <= 0:
            LOGGER.warning(
                "fcf_margin_ttm: non-positive TTM revenue for listing_id=%s", listing_id
            )
            return None

        fcf = self._compute_ttm_fcf(listing_id, repo, context="fcf_margin_ttm")
        if fcf is None:
            LOGGER.warning(
                "fcf_margin_ttm: missing TTM FCF for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=fcf.money / revenue.money,
            as_of=max(revenue.as_of, fcf.as_of),
        )

    def compute_gross_profit_to_assets_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        gross_profit = self._compute_ttm_gross_profit(
            listing_id,
            repo,
            context="gross_profit_to_assets_ttm",
        )
        if gross_profit is None:
            LOGGER.warning(
                "gross_profit_to_assets_ttm: missing TTM gross profit for"
                " listing_id=%s",
                listing_id,
            )
            return None

        avg_assets = AccrualsRatioCalculator().compute_avg_total_assets(
            listing_id, repo
        )
        if avg_assets is None:
            return None
        if avg_assets.money.amount <= 0:
            LOGGER.warning(
                "gross_profit_to_assets_ttm: non-positive average assets for"
                " listing_id=%s",
                listing_id,
            )
            return None

        return _RatioSnapshot(
            value=gross_profit.money / avg_assets.money,
            as_of=max(gross_profit.as_of, avg_assets.as_of),
        )

    def compute_roe_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        net_income = self._compute_ttm_amount(
            listing_id,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="roe_ttm",
        )
        if net_income is None:
            LOGGER.warning(
                "roe_ttm: missing TTM net income for listing_id=%s", listing_id
            )
            return None

        avg_equity = self._compute_avg_common_equity(
            listing_id, repo, context="roe_ttm"
        )
        if avg_equity is None:
            return None
        if avg_equity.money.amount <= 0:
            LOGGER.warning(
                "roe_ttm: non-positive average equity for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=net_income.money / avg_equity.money,
            as_of=max(net_income.as_of, avg_equity.as_of),
        )

    def compute_roa_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        net_income = self._compute_ttm_amount(
            listing_id,
            repo,
            NET_INCOME_CONCEPTS + NET_INCOME_COMMON_CONCEPTS,
            context="roa_ttm",
        )
        if net_income is None:
            LOGGER.warning(
                "roa_ttm: missing TTM net income for listing_id=%s", listing_id
            )
            return None

        avg_assets = AccrualsRatioCalculator().compute_avg_total_assets(
            listing_id, repo
        )
        if avg_assets is None:
            return None
        if avg_assets.money.amount <= 0:
            LOGGER.warning(
                "roa_ttm: non-positive average assets for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=net_income.money / avg_assets.money,
            as_of=max(net_income.as_of, avg_assets.as_of),
        )

    def compute_roetce_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        net_income = self._compute_ttm_amount(
            listing_id,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="roetce_ttm",
        )
        if net_income is None:
            LOGGER.warning(
                "roetce_ttm: missing TTM net income for listing_id=%s", listing_id
            )
            return None

        avg_tce = self._compute_avg_tangible_common_equity(
            listing_id,
            repo,
            context="roetce_ttm",
        )
        if avg_tce is None:
            return None
        if avg_tce.money.amount <= 0:
            LOGGER.warning(
                "roetce_ttm: non-positive average TCE for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=net_income.money / avg_tce.money,
            as_of=max(net_income.as_of, avg_tce.as_of),
        )

    def compute_dividend_yield_ttm(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[_RatioSnapshot]:
        dividends_paid = self._compute_ttm_amount(
            listing_id,
            repo,
            DIVIDENDS_PAID_CONCEPTS,
            context="dividend_yield_ttm",
            absolute=True,
        )
        if dividends_paid is not None:
            cash_yield = self._compute_cash_dividend_yield(
                listing_id,
                dividends_paid,
                repo,
                market_repo,
                context="dividend_yield_ttm",
            )
            if cash_yield is not None:
                return cash_yield

        dividend_per_share = self._latest_amount(
            listing_id,
            repo,
            DIVIDENDS_PER_SHARE_CONCEPTS,
            context="dividend_yield_ttm",
            max_age_days=MAX_FACT_AGE_DAYS,
            absolute=True,
        )
        if dividend_per_share is None:
            LOGGER.warning(
                "dividend_yield_ttm: missing cash dividends and DPS fallback for"
                " listing_id=%s",
                listing_id,
            )
            return None

        snapshot = market_repo.latest_snapshot_by_id(listing_id)
        if snapshot is None or snapshot.price is None or snapshot.price <= 0:
            LOGGER.warning(
                "dividend_yield_ttm: missing price snapshot for listing_id=%s",
                listing_id,
            )
            return None

        price_money = require_metric_amount_money(
            snapshot.price,
            getattr(snapshot, "currency", None),
            target_currency=dividend_per_share.money.currency,
            metric_id="dividend_yield_ttm",
            listing_id=listing_id,
            input_name="price",
            as_of=snapshot.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning(
                "dividend_yield_ttm: non-positive price for listing_id=%s", listing_id
            )
            return None

        return _RatioSnapshot(
            value=dividend_per_share.money / price_money,
            as_of=max(dividend_per_share.as_of, snapshot.as_of),
        )

    def compute_dividend_payout_ratio_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        dividends_paid = self._compute_ttm_amount(
            listing_id,
            repo,
            DIVIDENDS_PAID_CONCEPTS,
            context="dividend_payout_ratio_ttm",
            absolute=True,
        )
        if dividends_paid is None:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: missing TTM cash dividends for"
                " listing_id=%s",
                listing_id,
            )
            return None

        net_income = self._compute_ttm_amount(
            listing_id,
            repo,
            NET_INCOME_COMMON_CONCEPTS + NET_INCOME_CONCEPTS,
            context="dividend_payout_ratio_ttm",
        )
        if net_income is None:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: missing TTM net income for listing_id=%s",
                listing_id,
            )
            return None
        if net_income.money.amount <= 0:
            LOGGER.warning(
                "dividend_payout_ratio_ttm: non-positive TTM net income for"
                " listing_id=%s",
                listing_id,
            )
            return None

        return _RatioSnapshot(
            value=dividends_paid.money / net_income.money,
            as_of=max(dividends_paid.as_of, net_income.as_of),
        )

    def compute_revenue_cagr_10y(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        points = self._build_fy_amount_points(
            listing_id,
            repo,
            REVENUE_CONCEPTS,
            context="revenue_cagr_10y",
        )
        pair = self._select_exact_year_pair(
            points,
            years_back=TEN_YEARS,
            context="revenue_cagr_10y",
            listing_id=listing_id,
        )
        if pair is None:
            return None
        latest, prior = pair
        if latest.money.amount <= 0 or prior.money.amount <= 0:
            LOGGER.warning(
                "revenue_cagr_10y: non-positive revenue endpoints for listing_id=%s",
                listing_id,
            )
            return None

        return _RatioSnapshot(
            value=(latest.money / prior.money) ** (1.0 / TEN_YEARS) - 1.0,
            as_of=latest.as_of,
        )

    def compute_fcf_per_share_cagr_10y(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_RatioSnapshot]:
        fcf_points = self._build_fcf_fy_points(
            listing_id,
            repo,
            context="fcf_per_share_cagr_10y",
        )
        # Diluted shares are a *count* (ScalarFact, no currency), so FCF/share is
        # FCF (Money) divided by a share quantity -- a per-share Money amount, not
        # a dimensionless ratio. Reading them through the scalar boundary makes the
        # type system reject treating the share count as money.
        share_counts = self._build_fy_share_count_map(
            listing_id, repo, DILUTED_SHARES_CONCEPT
        )
        if not share_counts:
            LOGGER.warning(
                "fcf_per_share_cagr_10y: missing diluted share history for"
                " listing_id=%s",
                listing_id,
            )
            return None

        per_share_points: list[_MoneyFYPoint] = []
        for fcf_point in fcf_points:
            share_count = share_counts.get(fcf_point.year)
            if share_count is None or share_count <= 0:
                continue
            per_share_points.append(
                _MoneyFYPoint(
                    year=fcf_point.year,
                    money=fcf_point.money / share_count,
                    as_of=fcf_point.as_of,
                )
            )

        pair = self._select_exact_year_pair(
            per_share_points,
            years_back=TEN_YEARS,
            context="fcf_per_share_cagr_10y",
            listing_id=listing_id,
        )
        if pair is None:
            return None
        latest, prior = pair
        if latest.money.amount <= 0 or prior.money.amount <= 0:
            LOGGER.warning(
                "fcf_per_share_cagr_10y: non-positive FCF/share endpoints for"
                " listing_id=%s",
                listing_id,
            )
            return None

        return _RatioSnapshot(
            value=(latest.money / prior.money) ** (1.0 / TEN_YEARS) - 1.0,
            as_of=latest.as_of,
        )

    def _compute_cash_dividend_yield(
        self,
        listing_id: int,
        dividends_paid: _MoneySnapshot,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
        *,
        context: str,
    ) -> Optional[_RatioSnapshot]:
        cap = market_cap_money(
            listing_id,
            repo=repo,
            market_repo=market_repo,
            metric_id=context,
            target_currency=dividends_paid.money.currency,
            contexts=(market_repo, repo),
        )
        if cap is None:
            LOGGER.warning(
                "%s: missing market cap for listing_id=%s", context, listing_id
            )
            return None

        return _RatioSnapshot(
            value=dividends_paid.money / cap.money,
            as_of=max(dividends_paid.as_of, cap.as_of),
        )

    def _compute_ttm_gross_profit(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[_MoneySnapshot]:
        revenue = self._compute_ttm_amount(
            listing_id, repo, REVENUE_CONCEPTS, context=context
        )
        if revenue is None:
            return None
        cogs = self._compute_ttm_cogs(listing_id, repo, context=context)
        if cogs is None:
            return None
        return _MoneySnapshot(
            money=revenue.money - cogs.money,
            as_of=max(revenue.as_of, cogs.as_of),
        )

    def _compute_ttm_cogs(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[_MoneySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        revenue_records = self._filter_periods(
            repo.monetary_facts_for_concept(listing_id, REVENUE_CONCEPT),
            QUARTERLY_PERIODS,
        )
        if len(revenue_records) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly revenue records for listing_id=%s, found %s",
                context,
                listing_id,
                len(revenue_records),
            )
            return None
        latest_revenue = revenue_records[0]
        if not is_recent_fact(latest_revenue, max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest revenue quarter (%s) too old for listing_id=%s",
                context,
                latest_revenue.end_date,
                listing_id,
            )
            return None

        cogs_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, COST_OF_REVENUE_CONCEPT),
            QUARTERLY_PERIODS,
        )
        gross_profit_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, GROSS_PROFIT_CONCEPT),
            QUARTERLY_PERIODS,
        )

        quarter_cogs: list[Money] = []
        as_of_dates: list[str] = []
        for revenue_record in revenue_records[:4]:
            key = (
                revenue_record.end_date,
                (revenue_record.fiscal_period or "").upper(),
            )
            cogs_record = cogs_map.get(key)
            if cogs_record is not None:
                quarter_cogs.append(
                    self._money(
                        cogs_record,
                        listing_id=listing_id,
                        target_currency=target_currency,
                        context=context,
                    )
                )
                as_of_dates.extend([revenue_record.end_date, cogs_record.end_date])
                continue

            gross_profit_record = gross_profit_map.get(key)
            if gross_profit_record is None:
                LOGGER.warning(
                    "%s: missing quarterly COGS/gross profit on %s for listing_id=%s",
                    context,
                    revenue_record.end_date,
                    listing_id,
                )
                return None
            revenue_money = self._money(
                revenue_record,
                listing_id=listing_id,
                target_currency=target_currency,
                context=context,
            )
            gross_profit_money = self._money(
                gross_profit_record,
                listing_id=listing_id,
                target_currency=target_currency,
                context=context,
            )
            quarter_cogs.append(revenue_money - gross_profit_money)
            as_of_dates.extend([revenue_record.end_date, gross_profit_record.end_date])

        return _MoneySnapshot(
            money=sum_money(quarter_cogs),
            as_of=max(as_of_dates),
        )

    def _compute_ttm_fcf(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[_MoneySnapshot]:
        operating_cash = self._compute_ttm_amount(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
        )
        if operating_cash is None:
            return None

        capex = self._compute_ttm_amount(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            context=context,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for listing_id=%s; assuming zero",
                context,
                listing_id,
            )
            return operating_cash
        return _MoneySnapshot(
            money=operating_cash.money - capex.money,
            as_of=max(operating_cash.as_of, capex.as_of),
        )

    def _compute_avg_common_equity(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[_MoneySnapshot]:
        quarterly_points = self._build_common_equity_points(
            listing_id, repo, QUARTERLY_PERIODS, context=context
        )
        fy_points = self._build_common_equity_points(
            listing_id,
            repo,
            FY_PERIODS,
            context=context,
        )
        return self._compute_average_balance_points(
            quarterly_points,
            fy_points,
            listing_id=listing_id,
            context=context,
            allow_fy_fallback=True,
        )

    def _compute_avg_tangible_common_equity(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> Optional[_MoneySnapshot]:
        quarterly_points = self._build_tangible_common_equity_points(
            listing_id,
            repo,
            QUARTERLY_PERIODS,
            context=context,
        )
        fy_points = self._build_tangible_common_equity_points(
            listing_id,
            repo,
            FY_PERIODS,
            context=context,
        )
        return self._compute_average_balance_points(
            quarterly_points,
            fy_points,
            listing_id=listing_id,
            context=context,
            allow_fy_fallback=True,
        )

    def _build_common_equity_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
        *,
        context: str,
    ) -> list[_BalancePoint]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        equity_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, COMMON_EQUITY_CONCEPT), periods
        )
        points: list[_BalancePoint] = []
        for (_, fiscal_period), record in sorted(
            equity_map.items(),
            key=lambda item: (item[0][0], item[0][1]),
            reverse=True,
        ):
            points.append(
                _BalancePoint(
                    money=self._money(
                        record,
                        listing_id=listing_id,
                        target_currency=target_currency,
                        context=context,
                    ),
                    as_of=record.end_date,
                    fiscal_period=fiscal_period,
                )
            )
        return points

    def _build_tangible_common_equity_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        periods: set[str],
        *,
        context: str,
    ) -> list[_BalancePoint]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        equity_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, COMMON_EQUITY_CONCEPT), periods
        )
        goodwill_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, GOODWILL_CONCEPT), periods
        )
        intangibles_primary_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, INTANGIBLE_PRIMARY_CONCEPT),
            periods,
        )
        intangibles_fallback_map = self._period_record_map(
            repo.monetary_facts_for_concept(listing_id, INTANGIBLE_FALLBACK_CONCEPT),
            periods,
        )

        points: list[_BalancePoint] = []
        for key, equity_record in sorted(
            equity_map.items(),
            key=lambda item: (item[0][0], item[0][1]),
            reverse=True,
        ):
            zero = Money.of(0.0, target_currency)
            equity_money = self._money(
                equity_record,
                listing_id=listing_id,
                target_currency=target_currency,
                context=context,
            )
            goodwill_record = goodwill_map.get(key)
            goodwill_money = (
                self._money(
                    goodwill_record,
                    listing_id=listing_id,
                    target_currency=target_currency,
                    context=context,
                )
                if goodwill_record is not None
                else zero
            )

            intangibles_record = intangibles_primary_map.get(
                key
            ) or intangibles_fallback_map.get(key)
            intangible_money = (
                self._money(
                    intangibles_record,
                    listing_id=listing_id,
                    target_currency=target_currency,
                    context=context,
                )
                if intangibles_record is not None
                else zero
            )

            points.append(
                _BalancePoint(
                    money=equity_money - goodwill_money - intangible_money,
                    as_of=equity_record.end_date,
                    fiscal_period=key[1],
                )
            )
        return points

    def _compute_average_balance_points(
        self,
        quarterly_points: list[_BalancePoint],
        fy_points: list[_BalancePoint],
        *,
        listing_id: int,
        context: str,
        allow_fy_fallback: bool,
    ) -> Optional[_MoneySnapshot]:
        latest_quarter = self._select_latest_balance_point(
            quarterly_points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context=context,
            listing_id=listing_id,
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
                        return _MoneySnapshot(
                            money=(latest_quarter.money + point.money) / 2.0,
                            as_of=latest_quarter.as_of,
                        )

        if not allow_fy_fallback:
            LOGGER.warning(
                "%s: missing same-quarter prior-year pair for listing_id=%s",
                context,
                listing_id,
            )
            return None

        latest_fy = self._select_latest_balance_point(
            fy_points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context=context,
            listing_id=listing_id,
        )
        if latest_fy is None:
            return None

        latest_year = self._parse_year(latest_fy.as_of)
        if latest_year is None:
            LOGGER.warning(
                "%s: invalid latest FY date for listing_id=%s", context, listing_id
            )
            return None
        for point in fy_points[1:]:
            point_year = self._parse_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                return _MoneySnapshot(
                    money=(latest_fy.money + point.money) / 2.0,
                    as_of=latest_fy.as_of,
                )

        LOGGER.warning(
            "%s: missing strict prior FY pair for listing_id=%s", context, listing_id
        )
        return None

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> Optional[_MoneySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        for concept in concepts:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                continue
            monies = [
                self._money(
                    record,
                    listing_id=listing_id,
                    target_currency=target_currency,
                    context=context,
                    absolute=absolute,
                )
                for record in quarterly[:4]
            ]
            return _MoneySnapshot(money=sum_money(monies), as_of=quarterly[0].end_date)
        return None

    def _latest_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        max_age_days: int,
        absolute: bool = False,
    ) -> Optional[_MoneySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        for concept in concepts:
            record = repo.latest_monetary_fact(listing_id, concept)
            if record is None:
                continue
            if not is_recent_fact(record, max_age_days=max_age_days):
                continue
            return _MoneySnapshot(
                money=self._money(
                    record,
                    listing_id=listing_id,
                    target_currency=target_currency,
                    context=context,
                    absolute=absolute,
                ),
                as_of=record.end_date,
            )
        LOGGER.warning(
            "%s: missing recent %s for listing_id=%s",
            context,
            ",".join(concepts),
            listing_id,
        )
        return None

    def _build_fcf_fy_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        context: str,
    ) -> list[_MoneyFYPoint]:
        operating_map = self._build_fy_amount_map(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context=context,
        )
        capex_map = self._build_fy_amount_map(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            context=context,
        )
        points: list[_MoneyFYPoint] = []
        for year in sorted(operating_map.keys(), reverse=True):
            operating = operating_map[year]
            capex = capex_map.get(year)
            capex_money = (
                capex.money
                if capex is not None
                else Money.of(0.0, operating.money.currency)
            )
            as_of = max(
                [operating.as_of] + ([capex.as_of] if capex is not None else [])
            )
            points.append(
                _MoneyFYPoint(
                    year=year,
                    money=operating.money - capex_money,
                    as_of=as_of,
                )
            )
        return points

    def _build_fy_amount_points(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> list[_MoneyFYPoint]:
        amount_map = self._build_fy_amount_map(
            listing_id,
            repo,
            concepts,
            context=context,
            absolute=absolute,
        )
        points = [
            _MoneyFYPoint(year=year, money=amount.money, as_of=amount.as_of)
            for year, amount in sorted(amount_map.items(), reverse=True)
        ]
        if not points:
            LOGGER.warning(
                "%s: missing FY history for listing_id=%s", context, listing_id
            )
        return points

    def _build_fy_share_count_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
    ) -> dict[int, float]:
        """Map FY year -> diluted share *count* (a scalar, currency-less, fact)."""

        records = repo.scalar_facts_for_concept(listing_id, concept, fiscal_period="FY")
        mapped: dict[int, float] = {}
        seen_end_dates: set[str] = set()
        for record in sorted(records, key=lambda item: item.end_date, reverse=True):
            if (record.fiscal_period or "").upper() not in FY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            seen_end_dates.add(record.end_date)
            year = self._parse_year(record.end_date)
            if year is None or year in mapped:
                continue
            mapped[year] = record.value
        return mapped

    def _build_fy_amount_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
    ) -> dict[int, _MoneySnapshot]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        concept_maps = [
            self._fy_map(
                listing_id,
                repo,
                concept,
                target_currency=target_currency,
                context=context,
                absolute=absolute,
            )
            for concept in concepts
        ]
        merged: dict[int, _MoneySnapshot] = {}
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
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        *,
        target_currency: str,
        context: str,
        absolute: bool = False,
    ) -> dict[int, _MoneySnapshot]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _MoneySnapshot] = {}
        for record in ordered:
            year = self._parse_year(record.end_date)
            if year is None or year in mapped:
                continue
            mapped[year] = _MoneySnapshot(
                money=self._money(
                    record,
                    listing_id=listing_id,
                    target_currency=target_currency,
                    context=context,
                    absolute=absolute,
                ),
                as_of=record.end_date,
            )
        return mapped

    def _select_exact_year_pair(
        self,
        points: Sequence[_MoneyFYPoint],
        *,
        years_back: int,
        context: str,
        listing_id: int,
    ) -> Optional[tuple[_MoneyFYPoint, _MoneyFYPoint]]:
        if not points:
            LOGGER.warning(
                "%s: missing FY history for listing_id=%s", context, listing_id
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest FY (%s) too old for listing_id=%s",
                context,
                latest.as_of,
                listing_id,
            )
            return None
        prior = next(
            (point for point in points[1:] if point.year == latest.year - years_back),
            None,
        )
        if prior is None:
            LOGGER.warning(
                "%s: missing strict FY-%s pair for listing_id=%s",
                context,
                years_back,
                listing_id,
            )
            return None
        return latest, prior

    def _period_record_map(
        self,
        records: Sequence[MonetaryFact],
        periods: set[str],
    ) -> dict[tuple[str, str], MonetaryFact]:
        mapped: dict[tuple[str, str], MonetaryFact] = {}
        for record in self._filter_periods(records, periods):
            key = (record.end_date, (record.fiscal_period or "").upper())
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _filter_periods(
        self,
        records: Sequence[MonetaryFact],
        periods: set[str],
    ) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
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
        listing_id: int,
    ) -> Optional[_BalancePoint]:
        if not points:
            LOGGER.warning(
                "%s: missing denominator history for listing_id=%s", context, listing_id
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest point (%s) too old for listing_id=%s",
                context,
                latest.as_of,
                listing_id,
            )
            return None
        return latest

    def _money(
        self,
        fact: MonetaryFact,
        *,
        listing_id: int,
        target_currency: str,
        context: str,
        absolute: bool = False,
    ) -> Money:
        money = require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )
        return abs(money) if absolute else money

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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_gross_margin_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class OperatingMarginTTMMetric:
    """Compute trailing twelve-month operating margin."""

    id: str = "operating_margin_ttm"
    required_concepts = OPERATING_MARGIN_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_operating_margin_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class FCFMarginTTMMetric:
    """Compute trailing twelve-month free-cash-flow margin."""

    id: str = "fcf_margin_ttm"
    required_concepts = FCF_MARGIN_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_fcf_margin_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class ROETTMMetric:
    """Compute trailing twelve-month ROE using averaged common equity."""

    id: str = "roe_ttm"
    required_concepts = ROE_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roe_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class ROATTMMetric:
    """Compute trailing twelve-month ROA using average total assets."""

    id: str = "roa_ttm"
    required_concepts = ROA_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roa_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class ROETangibleCommonEquityTTMMetric:
    """Compute trailing twelve-month return on tangible common equity."""

    id: str = "roetce_ttm"
    required_concepts = ROETCE_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_roetce_ttm(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class DividendYieldTTMMetric:
    """Compute trailing twelve-month dividend yield."""

    id: str = "dividend_yield_ttm"
    required_concepts = DIVIDEND_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_dividend_yield_ttm(
            listing_id,
            repo,
            market_repo,
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class ShareholderYieldTTMMetric:
    """Compute trailing twelve-month shareholder yield."""

    id: str = "shareholder_yield_ttm"
    required_concepts = SHAREHOLDER_YIELD_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        dividend_yield = DividendYieldTTMMetric().compute(listing_id, repo, market_repo)
        if dividend_yield is None:
            LOGGER.warning(
                "shareholder_yield_ttm: missing dividend yield for listing_id=%s",
                listing_id,
            )
            return None
        buyback_yield = NetBuybackYieldMetric().compute(listing_id, repo, market_repo)
        if buyback_yield is None:
            LOGGER.warning(
                "shareholder_yield_ttm: missing net buyback yield for listing_id=%s",
                listing_id,
            )
            return None
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_dividend_payout_ratio_ttm(
                listing_id, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class RevenueCAGR10YMetric:
    """Compute 10-year revenue CAGR from strict FY endpoints."""

    id: str = "revenue_cagr_10y"
    required_concepts = REVENUE_CAGR_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ProfitabilityReturnsGrowthCalculator().compute_revenue_cagr_10y(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class FCFPerShareCAGR10YMetric:
    """Compute 10-year FCF-per-share CAGR from strict FY endpoints."""

    id: str = "fcf_per_share_cagr_10y"
    required_concepts = FCF_PER_SHARE_CAGR_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_fcf_per_share_cagr_10y(
                listing_id, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class OwnerEarningsCAGR10YMetric:
    """Compute 10-year enterprise owner-earnings CAGR using 3-year average endpoints."""

    id: str = "owner_earnings_cagr_10y"
    required_concepts = OE_ENTERPRISE_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEnterpriseCalculator().compute_10y_cagr(
            listing_id, repo
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class GrossProfitToAssetsTTMMetric:
    """Compute trailing twelve-month gross profit relative to average assets."""

    id: str = "gross_profit_to_assets_ttm"
    required_concepts = GROSS_PROFIT_TO_ASSETS_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = (
            ProfitabilityReturnsGrowthCalculator().compute_gross_profit_to_assets_ttm(
                listing_id, repo
            )
        )
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
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
