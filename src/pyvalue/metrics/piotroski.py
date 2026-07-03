"""Piotroski F-Score composite quality metric.

The canonical 9-signal fundamental score from Piotroski (2000): three
profitability signals, three leverage/liquidity signals, and three
operating-efficiency signals, one point each. Applied to a high
book-to-market universe the paper showed the high-score portfolio adding
roughly 7.5%/yr, which is why value screens use ``piotroski_f_score >= 7``
as a quality gate.

Faithful to the paper, the score is fiscal-year based: the latest FY (t)
against the prior FY (t-1), with return and turnover denominators using
*beginning-of-year* total assets -- which requires three consecutive FY
balance sheets (t, t-1, t-2). Facts are paired by the calendar year of
their FY end dates.

Strictness: every signal must be computable or no score is emitted --
a 5-of-7 is not comparable to a 5-of-9, so partial scores would poison
cross-sectional screens. The one deliberate exception: a missing
``LongTermDebt`` fact reads as zero debt (the absence of the line item is
itself the datum), and a debt-free pair earns the leverage point.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    extract_year,
    is_recent_date,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

NET_INCOME_CONCEPT = "NetIncomeLoss"
ASSETS_CONCEPT = "Assets"
CFO_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
LONG_TERM_DEBT_CONCEPT = "LongTermDebt"
CURRENT_ASSETS_CONCEPT = "AssetsCurrent"
CURRENT_LIABILITIES_CONCEPT = "LiabilitiesCurrent"
GROSS_PROFIT_CONCEPT = "GrossProfit"
COST_OF_REVENUE_CONCEPT = "CostOfRevenue"
REVENUE_CONCEPT = "Revenues"
SHARES_CONCEPT = "CommonStockSharesOutstanding"

REQUIRED_CONCEPTS = (
    NET_INCOME_CONCEPT,
    ASSETS_CONCEPT,
    CFO_CONCEPT,
    LONG_TERM_DEBT_CONCEPT,
    CURRENT_ASSETS_CONCEPT,
    CURRENT_LIABILITIES_CONCEPT,
    GROSS_PROFIT_CONCEPT,
    COST_OF_REVENUE_CONCEPT,
    REVENUE_CONCEPT,
    SHARES_CONCEPT,
)


@dataclass(frozen=True)
class _FYMoney:
    money: Money
    as_of: str


@dataclass
class PiotroskiFScoreMetric:
    """Compute the canonical 9-signal Piotroski F-Score from FY statements."""

    id: str = "piotroski_f_score"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; every monetary input is aligned to
        # it so cross-year comparisons and ratios are currency-safe (ratios
        # cancel the currency, but the invariant seam still applies).
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id
        )

        net_income = self._monetary_by_year(
            listing_id, repo, NET_INCOME_CONCEPT, target_currency
        )
        if not net_income:
            LOGGER.warning(
                "%s: missing FY net income for listing_id=%s", self.id, listing_id
            )
            return None
        # The latest net-income FY anchors the (t, t-1, t-2) year frame every
        # other concept is matched against.
        year = max(net_income)
        anchor = net_income[year]
        if not is_recent_date(anchor.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest FY (%s) too old for listing_id=%s",
                self.id,
                anchor.as_of,
                listing_id,
            )
            return None
        prior = year - 1

        if not self._has_years(net_income, (prior,), NET_INCOME_CONCEPT, listing_id):
            return None

        assets = self._monetary_by_year(
            listing_id, repo, ASSETS_CONCEPT, target_currency
        )
        # Beginning-of-year denominators (per the paper) need three consecutive
        # FY balance sheets: ROA_t uses Assets_(t-1), ROA_(t-1) uses Assets_(t-2),
        # and the leverage signal averages adjacent pairs.
        if not self._has_years(
            assets, (year, prior, year - 2), ASSETS_CONCEPT, listing_id
        ):
            return None
        if any(assets[y].money.amount <= 0 for y in (year, prior, year - 2)):
            LOGGER.warning(
                "%s: non-positive FY assets for listing_id=%s", self.id, listing_id
            )
            return None

        cfo = self._monetary_by_year(listing_id, repo, CFO_CONCEPT, target_currency)
        if not self._has_years(cfo, (year,), CFO_CONCEPT, listing_id):
            return None

        current_assets = self._monetary_by_year(
            listing_id, repo, CURRENT_ASSETS_CONCEPT, target_currency
        )
        current_liabilities = self._monetary_by_year(
            listing_id, repo, CURRENT_LIABILITIES_CONCEPT, target_currency
        )
        if not self._has_years(
            current_assets, (year, prior), CURRENT_ASSETS_CONCEPT, listing_id
        ):
            return None
        if not self._has_years(
            current_liabilities, (year, prior), CURRENT_LIABILITIES_CONCEPT, listing_id
        ):
            return None
        if any(current_liabilities[y].money.amount <= 0 for y in (year, prior)):
            LOGGER.warning(
                "%s: non-positive FY current liabilities for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        revenues = self._monetary_by_year(
            listing_id, repo, REVENUE_CONCEPT, target_currency
        )
        if not self._has_years(revenues, (year, prior), REVENUE_CONCEPT, listing_id):
            return None
        if any(revenues[y].money.amount <= 0 for y in (year, prior)):
            LOGGER.warning(
                "%s: non-positive FY revenue for listing_id=%s", self.id, listing_id
            )
            return None

        gross_pair = self._gross_profit_pair(
            listing_id, repo, revenues, (year, prior), target_currency
        )
        if gross_pair is None:
            return None
        gross_latest, gross_prior = gross_pair

        shares = self._shares_by_year(listing_id, repo)
        if not self._has_years(shares, (year, prior), SHARES_CONCEPT, listing_id):
            return None
        if shares[year] <= 0 or shares[prior] <= 0:
            LOGGER.warning(
                "%s: non-positive FY share count for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        long_term_debt = self._monetary_by_year(
            listing_id, repo, LONG_TERM_DEBT_CONCEPT, target_currency
        )
        ltd_latest = self._debt_or_zero(long_term_debt, year, target_currency)
        ltd_prior = self._debt_or_zero(long_term_debt, prior, target_currency)

        # --- the nine signals, one point each -------------------------------
        roa_latest = net_income[year].money / assets[prior].money
        roa_prior = net_income[prior].money / assets[year - 2].money

        # Paper definition: long-term debt over *average* total assets.
        leverage_latest = ltd_latest / (
            (assets[year].money + assets[prior].money) / 2.0
        )
        leverage_prior = ltd_prior / (
            (assets[prior].money + assets[year - 2].money) / 2.0
        )
        # A strict decrease earns the point; a debt-free pair also earns it --
        # 0 -> 0 is "did not lever up", and penalizing debt-free issuers is a
        # known quirk of the literal paper reading we deliberately avoid.
        debt_free = ltd_latest.amount == 0.0 and ltd_prior.amount == 0.0

        current_ratio_latest = (
            current_assets[year].money / current_liabilities[year].money
        )
        current_ratio_prior = (
            current_assets[prior].money / current_liabilities[prior].money
        )

        margin_latest = gross_latest / revenues[year].money
        margin_prior = gross_prior / revenues[prior].money

        turnover_latest = revenues[year].money / assets[prior].money
        turnover_prior = revenues[prior].money / assets[year - 2].money

        signals = (
            roa_latest > 0.0,  # F1: profitable
            cfo[year].money.amount > 0.0,  # F2: cash-generative
            roa_latest > roa_prior,  # F3: improving returns
            cfo[year].money.amount > net_income[year].money.amount,  # F4: accruals
            leverage_latest < leverage_prior or debt_free,  # F5: deleveraging
            current_ratio_latest > current_ratio_prior,  # F6: improving liquidity
            shares[year] <= shares[prior],  # F7: no dilution
            margin_latest > margin_prior,  # F8: improving gross margin
            turnover_latest > turnover_prior,  # F9: improving asset turnover
        )
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=float(sum(signals)),
            as_of=anchor.as_of,
        )

    def _monetary_by_year(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        target_currency: str,
    ) -> dict[int, _FYMoney]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        by_year: dict[int, _FYMoney] = {}
        # Newest end_date wins within a calendar year (restatements re-file the
        # same fiscal year with a later date).
        for record in sorted(records, key=lambda r: r.end_date, reverse=True):
            record_year = extract_year(record.end_date)
            if record_year is None or record_year in by_year:
                continue
            by_year[record_year] = _FYMoney(
                money=require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id=self.id,
                    listing_id=listing_id,
                    input_name=concept,
                    as_of=record.end_date,
                ),
                as_of=record.end_date,
            )
        return by_year

    def _shares_by_year(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> dict[int, float]:
        # Share counts are dimensionless scalars; EODHD back-adjusts history to
        # the current split basis, so a year-over-year comparison is valid.
        records = repo.scalar_facts_for_concept(
            listing_id, SHARES_CONCEPT, fiscal_period="FY"
        )
        by_year: dict[int, float] = {}
        for record in sorted(records, key=lambda r: r.end_date, reverse=True):
            record_year = extract_year(record.end_date)
            if record_year is None or record_year in by_year:
                continue
            by_year[record_year] = record.value
        return by_year

    def _has_years(
        self,
        mapping: dict[int, _FYMoney] | dict[int, float],
        years: Sequence[int],
        concept: str,
        listing_id: int,
    ) -> bool:
        missing = [y for y in years if y not in mapping]
        if missing:
            LOGGER.warning(
                "%s: missing FY %s for years %s (listing_id=%s)",
                self.id,
                concept,
                missing,
                listing_id,
            )
            return False
        return True

    def _debt_or_zero(
        self, mapping: dict[int, _FYMoney], year: int, target_currency: str
    ) -> Money:
        entry = mapping.get(year)
        if entry is None:
            return Money.of(0.0, target_currency)
        return entry.money

    def _gross_profit_pair(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        revenues: dict[int, _FYMoney],
        years: tuple[int, int],
        target_currency: str,
    ) -> Optional[tuple[Money, Money]]:
        # One consistent basis across the pair: reported GrossProfit for both
        # years, else Revenues - CostOfRevenue for both. Mixing bases across
        # years could flip the margin signal on presentation noise alone.
        latest_year, prior_year = years
        gross = self._monetary_by_year(
            listing_id, repo, GROSS_PROFIT_CONCEPT, target_currency
        )
        if latest_year in gross and prior_year in gross:
            return gross[latest_year].money, gross[prior_year].money

        cost = self._monetary_by_year(
            listing_id, repo, COST_OF_REVENUE_CONCEPT, target_currency
        )
        if latest_year in cost and prior_year in cost:
            return (
                revenues[latest_year].money - cost[latest_year].money,
                revenues[prior_year].money - cost[prior_year].money,
            )

        LOGGER.warning(
            "%s: no consistent gross-profit basis for listing_id=%s",
            self.id,
            listing_id,
        )
        return None


__all__ = ["PiotroskiFScoreMetric"]
