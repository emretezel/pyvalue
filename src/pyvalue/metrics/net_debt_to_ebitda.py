"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import paired_records, resolve_ttm_window
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
DA_PRIMARY_CONCEPTS = ("DepreciationDepletionAndAmortization",)
DA_FALLBACK_CONCEPTS = ("DepreciationFromCashFlow",)
DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
CASH_CONCEPTS = (
    "CashAndShortTermInvestments",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class NetDebtToEBITDAMetric:
    """Compute net debt to TTM EBITDA for EODHD-normalized facts."""

    id: str = "net_debt_to_ebitda"
    required_concepts = tuple(
        EBIT_CONCEPTS
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + DEBT_CONCEPTS
        + CASH_CONCEPTS
    )

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; every monetary input is aligned to
        # it before any Money arithmetic, so the ratio is currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id
        )

        ttm_ebitda = self._compute_ttm_ebitda(listing_id, repo, target_currency)
        if ttm_ebitda is None:
            LOGGER.warning(
                "net_debt_to_ebitda: missing TTM EBITDA for listing_id=%s", listing_id
            )
            return None
        if ttm_ebitda.money.amount <= 0:
            LOGGER.warning(
                "net_debt_to_ebitda: non-positive EBITDA for listing_id=%s", listing_id
            )
            return None

        net_debt = self._compute_net_debt(listing_id, repo, target_currency)
        if net_debt is None:
            LOGGER.warning(
                "net_debt_to_ebitda: missing net debt inputs for listing_id=%s",
                listing_id,
            )
            return None

        ratio = net_debt.money / ttm_ebitda.money
        as_of = max(ttm_ebitda.as_of, net_debt.as_of)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
            unit_kind="multiple",
        )

    def _compute_ttm_ebitda(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        resolution = resolve_ttm_window(
            repo.monetary_facts_for_concept(listing_id, EBIT_CONCEPTS[0])
        )
        window = resolution.window
        if window is None:
            LOGGER.warning(
                "net_debt_to_ebitda: %s (concept=%s, listing_id=%s)",
                resolution.failure,
                EBIT_CONCEPTS[0],
                listing_id,
            )
            return None

        # Primary D&A rows precede the fallback rows: paired_records keeps the
        # first candidate per end_date, so the primary concept wins a quarter
        # and the fallback only fills its holes -- the same per-quarter
        # primary-else-fallback rule as before the window refactor.
        pairs = paired_records(
            window,
            [
                *repo.monetary_facts_for_concept(listing_id, DA_PRIMARY_CONCEPTS[0]),
                *repo.monetary_facts_for_concept(listing_id, DA_FALLBACK_CONCEPTS[0]),
            ],
        )
        if pairs is None:
            LOGGER.warning(
                "net_debt_to_ebitda: missing D&A for a TTM window quarter (listing_id=%s)",
                listing_id,
            )
            return None

        quarter_totals: list[Money] = []
        for ebit_record, da_record in pairs:
            ebit_money = self._money(
                ebit_record, EBIT_CONCEPTS[0], target_currency, listing_id
            )
            da_money = self._money(
                da_record, DA_PRIMARY_CONCEPTS[0], target_currency, listing_id
            )
            quarter_totals.append(ebit_money + da_money)

        return _MoneyResult(money=sum_money(quarter_totals), as_of=window.as_of)

    def _compute_net_debt(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        short_debt = self._latest_recent_fact(repo, listing_id, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, listing_id, "LongTermDebt")
        if short_debt is None and long_debt is None:
            return None

        cash = self._compute_cash(listing_id, repo, target_currency)
        if cash is None:
            return None

        debt_money: Optional[Money] = None
        as_of_candidates = [cash.as_of]
        if short_debt is not None:
            debt_money = self._money(
                short_debt, "ShortTermDebt", target_currency, listing_id
            )
            as_of_candidates.append(short_debt.end_date)
        if long_debt is not None:
            long_money = self._money(
                long_debt, "LongTermDebt", target_currency, listing_id
            )
            debt_money = long_money if debt_money is None else debt_money + long_money
            as_of_candidates.append(long_debt.end_date)
        # At least one debt component is present (guarded above).
        assert debt_money is not None

        return _MoneyResult(money=debt_money - cash.money, as_of=max(as_of_candidates))

    def _compute_cash(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        primary = self._latest_recent_fact(
            repo, listing_id, "CashAndShortTermInvestments"
        )
        if primary is not None:
            return _MoneyResult(
                money=self._money(
                    primary, "CashAndShortTermInvestments", target_currency, listing_id
                ),
                as_of=primary.end_date,
            )

        cash_eq = self._latest_recent_fact(repo, listing_id, "CashAndCashEquivalents")
        if cash_eq is None:
            return None
        short_term_investments = self._latest_recent_fact(
            repo, listing_id, "ShortTermInvestments"
        )

        cash_money = self._money(
            cash_eq, "CashAndCashEquivalents", target_currency, listing_id
        )
        as_of_candidates = [cash_eq.end_date]
        if short_term_investments is not None:
            cash_money = cash_money + self._money(
                short_term_investments,
                "ShortTermInvestments",
                target_currency,
                listing_id,
            )
            as_of_candidates.append(short_term_investments.end_date)
        return _MoneyResult(money=cash_money, as_of=max(as_of_candidates))

    def _latest_recent_fact(
        self,
        repo: RegionFactsRepository,
        listing_id: int,
        concept: str,
    ) -> Optional[MonetaryFact]:
        record = repo.latest_monetary_fact(listing_id, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _money(
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        listing_id: int,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name=concept,
            as_of=fact.end_date,
        )


__all__ = ["NetDebtToEBITDAMetric"]
