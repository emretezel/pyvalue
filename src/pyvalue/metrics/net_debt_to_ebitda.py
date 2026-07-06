"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.balance_sheet import (
    CASH_CONCEPTS,
    DEBT_CONCEPTS,
    resolve_cash_position,
    resolve_total_debt,
)
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import paired_records, resolve_ttm_window
from pyvalue.metrics.utils import (
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
DA_PRIMARY_CONCEPTS = ("DepreciationDepletionAndAmortization",)
DA_FALLBACK_CONCEPTS = ("DepreciationFromCashFlow",)


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
        debt = resolve_total_debt(
            listing_id, repo, target_currency=target_currency, metric_id=self.id
        )
        if debt is None:
            return None

        cash = resolve_cash_position(
            listing_id, repo, target_currency=target_currency, metric_id=self.id
        )
        if cash is None:
            return None

        return _MoneyResult(
            money=debt.money - cash.money, as_of=max(debt.as_of, cash.as_of)
        )

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
