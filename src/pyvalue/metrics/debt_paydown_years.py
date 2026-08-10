"""Debt paydown and FCF-to-debt metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.balance_sheet import DEBT_EVIDENCE_CONCEPTS, resolve_total_debt
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import resolve_ttm_window
from pyvalue.metrics.utils import (
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

OPERATING_CASH_FLOW_CONCEPTS = ("NetCashProvidedByUsedInOperatingActivities",)
CAPEX_CONCEPTS = ("CapitalExpenditures",)
REQUIRED_CONCEPTS = tuple(
    OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS + DEBT_EVIDENCE_CONCEPTS
)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class _FCFDebtInputs:
    fcf: _MoneyResult
    debt: _MoneyResult
    as_of: str


class _FCFDebtCalculator:
    def compute_inputs(
        self, listing_id: int, repo: RegionFactsRepository, *, metric_id: str
    ) -> Optional[_FCFDebtInputs]:
        # Resolve the listing currency once; debt and FCF are both aligned to it,
        # so the debt/FCF (and FCF/debt) ratios are currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=metric_id
        )

        debt = self._compute_total_debt(
            listing_id, repo, metric_id=metric_id, target_currency=target_currency
        )
        if debt is None:
            LOGGER.warning(
                "%s: missing debt inputs for listing_id=%s", metric_id, listing_id
            )
            return None
        if debt.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive debt for listing_id=%s", metric_id, listing_id
            )
            return None

        fcf = self._compute_ttm_fcf(
            listing_id, repo, metric_id=metric_id, target_currency=target_currency
        )
        if fcf is None:
            LOGGER.warning(
                "%s: missing TTM FCF for listing_id=%s", metric_id, listing_id
            )
            return None
        if fcf.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive FCF for listing_id=%s", metric_id, listing_id
            )
            return None

        return _FCFDebtInputs(
            fcf=fcf,
            debt=debt,
            as_of=max(debt.as_of, fcf.as_of),
        )

    def _compute_total_debt(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        """Delegate to the one shared debt definition.

        This method used to carry its own precedence (both components, else
        the rollup, else a single side) -- one of five private copies that
        disagreed with each other and with the shared resolver. The 2026-08
        debt audit collapsed them: debt means the same thing everywhere or the
        screen mixes balance sheets across its own gates.
        """

        position = resolve_total_debt(
            listing_id,
            repo,
            target_currency=target_currency,
            metric_id=metric_id,
        )
        if position is None:
            return None
        return _MoneyResult(money=position.money, as_of=position.as_of)

    def _compute_ttm_fcf(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        operating = self._ttm_sum(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            metric_id=metric_id,
            target_currency=target_currency,
        )
        if operating is None:
            return None

        capex = self._ttm_sum(
            listing_id,
            repo,
            CAPEX_CONCEPTS,
            metric_id=metric_id,
            target_currency=target_currency,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for listing_id=%s; assuming zero",
                metric_id,
                listing_id,
            )
            return operating

        return _MoneyResult(
            money=operating.money - capex.money,
            as_of=max(operating.as_of, capex.as_of),
        )

    def _ttm_sum(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        *,
        metric_id: str,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        for concept in concepts:
            resolution = resolve_ttm_window(
                repo.monetary_facts_for_concept(listing_id, concept)
            )
            window = resolution.window
            if window is None:
                LOGGER.warning(
                    "%s: %s (concept=%s, listing_id=%s)",
                    metric_id,
                    resolution.failure,
                    concept,
                    listing_id,
                )
                continue

            monies = [
                self._money(record, concept, target_currency, listing_id, metric_id)
                for record in window.records
            ]
            return _MoneyResult(money=sum_money(monies), as_of=window.as_of)
        return None

    def _money(
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        listing_id: int,
        metric_id: str,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=concept,
            as_of=fact.end_date,
        )


@dataclass
class DebtPaydownYearsMetric:
    """Compute total debt divided by TTM free cash flow (EODHD-only)."""

    id: str = "debt_paydown_years"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(
            listing_id, repo, metric_id=self.id
        )
        if inputs is None:
            return None
        ratio = inputs.debt.money / inputs.fcf.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="multiple",
        )


@dataclass
class FCFToDebtMetric:
    """Compute TTM free cash flow divided by total debt (EODHD-only)."""

    id: str = "fcf_to_debt"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(
            listing_id, repo, metric_id=self.id
        )
        if inputs is None:
            return None
        ratio = inputs.fcf.money / inputs.debt.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="ratio",
        )


__all__ = ["DebtPaydownYearsMetric", "FCFToDebtMetric"]
