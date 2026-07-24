"""Net debt to EBITDA metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.balance_sheet import (
    CASH_CONCEPTS,
    DEBT_EVIDENCE_CONCEPTS,
    resolve_cash_position,
    resolve_total_debt,
)
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.depreciation import (
    DA_FALLBACK_CONCEPTS,
    DA_PRIMARY_CONCEPTS,
)
from pyvalue.metrics.ebitda import (
    EBIT_CONCEPT,
    VENDOR_EBITDA_CONCEPT,
    compute_component_ttm_ebitda,
)
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class NetDebtToEBITDAMetric:
    """Compute net debt to TTM EBITDA for EODHD-normalized facts."""

    id: str = "net_debt_to_ebitda"
    required_concepts = tuple(
        (EBIT_CONCEPT, VENDOR_EBITDA_CONCEPT)
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + DEBT_EVIDENCE_CONCEPTS
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

        # The EBITDA construction (component EBIT + D&A over one resolved
        # window, annual cadence opted in) lives in the shared helper so every
        # EBITDA-based metric applies the same derivation policy.
        ttm_ebitda = compute_component_ttm_ebitda(
            listing_id, repo, target_currency=target_currency, context=self.id
        )
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

        # Match the balance-sheet freshness to the income cadence: an annual
        # filer's debt/cash rows are as fresh as its once-a-year EBITDA.
        balance_sheet_max_age = (
            MAX_FY_FACT_AGE_DAYS
            if ttm_ebitda.cadence == "annual"
            else MAX_FACT_AGE_DAYS
        )
        net_debt = self._compute_net_debt(
            listing_id, repo, target_currency, max_age_days=balance_sheet_max_age
        )
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

    def _compute_net_debt(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
        *,
        max_age_days: int,
    ) -> Optional[_MoneyResult]:
        debt = resolve_total_debt(
            listing_id,
            repo,
            target_currency=target_currency,
            metric_id=self.id,
            max_age_days=max_age_days,
        )
        if debt is None:
            return None

        cash = resolve_cash_position(
            listing_id,
            repo,
            target_currency=target_currency,
            metric_id=self.id,
            max_age_days=max_age_days,
        )
        if cash is None:
            return None

        return _MoneyResult(
            money=debt.money - cash.money, as_of=max(debt.as_of, cash.as_of)
        )


__all__ = ["NetDebtToEBITDAMetric"]
