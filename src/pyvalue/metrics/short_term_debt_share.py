"""Short-term debt share metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.balance_sheet import DEBT_EVIDENCE_CONCEPTS, resolve_total_debt
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)


@dataclass
class _DebtAmount:
    money: Money
    as_of: str


@dataclass
class ShortTermDebtShareMetric:
    """Compute short-term debt as a share of total debt (EODHD-only)."""

    id: str = "short_term_debt_share"
    required_concepts = DEBT_EVIDENCE_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        short_record = self._latest_recent_fact(repo, listing_id, "ShortTermDebt")
        if short_record is None:
            LOGGER.warning(
                "short_term_debt_share: missing short-term debt for listing_id=%s",
                listing_id,
            )
            return None

        # Resolve the listing currency once; every debt input is then aligned to
        # it before any Money arithmetic, so the ratio is currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="ShortTermDebt",
            as_of=short_record.end_date,
        )
        short_debt = _DebtAmount(
            money=self._money(
                short_record, "ShortTermDebt", target_currency, listing_id
            ),
            as_of=short_record.end_date,
        )

        # The one shared debt definition -- this metric used to carry its own
        # (long side if present, else the rollup), which could put a
        # lease-blind denominator under a lease-inclusive numerator. Note the
        # denominator is now the broader reading universe-wide, so the ratio
        # sits lower than it did before the 2026-08 debt audit.
        total_debt = resolve_total_debt(
            listing_id,
            repo,
            target_currency=target_currency,
            metric_id=self.id,
        )
        if total_debt is None:
            LOGGER.warning(
                "short_term_debt_share: missing usable total debt for listing_id=%s",
                listing_id,
            )
            return None

        if total_debt.money.amount <= 0:
            LOGGER.warning(
                "short_term_debt_share: non-positive total debt for listing_id=%s",
                listing_id,
            )
            return None

        ratio = short_debt.money / total_debt.money
        if ratio < 0 or ratio > 1:
            LOGGER.warning(
                "short_term_debt_share: ratio out of bounds for listing_id=%s",
                listing_id,
            )
            return None

        as_of = max(short_debt.as_of, total_debt.as_of)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
            unit_kind="percent",
        )

    def _latest_recent_fact(
        self, repo: RegionFactsRepository, listing_id: int, concept: str
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


__all__ = ["ShortTermDebtShareMetric"]
