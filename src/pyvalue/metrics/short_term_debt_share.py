"""Short-term debt share metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

DEBT_COMPONENT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
TOTAL_DEBT_FALLBACK_CONCEPTS = ("TotalDebtFromBalanceSheet",)


@dataclass
class _DebtAmount:
    money: Money
    as_of: str


@dataclass
class ShortTermDebtShareMetric:
    """Compute short-term debt as a share of total debt (EODHD-only)."""

    id: str = "short_term_debt_share"
    required_concepts = tuple(DEBT_COMPONENT_CONCEPTS + TOTAL_DEBT_FALLBACK_CONCEPTS)

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        short_record = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        if short_record is None:
            LOGGER.warning(
                "short_term_debt_share: missing short-term debt for %s", symbol
            )
            return None

        # Resolve the listing currency once; every debt input is then aligned to
        # it before any Money arithmetic, so the ratio is currency-safe.
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=self.id,
            input_name="ShortTermDebt",
            as_of=short_record.end_date,
        )
        short_debt = _DebtAmount(
            money=self._money(short_record, "ShortTermDebt", target_currency, symbol),
            as_of=short_record.end_date,
        )

        total_debt = self._compute_total_debt(
            symbol=symbol,
            repo=repo,
            short_debt=short_debt,
            target_currency=target_currency,
        )
        if total_debt is None:
            LOGGER.warning(
                "short_term_debt_share: missing usable total debt for %s", symbol
            )
            return None

        if total_debt.money.amount <= 0:
            LOGGER.warning(
                "short_term_debt_share: non-positive total debt for %s", symbol
            )
            return None

        ratio = short_debt.money / total_debt.money
        if ratio < 0 or ratio > 1:
            LOGGER.warning("short_term_debt_share: ratio out of bounds for %s", symbol)
            return None

        as_of = max(short_debt.as_of, total_debt.as_of)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
            unit_kind="percent",
        )

    def _compute_total_debt(
        self,
        *,
        symbol: str,
        repo: RegionFactsRepository,
        short_debt: _DebtAmount,
        target_currency: str,
    ) -> Optional[_DebtAmount]:
        long_record = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        if long_record is not None:
            long_money = self._money(
                long_record, "LongTermDebt", target_currency, symbol
            )
            return _DebtAmount(
                money=short_debt.money + long_money,
                as_of=max(short_debt.as_of, long_record.end_date),
            )

        total_record = self._latest_recent_fact(
            repo, symbol, "TotalDebtFromBalanceSheet"
        )
        if total_record is None:
            return None
        return _DebtAmount(
            money=self._money(
                total_record, "TotalDebtFromBalanceSheet", target_currency, symbol
            ),
            as_of=total_record.end_date,
        )

    def _latest_recent_fact(
        self, repo: RegionFactsRepository, symbol: str, concept: str
    ) -> Optional[MonetaryFact]:
        record = repo.latest_monetary_fact(symbol, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _money(
        self,
        fact: MonetaryFact,
        concept: str,
        target_currency: str,
        symbol: str,
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name=concept,
            as_of=fact.end_date,
        )


__all__ = ["ShortTermDebtShareMetric"]
