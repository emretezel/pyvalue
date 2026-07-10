"""Net current asset value (NCAV) metrics.

Graham's "net-net" test: current assets minus *total* liabilities is the
crudest liquidation proxy -- what would remain for shareholders if the
business stopped, collected its current assets at book, and settled every
liability. Buying below two-thirds of that value is the classic Graham (and
Burry/Chou) deep-value rule; screens express it as ``price_to_ncav <= 0.67``.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    SHARE_RESOLVER_REQUIRED_CONCEPTS,
    is_recent_fact,
    market_cap_money,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

CURRENT_ASSETS_CONCEPT = "AssetsCurrent"
# Deliberately *total* liabilities, not current: the net-net rule charges the
# current-asset pile with every claim ahead of shareholders, unlike working
# capital which nets only near-term obligations.
TOTAL_LIABILITIES_CONCEPT = "Liabilities"

NCAV_REQUIRED_CONCEPTS = (CURRENT_ASSETS_CONCEPT, TOTAL_LIABILITIES_CONCEPT)


@dataclass(frozen=True)
class _NCAVResult:
    money: Money
    as_of: str


def compute_ncav(
    listing_id: int, repo: RegionFactsRepository, *, context: str
) -> Optional[_NCAVResult]:
    """Resolve NCAV = latest current assets - latest total liabilities.

    Both inputs are aligned to the listing currency; ``as_of`` is the newer of
    the two balance-sheet dates and must pass the standard freshness gate.
    Returns ``None`` when either fact is missing or stale.
    """

    assets = repo.latest_monetary_fact(listing_id, CURRENT_ASSETS_CONCEPT)
    liabilities = repo.latest_monetary_fact(listing_id, TOTAL_LIABILITIES_CONCEPT)
    if assets is None or liabilities is None:
        LOGGER.warning(
            "%s: missing current assets/total liabilities for listing_id=%s",
            context,
            listing_id,
        )
        return None

    as_of_record = assets if assets.end_date >= liabilities.end_date else liabilities
    if not is_recent_fact(as_of_record):
        LOGGER.warning(
            "%s: latest assets/liabilities too old for listing_id=%s (%s)",
            context,
            listing_id,
            as_of_record.end_date,
        )
        return None

    as_of = as_of_record.end_date
    target_currency = require_metric_ticker_currency(
        listing_id, repo, metric_id=context, as_of=as_of
    )
    assets_money = require_metric_money(
        assets.money,
        target_currency=target_currency,
        metric_id=context,
        listing_id=listing_id,
        input_name=CURRENT_ASSETS_CONCEPT,
        as_of=assets.end_date,
    )
    liabilities_money = require_metric_money(
        liabilities.money,
        target_currency=target_currency,
        metric_id=context,
        listing_id=listing_id,
        input_name=TOTAL_LIABILITIES_CONCEPT,
        as_of=liabilities.end_date,
    )
    return _NCAVResult(money=assets_money - liabilities_money, as_of=as_of)


@dataclass
class NCAVMetric:
    """Compute net current asset value: current assets minus total liabilities."""

    id: str = "ncav"
    required_concepts = NCAV_REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        ncav = compute_ncav(listing_id, repo, context=self.id)
        if ncav is None:
            return None
        # Negative NCAV is the *normal* case (most going concerns carry more
        # total liabilities than current assets) and is meaningful screening
        # information, so it is emitted rather than suppressed.
        return MetricResult.monetary(
            listing_id=listing_id,
            metric_id=self.id,
            value=ncav.money.amount,
            as_of=ncav.as_of,
            currency=ncav.money.currency,
        )


@dataclass
class PriceToNCAVMetric:
    """Compute market cap divided by NCAV (the Graham net-net multiple)."""

    id: str = "price_to_ncav"
    # Market cap (= shares x price) reads a share-count fact, so those concepts
    # must be preloaded alongside the NCAV balance-sheet inputs.
    required_concepts = NCAV_REQUIRED_CONCEPTS + SHARE_RESOLVER_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        ncav = compute_ncav(listing_id, repo, context=self.id)
        if ncav is None:
            return None
        if ncav.money.amount <= 0:
            # A multiple against a non-positive liquidation value is
            # uninterpretable; the net-net screen only ever prices positive NCAV.
            LOGGER.warning(
                "%s: non-positive NCAV for listing_id=%s", self.id, listing_id
            )
            return None

        cap = market_cap_money(
            listing_id,
            repo=repo,
            market_repo=market_repo,
            metric_id=self.id,
            target_currency=ncav.money.currency,
            contexts=(market_repo, repo),
        )
        if cap is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=cap.money / ncav.money,
            as_of=ncav.as_of,
        )


__all__ = ["NCAVMetric", "PriceToNCAVMetric"]
