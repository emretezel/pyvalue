"""Market capitalization metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import SHARE_COUNT_CONCEPTS, market_cap_money
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class MarketCapitalizationMetric:
    # Market cap is now derived (shares-outstanding fact x price as of that
    # fact's date) rather than read from a stored column, so this metric reads
    # financial facts and must declare the share-count concepts it preloads.
    id: str = "market_cap"
    required_concepts: tuple[str, ...] = SHARE_COUNT_CONCEPTS
    uses_market_data = True
    uses_financial_facts = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        cap = market_cap_money(
            symbol,
            repo=repo,
            market_repo=market_repo,
            metric_id=self.id,
            contexts=(market_repo, repo),
        )
        if cap is None:
            LOGGER.warning("market_cap: no market cap for %s", symbol)
            return None
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=cap.money.amount,
            as_of=cap.as_of,
            currency=cap.money.currency,
        )


__all__ = ["MarketCapitalizationMetric"]
