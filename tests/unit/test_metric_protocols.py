"""Unit tests for the two metric protocol shapes and their discriminator.

The CLI decides how to call ``compute`` from the class-level ``uses_market_data``
flag. These tests pin the invariant that makes that dispatch safe: a metric's
flag must agree with its ``compute`` signature for every registered class, and
``needs_market_data`` must read the flag the way the type checker is told it does.

Author: Emre Tezel
"""

from __future__ import annotations

import inspect
from typing import Optional

import pytest

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics import REGISTRY, needs_market_data
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.market_capitalization import MarketCapitalizationMetric
from pyvalue.metrics.working_capital import WorkingCapitalMetric

MARKET_REPO_PARAMETER = "market_repo"


class _FlaglessFactsMetric:
    """A facts-only metric that never mentions the market-data flag."""

    id = "test_flagless_facts_metric"
    required_concepts = ("Assets",)

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        return None


class _ExplicitlyFactsOnlyMetric(_FlaglessFactsMetric):
    """A facts-only metric that spells the flag out as ``False``."""

    id = "test_explicit_facts_metric"
    uses_market_data = False


def test_market_data_metrics_are_recognised() -> None:
    """Price-linked metrics declare the flag and are narrowed as such."""

    assert needs_market_data(MarketCapitalizationMetric()) is True
    assert needs_market_data(EarningsYieldMetric()) is True


def test_facts_only_metrics_are_not_recognised() -> None:
    """Both facts-only spellings -- flag omitted or ``False`` -- read as such."""

    assert needs_market_data(WorkingCapitalMetric()) is False
    assert needs_market_data(_FlaglessFactsMetric()) is False
    assert needs_market_data(_ExplicitlyFactsOnlyMetric()) is False


@pytest.mark.parametrize("metric_id", sorted(REGISTRY))
def test_registry_flag_matches_compute_signature(metric_id: str) -> None:
    """Every registered class's flag agrees with its ``compute`` arity.

    The flag is what the CLI dispatches on; the signature is what actually
    runs. A class carrying one without the other would either be handed a
    repository it cannot accept or be denied one it needs.
    """

    metric_cls = REGISTRY[metric_id]
    parameters = inspect.signature(metric_cls.compute).parameters
    takes_market_repo = MARKET_REPO_PARAMETER in parameters
    assert needs_market_data(metric_cls()) is takes_market_repo
