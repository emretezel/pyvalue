"""Regression: capital-light growth must score the iroic cap, not NA.

``iroic_5y`` (DeltaNOPAT / DeltaIC over a strict 5-year lookback) returned
``None`` whenever invested capital shrank or moved less than the 1%
materiality floor -- which NA'd exactly the businesses a reinvestment gate
exists to find: NOPAT compounding on flat or *released* capital
(buyback-heavy compounders, asset-lightening businesses). ~6k listings hit
the non-positive-DeltaIC reason in the 2026-07 screener audit, and QARP's
``iroic_5y >= 12%`` gate failed all of them.

With growing NOPAT and DeltaIC at or below the floor the metric now emits
the documented ``IROIC_CAP`` (1.0 = 100%) -- a convention for an
economically unbounded ratio, clipped to 0.50 by QARP's ranking cap.
Shrinking-or-flat NOPAT stays NA. The capped test fails on the old code
(``None``) and passes with the cap.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.metrics.roic_fy_series import IROIC_CAP, IncrementalROICFiveYearMetric
from test_metrics import (
    LISTING_ID,
    _base_roic_10y_concepts,
    _build_ic_repo,
)


def test_shrinking_capital_with_growing_nopat_scores_the_cap() -> None:
    """Capital released while NOPAT grew: the Akre shape, not a data gap."""

    latest_year = date.today().year - 1
    # Invested capital shrinks over the lookback (debt paid down), EBIT grows
    # (the base fixture's default ramp).
    shrinking_short_debt = {
        year: 200.0 - 10.0 * (year - (latest_year - 10))
        for year in range(latest_year - 10, latest_year + 1)
    }
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=shrinking_short_debt,
    )
    repo = _build_ic_repo(concept_records=concepts)

    result = IncrementalROICFiveYearMetric().compute(LISTING_ID, repo)

    # Old behavior: None ("non-positive delta invested capital").
    assert result is not None
    assert result.value == IROIC_CAP


def test_shrinking_capital_with_shrinking_nopat_stays_na() -> None:
    """A melting business releasing capital has nothing rewardable to cap."""

    latest_year = date.today().year - 1
    shrinking_short_debt = {
        year: 200.0 - 10.0 * (year - (latest_year - 10))
        for year in range(latest_year - 10, latest_year + 1)
    }
    # EBIT declines toward the latest year, so DeltaNOPAT over the 5-year
    # lookback is negative.
    shrinking_ebit = {
        year: 300.0 - 20.0 * (year - (latest_year - 9))
        for year in range(latest_year - 9, latest_year + 1)
    }
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ebit_by_year=shrinking_ebit,
        ic_short_by_year=shrinking_short_debt,
    )
    repo = _build_ic_repo(concept_records=concepts)

    assert IncrementalROICFiveYearMetric().compute(LISTING_ID, repo) is None
