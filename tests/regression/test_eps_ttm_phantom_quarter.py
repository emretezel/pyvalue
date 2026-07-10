"""Regression: eps_ttm phantom quarter end-to-end (raw payload -> normalize -> metric).

Replays the GOOGL P4 defect (2026-07 audit,
``docs/research/qarp-dvg-metric-verification-2026-07.md``) through the
*production* path on a temp DB: EODHD pre-fills the next, not-yet-reported
quarter in ``Earnings.History`` with a literal ``epsActual: 0`` (reportDate
after the payload's own ``General.UpdatedAt``). On pre-fix code that zero was
stored as a real quarterly EPS fact, became the newest row of the
single-concept EPS series, and anchored the TTM window: eps_ttm summed
0.00 + the three newest real quarters (8.00 for GOOGL) instead of the four
real quarters (10.81). The normalizer must now drop the placeholder so the
window anchors on the last reported quarter.

No network: normalization reads ``fundamentals_raw``; eps_ttm needs no prices.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from conftest import (
    resolve_listing_id,
    seed_exchange,
    seed_raw_fundamentals,
    seed_supported_listings,
)
from pyvalue.cli.normalize import cmd_normalize_eodhd_fundamentals_bulk
from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.persistence.storage import FinancialFactsRepository
from pyvalue.universe import Listing

_TODAY = date.today()
UPDATED_AT = (_TODAY - timedelta(days=5)).isoformat()

# Quarter grid relative to today so the shared TTM resolver's freshness and
# cadence gates hold whenever the suite runs: adjacent gaps of ~91 days sit
# inside the resolver's quarterly band, and the newest real quarter stays
# recent. The phantom sits one cadence step after the newest real quarter,
# exactly where EODHD publishes the upcoming-quarter placeholder.
PHANTOM_END = (_TODAY - timedelta(days=9)).isoformat()
PHANTOM_REPORT_DATE = (_TODAY + timedelta(days=21)).isoformat()
REAL_QUARTER_ENDS = [
    (_TODAY - timedelta(days=100)).isoformat(),
    (_TODAY - timedelta(days=191)).isoformat(),
    (_TODAY - timedelta(days=282)).isoformat(),
    (_TODAY - timedelta(days=373)).isoformat(),
]
# GOOGL FY2025 quarterly EPS, newest first (Q4..Q1); sums to the audit's true
# trailing 10.81. The pre-fix window (phantom 0.00 + the three newest) summed
# to the audit's buggy 8.00.
REAL_QUARTER_EPS = [2.82, 2.87, 2.31, 2.81]
TRUE_TTM_EPS = 10.81
BUGGY_TTM_EPS = 8.00


def _googl_shaped_payload() -> dict[str, object]:
    history: dict[str, object] = {
        # The poison row, verbatim shape from the stored GOOGL payload: a
        # literal zero actual (not null), an estimate, and a report date the
        # payload itself says has not arrived yet.
        PHANTOM_END: {
            "date": PHANTOM_END,
            "reportDate": PHANTOM_REPORT_DATE,
            "epsActual": 0,
            "epsEstimate": 2.53,
            "surprisePercent": -100,
            "currency": "USD",
        },
    }
    for quarter_end, eps in zip(REAL_QUARTER_ENDS, REAL_QUARTER_EPS):
        report_date = (date.fromisoformat(quarter_end) + timedelta(days=35)).isoformat()
        history[quarter_end] = {
            "date": quarter_end,
            "reportDate": report_date,
            "epsActual": eps,
            "currency": "USD",
        }
    return {
        "General": {"CurrencyCode": "USD", "UpdatedAt": UPDATED_AT},
        "Earnings": {"History": history},
    }


def test_eps_ttm_pipeline_ignores_unreported_placeholder_quarter(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "phantom-eps.db"

    seed_exchange(db_path)
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="GOOGL.US",
                security_name="Alphabet Inc Class A",
                exchange="US",
                currency="USD",
            )
        ],
    )
    seed_raw_fundamentals(db_path, "EODHD", "GOOGL.US", _googl_shaped_payload())

    rc = cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path), symbols=["GOOGL.US"], force=False
    )
    assert rc == 0

    listing_id = resolve_listing_id(db_path, "GOOGL.US")
    facts_repo = FinancialFactsRepository(db_path)

    # The placeholder must never become a fact; the four reported quarters
    # must all survive (via the EarningsPerShare alias eps_ttm reads).
    stored = {
        row.end_date: row.value
        for row in facts_repo.facts_for_concept(listing_id, "EarningsPerShare")
    }
    assert PHANTOM_END not in stored
    assert stored == dict(zip(REAL_QUARTER_ENDS, REAL_QUARTER_EPS))

    result = EarningsPerShareTTM().compute(
        listing_id, RegionFactsRepository(facts_repo)
    )

    assert result is not None
    # Anchor on the newest *reported* quarter, not the placeholder date.
    assert result.as_of == REAL_QUARTER_ENDS[0]
    assert result.value == pytest.approx(TRUE_TTM_EPS)
    assert result.value != pytest.approx(BUGGY_TTM_EPS)
