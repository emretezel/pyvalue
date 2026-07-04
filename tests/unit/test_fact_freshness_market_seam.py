"""report-fact-freshness market-data seam summary.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pyvalue.cli import cmd_report_fact_freshness
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.market_capitalization import MarketCapitalizationMetric
from pyvalue.universe import Listing

from conftest import seed_exchange, seed_price, seed_supported_listings

CURRENT_RATIO = CurrentRatioMetric.id
MARKET_CAP = MarketCapitalizationMetric.id


def _seed_universe(db_path: Path) -> None:
    seed_exchange(db_path, "US", provider="EODHD")
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US", security_name="AAA", exchange="NYSE", currency="USD"
            ),
            Listing(
                symbol="BBB.US", security_name="BBB", exchange="NYSE", currency="USD"
            ),
            Listing(
                symbol="CCC.US", security_name="CCC", exchange="NYSE", currency="USD"
            ),
        ],
    )


def _report(db_path: Path, metric_ids: list[str]) -> int:
    return cmd_report_fact_freshness(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=metric_ids,
        max_age_days=400,
        output_csv=None,
        show_all=False,
    )


def test_market_seam_counts_missing_and_stale_snapshots(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "seam.db"
    _seed_universe(db_path)
    # AAA fresh price, BBB stale price, CCC no snapshot at all.
    seed_price(db_path, "AAA.US", date.today().isoformat(), 10.0, currency="USD")
    seed_price(db_path, "BBB.US", "2019-01-01", 10.0, currency="USD")

    assert _report(db_path, [MARKET_CAP]) == 0
    output = capsys.readouterr().out
    assert (
        "Market-data seam (1 selected metrics use price snapshots): "
        "fresh=1/3, stale=1, missing=1"
    ) in output


def test_market_seam_line_absent_without_market_metrics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "seam-absent.db"
    _seed_universe(db_path)

    assert _report(db_path, [CURRENT_RATIO]) == 0
    output = capsys.readouterr().out
    assert "Market-data seam" not in output
