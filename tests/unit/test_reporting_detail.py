"""Per-listing concept detail (compute_fact_detail).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.persistence.storage import FactRecord, FinancialFactsRepository
from pyvalue.reporting import compute_fact_detail
from pyvalue.universe import Listing

from conftest import (
    resolve_listing_id,
    seed_exchange,
    seed_facts,
    seed_supported_listings,
)


def _seed_listing(db_path: Path) -> int:
    seed_exchange(db_path, "US", provider="EODHD")
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US", security_name="AAA", exchange="NYSE", currency="USD"
            )
        ],
    )
    return resolve_listing_id(db_path, "AAA.US")


def _fact(
    concept: str,
    end_date: str,
    fiscal_period: str = "Q1",
    value: float = 100.0,
    filed: str | None = None,
) -> FactRecord:
    return FactRecord(
        symbol="AAA.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind="monetary",
        currency="USD",
        value=value,
        filed=filed,
    )


def test_fact_detail_reports_latest_point_and_history_depth(tmp_path: Path) -> None:
    db_path = tmp_path / "detail.db"
    listing_id = _seed_listing(db_path)
    recent = date.today().isoformat()
    seed_facts(
        db_path,
        "AAA.US",
        [
            _fact("AssetsCurrent", "2023-12-31", "FY", 80.0),
            _fact("AssetsCurrent", "2024-12-31", "FY", 90.0),
            _fact("AssetsCurrent", "2025-03-31", "Q1", 95.0),
            _fact("AssetsCurrent", recent, "Q2", 100.0, filed=recent),
        ],
    )

    details = compute_fact_detail(
        FinancialFactsRepository(db_path), listing_id, CurrentRatioMetric
    )

    # Ordered exactly like the metric's required_concepts declaration.
    assert [d.concept for d in details] == ["AssetsCurrent", "LiabilitiesCurrent"]
    assets = details[0]
    assert assets.present and assets.fresh
    assert assets.latest_end_date == recent
    assert assets.latest_fiscal_period == "Q2"
    assert assets.latest_filed == recent
    assert assets.latest_value == 100.0
    assert assets.latest_currency == "USD"
    assert assets.fy_rows == 2
    assert assets.quarterly_rows == 2
    assert assets.total_rows == 4

    liabilities = details[1]
    assert not liabilities.present
    assert not liabilities.fresh
    assert liabilities.latest_end_date is None
    assert liabilities.total_rows == 0


def test_fact_detail_flags_stale_latest_point(tmp_path: Path) -> None:
    db_path = tmp_path / "detail-stale.db"
    listing_id = _seed_listing(db_path)
    seed_facts(db_path, "AAA.US", [_fact("AssetsCurrent", "2020-01-01", "FY")])

    details = compute_fact_detail(
        FinancialFactsRepository(db_path), listing_id, CurrentRatioMetric
    )

    assets = details[0]
    assert assets.present
    assert not assets.fresh
    assert assets.latest_end_date == "2020-01-01"
