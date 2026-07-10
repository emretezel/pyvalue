"""Regression: classifying a listing secondary must not delete its data.

Until 2026-07 the listing-status writers eagerly purged a reclassified-secondary
listing's ``financial_facts``/``market_data``/``metrics`` (plus its
normalization/fetch state). Operator policy now is the opposite: secondary
listings keep everything they accumulated while primary, and they are excluded
from universe work solely by the primary-only scope filters
(``primary_listing_status != 'secondary'``). This test fails on the old
purge-on-reclassify code and pins the retention + scope-exclusion contract.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_facts, seed_metric, seed_price
from pyvalue.persistence.storage import (
    FactRecord,
    SecurityListingStatusRecord,
    SecurityListingStatusRepository,
    SupportedTickerRepository,
)


def test_secondary_reclassification_retains_downstream_data(tmp_path: Path) -> None:
    db_path = tmp_path / "secondary-retention.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {
                "Code": "AAA",
                "Name": "AAA Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
            {
                "Code": "BBB",
                "Name": "BBB Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
        ],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    bbb_id = by_symbol["BBB.US"].security_id

    # Downstream data accumulated while BBB was primary: one fact, one price
    # row, one metric. All three must survive the flip to secondary.
    seed_facts(
        db_path,
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="USD",
            )
        ],
    )
    seed_price(db_path, "BBB.US", "2026-01-02", 12.5, currency="USD")
    seed_metric(db_path, "BBB.US", "price_to_book", 1.5, "2026-01-02")

    SecurityListingStatusRepository(db_path).upsert_many(
        [
            SecurityListingStatusRecord(
                security_id=bbb_id,
                source_provider="EODHD",
                provider_symbol="BBB.US",
                raw_fetched_at="2026-01-01T00:00:00+00:00",
                is_primary_listing=False,
                primary_provider_symbol="AAA.US",
                classification_basis="different_primary_ticker",
            )
        ]
    )

    # Retention: the reclassification wrote only listing.primary_listing_status.
    with sqlite3.connect(db_path) as conn:
        counts = {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE listing_id = ?",
                (bbb_id,),
            ).fetchone()[0]
            for table in ("financial_facts", "market_data", "metrics")
        }
    assert counts == {"financial_facts": 1, "market_data": 1, "metrics": 1}

    # Exclusion happens at scope resolution, not by deletion: the primary-only
    # universe read drops BBB while the unfiltered read still carries it.
    primary_scope = {
        symbol for _, symbol in ticker_repo.list_canonical_listings(primary_only=True)
    }
    full_scope = {symbol for _, symbol in ticker_repo.list_canonical_listings()}
    assert "BBB.US" not in primary_scope
    assert "AAA.US" in primary_scope
    assert "BBB.US" in full_scope
