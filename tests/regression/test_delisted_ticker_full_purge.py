"""Regression: a fully delisted ticker is removed outright, identity included.

`refresh-supported-tickers` always pruned a vanished ticker's provider mapping
plus its raw payload and fetch/normalization state, but left the canonical
`listing` row and its `financial_facts`/`market_data`/`metrics` behind as
permanent orphans. Operator policy (2026-07) is full removal: when the prune
leaves a listing with no provider mapping at all, its data, its `listing` row,
and -- if it was the issuer's last listing -- the `issuer` row are deleted in
the same per-exchange transaction. A listing another provider still carries
must survive untouched (the purge keys on "no provider mapping left", not on
"this provider dropped it"). These tests fail on the old orphaning code.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import (
    seed_exchange,
    seed_facts,
    seed_metric,
    seed_metric_status,
    seed_normalization_success,
    seed_price,
    seed_raw_fundamentals,
)
from pyvalue.persistence.storage import (
    FactRecord,
    FinancialFactsRefreshStateRepository,
    MarketDataFetchStateRepository,
    MetricComputeStatusRecord,
    SupportedTickerRepository,
)

_AAA_ROW = {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}
_BBB_ROW = {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock", "Currency": "USD"}


def _seed_catalog_with_downstream_data(db_path: Path) -> tuple[int, int]:
    """Catalog AAA + BBB on US and give BBB one row in every downstream table.

    Returns ``(aaa_listing_id, bbb_listing_id)``.
    """
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange("EODHD", "US", [_AAA_ROW, _BBB_ROW])
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    aaa_id = by_symbol["AAA.US"].security_id
    bbb_id = by_symbol["BBB.US"].security_id

    seed_raw_fundamentals(db_path, "EODHD", "BBB.US", {"General": {}}, exchange="US")
    seed_normalization_success(db_path, "BBB.US")
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
    FinancialFactsRefreshStateRepository(db_path).mark_security_refreshed(
        bbb_id, refreshed_at="2026-01-01T00:00:00+00:00"
    )
    seed_price(db_path, "BBB.US", "2026-01-02", 12.5, currency="USD")
    seed_metric(db_path, "BBB.US", "price_to_book", 1.5, "2026-01-02")
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="BBB.US",
            metric_id="price_to_book",
            status="success",
            attempted_at="2026-01-02T00:00:00+00:00",
            value_as_of="2026-01-02",
        ),
    )
    MarketDataFetchStateRepository(db_path).mark_failure("EODHD", "BBB.US", "boom")
    return aaa_id, bbb_id


def test_delisted_ticker_is_fully_purged(tmp_path: Path) -> None:
    db_path = tmp_path / "delist-full-purge.db"
    aaa_id, bbb_id = _seed_catalog_with_downstream_data(db_path)
    ticker_repo = SupportedTickerRepository(db_path)

    # BBB vanishes from the refreshed EODHD snapshot.
    result = ticker_repo.replace_for_exchange("EODHD", "US", [_AAA_ROW])

    assert result.removed == 1
    assert result.purged_listings == 1

    with sqlite3.connect(db_path) as conn:
        # Identity rows: BBB's listing and issuer are gone, AAA's survive.
        listing_symbols = {row[0] for row in conn.execute("SELECT symbol FROM listing")}
        issuer_names = {row[0] for row in conn.execute("SELECT name FROM issuer")}
        # Every child table is empty for the purged listing_id.
        child_counts = {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE listing_id = ?",
                (bbb_id,),
            ).fetchone()[0]
            for table in (
                "financial_facts",
                "financial_facts_refresh_state",
                "market_data",
                "metrics",
                "metric_compute_status",
                "provider_listing",
            )
        }
        # The manual children-first deletes must leave referential integrity
        # intact (every FK is NO ACTION -- nothing cascades on our behalf).
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert listing_symbols == {"AAA"}
    assert issuer_names == {"AAA Inc"}
    assert child_counts == {
        "financial_facts": 0,
        "financial_facts_refresh_state": 0,
        "market_data": 0,
        "metrics": 0,
        "metric_compute_status": 0,
        "provider_listing": 0,
    }
    assert fk_violations == []
    assert aaa_id != bbb_id  # sanity: the survivor was never the purged id


def test_listing_with_another_provider_mapping_survives_prune(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "delist-multi-provider.db"
    _, bbb_id = _seed_catalog_with_downstream_data(db_path)
    ticker_repo = SupportedTickerRepository(db_path)

    # A second provider maps the same canonical exchange/symbol, so it resolves
    # to the SAME listing row (listing is UNIQUE on (exchange_id, symbol)).
    seed_exchange(db_path, "US", provider="OTHER")
    ticker_repo.replace_for_exchange("OTHER", "US", [_BBB_ROW])

    # EODHD drops BBB, but OTHER still carries it: prune the EODHD mapping,
    # keep the listing and all its data.
    result = ticker_repo.replace_for_exchange("EODHD", "US", [_AAA_ROW])

    assert result.removed == 1
    assert result.purged_listings == 0

    with sqlite3.connect(db_path) as conn:
        listing_exists = conn.execute(
            "SELECT COUNT(*) FROM listing WHERE listing_id = ?", (bbb_id,)
        ).fetchone()[0]
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?", (bbb_id,)
        ).fetchone()[0]
        metric_rows = conn.execute(
            "SELECT COUNT(*) FROM metrics WHERE listing_id = ?", (bbb_id,)
        ).fetchone()[0]
        mapping_providers = {
            row[0]
            for row in conn.execute(
                """
                SELECT p.provider_code
                FROM provider_listing pl
                JOIN provider_exchange px
                  ON px.provider_exchange_id = pl.provider_exchange_id
                JOIN provider p ON p.provider_id = px.provider_id
                WHERE pl.listing_id = ?
                """,
                (bbb_id,),
            )
        }

    assert listing_exists == 1
    assert fact_rows == 1
    assert metric_rows == 1
    assert mapping_providers == {"OTHER"}
