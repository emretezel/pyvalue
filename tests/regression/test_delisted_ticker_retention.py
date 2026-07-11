"""Regression: a refresh prunes only the provider layer -- canonical data stays.

On 2026-07-11 EODHD answered a plan-dropped exchange (BE) with HTTP 200 and a
truncated 30-symbol payload; the then-current full-purge-on-delist policy
trusted it and deleted 2,835 listings' canonical rows and data (facts, market
data, metrics) irrecoverably. The design was flipped the same day: a provider
payload's absence signal cannot distinguish real delisting from a plan change,
a glitch, or a truncated response, so `replace_for_exchange` now removes only
the provider layer (`provider_listing` + raw fundamentals + fetch/normalization
state). Canonical rows (`listing`/`issuer`) and canonical data are
provider-independent and are never deleted by a refresh -- an unmapped listing
is merely unreachable, because every scope resolver and catalog view joins
through `provider_listing`. These tests fail on the full-purge code.

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
    SecurityRepository,
    SupportedTickerRepository,
)

_AAA_ROW = {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}
_BBB_ROW = {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock", "Currency": "USD"}

# Tables keyed by listing_id: canonical data a refresh must never delete.
_CANONICAL_TABLES = (
    "financial_facts",
    "financial_facts_refresh_state",
    "market_data",
    "metrics",
    "metric_compute_status",
)
# Provider-layer tables seeded for BBB below (all keyed by provider_listing_id,
# except provider_listing itself): the prune must empty exactly these. Note the
# market-data pair: provider_market_data is pruned here while canonical
# market_data (in _CANONICAL_TABLES above) must survive.
_PROVIDER_TABLES = (
    "fundamentals_raw",
    "fundamentals_normalization_state",
    "market_data_fetch_state",
    "provider_market_data",
)


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


def test_delisted_ticker_keeps_canonical_rows_and_data(tmp_path: Path) -> None:
    db_path = tmp_path / "delist-retention.db"
    aaa_id, bbb_id = _seed_catalog_with_downstream_data(db_path)
    ticker_repo = SupportedTickerRepository(db_path)

    # BBB vanishes from the refreshed EODHD snapshot.
    result = ticker_repo.replace_for_exchange("EODHD", "US", [_AAA_ROW])

    assert result.removed == 1
    assert result.orphaned_listings == 1

    with sqlite3.connect(db_path) as conn:
        # Canonical identity survives the prune: both listings, both issuers.
        listing_symbols = {row[0] for row in conn.execute("SELECT symbol FROM listing")}
        issuer_names = {row[0] for row in conn.execute("SELECT name FROM issuer")}
        # Canonical data keyed by BBB's listing_id is fully retained.
        canonical_counts = {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE listing_id = ?",
                (bbb_id,),
            ).fetchone()[0]
            for table in _CANONICAL_TABLES
        }
        # The provider layer is gone: BBB's mapping plus every
        # provider_listing_id-keyed row (only BBB had rows in these tables).
        bbb_mappings = conn.execute(
            "SELECT COUNT(*) FROM provider_listing WHERE listing_id = ?",
            (bbb_id,),
        ).fetchone()[0]
        provider_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in _PROVIDER_TABLES
        }
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert listing_symbols == {"AAA", "BBB"}
    assert issuer_names == {"AAA Inc", "BBB Inc"}
    assert canonical_counts == {table: 1 for table in _CANONICAL_TABLES}
    assert bbb_mappings == 0
    assert provider_counts == {table: 0 for table in _PROVIDER_TABLES}
    assert fk_violations == []

    # The orphan is unreachable through every provider-joined read: the scope
    # resolvers and the catalog view no longer surface BBB.US.
    security_repo = SecurityRepository(db_path)
    assert security_repo.list_supported_listings() == [(aaa_id, "AAA.US")]
    assert security_repo.list_supported_listings_for_symbols(["BBB.US"]) == {}
    with sqlite3.connect(db_path) as conn:
        catalog_rows = conn.execute(
            "SELECT COUNT(*) FROM provider_listing_catalog WHERE security_id = ?",
            (bbb_id,),
        ).fetchone()[0]
    assert catalog_rows == 0


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
    # keep the listing reachable through OTHER (not orphaned at all).
    result = ticker_repo.replace_for_exchange("EODHD", "US", [_AAA_ROW])

    assert result.removed == 1
    assert result.orphaned_listings == 0

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
