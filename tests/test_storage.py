import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import pyvalue.persistence.storage as storage
from pyvalue.persistence.storage import (
    ExchangeProviderRepository,
    ExchangeRepository,
    FXRateRecord,
    FXRatesRepository,
    FinancialFactsRefreshStateRepository,
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    FactRecord,
    IdKeyedStoredMetricRow,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricComputeStatusRecord,
    MetricComputeStatusRepository,
    MetricsRepository,
    SecurityMetadataUpdate,
    SecurityRepository,
    SecurityListingStatusRepository,
    SupportedTickerRepository,
)
from pyvalue.marketdata import MarketDataUpdate
from pyvalue.universe import Listing
from collections.abc import Sequence
from types import TracebackType
from typing import Literal, NoReturn, Optional, Type

from conftest import (
    fundamentals_payload_exists,
    resolve_listing_id,
    resolve_provider_listing_id,
    seed_exchange,
    seed_facts,
    seed_metric,
    seed_metric_status,
    seed_normalization_success,
    seed_price,
    seed_raw_fundamentals,
    seed_security_metadata,
    seed_supported_listings,
)


def _listing(symbol: str, is_etf: bool = False, currency: str = "USD") -> Listing:
    """Helper to instantiate listings in a compact way.

    ``listing.currency`` is now NOT NULL with no fallback, so every listing must
    carry a currency. The helper defaults to ``"USD"`` (the NYSE convention used
    throughout these tests) and lets callers override it where a specific code
    matters.

    Author: Emre Tezel
    """

    return Listing(
        symbol=f"{symbol}.US" if "." not in symbol else symbol,
        security_name=f"Company {symbol}",
        exchange="NYSE",
        market_category="N",
        is_etf=is_etf,
        is_test_issue=False,
        status="N",
        round_lot_size=100,
        source="test",
        isin=None,
        currency=currency,
    )


def _seed_listing(
    db_path: Path,
    symbol: str,
    *,
    currency: str = "USD",
    provider: str = "EODHD",
) -> None:
    """Create a cataloged listing carrying a currency so repos (which no
    longer auto-create currency-less listings) can attach data to it.

    The provider defaults to EODHD; pass ``provider="SEC"`` for tests that
    upsert SEC fundamentals so the matching SEC provider listing exists.

    ``replace_for_exchange`` overwrites the whole (provider, exchange) slice, so
    to make this helper safe to call repeatedly for symbols on the same exchange
    we fold the new ticker into the existing roster before re-writing it.
    """
    ticker, _, suffix = symbol.partition(".")
    exchange = suffix or "US"
    seed_exchange(db_path, exchange, provider=provider, currency=currency)
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    rows = {
        existing.code: {
            "Code": existing.code,
            "Type": "Common Stock",
            "Currency": existing.currency,
        }
        for existing in repo.list_for_provider(provider, exchange_codes=[exchange])
    }
    rows[ticker] = {"Code": ticker, "Type": "Common Stock", "Currency": currency}
    repo.replace_for_exchange(provider, exchange, list(rows.values()))


def test_supported_ticker_repository_normalizes_exchange_case(
    tmp_path: Path,
) -> None:
    """``list_for_provider`` normalises provider/exchange case before matching."""
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()
    listing = Listing(
        symbol="FOO.LSE",
        security_name="Foo PLC",
        exchange="LSE",
        market_category="",
        is_etf=False,
        status="Active",
        round_lot_size=0,
        source="test",
        isin="GB00TEST",
        currency="GBP",
    )
    seed_exchange(tmp_path / "universe.db", "LSE")
    seed_supported_listings(tmp_path / "universe.db", "EODHD", "LSE", [listing])

    assert [
        t.provider_symbol
        for t in repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    ] == ["FOO.LSE"]
    assert [
        t.provider_symbol
        for t in repo.list_for_provider("eodhd", exchange_codes=["lse"])
    ] == ["FOO.LSE"]


def test_fundamentals_repository_classifies_secondary_and_retains_data(
    tmp_path: Path,
) -> None:
    """Storing a payload that classifies a listing secondary keeps its data.

    ``FundamentalsRepository.upsert_many`` (which ``seed_raw_fundamentals``
    routes through) refreshes ``listing.primary_listing_status`` from
    ``General.PrimaryTicker`` but must not touch the listing's accumulated
    facts/prices/metrics/state: secondary listings are excluded from universe
    work by the primary-only scope filters, never by deletion.
    """
    db_path = tmp_path / "listing-status.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
        ],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}

    seed_facts(
        db_path,
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="GBP",
            )
        ],
    )
    FinancialFactsRefreshStateRepository(db_path).mark_security_refreshed(
        by_symbol["AAA.LSE"].security_id,
        refreshed_at="2025-01-01T00:00:00+00:00",
    )
    MarketDataRepository(db_path).upsert_prices(
        [
            MarketDataUpdate(
                security_id=by_symbol["AAA.LSE"].security_id,
                symbol="AAA.LSE",
                as_of="2025-01-02",
                price=10.0,
                volume=100,
                currency="GBP",
            )
        ]
    )
    seed_metric(
        db_path,
        "AAA.LSE",
        "market_cap",
        1000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.LSE",
            metric_id="market_cap",
            status="success",
            attempted_at="2025-01-02T00:00:00+00:00",
            value_as_of="2025-01-02",
        ),
    )
    seed_normalization_success(db_path, "AAA.LSE", payload_hash="a" * 64)
    MarketDataFetchStateRepository(db_path).mark_success(
        "EODHD",
        "AAA.LSE",
        fetched_at="2025-01-02T00:00:00+00:00",
    )

    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )

    status_repo = SecurityListingStatusRepository(db_path)
    reconciled = status_repo.reconcile_eodhd_fundamentals()
    assert [row.provider_symbol for row in reconciled] == [
        "AAA.LSE",
        "AAA.US",
        "BBB.LSE",
    ]

    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?",
            (by_symbol["AAA.LSE"].security_id,),
        ).fetchone()[0]
        refresh_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts_refresh_state WHERE listing_id = ?",
            (by_symbol["AAA.LSE"].security_id,),
        ).fetchone()[0]
        market_rows = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE listing_id = ?",
            (by_symbol["AAA.LSE"].security_id,),
        ).fetchone()[0]
        metric_rows = conn.execute(
            "SELECT COUNT(*) FROM metrics WHERE listing_id = ?",
            (by_symbol["AAA.LSE"].security_id,),
        ).fetchone()[0]
        status_rows = conn.execute(
            "SELECT COUNT(*) FROM metric_compute_status WHERE listing_id = ?",
            (by_symbol["AAA.LSE"].security_id,),
        ).fetchone()[0]
        normalization_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals_normalization_state
            WHERE provider_listing_id = (
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            )
            """
        ).fetchone()[0]
        market_state_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM market_data_fetch_state
            WHERE provider_listing_id = (
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            )
            """
        ).fetchone()[0]

    assert statuses == [
        ("AAA.LSE", "secondary"),
        ("AAA.US", "primary"),
        ("BBB.LSE", "primary"),
    ]
    # Everything seeded for the now-secondary AAA.LSE survives untouched.
    assert fact_rows == 1
    assert refresh_rows == 1
    assert market_rows == 1
    assert metric_rows == 1
    assert status_rows == 1
    assert normalization_rows == 1
    assert market_state_rows == 1


def test_migration_078_backfills_unknown_status_and_purges_secondary(
    tmp_path: Path,
) -> None:
    """Migration 078 resolves leftover 'unknown' EODHD classification.

    It is the one-time backstop that lets read/compute commands stop reconciling
    on read: every still-'unknown' listing with fundamentals is classified, and
    the derived data of any that turns out secondary is purged -- the
    eager-purge policy in force when 078 shipped. The repository layer has
    since dropped that purge (secondary listings now retain their data), but an
    already-applied migration keeps the behaviour it ran with, so this test
    pins 078 as shipped.
    """
    from pyvalue.persistence.storage.migrations import (
        _migration_078_backfill_unknown_listing_status,
    )

    db_path = tmp_path / "migration-078.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
        ],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    aaa_lse_id = by_symbol["AAA.LSE"].security_id

    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )

    # Simulate a database whose classification never ran: force every listing
    # back to 'unknown', then (re)seed derived data on the listing that should
    # become secondary so the migration's purge step has something to remove.
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE listing SET primary_listing_status = 'unknown'")
    seed_facts(
        db_path,
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="GBP",
            )
        ],
    )
    MarketDataRepository(db_path).upsert_prices(
        [
            MarketDataUpdate(
                security_id=aaa_lse_id,
                symbol="AAA.LSE",
                as_of="2025-01-02",
                price=10.0,
                volume=100,
                currency="GBP",
            )
        ]
    )
    seed_metric(
        db_path,
        "AAA.LSE",
        "market_cap",
        1000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )

    with sqlite3.connect(db_path) as conn:
        _migration_078_backfill_unknown_listing_status(conn)
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?",
            (aaa_lse_id,),
        ).fetchone()[0]
        market_rows = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE listing_id = ?",
            (aaa_lse_id,),
        ).fetchone()[0]
        metric_rows = conn.execute(
            "SELECT COUNT(*) FROM metrics WHERE listing_id = ?",
            (aaa_lse_id,),
        ).fetchone()[0]

    assert statuses == [
        ("AAA.LSE", "secondary"),
        ("AAA.US", "primary"),
        ("BBB.LSE", "primary"),
    ]
    assert fact_rows == 0
    assert market_rows == 0
    assert metric_rows == 0


def test_exchange_provider_repository_replaces_rows_per_provider(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "exchange-provider.db"
    repo = ExchangeProviderRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_provider(
        "EODHD",
        [
            {"Code": "LSE", "Name": "London Exchange"},
            {"Code": "US", "Name": "USA Stocks"},
        ],
    )
    repo.replace_for_provider(
        "SEC",
        [{"Code": "US", "Name": "United States"}],
    )

    result = repo.replace_for_provider(
        "eodhd",
        [{"Code": "lse", "Name": "London Exchange Refreshed"}],
    )

    assert result.stored == 1
    # US vanished from the EODHD payload: it is dropped from the catalog (it
    # had no provider listings, so the cascade purged nothing). The SEC slice
    # is untouched -- the sync is strictly provider-scoped.
    assert [
        (dropped.code, dropped.purged_provider_listings) for dropped in result.dropped
    ] == [("US", 0)]
    rows = repo.list_all("EODHD")
    assert [(row.provider, row.code, row.name) for row in rows] == [
        ("EODHD", "LSE", "London Exchange Refreshed"),
    ]
    sec_rows = repo.list_all("SEC")
    assert [(row.provider, row.code, row.name) for row in sec_rows] == [
        ("SEC", "US", "United States")
    ]
    exchange_rows = ExchangeRepository(db_path).list_all()
    assert [row.code for row in exchange_rows] == ["LSE", "US"]


def test_exchange_provider_repository_fetch_normalizes_code(tmp_path: Path) -> None:
    repo = ExchangeProviderRepository(tmp_path / "exchange-provider.db")
    repo.initialize_schema()
    repo.replace_for_provider(
        "EODHD",
        [
            {
                "Code": " lse ",
                "Name": " London Exchange ",
                "Country": " UK ",
                "Currency": " GBP ",
                "OperatingMIC": " XLON ",
                "CountryISO2": " GB ",
                "CountryISO3": " GBR ",
            }
        ],
    )

    record = repo.fetch("eodhd", "LSe")

    assert record is not None
    assert record.provider == "EODHD"
    assert record.exchange_code == "LSE"
    assert record.code == "LSE"
    assert record.name == "London Exchange"
    assert record.country == "UK"
    assert record.currency == "GBP"
    assert record.operating_mic == "XLON"
    assert record.country_iso2 == "GB"
    assert record.country_iso3 == "GBR"


def test_supported_ticker_repository_replaces_rows_per_exchange(tmp_path: Path) -> None:
    repo = SupportedTickerRepository(tmp_path / "supported-tickers.db")
    repo.initialize_schema()
    seed_exchange(tmp_path / "supported-tickers.db", "US", "LSE")
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BRK.B",
                "Name": "Share Class",
                "Type": "Preferred Stock",
                "Currency": "GBX",
            },
        ],
    )
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "CCC", "Name": "CCC Inc", "Type": "Common Stock", "Currency": "USD"}],
    )

    result = repo.replace_for_exchange(
        "eodhd",
        "lse",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc refreshed",
                "Type": "Common Stock",
                "Currency": "GBX",
            }
        ],
    )

    assert result.inserted == 1
    assert result.skipped_no_currency == ()
    lse = repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    us = repo.list_for_provider("EODHD", exchange_codes=["US"])
    assert [(row.symbol, row.security_name) for row in lse] == [
        ("AAA.LSE", "AAA plc refreshed")
    ]
    assert [(row.symbol, row.security_name) for row in us] == [("CCC.US", "CCC Inc")]

    repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {
                "Code": "BRK.B",
                "Name": "Berkshire B",
                "Type": "Common Stock",
                "Currency": "USD",
            }
        ],
    )

    us = repo.list_for_provider("EODHD", exchange_codes=["US"])
    assert [(row.symbol, row.code) for row in us] == [("BRK.B.US", "BRK.B")]


def test_supported_ticker_repository_reports_skipped_no_currency(
    tmp_path: Path,
) -> None:
    """Catalog entries with no currency are skipped (not stored) and reported.

    listing.currency is NOT NULL with no fallback, so a payload row whose
    currency is missing or blank cannot be modelled; replace_for_exchange must
    drop it, surface it via skipped_no_currency, and catalog only the rest.
    """

    repo = SupportedTickerRepository(tmp_path / "skip-no-ccy.db")
    repo.initialize_schema()

    seed_exchange(tmp_path / "skip-no-ccy.db", "LSE")
    result = repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {"Code": "BBB", "Name": "BBB plc", "Type": "Common Stock"},  # no currency
            {
                "Code": "CCC",
                "Name": "CCC plc",
                "Type": "Common Stock",
                "Currency": "",
            },  # blank
        ],
    )

    assert result.inserted == 1
    assert result.skipped_no_currency == ("BBB", "CCC")
    # Only the currency-bearing ticker is catalogued.
    assert [
        row.symbol for row in repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    ] == ["AAA.LSE"]


def test_supported_ticker_repository_lists_eligible_symbols(tmp_path: Path) -> None:
    db_path = tmp_path / "supported-tickers.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "LSE")
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Preferred Stock",
                "Currency": "GBX",
            },
        ],
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.LSE", {"General": {"CurrencyCode": "GBP"}}
    )

    rows = repo.list_eligible_for_fundamentals(
        "EODHD",
        exchange_codes=["LSE"],
        max_age_days=None,
        missing_only=True,
    )

    assert [row.symbol for row in rows] == ["BBB.LSE"]


def test_list_eligible_orders_missing_then_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "eligible-missing-stale.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "LSE")
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": code,
                "Name": f"{code} plc",
                "Type": "Common Stock",
                "Currency": "GBP",
            }
            for code in ("AAA", "BBB", "CCC")
        ],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    # BBB and CCC have stored fundamentals; AAA has none, so it is "missing".
    seed_raw_fundamentals(db_path, "EODHD", "BBB.LSE", {"General": {}})
    seed_raw_fundamentals(db_path, "EODHD", "CCC.LSE", {"General": {}})
    # Age BBB well past the freshness cutoff; CCC stays fresh (just upserted).
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET last_fetched_at = '2000-01-01T00:00:00+00:00'
            WHERE provider_listing_id = (
                SELECT provider_listing_id FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'BBB.LSE'
            )
            """
        )

    rows = repo.list_eligible_for_fundamentals(
        "EODHD", exchange_codes=["LSE"], max_age_days=30
    )

    # Missing (AAA) comes first in symbol order, then stale (BBB); fresh CCC is
    # excluded entirely. This exercises the narrowed missing+stale branches.
    assert [row.symbol for row in rows] == ["AAA.LSE", "BBB.LSE"]


def test_list_eligible_reads_base_tables_not_catalog_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "eligible-plan.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "LSE")
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBP"}],
    )

    # Capture the SQL the repository actually executes so we assert on the real
    # query, not a copy. The eligibility SELECT must read the base tables
    # directly: routing it back through provider_listing_catalog would drag in
    # the issuer/exchange/listing/provider joins this refactor removed.
    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.list_eligible_for_fundamentals(
        "EODHD", exchange_codes=["LSE"], max_age_days=None, missing_only=True
    )

    eligibility_sql = [
        sql for sql in captured if "fundamentals_raw" in sql and "ORDER BY" in sql
    ]
    assert eligibility_sql, "expected the eligibility SELECT to be captured"
    for sql in eligibility_sql:
        assert "provider_listing_catalog" not in sql
        assert "FROM provider_listing pl" in sql
        assert "JOIN provider_exchange px" in sql


def test_fundamentals_upsert_never_overwrites_listing_currency(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals-currency-owner.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "LSE")
    # refresh-supported-tickers is the sole writer of listing.currency.
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBP"}],
    )

    # The payload reports a *different* currency; fundamentals ingest must not
    # let it leak into the listing.
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.LSE", {"General": {"CurrencyCode": "USD"}}
    )

    assert fundamentals_payload_exists(db_path, "EODHD", "AAA.LSE")
    with sqlite3.connect(db_path) as conn:
        currency = conn.execute(
            """
            SELECT currency FROM provider_listing_catalog
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]
    assert currency == "GBP"


def test_financial_facts_repository_replace_fact_rows_replaces_by_id(
    tmp_path: Path,
) -> None:
    """The id-keyed ``replace_fact_rows`` deletes the prior slice before insert.

    Seeds an initial fact through the ``seed_facts`` helper (the ``FactRecord``
    projection path), then replaces the listing's entire fact slice with a direct
    ``replace_fact_rows(listing_id, rows)`` call -- proving the raw-tuple write is
    keyed purely by ``listing_id`` and supersedes the earlier write.
    """
    db_path = tmp_path / "financial-facts.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None

    inserted = seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="USD",
            )
        ],
    )

    assert inserted == 1

    replaced = repo.replace_fact_rows(
        listing_id,
        [
            (
                "Liabilities",
                "FY",
                "2024-12-31",
                "monetary",
                40.0,
                None,
                "USD",
            )
        ],
    )

    assert replaced == 1

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
                SELECT s.canonical_symbol, ff.concept, ff.value
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.listing_id
                ORDER BY ff.concept
                """
        ).fetchall()

    assert rows == [("AAA.US", "Liabilities", 40.0)]


def test_financial_facts_repository_replace_fact_rows_updates_refresh_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "facts-refresh-state.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")

    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=10.0,
            )
        ],
    )

    refresh_repo = FinancialFactsRefreshStateRepository(db_path)
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    refresh_record = refresh_repo.fetch_by_id(id_aaa)

    assert refresh_record is not None
    assert refresh_record.listing_id == id_aaa
    assert refresh_record.refreshed_at


def test_normalization_units_keyed_by_id_with_freshness(tmp_path: Path) -> None:
    """``normalization_units`` keys by ``provider_listing_id`` and carries the
    ``listing_id``, label, currency, and freshness hashes; listings without a raw
    payload are absent (the INNER JOIN to ``fundamentals_raw`` is the filter)."""
    db_path = tmp_path / "normalization-units.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    # AAA + BBB carry raw payloads; CCC is a bare listing with no raw row.
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    _seed_listing(db_path, "CCC.US")
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )
    seed_raw_fundamentals(
        db_path, "EODHD", "BBB.US", {"General": {"Name": "BBB"}}, exchange="US"
    )
    # Mark AAA already normalized so its freshness hash is populated.
    seed_normalization_success(db_path, "AAA.US", payload_hash="a" * 64)

    units = fund_repo.normalization_units("EODHD", primary_only=True)

    # CCC (no raw) is absent; every unit is keyed by its own provider_listing_id.
    assert {unit.provider_symbol for unit in units.values()} == {"AAA.US", "BBB.US"}
    for provider_listing_id, unit in units.items():
        assert unit.provider_listing_id == provider_listing_id
        assert unit.currency == "USD"
        assert unit.listing_id > 0
        assert len(unit.raw_payload_hash) == 64
    aaa = next(u for u in units.values() if u.provider_symbol == "AAA.US")
    bbb = next(u for u in units.values() if u.provider_symbol == "BBB.US")
    assert aaa.normalized_payload_hash == "a" * 64
    assert bbb.normalized_payload_hash is None


def test_normalization_units_scoped_by_listing_ids(tmp_path: Path) -> None:
    """The bounded ``listing_ids`` path returns only the requested listings."""
    db_path = tmp_path / "normalization-units-scoped.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )
    seed_raw_fundamentals(
        db_path, "EODHD", "BBB.US", {"General": {"Name": "BBB"}}, exchange="US"
    )
    all_units = fund_repo.normalization_units("EODHD", primary_only=True)
    aaa_listing_id = next(
        u.listing_id for u in all_units.values() if u.provider_symbol == "AAA.US"
    )

    scoped = fund_repo.normalization_units(
        "EODHD", primary_only=True, listing_ids=[aaa_listing_id]
    )

    assert {u.provider_symbol for u in scoped.values()} == {"AAA.US"}


def test_normalization_units_normalizes_gbx_currency_to_gbp(tmp_path: Path) -> None:
    """A GBX quote currency is collapsed to its base GBP in the unit, matching the
    currency the worker would otherwise resolve via ``ticker_currency_by_id``."""
    db_path = tmp_path / "normalization-units-gbx.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    _seed_listing(db_path, "FOO.LSE", currency="GBX")
    seed_raw_fundamentals(
        db_path, "EODHD", "FOO.LSE", {"General": {"Name": "Foo"}}, exchange="LSE"
    )

    units = fund_repo.normalization_units("EODHD", primary_only=False)

    assert {u.provider_symbol for u in units.values()} == {"FOO.LSE"}
    assert next(iter(units.values())).currency == "GBP"


def test_fetch_payload_with_hash_by_id_reads_by_pk(tmp_path: Path) -> None:
    """``fetch_payload_with_hash_by_id`` returns the payload + hash by its PK and
    ``None`` for an unknown id."""
    db_path = tmp_path / "fetch-payload-by-id.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )
    unit = next(
        iter(fund_repo.normalization_units("EODHD", primary_only=True).values())
    )

    fetched = fund_repo.fetch_payload_with_hash_by_id(unit.provider_listing_id)

    assert fetched is not None
    payload, payload_hash = fetched
    assert payload["General"]["Name"] == "AAA"
    assert payload_hash == unit.raw_payload_hash
    assert fund_repo.fetch_payload_with_hash_by_id(999999) is None


def test_mark_success_by_id_upserts_state(tmp_path: Path) -> None:
    """``mark_success_by_id`` writes the watermark keyed by ``provider_listing_id``
    and a second call overwrites it in place (idempotent upsert)."""
    db_path = tmp_path / "mark-success-by-id.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    _seed_listing(db_path, "AAA.US")
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )
    unit = next(
        iter(fund_repo.normalization_units("EODHD", primary_only=True).values())
    )

    state_repo.mark_success_by_id(unit.provider_listing_id, "b" * 64)
    after_first = fund_repo.normalization_units("EODHD", primary_only=True)
    assert after_first[unit.provider_listing_id].normalized_payload_hash == "b" * 64

    state_repo.mark_success_by_id(unit.provider_listing_id, "c" * 64)
    after_second = fund_repo.normalization_units("EODHD", primary_only=True)
    assert after_second[unit.provider_listing_id].normalized_payload_hash == "c" * 64


def test_normalization_units_is_view_free(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The unit query reads base tables only -- never the
    ``provider_listing_catalog`` view (which would drag in issuer/exchange) and
    never ``financial_facts``. Guards the 5-table base-join win."""
    db_path = tmp_path / "normalization-units-view-free.db"
    seen_sql: list[str] = []

    class _LoggingConnection:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def __enter__(self) -> "_LoggingConnection":
            self._conn.__enter__()
            return self

        def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
        ) -> bool:
            return bool(self._conn.__exit__(exc_type, exc_value, traceback))

        def execute(
            self, sql: str, parameters: Sequence[object] = ()
        ) -> sqlite3.Cursor:
            seen_sql.append(" ".join(sql.split()))
            return self._conn.execute(sql, parameters)

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )

    real_connect = fund_repo._connect
    monkeypatch.setattr(
        fund_repo, "_connect", lambda: _LoggingConnection(real_connect())
    )

    units = fund_repo.normalization_units("EODHD", primary_only=True)

    assert {u.provider_symbol for u in units.values()} == {"AAA.US"}
    selects = [sql for sql in seen_sql if sql.lower().startswith("select")]
    assert selects  # the unit query ran through the logging proxy
    assert not any("provider_listing_catalog" in sql for sql in seen_sql)
    assert not any("FROM financial_facts" in sql for sql in seen_sql)


def test_replace_fact_rows_writes_by_id_without_resolving_symbol(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``replace_fact_rows`` is keyed purely by ``listing_id``.

    It never resolves a symbol to an id, so the write touches only
    ``financial_facts`` for the given id. The monkeypatched ``resolve_ids_many``
    (the sole symbol->id resolver) would raise if the writer ever reached it.
    """
    db_path = tmp_path / "replace-known-id.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    security_id = resolve_listing_id(db_path, "AAA.US")
    assert security_id is not None

    def _boom(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(
            "replace_fact_rows must not resolve/create a listing from a symbol"
        )

    monkeypatch.setattr(SecurityRepository, "resolve_ids_many", _boom)

    stored = repo.replace_fact_rows(
        security_id,
        [("Assets", "FY", "2024-12-31", "monetary", 100.0, None, "USD")],
    )

    assert stored == 1
    with repo._connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?",
            (security_id,),
        ).fetchone()[0]
    assert count == 1


def test_financial_facts_repository_replace_fact_rows_replaces_listing_slice(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "financial-facts-replace.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None

    repo.replace_fact_rows(
        listing_id,
        [
            (
                "Assets",
                "FY",
                "2024-12-31",
                "monetary",
                100.0,
                None,
                "USD",
            ),
            (
                "Liabilities",
                "FY",
                "2024-12-31",
                "monetary",
                55.0,
                None,
                "USD",
            ),
        ],
    )

    repo.replace_fact_rows(
        listing_id,
        [
            (
                "StockholdersEquity",
                "FY",
                "2024-12-31",
                "monetary",
                45.0,
                None,
                "USD",
            )
        ],
    )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
                SELECT ff.concept, ff.value
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.listing_id
                WHERE s.canonical_symbol = 'AAA.US'
                ORDER BY ff.concept
                """
        ).fetchall()

    assert rows == [("StockholdersEquity", 45.0)]


def test_financial_facts_repository_initialize_schema_ignores_locked_perf_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = FinancialFactsRepository(tmp_path / "locked-index.db")
    # ``initialize_schema`` calls ``apply_migrations`` bound in the
    # ``financial_facts`` submodule (storage was split into a package), so patch
    # it there rather than on the package facade.
    monkeypatch.setattr(
        storage.financial_facts, "apply_migrations", lambda db_path: None
    )
    monkeypatch.setattr(repo._security_repo(), "initialize_schema", lambda: None)

    class FakeConn:
        """Connection stub whose perf-index DDL always reports a locked DB.

        Installed through ``monkeypatch.setattr`` so it need not be a real
        ``sqlite3.Connection``; ``initialize_schema`` only context-manages it
        and calls ``execute``, which is all this stub implements.
        """

        def __enter__(self) -> "FakeConn":
            return self

        def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
        ) -> Literal[False]:
            return False

        def execute(self, sql: str, params: Sequence[object] = ()) -> None:
            if "idx_fin_facts_security_concept_latest" in sql:
                raise sqlite3.OperationalError("database is locked")
            return None

    monkeypatch.setattr(repo, "_connect", lambda: FakeConn())

    repo.initialize_schema()


def test_fundamentals_repository_upsert_clears_active_fetch_failure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals-fetch-state.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    state_repo = storage.FundamentalsFetchStateRepository(db_path)
    state_repo.mark_failure("EODHD", "AAA.US", "boom", base_backoff_seconds=60)
    assert state_repo.fetch("EODHD", "AAA.US") is not None

    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )

    assert state_repo.fetch("EODHD", "AAA.US") is None


def test_fundamentals_repository_upsert_many_overwrites_by_id(tmp_path: Path) -> None:
    """upsert_many writes payloads keyed by provider_listing_id and overwrites a
    listing's prior payload on re-upsert (rather than inserting a duplicate row)."""
    db_path = tmp_path / "fundamentals-batch.db"
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

    seed_raw_fundamentals(db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}})
    seed_raw_fundamentals(db_path, "EODHD", "BBB.US", {"General": {"Name": "BBB"}})
    # Re-upsert AAA with new data: must overwrite, not insert a second row.
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA Updated"}}
    )

    fund_repo = FundamentalsRepository(db_path)
    by_symbol = {
        unit.provider_symbol: unit
        for unit in fund_repo.normalization_units("EODHD", primary_only=False).values()
    }
    aaa = fund_repo.fetch_payload_with_hash_by_id(
        by_symbol["AAA.US"].provider_listing_id
    )
    bbb = fund_repo.fetch_payload_with_hash_by_id(
        by_symbol["BBB.US"].provider_listing_id
    )
    assert aaa is not None and aaa[0]["General"]["Name"] == "AAA Updated"
    assert bbb is not None and bbb[0]["General"]["Name"] == "BBB"

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0]
    assert count == 2  # overwrite, not duplicate insert


def test_supported_ticker_repository_lists_market_data_symbols_missing_then_oldest(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "supported-market-data.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "US")
    repo.replace_for_exchange(
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
            {
                "Code": "CCC",
                "Name": "CCC Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
            {
                "Code": "DDD",
                "Name": "DDD Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
        ],
    )

    seed_price(db_path, "BBB.US", (date.today() - timedelta(days=1)).isoformat(), 10.0)
    seed_price(db_path, "CCC.US", (date.today() - timedelta(days=30)).isoformat(), 10.0)
    seed_price(db_path, "DDD.US", (date.today() - timedelta(days=12)).isoformat(), 10.0)

    rows = repo.list_eligible_for_market_data(
        "EODHD",
        exchange_codes=["US"],
        max_age_days=7,
    )

    assert [row.symbol for row in rows] == ["AAA.US", "CCC.US", "DDD.US"]
    # The provider-listing key is threaded through so the refresh can dual-write
    # provider_market_data without re-resolving each row.
    assert all(row.provider_listing_id is not None for row in rows)


def test_list_eligible_for_market_data_reads_base_tables_not_catalog_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "market-eligible-plan.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "US")
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )

    # Capture the SQL the repository runs. The eligibility SELECT must read the
    # base tables directly and compute freshness inside a MATERIALIZED CTE:
    # routing it back through provider_listing_catalog would drag in the
    # issuer/exchange joins this refactor removed, and dropping the
    # materialisation barrier lets the planner re-run the MAX(as_of) probe per
    # row.
    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.list_eligible_for_market_data(
        "EODHD", exchange_codes=["US"], max_age_days=7, primary_only=True
    )

    eligibility_sql = [
        sql for sql in captured if "provider_listing pl" in sql and "ORDER BY" in sql
    ]
    assert eligibility_sql, "expected the eligibility SELECT to be captured"
    for sql in eligibility_sql:
        assert "provider_listing_catalog" not in sql
        assert "FROM provider_listing pl" in sql
        assert "JOIN provider_exchange px" in sql
        assert "MATERIALIZED" in sql


def test_list_eligible_for_market_data_primary_only_excludes_secondary(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market-eligible-primary.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
        ],
    )
    # Classify AAA.LSE as a secondary listing of the US primary; BBB.LSE stays
    # primary. (Same setup the primary_only catalog test relies on below.)
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path, "EODHD", "BBB.LSE", {"General": {"Name": "BBB plc"}}, exchange="LSE"
    )

    # No market_data rows -> every listing is "missing" and thus eligible, so the
    # only thing filtering the LSE rows is the primary_only flag.
    all_rows = ticker_repo.list_eligible_for_market_data(
        "EODHD", exchange_codes=["LSE"], max_age_days=7
    )
    assert {row.symbol for row in all_rows} == {"AAA.LSE", "BBB.LSE"}

    primary_rows = ticker_repo.list_eligible_for_market_data(
        "EODHD", exchange_codes=["LSE"], max_age_days=7, primary_only=True
    )
    assert [row.symbol for row in primary_rows] == ["BBB.LSE"]


def test_supported_ticker_repository_primary_only_filters_secondary_listings(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "supported-primary-only.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
        ],
    )

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )

    assert [
        t.provider_symbol
        for t in ticker_repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    ] == [
        "AAA.LSE",
        "BBB.LSE",
    ]
    assert [
        t.provider_symbol
        for t in ticker_repo.list_for_provider(
            "EODHD", exchange_codes=["LSE"], primary_only=True
        )
    ] == ["BBB.LSE"]
    assert [
        symbol for _, symbol in ticker_repo.list_canonical_listings(primary_only=True)
    ] == [
        "AAA.US",
        "BBB.LSE",
    ]


def test_supported_ticker_repository_count_for_provider_matches_list(
    tmp_path: Path,
) -> None:
    """count_for_provider returns the scope size without hydrating rows.

    It must agree with ``len(list_for_provider(...))`` for every scope -- the
    whole point is that ``reconcile-listing-status`` (and ``update-market-data``)
    can report the supported-ticker count without materialising every row across
    the 6-table catalog view. Same primary/secondary fixture as the test above:
    AAA.US (primary), AAA.LSE (secondary of AAA.US), BBB.LSE (primary; no
    PrimaryTicker), i.e. 3 listings total, 2 primary.
    """
    db_path = tmp_path / "count-for-provider.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
            {
                "Code": "BBB",
                "Name": "BBB plc",
                "Type": "Common Stock",
                "Currency": "GBX",
            },
        ],
    )

    fundamentals = FundamentalsRepository(db_path)
    fundamentals.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path, "EODHD", "BBB.LSE", {"General": {"Name": "BBB plc"}}, exchange="LSE"
    )

    # Equivalence with the row-hydrating method across every scope shape.
    assert ticker_repo.count_for_provider("EODHD") == len(
        ticker_repo.list_for_provider("EODHD")
    )
    assert ticker_repo.count_for_provider("EODHD", primary_only=True) == len(
        ticker_repo.list_for_provider("EODHD", primary_only=True)
    )
    assert ticker_repo.count_for_provider("EODHD", exchange_codes=["LSE"]) == len(
        ticker_repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    )
    assert ticker_repo.count_for_provider("EODHD", provider_symbols=["AAA.US"]) == len(
        ticker_repo.list_for_provider("EODHD", provider_symbols=["AAA.US"])
    )

    # Absolute counts so the equivalence above cannot pass vacuously.
    assert ticker_repo.count_for_provider("EODHD") == 3
    assert ticker_repo.count_for_provider("EODHD", primary_only=True) == 2
    assert ticker_repo.count_for_provider("EODHD", exchange_codes=["LSE"]) == 2
    assert (
        ticker_repo.count_for_provider(
            "EODHD", exchange_codes=["LSE"], primary_only=True
        )
        == 1
    )

    # Absent exchange -> empty scope: count 0, matching an empty row list.
    assert ticker_repo.count_for_provider("EODHD", exchange_codes=["XETRA"]) == 0
    assert ticker_repo.list_for_provider("EODHD", exchange_codes=["XETRA"]) == []


def test_market_data_fetch_state_repository_tracks_success_and_failure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market-state.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    repo = MarketDataFetchStateRepository(db_path)
    repo.initialize_schema()

    repo.mark_failure("eodhd", "aaa.us", "boom", base_backoff_seconds=60)
    failed = repo.fetch("EODHD", "AAA.US")

    assert failed is not None
    assert failed["last_status"] == "error"
    assert failed["last_error"] == "boom"
    assert failed["attempts"] == 1
    assert failed["next_eligible_at"] is not None

    repo.mark_success("EODHD", "AAA.US")
    success = repo.fetch("eodhd", "aaa.us")

    assert success is not None
    assert success["last_status"] == "ok"
    assert success["last_error"] is None
    assert success["next_eligible_at"] is None
    assert success["attempts"] == 0


def test_market_data_repository_upsert_prices_batches_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "market-data-batch.db"
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
    rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["US"])
    by_symbol = {row.symbol: row for row in rows}

    repo = MarketDataRepository(db_path)
    repo.initialize_schema()
    repo.upsert_prices(
        [
            MarketDataUpdate(
                security_id=by_symbol["AAA.US"].security_id,
                symbol="AAA.US",
                as_of="2026-03-29",
                price=10.0,
                volume=100,
                currency="USD",
            ),
            MarketDataUpdate(
                security_id=by_symbol["BBB.US"].security_id,
                symbol="BBB.US",
                as_of="2026-03-29",
                price=20.0,
                volume=200,
                currency="USD",
            ),
        ]
    )

    aaa_snapshot = repo.latest_snapshot_by_id(by_symbol["AAA.US"].security_id)
    bbb_snapshot = repo.latest_snapshot_by_id(by_symbol["BBB.US"].security_id)
    assert aaa_snapshot is not None
    assert bbb_snapshot is not None
    assert aaa_snapshot.price == 10.0
    assert bbb_snapshot.price == 20.0


def test_market_data_repository_dual_writes_provider_layer(tmp_path: Path) -> None:
    """upsert_prices writes provider_market_data + canonical market_data together.

    A row carrying ``provider_listing_id`` lands in both layers; a row without
    one is canonical-only (the write path for listings whose provider layer was
    purged). A re-upsert of the same ``(key, as_of)`` updates both layers.
    """
    db_path = tmp_path / "market-data-dual.db"
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
    aaa_id = resolve_listing_id(db_path, "AAA.US")
    bbb_id = resolve_listing_id(db_path, "BBB.US")
    aaa_provider_listing_id = resolve_provider_listing_id(db_path, aaa_id)
    assert aaa_provider_listing_id is not None

    repo = MarketDataRepository(db_path)
    repo.upsert_prices(
        [
            MarketDataUpdate(
                security_id=aaa_id,
                symbol="AAA.US",
                as_of="2026-03-29",
                price=10.0,
                volume=100,
                currency="USD",
                provider_listing_id=aaa_provider_listing_id,
            ),
            # No provider mapping threaded -> canonical-only write.
            MarketDataUpdate(
                security_id=bbb_id,
                symbol="BBB.US",
                as_of="2026-03-29",
                price=20.0,
                volume=200,
                currency="USD",
            ),
        ]
    )

    with sqlite3.connect(db_path) as conn:
        provider_rows = conn.execute(
            """
            SELECT provider_listing_id, as_of, price, volume
            FROM provider_market_data
            ORDER BY provider_listing_id
            """
        ).fetchall()
        canonical_rows = conn.execute(
            "SELECT listing_id, price FROM market_data ORDER BY listing_id"
        ).fetchall()
    assert provider_rows == [(aaa_provider_listing_id, "2026-03-29", 10.0, 100)]
    assert canonical_rows == [(aaa_id, 10.0), (bbb_id, 20.0)]

    # Conflict path: same (key, as_of) refreshes price/volume in both layers.
    repo.upsert_prices(
        [
            MarketDataUpdate(
                security_id=aaa_id,
                symbol="AAA.US",
                as_of="2026-03-29",
                price=11.5,
                volume=150,
                currency="USD",
                provider_listing_id=aaa_provider_listing_id,
            )
        ]
    )
    with sqlite3.connect(db_path) as conn:
        provider_row = conn.execute(
            "SELECT price, volume FROM provider_market_data WHERE provider_listing_id = ?",
            (aaa_provider_listing_id,),
        ).fetchone()
        canonical_row = conn.execute(
            "SELECT price, volume FROM market_data WHERE listing_id = ?",
            (aaa_id,),
        ).fetchone()
    assert provider_row == (11.5, 150)
    assert canonical_row == (11.5, 150)


def test_market_data_repository_clear_wipes_both_layers(tmp_path: Path) -> None:
    db_path = tmp_path / "market-data-clear.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    # seed_price threads the provider mapping, populating both layers.
    seed_price(db_path, "AAA.US", "2026-03-29", 10.0, volume=100, currency="USD")

    repo = MarketDataRepository(db_path)
    repo.clear()

    with sqlite3.connect(db_path) as conn:
        provider_count = conn.execute(
            "SELECT COUNT(*) FROM provider_market_data"
        ).fetchone()[0]
        canonical_count = conn.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
    assert provider_count == 0
    assert canonical_count == 0


def test_market_data_fetch_state_repository_batch_methods(tmp_path: Path) -> None:
    db_path = tmp_path / "market-state-batch.db"
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
    repo = MarketDataFetchStateRepository(db_path)
    repo.initialize_schema()

    repo.mark_failure_many(
        "EODHD",
        [("AAA.US", "boom"), ("BBB.US", "bang")],
        base_backoff_seconds=60,
    )
    aaa_failed = repo.fetch("EODHD", "AAA.US")
    bbb_failed = repo.fetch("EODHD", "BBB.US")
    assert aaa_failed is not None
    assert bbb_failed is not None
    assert aaa_failed["last_status"] == "error"
    assert bbb_failed["attempts"] == 1

    repo.mark_success_many("EODHD", ["AAA.US", "BBB.US"])
    aaa_ok = repo.fetch("EODHD", "AAA.US")
    bbb_ok = repo.fetch("EODHD", "BBB.US")
    assert aaa_ok is not None
    assert bbb_ok is not None
    assert aaa_ok["last_status"] == "ok"
    assert bbb_ok["attempts"] == 0


def test_market_data_fetch_state_writes_resolve_via_base_tables_in_one_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "market-state-resolve-plan.db"
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
            {
                "Code": "CCC",
                "Name": "CCC Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
        ],
    )
    repo = MarketDataFetchStateRepository(db_path)
    repo.initialize_schema()

    # Capture the SQL the write path runs. Resolving provider_listing_id for a
    # whole batch must be ONE base-table query, not one catalog-view scan per
    # symbol (the O(symbols x catalog) slow path this replaced).
    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.mark_success_many("EODHD", ["AAA.US", "BBB.US", "CCC.US"])

    resolution_sql = [sql for sql in captured if "FROM provider_listing pl" in sql]
    assert len(resolution_sql) == 1, (
        f"expected one bulk resolution query, got {len(resolution_sql)}"
    )
    assert "provider_listing_catalog" not in resolution_sql[0]
    assert "JOIN provider_exchange px" in resolution_sql[0]

    # All three rows resolved and landed via the bulk path.
    assert all(
        repo.fetch("EODHD", sym) is not None for sym in ("AAA.US", "BBB.US", "CCC.US")
    )


def test_fundamentals_fetch_state_writes_resolve_via_base_tables_in_one_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "fundamentals-state-resolve-plan.db"
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
            {
                "Code": "CCC",
                "Name": "CCC Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            },
        ],
    )
    repo = FundamentalsFetchStateRepository(db_path)
    repo.initialize_schema()

    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.mark_failure_many(
        "EODHD",
        [("AAA.US", "boom"), ("BBB.US", "bang"), ("CCC.US", "crash")],
        base_backoff_seconds=60,
    )

    resolution_sql = [sql for sql in captured if "FROM provider_listing pl" in sql]
    assert len(resolution_sql) == 1, (
        f"expected one bulk resolution query, got {len(resolution_sql)}"
    )
    assert "provider_listing_catalog" not in resolution_sql[0]
    assert "JOIN provider_exchange px" in resolution_sql[0]

    assert repo.fetch("EODHD", "AAA.US") is not None


def test_financial_facts_repository_latest_share_counts_many_matches_single_lookup(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "share-counts-many.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="EntityCommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="count",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="count",
                value=222.0,
            ),
        ],
    )
    seed_facts(
        db_path,
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit_kind="count",
                value=300.0,
            ),
            FactRecord(
                symbol="BBB.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="count",
                value=333.0,
            ),
        ],
    )

    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    # CCC.US is uncataloged (never seeded), so its id resolves to None and it is
    # absent from the result. The id reader prefers the latest end_date and the
    # ``CommonStockSharesOutstanding`` concept, so AAA picks 222.0 (its
    # CommonStock fact) and BBB picks 333.0 (its 2024 fact over the 2023 one).
    counts_by_ids = repo.latest_share_counts_many_by_ids([id_aaa, id_bbb])
    assert counts_by_ids == {id_aaa: 222.0, id_bbb: 333.0}


def test_financial_facts_repository_facts_for_ids_many_groups_by_listing(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "facts-many.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit_kind="monetary",
                currency="USD",
                value=101.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=11.0,
            ),
        ],
    )
    seed_facts(
        db_path,
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=222.0,
            ),
            FactRecord(
                symbol="BBB.US",
                concept="Revenue",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=333.0,
            ),
        ],
    )

    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    facts = repo.facts_for_ids_many([id_aaa, id_bbb], chunk_size=1)

    assert {record.concept for record in facts[id_aaa]} == {
        "AssetsCurrent",
        "LiabilitiesCurrent",
    }
    assert {record.concept for record in facts[id_bbb]} == {
        "AssetsCurrent",
        "Revenue",
    }


def test_financial_facts_repository_facts_for_ids_many_concept_filter(
    tmp_path: Path,
) -> None:
    """A non-empty ``concepts`` argument restricts the preload to that subset."""

    db_path = tmp_path / "facts-many-concepts.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=11.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="Revenues",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=999.0,
            ),
        ],
    )

    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None

    filtered = repo.facts_for_ids_many(
        [id_aaa],
        concepts=["AssetsCurrent", "LiabilitiesCurrent"],
    )
    concepts = {record.concept for record in filtered[id_aaa]}
    assert concepts == {"AssetsCurrent", "LiabilitiesCurrent"}

    # Empty concepts list short-circuits to the unfiltered query.
    unfiltered = repo.facts_for_ids_many([id_aaa], concepts=[])
    assert {r.concept for r in unfiltered[id_aaa]} == {
        "AssetsCurrent",
        "LiabilitiesCurrent",
        "Revenues",
    }


def test_metrics_repository_upsert_many_by_id_with_external_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When a connection is supplied the persistence path reuses it."""

    db_path = tmp_path / "metrics-external-conn.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    # Seed cataloged listings (which carry a currency) and resolve their ids up
    # front: the id-keyed writer takes ``listing_id`` directly and never resolves
    # a symbol, so the write path opens no connection of its own.
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    sec_repo = repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    rows: list[IdKeyedStoredMetricRow] = [
        (id_aaa, "dummy_metric", 1.0, "2024-01-01", "monetary", "USD", None),
        (id_bbb, "dummy_metric", 2.0, "2024-01-01", "monetary", "USD", None),
    ]

    # Stub initialize_schema -- the table+migrations are already in place from the
    # warm-up above, so any further _connect() open during upsert_many_by_id can
    # only come from the persistence path itself.
    monkeypatch.setattr(repo, "initialize_schema", lambda: None)
    monkeypatch.setattr(sec_repo, "initialize_schema", lambda: None)

    write_conn = repo.open_persistent_connection()
    try:
        opened: list[int] = []
        original_connect = repo._connect

        def tracking_connect() -> sqlite3.Connection:
            opened.append(1)
            return original_connect()

        original_sec_connect = sec_repo._connect
        sec_opened: list[int] = []

        def tracking_sec_connect() -> sqlite3.Connection:
            sec_opened.append(1)
            return original_sec_connect()

        monkeypatch.setattr(repo, "_connect", tracking_connect)
        monkeypatch.setattr(sec_repo, "_connect", tracking_sec_connect)
        persisted = repo.upsert_many_by_id(rows, connection=write_conn)
    finally:
        write_conn.close()

    assert persisted == len(rows)
    # With initialize_schema stubbed out, the id-keyed write opens no new
    # connection (it reuses write_conn) and never touches the security repo.
    assert opened == []
    assert sec_opened == []

    # Confirm rows actually landed by reading via a fresh connection.
    with sqlite3.connect(db_path) as verify_conn:
        verify_conn.row_factory = sqlite3.Row
        stored = verify_conn.execute(
            "SELECT metric_id, value FROM metrics ORDER BY metric_id, value"
        ).fetchall()
    assert [(row["metric_id"], row["value"]) for row in stored] == [
        ("dummy_metric", 1.0),
        ("dummy_metric", 2.0),
    ]


def test_sqlite_store_connect_applies_performance_pragmas(tmp_path: Path) -> None:
    """Centralised pragma setup configures cache, sync, mmap, and temp store."""

    db_path = tmp_path / "pragma-check.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    with repo._connect() as conn:
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
        cache_size = conn.execute("PRAGMA cache_size").fetchone()[0]
        temp_store = conn.execute("PRAGMA temp_store").fetchone()[0]
        mmap_size = conn.execute("PRAGMA mmap_size").fetchone()[0]
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

    # synchronous=NORMAL == 1
    assert synchronous == 1
    # cache_size negative encodes KiB; we ask for 64 MiB.
    assert cache_size == -65536
    # temp_store=MEMORY == 2
    assert temp_store == 2
    # 256 MiB mmap region.
    assert mmap_size == 268435456
    # WAL mode, applied per-connection.
    assert journal_mode.lower() == "wal"


def test_migration_029_creates_fin_facts_security_concept_latest_index(
    tmp_path: Path,
) -> None:
    """Migration 029 ensures the composite preload index exists on existing DBs."""

    from pyvalue.persistence.storage.migrations import apply_migrations

    db_path = tmp_path / "migration-029.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    # Drop the index then re-apply migrations to confirm 029 recreates it.
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
        before = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name='idx_fin_facts_security_concept_latest'"
        ).fetchone()
    assert before is None

    # Force the migration to re-run by lowering schema_version below 029.
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE schema_migrations SET version = 28")

    # Apply only THROUGH migration 029. Replaying the whole chain would re-run
    # migration 043, whose dedupe SQL references the legacy ``unit`` column that
    # migration 071 has already renamed to ``unit_kind`` on this head schema.
    apply_migrations(db_path, target_version=29)

    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name='idx_fin_facts_security_concept_latest'"
        ).fetchone()
    assert after is not None


def test_latest_share_counts_many_prefers_best_same_date_share_fact(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "share-count-selection.db"
    seed_exchange(db_path, "US")
    seed_supported_listings(db_path, "EODHD", "US", [_listing("AAA")])

    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="EntityCommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit_kind="monetary",
                value=1_000_000.0,
                filed="2026-03-27",
                currency="USD",
            ),
            FactRecord(
                symbol="AAA.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit_kind="count",
                value=1_000.0,
                filed=None,
                currency=None,
            ),
        ],
    )

    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    counts = repo.latest_share_counts_many_by_ids([id_aaa])

    assert counts == {id_aaa: 1000.0}


def test_sqlite_store_connect_context_closes_connection(tmp_path: Path) -> None:
    repo = MarketDataRepository(tmp_path / "connect-close.db")

    with repo._connect() as conn:
        conn.execute("SELECT 1")

    try:
        conn.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        pass
    else:  # pragma: no cover - defensive
        raise AssertionError(
            "SQLite connection should be closed after the context exits"
        )


def test_sqlite_store_enable_wal_mode(tmp_path: Path) -> None:
    repo = MarketDataRepository(tmp_path / "wal-mode.db")
    repo.initialize_schema()

    mode = repo.enable_wal_mode()

    assert mode == "wal"
    assert repo.current_journal_mode() == "wal"


def test_metrics_repository_upsert_many_by_id_persists_and_overwrites(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "metrics-upsert-many.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    # Seed an initial value, then prove the batch write overwrites it (same
    # listing_id + metric_id) and inserts the new rows.
    seed_metric(db_path, "AAA.US", "metric_one", 1.0, "2024-01-01")
    rows: list[IdKeyedStoredMetricRow] = [
        (id_aaa, "metric_one", 2.0, "2024-02-01", "other", None, None),
        (id_aaa, "metric_two", 3.0, "2024-02-01", "other", None, None),
        (id_bbb, "metric_one", 4.0, "2024-02-01", "other", None, None),
    ]
    updated = repo.upsert_many_by_id(rows)

    assert updated == 3
    assert repo.fetch_by_id(id_aaa, "metric_one") == (2.0, "2024-02-01")
    assert repo.fetch_by_id(id_aaa, "metric_two") == (3.0, "2024-02-01")
    assert repo.fetch_by_id(id_bbb, "metric_one") == (4.0, "2024-02-01")


def test_metrics_repository_upsert_many_by_id_retries_transient_locked_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metrics-upsert-retry.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    # Resolve the id up front, before _connect is patched, so the write is a pure
    # id-keyed write and the read-back never re-resolves a symbol.
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    monkeypatch.setattr(repo, "initialize_schema", lambda: None)

    original_connect = repo._connect
    attempts = {"count": 0}

    class LockedOnceConnection:
        """Connection stub that raises ``database is locked`` on first entry.

        Installed via ``monkeypatch.setattr`` to drive the retry path of
        ``upsert_many_by_id``; the second entry delegates to a real connection.
        """

        _conn: sqlite3.Connection

        def __enter__(self) -> sqlite3.Connection:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            self._conn = original_connect()
            return self._conn.__enter__()

        def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
        ) -> bool:
            return bool(self._conn.__exit__(exc_type, exc_value, traceback))

    monkeypatch.setattr(repo, "_connect", lambda: LockedOnceConnection())

    retry_rows: list[IdKeyedStoredMetricRow] = [
        (id_aaa, "metric_one", 2.0, "2024-02-01", "other", None, None),
    ]
    updated = repo.upsert_many_by_id(retry_rows)

    assert attempts["count"] == 2
    assert updated == 1
    assert repo.fetch_by_id(id_aaa, "metric_one") == (2.0, "2024-02-01")


def test_metrics_repository_fetch_many_by_ids_returns_requested_metrics(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "metrics-fetch-many.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    _seed_listing(db_path, "CCC.US")
    seed_metric(db_path, "AAA.US", "metric_one", 1.0, "2024-01-01")
    seed_metric(db_path, "AAA.US", "metric_two", 2.0, "2024-01-02")
    seed_metric(db_path, "BBB.US", "metric_one", 3.0, "2024-01-03")
    seed_metric(db_path, "CCC.US", "metric_three", 4.0, "2024-01-04")

    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    id_ccc = resolve_listing_id(db_path, "CCC.US")
    assert id_aaa is not None
    assert id_bbb is not None
    assert id_ccc is not None

    # Only the requested metric ids are returned (metric_three is excluded), and
    # only listings that actually have a row for one of them appear: CCC has only
    # metric_three, so it is absent from the result entirely.
    fetched = repo.fetch_many_by_ids(
        [id_aaa, id_bbb, id_ccc],
        ["metric_one", "metric_two"],
        chunk_size=1,
    )

    assert fetched == {
        id_aaa: {
            "metric_one": (1.0, "2024-01-01"),
            "metric_two": (2.0, "2024-01-02"),
        },
        id_bbb: {
            "metric_one": (3.0, "2024-01-03"),
        },
    }


def test_metric_compute_status_repository_upsert_and_fetch_many(tmp_path: Path) -> None:
    db_path = tmp_path / "metric-status.db"
    repo = MetricComputeStatusRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    updated = repo.upsert_many_by_id(
        [
            MetricComputeStatusRecord(
                listing_id=id_aaa,
                metric_id="metric_one",
                status="success",
                attempted_at="2024-01-02T00:00:00+00:00",
                value_as_of="2024-01-01",
            ),
            MetricComputeStatusRecord(
                listing_id=id_bbb,
                metric_id="metric_one",
                status="failure",
                attempted_at="2024-01-02T00:00:00+00:00",
                reason_code="missing_data",
                reason_detail="Need more facts",
                facts_refreshed_at="2024-01-02T00:00:00+00:00",
            ),
        ]
    )

    assert updated == 2

    single = repo.fetch_by_id(id_bbb, "metric_one")
    assert single is not None
    assert single.status == "failure"
    assert single.reason_code == "missing_data"

    fetched = repo.fetch_many_by_ids(
        [id_aaa, id_bbb],
        ["metric_one"],
        chunk_size=1,
    )

    assert fetched[id_aaa]["metric_one"].status == "success"
    assert fetched[id_aaa]["metric_one"].value_as_of == "2024-01-01"
    assert fetched[id_bbb]["metric_one"].reason_detail == "Need more facts"


def test_metrics_repository_persists_unit_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics-metadata.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")

    seed_metric(
        db_path,
        "AAA.US",
        "market_cap",
        100.0,
        "2024-01-01",
        unit_kind="monetary",
        currency="GBX",
        unit_label="money",
    )
    seed_metric(
        db_path,
        "AAA.US",
        "earnings_yield",
        0.08,
        "2024-01-01",
        unit_kind="percent",
        currency="USD",
        unit_label="pct",
    )

    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    market_cap = repo.fetch_by_id(id_aaa, "market_cap")
    earnings_yield = repo.fetch_by_id(id_aaa, "earnings_yield")

    assert market_cap is not None
    assert market_cap.unit_kind == "monetary"
    assert market_cap.currency == "GBP"
    assert market_cap.unit_label == "money"
    assert earnings_yield is not None
    assert earnings_yield.unit_kind == "percent"
    assert earnings_yield.currency is None
    assert earnings_yield.unit_label == "pct"


def test_metrics_repository_normalizes_configured_subunit_currencies(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "metrics-subunits.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.JSE", currency="ZAC")
    _seed_listing(db_path, "BBB.TA", currency="ILA")

    seed_metric(
        db_path,
        "AAA.JSE",
        "market_cap",
        237.5,
        "2024-01-01",
        unit_kind="monetary",
        currency="ZAC",
    )
    seed_metric(
        db_path,
        "BBB.TA",
        "eps_ttm",
        12.34,
        "2024-01-01",
        unit_kind="per_share",
        currency="ILA",
    )

    repo._security_repo()
    id_aaa = resolve_listing_id(db_path, "AAA.JSE")
    id_bbb = resolve_listing_id(db_path, "BBB.TA")
    assert id_aaa is not None
    assert id_bbb is not None
    market_cap = repo.fetch_by_id(id_aaa, "market_cap")
    eps_ttm = repo.fetch_by_id(id_bbb, "eps_ttm")

    assert market_cap is not None
    assert market_cap.currency == "ZAR"
    assert eps_ttm is not None
    assert eps_ttm.currency == "ILS"


def test_fx_rates_repository_latest_on_or_before_and_discover_currencies(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fx-repo.db"
    _seed_listing(db_path, "AAA.LSE", currency="GBX")
    _seed_listing(db_path, "BBB.JSE", currency="ZAC")
    _seed_listing(db_path, "CCC.TA", currency="ILA")
    # One fact per listing, each in that listing's *major* currency. Subunit
    # codes (GBX/ZAC/ILA) can never enter financial_facts post-migration-071, so
    # the GBX/ZAC/ILA listings store GBP/ZAR/ILS facts. Each fact lives under its
    # own listing because the PK (listing_id, concept, fiscal_period, end_date)
    # no longer includes the unit, so three facts under one listing would
    # collapse to a single row.
    seed_facts(
        db_path,
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit_kind="monetary",
                value=1000.0,
                currency="GBP",
            )
        ],
    )
    seed_facts(
        db_path,
        "BBB.JSE",
        [
            FactRecord(
                symbol="BBB.JSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit_kind="monetary",
                value=1000.0,
                currency="ZAR",
            )
        ],
    )
    seed_facts(
        db_path,
        "CCC.TA",
        [
            FactRecord(
                symbol="CCC.TA",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit_kind="monetary",
                value=1000.0,
                currency="ILS",
            )
        ],
    )
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(
        [
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-01",
                base_currency="USD",
                quote_currency="EUR",
                rate=0.8,
                fetched_at="2024-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-10",
                base_currency="USD",
                quote_currency="EUR",
                rate=0.9,
                fetched_at="2024-01-10T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )

    record = repo.latest_on_or_before("EODHD", "USD", "EUR", "2024-01-05")

    assert record is not None
    assert record.rate_date == "2024-01-01"
    assert repo.discover_currencies() == ["GBP", "ILS", "ZAR"]
    assert (
        repo.fully_covered_quotes_for_window(
            "EODHD",
            "USD",
            ["EUR", "GBP"],
            date(2024, 1, 1),
            date(2024, 1, 10),
        )
        == set()
    )
    assert repo.fully_covered_quotes_for_window(
        "EODHD",
        "USD",
        ["EUR", "GBP"],
        date(2024, 1, 1),
        date(2024, 1, 1),
    ) == {"EUR"}


def test_fx_rates_repository_discover_currencies_excludes_secondary_supported_tickers(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fx-secondary-supported.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE", "JSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBP"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "JSE",
        [{"Code": "BBB", "Name": "BBB Ltd", "Type": "Common Stock", "Currency": "ZAC"}],
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.JSE",
        {"General": {"Name": "BBB Ltd"}},
        exchange="JSE",
    )

    repo = FXRatesRepository(db_path)
    repo.initialize_schema()

    # The primary US (USD) and JSE (ZAC -> ZAR) listings are discovered, but the
    # secondary AAA.LSE listing is excluded -- GBP never appears. (Listings now
    # carry a NOT NULL currency, so the primary USD listing contributes USD too.)
    assert repo.discover_currencies() == ["USD", "ZAR"]


def test_security_repository_upserts_sector_and_industry_metadata(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "security-metadata.db"
    repo = SecurityRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")

    seed_security_metadata(
        db_path,
        "AAA.US",
        entity_name="AAA Corp",
        description="AAA description",
        sector="Technology",
        industry="Software",
    )

    security = repo.fetch(resolve_listing_id(db_path, "AAA.US"))

    assert security is not None
    assert security.entity_name == "AAA Corp"
    assert security.description == "AAA description"
    assert security.sector == "Technology"
    assert security.industry == "Software"


def test_security_repository_fetch_many_by_id_returns_security_records(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "entity-metadata.db"
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_security_metadata(db_path, "AAA.US", sector="Technology", industry="Software")
    seed_security_metadata(
        db_path, "BBB.US", sector="Industrials", industry="Machinery"
    )

    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_aaa is not None
    assert id_bbb is not None

    rows = security_repo.fetch_many_by_id([id_aaa, id_bbb])

    assert rows[id_aaa].sector == "Technology"
    assert rows[id_aaa].industry == "Software"
    assert rows[id_bbb].sector == "Industrials"
    assert rows[id_bbb].industry == "Machinery"


def test_security_repository_upsert_metadata_many_updates_existing_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "security-metadata-batch.db"
    repo = SecurityRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_security_metadata(db_path, "AAA.US", entity_name="AAA Corp")
    seed_security_metadata(
        db_path, "BBB.US", entity_name="BBB Corp", description="BBB description"
    )
    aaa_id = resolve_listing_id(db_path, "AAA.US")
    bbb_id = resolve_listing_id(db_path, "BBB.US")

    updated = repo.upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=aaa_id,
                sector="Technology",
                industry="Software",
            ),
            SecurityMetadataUpdate(
                security_id=bbb_id,
                description="BBB refreshed",
                sector="Industrials",
            ),
        ]
    )

    assert updated == 2
    aaa_row = repo.fetch(resolve_listing_id(db_path, "AAA.US"))
    bbb_row = repo.fetch(resolve_listing_id(db_path, "BBB.US"))
    assert aaa_row is not None
    assert aaa_row.entity_name == "AAA Corp"
    assert aaa_row.sector == "Technology"
    assert aaa_row.industry == "Software"
    assert bbb_row is not None
    assert bbb_row.entity_name == "BBB Corp"
    assert bbb_row.description == "BBB refreshed"
    assert bbb_row.sector == "Industrials"


def test_fundamentals_repository_fetch_metadata_candidates_extracts_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals-metadata-candidates.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    # AAA carries both an EODHD and a SEC payload; BBB is SEC-only; CCC has only
    # a bare listing. Seed a provider listing for each provider the test upserts.
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "AAA.US", provider="SEC")
    _seed_listing(db_path, "BBB.US", provider="SEC")
    _seed_listing(db_path, "CCC.US")
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {
            "General": {
                "Name": "AAA Holdings",
                "Description": "AAA business",
                "Sector": "Technology",
                "Industry": "Software",
            }
        },
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path, "SEC", "AAA.US", {"entityName": "AAA SEC Name", "facts": {}}
    )
    seed_raw_fundamentals(
        db_path, "SEC", "BBB.US", {"entityName": "BBB SEC Name", "facts": {}}
    )

    security_repo = SecurityRepository(db_path)
    security_ids = security_repo.resolve_ids_many(["AAA.US", "BBB.US", "CCC.US"])
    rows = repo.fetch_metadata_candidates(list(security_ids.values()))

    assert rows[security_ids["AAA.US"]].entity_name == "AAA Holdings"
    assert rows[security_ids["AAA.US"]].description == "AAA business"
    assert rows[security_ids["AAA.US"]].sector == "Technology"
    assert rows[security_ids["AAA.US"]].industry == "Software"
    assert rows[security_ids["BBB.US"]].entity_name == "BBB SEC Name"
    assert rows[security_ids["BBB.US"]].sector is None
    assert security_ids["CCC.US"] not in rows


def test_fetch_metadata_candidates_reads_raw_payload_not_canonical_issuer(
    tmp_path: Path,
) -> None:
    """Candidates come from ``fundamentals_raw``, never the canonical issuer row.

    Guards the query that bypasses ``provider_listing_catalog``: it must read the
    payload's ``General.*`` fields, not the issuer columns the old view join
    pulled in. The canonical issuer below is deliberately set to stale values
    that differ from the raw payload -- if a future edit reintroduced an issuer
    read, those stale values would leak into the candidate and fail this test.
    """
    db_path = tmp_path / "candidates-from-raw.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {
            "General": {
                "Name": "AAA Holdings",
                "Description": "Raw description",
                "Sector": "Technology",
                "Industry": "Software",
            }
        },
        exchange="US",
    )
    # Canonical issuer diverges from the raw payload on every field, so a query
    # that mistakenly read issuer instead of the payload would be caught.
    seed_security_metadata(
        db_path,
        "AAA.US",
        entity_name="STALE Name",
        description="Stale description",
        sector="Energy",
        industry="Oil & Gas",
    )

    security_id = SecurityRepository(db_path).resolve_ids_many(["AAA.US"])["AAA.US"]
    candidate = repo.fetch_metadata_candidates([security_id])[security_id]

    assert candidate.entity_name == "AAA Holdings"
    assert candidate.description == "Raw description"
    assert candidate.sector == "Technology"
    assert candidate.industry == "Software"


def test_replace_for_exchange_requires_seeded_exchange(tmp_path: Path) -> None:
    """The exchange catalog is owned by refresh-supported-exchanges.

    ``replace_for_exchange`` resolves the provider_exchange read-only and raises
    a clear error (rather than fabricating a stub) when the exchange has not been
    seeded -- the operator must run refresh-supported-exchanges first.
    """
    repo = SupportedTickerRepository(tmp_path / "needs-exchange.db")
    repo.initialize_schema()
    with pytest.raises(ValueError, match="refresh-supported-exchanges"):
        repo.replace_for_exchange(
            "EODHD",
            "US",
            [
                {
                    "Code": "AAA",
                    "Name": "AAA Inc",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ],
        )


def test_replace_for_exchange_does_not_write_exchange_metadata(tmp_path: Path) -> None:
    """Refreshing tickers must not overwrite provider_exchange metadata.

    The provider symbol-list payload carries security-level Name/Country/Currency,
    not exchange metadata. refresh-supported-tickers only *reads* the exchange
    catalog, so the rich metadata seeded by refresh-supported-exchanges (a proper
    exchange name, country, and operating MIC) survives a ticker refresh whose
    rows carry a company name.
    """
    db_path = tmp_path / "exchange-metadata-owner.db"
    exchanges = ExchangeProviderRepository(db_path)
    exchanges.replace_for_provider(
        "EODHD",
        [
            {
                "Code": "US",
                "Name": "USA Stocks",
                "Country": "USA",
                "Currency": "USD",
                "OperatingMIC": "XNAS",
            }
        ],
    )

    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD",
        "US",
        [
            {
                "Code": "AAA",
                "Name": "Apple Inc",
                "Country": "Freedonia",
                "Type": "Common Stock",
                "Currency": "USD",
            }
        ],
    )

    record = exchanges.fetch("EODHD", "US")
    assert record is not None
    # Untouched by the ticker refresh -- NOT clobbered with the ticker's company
    # name / country (the latent bug this change removes).
    assert record.name == "USA Stocks"
    assert record.country == "USA"
    assert record.operating_mic == "XNAS"


def test_replace_for_exchange_cascade_purges_both_fetch_states(tmp_path: Path) -> None:
    """Dropping a provider listing cascades to BOTH fetch-state tables.

    A ticker absent from the refreshed payload is removed, and
    ``_delete_provider_listing_ids`` purges its fundamentals_raw,
    fundamentals_fetch_state, and market_data_fetch_state rows. The refresh
    handles removals via this cascade rather than a separate delete; the dropped
    count is reported via ``SupportedTickerRefreshResult.removed``.
    """
    db_path = tmp_path / "refresh-cascade.db"
    seed_exchange(db_path, "US")
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
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

    # Give AAA and BBB downstream rows. upsert clears fundamentals fetch-state, so
    # mark the fundamentals failure AFTER upserting the raw payloads.
    seed_raw_fundamentals(db_path, "EODHD", "AAA.US", {"General": {}}, exchange="US")
    seed_raw_fundamentals(db_path, "EODHD", "BBB.US", {"General": {}}, exchange="US")
    storage.FundamentalsFetchStateRepository(db_path).mark_failure(
        "EODHD", "BBB.US", "boom"
    )
    MarketDataFetchStateRepository(db_path).mark_failure("EODHD", "AAA.US", "boom")
    MarketDataFetchStateRepository(db_path).mark_failure("EODHD", "BBB.US", "boom")

    # Refresh with only AAA -> BBB is dropped and fully purged.
    result = repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {
                "Code": "AAA",
                "Name": "AAA Inc",
                "Type": "Common Stock",
                "Currency": "USD",
            }
        ],
    )

    assert result.removed == 1
    assert [
        row.symbol for row in repo.list_for_provider("EODHD", exchange_codes=["US"])
    ] == ["AAA.US"]
    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM provider_listing WHERE provider_symbol = 'BBB'"
            ).fetchone()[0]
            == 0
        )
        # BBB's fundamentals fetch-state was the only one -> table is empty now.
        assert (
            conn.execute("SELECT COUNT(*) FROM fundamentals_fetch_state").fetchone()[0]
            == 0
        )
        market_state_symbols = {
            row[0]
            for row in conn.execute(
                """
                SELECT pl.provider_symbol
                FROM market_data_fetch_state ms
                JOIN provider_listing pl
                  ON pl.provider_listing_id = ms.provider_listing_id
                """
            )
        }
        raw_symbols = {
            row[0]
            for row in conn.execute(
                """
                SELECT pl.provider_symbol
                FROM fundamentals_raw fr
                JOIN provider_listing pl
                  ON pl.provider_listing_id = fr.provider_listing_id
                """
            )
        }
    # AAA's market-data fetch-state and raw payload survive; BBB's are purged.
    assert market_state_symbols == {"AAA"}
    assert raw_symbols == {"AAA"}


def test_replace_for_exchange_write_path_avoids_catalog_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cataloging a ticker must not re-read it back through the 6-table view.

    ``_ensure_provider_listing`` once issued a per-ticker
    ``provider_listing_catalog`` SELECT whose ``provider_listing_id`` both callers
    discarded. The write path now resolves and writes against base tables only.
    """
    db_path = tmp_path / "write-path-no-view.db"
    seed_exchange(db_path, "US")
    repo = SupportedTickerRepository(db_path)
    # Run migrations now so the CREATE VIEW DDL is not captured by the trace
    # below (migrations open their own connection, not repo._connect).
    repo.initialize_schema()

    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )

    assert captured, "expected the refresh to execute SQL on the traced connection"
    # The write path touches base tables only -- never the six-table catalog view.
    assert not any("provider_listing_catalog" in sql for sql in captured)
    # Sanity: it really did write the provider_listing row via the base table.
    assert any("INSERT INTO provider_listing" in sql for sql in captured)


def test_replace_for_exchange_skips_writes_when_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A re-refresh with identical rows must issue no writes (skip-unchanged).

    Re-cataloging tickers that already match everything the command owns
    (listing.currency, issuer.name, the provider_listing mapping) is detected by
    a single read, so a steady-state re-run does zero INSERT/UPDATE/DELETE.
    """
    db_path = tmp_path / "skip-unchanged.db"
    seed_exchange(db_path, "US")
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    rows = [
        {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"},
        {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock", "Currency": "USD"},
    ]
    repo.replace_for_exchange("EODHD", "US", rows)  # first build -- writes

    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    # Identical second run -> nothing this command owns has changed.
    result = repo.replace_for_exchange("EODHD", "US", rows)

    assert captured, "expected the re-refresh to execute SQL on the traced connection"
    writes = [
        sql
        for sql in captured
        if any(kw in sql.upper() for kw in ("INSERT", "UPDATE", "DELETE"))
    ]
    assert writes == [], f"re-refresh should issue no writes, got: {writes}"
    # The catalog is intact and the tickers are still reported as retained.
    assert result.inserted == 2
    assert result.removed == 0
    assert [
        row.symbol for row in repo.list_for_provider("EODHD", exchange_codes=["US"])
    ] == [
        "AAA.US",
        "BBB.US",
    ]


def test_replace_for_exchange_applies_currency_change_on_re_refresh(
    tmp_path: Path,
) -> None:
    """Skip-unchanged must not skip a real change: a new currency is applied."""
    db_path = tmp_path / "skip-currency-change.db"
    seed_exchange(db_path, "LSE")
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBP"}],
    )
    assert repo.list_for_provider("EODHD", exchange_codes=["LSE"])[0].currency == "GBP"

    # Same ticker, different currency -> must fall through the skip check and
    # update rather than skip.
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "USD"}],
    )
    assert repo.list_for_provider("EODHD", exchange_codes=["LSE"])[0].currency == "USD"


def test_list_supported_listings_returns_ids_matching_symbol_scope(
    tmp_path: Path,
) -> None:
    """``list_supported_listings`` surfaces the listing_id the scope join holds.

    The id-bearing scope query returns canonical symbols in stable order, each
    paired with the listing_id that ``resolve_ids_many`` would otherwise
    reconstruct -- that equivalence is what lets the canonical-scope commands
    carry the id instead of re-resolving it.
    """
    db_path = tmp_path / "supported-listings.db"
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    _seed_listing(db_path, "CCC.LSE")

    repo = SecurityRepository(db_path)
    listings = repo.list_supported_listings()

    # Canonical symbols in stable (canonical_symbol ORDER BY) order.
    assert [symbol for _, symbol in listings] == ["AAA.US", "BBB.US", "CCC.LSE"]

    # Ids agree with the resolver they are meant to replace.
    resolved = repo.resolve_ids_many(["AAA.US", "BBB.US", "CCC.LSE"])
    assert {symbol: listing_id for listing_id, symbol in listings} == resolved

    # Exchange filter narrows the scope just like the symbol-only variant.
    lse_only = repo.list_supported_listings(["LSE"])
    assert [symbol for _, symbol in lse_only] == ["CCC.LSE"]


def test_entity_names_by_ids_keys_by_listing_id(tmp_path: Path) -> None:
    """``entity_names_by_ids`` returns issuer names keyed by listing_id (the id the
    scope already holds), for run-screen's display labels -- no symbol/exchange read."""
    db_path = tmp_path / "entity-names.db"
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    seed_security_metadata(db_path, "AAA.US", entity_name="AAA Holdings")

    repo = SecurityRepository(db_path)
    ids = repo.resolve_ids_many(["AAA.US", "BBB.US"])
    names = repo.entity_names_by_ids(list(ids.values()))

    assert names[ids["AAA.US"]] == "AAA Holdings"
    assert set(names) == set(ids.values())  # keyed by listing_id, both present
    assert repo.entity_names_by_ids([]) == {}


def test_list_supported_listings_for_symbols_targeted_lookup(
    tmp_path: Path,
) -> None:
    """Targeted ``--symbols`` resolve returns (listing_id, is_primary) per match.

    Only supported listings appear; an unsupported symbol is simply absent, and a
    secondary listing reports ``is_primary=False``. The ids agree with the
    whole-universe ``list_supported_listings`` they replace for the ``--symbols``
    scope path.
    """
    db_path = tmp_path / "supported-listings-for-symbols.db"
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    _seed_listing(db_path, "CCC.LSE")
    # Mark BBB.US secondary so is_primary must reflect it.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE listing SET primary_listing_status = 'secondary' WHERE symbol = ?",
            ("BBB",),
        )

    repo = SecurityRepository(db_path)
    resolved = repo.list_supported_listings_for_symbols(["AAA.US", "BBB.US", "ZZZ.US"])

    # Unsupported symbol is absent; supported ones carry id + primary flag.
    assert set(resolved) == {"AAA.US", "BBB.US"}
    assert resolved["AAA.US"][1] is True
    assert resolved["BBB.US"][1] is False

    # Ids match the whole-universe scope read they replace.
    full = {symbol: listing_id for listing_id, symbol in repo.list_supported_listings()}
    assert resolved["AAA.US"][0] == full["AAA.US"]
    assert resolved["BBB.US"][0] == full["BBB.US"]


def test_resolve_ids_many_does_not_cross_match_across_exchanges(
    tmp_path: Path,
) -> None:
    """The two-IN predicate must not resolve unrequested cross-listing pairs.

    With the same ticker listed on two exchanges, a batch spanning both
    exchanges produces a symbol x exchange cross product in SQL; the resolver
    must filter it back to exactly the requested ``SYMBOL.EXCHANGE`` pairs.
    """
    db_path = tmp_path / "resolve-cross-exchange.db"
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "AAA.LSE")
    _seed_listing(db_path, "BBB.US")
    _seed_listing(db_path, "BBB.LSE")

    repo = SecurityRepository(db_path)
    resolved = repo.resolve_ids_many(["AAA.US", "BBB.LSE"])

    # Only the exact requested pairs come back -- never AAA.LSE or BBB.US.
    assert set(resolved) == {"AAA.US", "BBB.LSE"}
    full = repo.resolve_ids_many(["AAA.US", "AAA.LSE", "BBB.US", "BBB.LSE"])
    assert resolved["AAA.US"] == full["AAA.US"]
    assert resolved["BBB.LSE"] == full["BBB.LSE"]
    # All four listings are genuinely distinct ids.
    assert len(set(full.values())) == 4


def test_id_keyed_metric_and_market_reads(tmp_path: Path) -> None:
    """The ``*_by_id(s)`` readers return the seeded metric/market data.

    Now that the symbol-keyed read wrappers have been removed, the pipeline reads
    everything by ``listing_id``. This pins the metric reads to the seeded values,
    the market read to the latest ``as_of`` and to reconstructing the canonical
    display symbol from ``listing ⋈ exchange`` (no ``securities`` view /
    double-listing join), and the listing-currency read (whose symbol form is a
    kept resolver edge) to the seeded ``GBP``.
    """

    db_path = tmp_path / "id-reads.db"
    seed_exchange(db_path, "LSE", currency="GBP")
    seed_supported_listings(
        db_path,
        "EODHD",
        "LSE",
        [
            Listing(
                symbol="AAA.LSE", security_name="AAA", exchange="LSE", currency="GBP"
            ),
            Listing(
                symbol="BBB.LSE", security_name="BBB", exchange="LSE", currency="GBP"
            ),
        ],
    )
    aaa_id = resolve_listing_id(db_path, "AAA.LSE")
    bbb_id = resolve_listing_id(db_path, "BBB.LSE")

    metrics_repo = MetricsRepository(db_path)
    seed_metric(
        db_path,
        "AAA.LSE",
        "market_cap",
        1000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )
    seed_metric(
        db_path, "AAA.LSE", "current_ratio", 1.5, "2025-01-02", unit_kind="ratio"
    )
    seed_metric(
        db_path,
        "BBB.LSE",
        "market_cap",
        2000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )

    market_repo = MarketDataRepository(db_path)
    market_repo.upsert_prices(
        [
            MarketDataUpdate(
                security_id=aaa_id,
                symbol="AAA.LSE",
                as_of="2025-01-02",
                price=10.0,
                volume=100,
                currency="GBP",
            ),
            MarketDataUpdate(
                security_id=aaa_id,
                symbol="AAA.LSE",
                as_of="2025-01-03",
                price=11.0,
                volume=120,
                currency="GBP",
            ),
            MarketDataUpdate(
                security_id=bbb_id,
                symbol="BBB.LSE",
                as_of="2025-01-02",
                price=20.0,
                volume=200,
                currency="GBP",
            ),
        ]
    )

    # Single metric read: the seeded value is returned; unknown metric is None.
    aaa_market_cap = metrics_repo.fetch_by_id(aaa_id, "market_cap")
    assert aaa_market_cap is not None
    assert aaa_market_cap.value == 1000.0
    assert metrics_repo.fetch_by_id(aaa_id, "missing") is None

    # Bulk metric read: the id-keyed map carries the seeded records, keyed by id.
    by_id = metrics_repo.fetch_many_by_ids(
        [aaa_id, bbb_id], ["market_cap", "current_ratio"]
    )
    assert by_id[aaa_id]["market_cap"].value == 1000.0
    assert by_id[aaa_id]["current_ratio"].value == 1.5
    assert by_id[bbb_id]["market_cap"].value == 2000.0

    # Latest snapshot: picks the most-recent as_of and rebuilds the canonical
    # symbol from ``listing ⋈ exchange``.
    rec_by_id = market_repo.latest_snapshot_record_by_id(aaa_id)
    assert rec_by_id is not None
    assert rec_by_id.as_of == "2025-01-03"
    assert rec_by_id.symbol == "AAA.LSE"
    assert rec_by_id.security_id == aaa_id

    snaps_by_id = market_repo.latest_snapshots_many_by_ids([aaa_id, bbb_id])
    assert snaps_by_id[aaa_id].as_of == "2025-01-03"
    assert snaps_by_id[bbb_id].as_of == "2025-01-02"

    # Listing currency: the id-keyed lookup collapses the GBX subunit to GBP.
    assert market_repo.ticker_currency_by_id(aaa_id) == "GBP"
