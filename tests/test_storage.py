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
    FundamentalsUpdate,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FundamentalsNormalizationCandidate,
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
    SecurityListingStatusRecord,
    SecurityListingStatusRepository,
    SupportedTickerRepository,
)
from pyvalue.persistence.storage.fundamentals import _resolve_provider_listing_id
from pyvalue.marketdata import MarketDataUpdate
from pyvalue.universe import Listing
from collections.abc import Sequence
from types import TracebackType
from typing import Literal, NoReturn, Optional, Tuple, Type

from conftest import (
    seed_exchange,
    seed_facts,
    seed_metric,
    seed_metric_status,
    seed_price,
    seed_security_metadata,
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
        for existing in repo.list_for_exchange(provider, exchange)
    }
    rows[ticker] = {"Code": ticker, "Type": "Common Stock", "Currency": currency}
    repo.replace_for_exchange(provider, exchange, list(rows.values()))


def test_supported_ticker_repository_replace_from_listings_persists_rows(
    tmp_path: Path,
) -> None:
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    seed_exchange(tmp_path / "universe.db", "US", provider="SEC")
    result = repo.replace_from_listings(
        "SEC",
        "US",
        [_listing("AAA"), _listing("BBB", is_etf=True)],
    )

    assert result.inserted == 2
    assert result.skipped_no_currency == ()

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            """
            SELECT p.provider_code, px.provider_exchange_code, pl.provider_symbol,
                   e.exchange_code, l.currency
            FROM provider_listing pl
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY pl.provider_symbol
            """
        ).fetchall()
        provider_listing_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(provider_listing)")
        }

    assert rows == [
        ("SEC", "US", "AAA", "US", "USD"),
        ("SEC", "US", "BBB", "US", "USD"),
    ]
    assert "security_type" not in provider_listing_columns
    assert "currency" not in provider_listing_columns


def test_supported_ticker_repository_replace_from_listings_overwrites_exchange_slice(
    tmp_path: Path,
) -> None:
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    seed_exchange(tmp_path / "universe.db", "US", provider="SEC")
    repo.replace_from_listings("SEC", "US", [_listing("AAA")])
    repo.replace_from_listings("SEC", "US", [_listing("CCC")])

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            "SELECT provider_symbol FROM supported_tickers ORDER BY provider_symbol"
        ).fetchall()

    assert rows == [("CCC.US",)]


def test_supported_ticker_repository_list_symbols_initializes_schema(
    tmp_path: Path,
) -> None:
    repo = SupportedTickerRepository(tmp_path / "universe.db")

    assert repo.list_symbols_by_exchange("SEC", "US") == []


def test_supported_ticker_repository_normalizes_exchange_and_fetches_currency(
    tmp_path: Path,
) -> None:
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
    repo.replace_from_listings("EODHD", "LSE", [listing])

    assert repo.list_symbols_by_exchange("EODHD", "LSE") == ["FOO.LSE"]
    assert repo.list_symbols_by_exchange("eodhd", "lse") == ["FOO.LSE"]
    assert repo.fetch_currency("FOO.LSE", provider="EODHD") == "GBP"


def test_fundamentals_repository_normalizes_provider(tmp_path: Path) -> None:
    db_path = tmp_path / "funds.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "FOO.LSE", currency="GBX")
    repo.upsert("eodhd", "FOO.LSE", payload={"bar": 1})

    assert repo.symbols("EODHD") == ["FOO.LSE"]
    assert repo.symbols("eodhd") == ["FOO.LSE"]


def test_fundamentals_repository_classifies_and_purges_secondary_listings(
    tmp_path: Path,
) -> None:
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
    FundamentalsNormalizationStateRepository(db_path).mark_success(
        "EODHD",
        "AAA.LSE",
        "a" * 64,
    )
    MarketDataFetchStateRepository(db_path).mark_success(
        "EODHD",
        "AAA.LSE",
        fetched_at="2025-01-02T00:00:00+00:00",
    )

    repo = FundamentalsRepository(db_path)
    repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    repo.upsert(
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    repo.upsert(
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
    assert fact_rows == 0
    assert refresh_rows == 0
    assert market_rows == 0
    assert metric_rows == 0
    assert status_rows == 0
    assert normalization_rows == 0
    assert market_state_rows == 0


def test_migration_078_backfills_unknown_status_and_purges_secondary(
    tmp_path: Path,
) -> None:
    """Migration 078 resolves leftover 'unknown' EODHD classification.

    It is the one-time backstop that lets read/compute commands stop reconciling
    on read: every still-'unknown' listing with fundamentals is classified, and
    the derived data of any that turns out secondary is purged -- mirroring
    ``reconcile-listing-status`` and ``_build_status_record``.
    """
    from pyvalue.persistence.migrations import (
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

    repo = FundamentalsRepository(db_path)
    repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    repo.upsert(
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    repo.upsert(
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

    inserted = repo.replace_for_provider(
        "eodhd",
        [{"Code": "lse", "Name": "London Exchange Refreshed"}],
    )

    assert inserted == 1
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
    lse = repo.list_for_exchange("EODHD", "LSE")
    us = repo.list_for_exchange("EODHD", "US")
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

    us = repo.list_for_exchange("EODHD", "US")
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
    assert [row.symbol for row in repo.list_for_exchange("EODHD", "LSE")] == ["AAA.LSE"]


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
    fund_repo.upsert("EODHD", "AAA.LSE", {"General": {"CurrencyCode": "GBP"}})

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
    fund_repo.upsert("EODHD", "BBB.LSE", {"General": {}})
    fund_repo.upsert("EODHD", "CCC.LSE", {"General": {}})
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


def test_fundamentals_upsert_resolves_via_base_tables_not_catalog_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "upsert-resolve-plan.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "LSE")
    # refresh-supported-tickers catalogs the listing the payload attaches to.
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBP"}],
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()

    # Capture the SQL the write path runs. Resolving provider_listing_id must hit
    # the base tables by the provider_listing natural key: routing it back through
    # provider_listing_catalog would filter on the view's *computed*
    # provider_symbol, which no index can serve (the slow path this replaced).
    captured: list[str] = []
    original_connect = fund_repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(fund_repo, "_connect", _tracing_connect)

    fund_repo.upsert("EODHD", "AAA.LSE", {"General": {"Name": "AAA plc"}})

    resolution_sql = [
        sql
        for sql in captured
        if "provider_listing_id" in sql
        and "(provider_exchange_id, provider_symbol) IN" in sql
    ]
    assert resolution_sql, "expected the provider_listing_id resolution SELECT"
    for sql in resolution_sql:
        assert "provider_listing_catalog" not in sql
        assert "FROM provider_listing" in sql

    # The payload actually landed, proving the natural-key lookup matched the row
    # (guards against the bare-symbol derivation regressing).
    assert fund_repo.fetch("EODHD", "AAA.LSE") is not None


def test_fundamentals_upsert_many_resolves_listing_ids_in_one_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "fundamentals-upsert-bulk.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": c, "Name": f"{c} Inc", "Type": "Common Stock", "Currency": "USD"}
            for c in ("AAA", "BBB", "CCC")
        ],
    )
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()

    # A whole ingest batch: provider_listing_id must be resolved for all three
    # payloads in ONE row-value (provider_exchange_id, provider_symbol) IN query
    # served by the natural-key index, not one indexed seek per payload (the
    # per-update round-trip this replaced) and never the catalog view.
    updates = [
        FundamentalsUpdate(
            security_id=0,
            provider_symbol=f"{c}.US",
            provider_exchange_code="US",
            data="{}",
            payload_hash=f"{i:064x}",  # fundamentals_raw.payload_hash CHECK len == 64
            last_fetched_at="2026-01-01T00:00:00+00:00",
        )
        for i, c in enumerate(("AAA", "BBB", "CCC"))
    ]

    captured: list[str] = []
    original_connect = repo._connect

    def _tracing_connect() -> sqlite3.Connection:
        conn = original_connect()
        conn.set_trace_callback(captured.append)
        return conn

    monkeypatch.setattr(repo, "_connect", _tracing_connect)

    repo.upsert_many("EODHD", updates)

    resolution_sql = [
        sql
        for sql in captured
        if "provider_listing_id" in sql
        and "(provider_exchange_id, provider_symbol) IN" in sql
    ]
    assert len(resolution_sql) == 1, (
        f"expected one bulk resolution query, got {len(resolution_sql)}"
    )
    assert "provider_listing_catalog" not in resolution_sql[0]

    # All three payloads resolved and landed via the bulk path.
    assert all(
        repo.fetch("EODHD", f"{c}.US") is not None for c in ("AAA", "BBB", "CCC")
    )


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

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    # The payload reports a *different* currency; fundamentals ingest must not
    # let it leak into the listing.
    fund_repo.upsert("EODHD", "AAA.LSE", {"General": {"CurrencyCode": "USD"}})

    assert fund_repo.fetch("EODHD", "AAA.LSE") is not None
    with sqlite3.connect(db_path) as conn:
        currency = conn.execute(
            """
            SELECT currency FROM provider_listing_catalog
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]
    assert currency == "GBP"


def test_fundamentals_upsert_skips_uncatalogued_listing(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals-uncatalogued.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    # No catalog refresh has run, so NEW.LSE is unknown. Fundamentals ingest must
    # not create it (that would require writing listing.currency, owned solely by
    # refresh-supported-tickers); the payload is skipped instead.
    fund_repo.upsert(
        "EODHD", "NEW.LSE", {"General": {"CurrencyCode": "GBP"}}, exchange="LSE"
    )

    assert fund_repo.fetch("EODHD", "NEW.LSE") is None
    with sqlite3.connect(db_path) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0]
        listing_count = conn.execute("SELECT COUNT(*) FROM listing").fetchone()[0]
    assert raw_count == 0
    assert listing_count == 0


def test_purge_downstream_for_secondary_purges_only_secondary(tmp_path: Path) -> None:
    db_path = tmp_path / "purge-secondary.db"
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
    aaa_id = by_symbol["AAA.US"].security_id
    bbb_id = by_symbol["BBB.US"].security_id

    # Seed downstream facts for both listings so we can prove the primary's
    # survive and only the secondary's are purged.
    for symbol in ("AAA.US", "BBB.US"):
        seed_facts(
            db_path,
            symbol,
            [
                FactRecord(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="FY",
                    end_date="2024-12-31",
                    unit_kind="monetary",
                    value=100.0,
                    currency="USD",
                )
            ],
        )

    repo = SecurityListingStatusRepository(db_path)
    records = [
        SecurityListingStatusRecord(
            security_id=aaa_id,
            source_provider="EODHD",
            provider_symbol="AAA.US",
            raw_fetched_at="2026-01-01T00:00:00+00:00",
            is_primary_listing=True,
            primary_provider_symbol="AAA.US",
            classification_basis="matched_primary_ticker",
        ),
        SecurityListingStatusRecord(
            security_id=bbb_id,
            source_provider="EODHD",
            provider_symbol="BBB.US",
            raw_fetched_at="2026-01-01T00:00:00+00:00",
            is_primary_listing=False,
            primary_provider_symbol="AAA.US",
            classification_basis="different_primary_ticker",
        ),
    ]

    purged = repo.purge_downstream_for_secondary(records)

    # Only the secondary record is returned and purged; the primary is untouched.
    assert [record.provider_symbol for record in purged] == ["BBB.US"]
    with sqlite3.connect(db_path) as conn:
        listing_ids_with_facts = {
            row[0]
            for row in conn.execute("SELECT DISTINCT listing_id FROM financial_facts")
        }
    assert aaa_id in listing_ids_with_facts
    assert bbb_id not in listing_ids_with_facts


def test_purge_downstream_for_secondary_noop_when_all_primary(tmp_path: Path) -> None:
    db_path = tmp_path / "purge-secondary-noop.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    aaa_id = next(iter(ticker_repo.list_for_provider("EODHD"))).security_id
    seed_facts(
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

    repo = SecurityListingStatusRepository(db_path)
    purged = repo.purge_downstream_for_secondary(
        [
            SecurityListingStatusRecord(
                security_id=aaa_id,
                source_provider="EODHD",
                provider_symbol="AAA.US",
                raw_fetched_at="2026-01-01T00:00:00+00:00",
                is_primary_listing=True,
                primary_provider_symbol="AAA.US",
                classification_basis="matched_primary_ticker",
            )
        ]
    )

    assert purged == []
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?", (aaa_id,)
        ).fetchone()[0]
    assert count == 1


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
    listing_id = repo._security_repo().resolve_id("AAA.US")
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
    id_aaa = SecurityRepository(db_path).resolve_id("AAA.US")
    assert id_aaa is not None
    refresh_record = refresh_repo.fetch_by_id(id_aaa)

    assert refresh_record is not None
    assert refresh_record.listing_id == id_aaa
    assert refresh_record.refreshed_at


def test_fundamentals_repository_normalization_candidates_match_state_and_facts(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "normalization-candidates.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    state_repo.initialize_schema()

    _seed_listing(db_path, "AAA.US", provider="SEC")
    _seed_listing(db_path, "BBB.US", provider="SEC")
    _seed_listing(db_path, "CCC.US", provider="SEC")
    fund_repo.upsert("SEC", "AAA.US", {"entityName": "AAA", "facts": {}})
    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB", "facts": {}})
    fund_repo.upsert("SEC", "CCC.US", {"entityName": "CCC", "facts": {}})

    aaa_security_id = (
        fund_repo._security_repo().ensure_from_symbol("AAA.US").security_id
    )

    aaa_record = fund_repo.fetch_payload_with_hash("SEC", "AAA.US")
    assert aaa_record is not None
    _, aaa_payload_hash = aaa_record
    bbb_record = fund_repo.fetch_payload_with_hash("SEC", "BBB.US")
    assert bbb_record is not None
    _, bbb_payload_hash = bbb_record

    state_repo.mark_success("SEC", "AAA.US", aaa_payload_hash)
    state_repo.mark_success("SEC", "BBB.US", bbb_payload_hash)
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                currency="USD",
                value=100.0,
            )
        ],
    )
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
                currency="USD",
                value=200.0,
            )
        ],
    )

    candidates = fund_repo.normalization_candidates(
        "SEC",
        ["AAA.US", "BBB.US", "CCC.US"],
    )

    assert candidates["AAA.US"] == FundamentalsNormalizationCandidate(
        provider_symbol="AAA.US",
        security_id=aaa_security_id,
        raw_payload_hash=aaa_payload_hash,
        normalized_payload_hash=aaa_payload_hash,
        normalized_at=candidates["AAA.US"].normalized_at,
    )
    assert candidates["BBB.US"].normalized_payload_hash == bbb_payload_hash
    assert candidates["CCC.US"].normalized_payload_hash is None


def test_fundamentals_repository_normalization_candidates_skip_facts_scan_without_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "normalization-candidates-no-state.db"
    seen_sql: list[str] = []

    class _LoggingConnection:
        """Context-manager proxy that records each executed SQL statement.

        It wraps the repository's real connection and is installed via
        ``monkeypatch.setattr`` (not a method override) so there is no static
        return-type contract to satisfy. The repository's
        ``normalization_candidates`` path only uses the connection as a context
        manager and calls ``execute`` on it, so those are the only members the
        proxy needs to expose. The recorded statements back the assertion that
        the no-state path never scans ``financial_facts``.
        """

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
    _seed_listing(db_path, "BBB.US")
    fund_repo.upsert("EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US")
    fund_repo.upsert("EODHD", "BBB.US", {"General": {"Name": "BBB"}}, exchange="US")

    # Only the candidate query's SQL is under test, so install the logging
    # proxy after setup. That keeps the proxy's surface minimal (just the
    # context-manager protocol and ``execute``) -- the write-heavy setup above
    # still runs on the unwrapped connection.
    real_connect = fund_repo._connect
    monkeypatch.setattr(
        fund_repo, "_connect", lambda: _LoggingConnection(real_connect())
    )

    candidates = fund_repo.normalization_candidates("EODHD", ["AAA.US", "BBB.US"])

    assert sorted(candidates) == ["AAA.US", "BBB.US"]
    assert all(
        candidate.normalized_payload_hash is None for candidate in candidates.values()
    )
    assert not any("FROM financial_facts" in sql for sql in seen_sql)


def test_resolve_provider_listing_id_natural_key_and_view_fallback(
    tmp_path: Path,
) -> None:
    """``_resolve_provider_listing_id`` resolves via the provider_listing natural
    key, falls back to the catalog view, and returns ``None`` for unknowns."""
    db_path = tmp_path / "resolve-listing-id.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    fund_repo.upsert("EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US")

    with fund_repo._connect() as conn:
        expected = conn.execute(
            """
            SELECT provider_listing_id
            FROM provider_listing_catalog
            WHERE provider = ? AND provider_symbol = ?
            """,
            ("EODHD", "AAA.US"),
        ).fetchone()["provider_listing_id"]
        provider_id = conn.execute(
            "SELECT provider_id FROM provider WHERE provider_code = ?",
            ("EODHD",),
        ).fetchone()["provider_id"]

        # Fast path: natural key.
        assert (
            _resolve_provider_listing_id(conn, provider_id, "EODHD", "AAA.US")
            == expected
        )
        # Fallback path: a None provider_id skips the natural-key branch, so
        # resolution falls through to the catalog-view lookup -- the branch that
        # keeps non-EODHD providers (e.g. SEC's synthetic '.US' suffix) correct.
        assert _resolve_provider_listing_id(conn, None, "EODHD", "AAA.US") == expected
        # Unknown symbol resolves to nothing.
        assert (
            _resolve_provider_listing_id(conn, provider_id, "EODHD", "ZZZ.US") is None
        )


def test_fetch_payload_with_hash_resolves_without_scanning_catalog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The per-symbol payload read resolves the listing by its natural key and
    reads ``fundamentals_raw`` by primary key, never walking the
    computed-``provider_symbol`` catalog view. Regression guard for the
    ~33 ms-per-symbol full catalog scan the view filter forced."""
    db_path = tmp_path / "fetch-payload-plan.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    # Several exchanges/listings so a stray full catalog scan would be obvious.
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.LSE")
    _seed_listing(db_path, "CCC.AU")
    fund_repo.upsert("EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US")

    seen: list[tuple[str, tuple[object, ...]]] = []

    class _LoggingConnection:
        """Records each executed statement and its parameters so the test can
        re-run EXPLAIN QUERY PLAN on them."""

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
            seen.append((" ".join(sql.split()), tuple(parameters)))
            return self._conn.execute(sql, parameters)

    real_connect = fund_repo._connect
    monkeypatch.setattr(
        fund_repo, "_connect", lambda: _LoggingConnection(real_connect())
    )

    record = fund_repo.fetch_payload_with_hash("EODHD", "AAA.US")
    assert record is not None

    selects = [
        (sql, params) for sql, params in seen if sql.lower().startswith("select")
    ]
    assert selects, "expected the read path to issue at least one SELECT"
    # The fast path never touches the 6-table catalog view...
    assert not any("provider_listing_catalog" in sql for sql, _ in selects)
    # ...and no statement degrades into a full catalog walk (which always begins
    # by scanning the exchange table at the base of the view's join).
    with sqlite3.connect(db_path) as plan_conn:
        plan_conn.row_factory = sqlite3.Row
        for sql, params in selects:
            plan = plan_conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
            plan_text = " ".join(str(row["detail"]) for row in plan)
            assert "SCAN TABLE exchange" not in plan_text


def test_replace_fact_rows_writes_by_id_without_resolving_symbol(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``replace_fact_rows`` is keyed purely by ``listing_id``.

    It never resolves a symbol and never falls through to the create-or-update
    ``ensure_from_symbol`` path, so the write touches only ``financial_facts`` for
    the given id. The monkeypatched ``ensure_from_symbol`` would raise if the
    writer ever reached it.
    """
    db_path = tmp_path / "replace-known-id.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    security_id = repo._security_repo().resolve_id("AAA.US")
    assert security_id is not None

    def _boom(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(
            "replace_fact_rows must not resolve/create a listing from a symbol"
        )

    monkeypatch.setattr(repo._security_repo(), "ensure_from_symbol", _boom)

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


def test_supported_ticker_repository_provider_symbols_honours_primary_only(
    tmp_path: Path,
) -> None:
    """``provider_symbols`` returns qualified symbols and honours
    ``primary_only`` (excluding secondary listings)."""
    db_path = tmp_path / "provider-symbols.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    # Mark BBB secondary so it drops out of the primary-only projection.
    with repo._connect() as conn:
        conn.execute(
            "UPDATE listing SET primary_listing_status = 'secondary' WHERE symbol = ?",
            ("BBB",),
        )
        conn.commit()

    assert sorted(repo.provider_symbols("EODHD")) == ["AAA.US", "BBB.US"]
    assert repo.provider_symbols("EODHD", primary_only=True) == ["AAA.US"]


def test_financial_facts_repository_replace_fact_rows_replaces_listing_slice(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "financial-facts-replace.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    listing_id = repo._security_repo().resolve_id("AAA.US")
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

    repo.upsert("EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US")

    assert state_repo.fetch("EODHD", "AAA.US") is None


def test_fundamentals_repository_upsert_many_uses_resolved_metadata_and_overwrites(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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
    tickers = {row.symbol: row for row in ticker_repo.list_for_exchange("EODHD", "US")}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()

    # ``upsert_many`` must resolve metadata in bulk, never per symbol. Patch
    # ``_resolve_security`` (via monkeypatch, which auto-restores after the
    # test) to explode if the per-symbol slow path is ever taken. The stub
    # mirrors the real signature so the substitution stays type-correct.
    def _fail_resolve(
        provider: str,
        symbol: str,
        exchange: Optional[str],
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        raise AssertionError("upsert_many should not resolve securities per symbol")

    monkeypatch.setattr(repo, "_resolve_security", _fail_resolve)
    aaa_data = '{"General":{"CurrencyCode":"USD","Name":"AAA"}}'
    bbb_data = '{"General":{"CurrencyCode":"USD","Name":"BBB"}}'
    aaa_updated_data = '{"General":{"CurrencyCode":"USD","Name":"AAA Updated"}}'

    repo.upsert_many(
        "EODHD",
        [
            FundamentalsUpdate(
                security_id=tickers["AAA.US"].security_id,
                provider_symbol="AAA.US",
                provider_exchange_code="US",
                data=aaa_data,
                payload_hash=storage.fundamentals_payload_hash(aaa_data),
                last_fetched_at="2026-03-30T00:00:00+00:00",
            ),
            FundamentalsUpdate(
                security_id=tickers["BBB.US"].security_id,
                provider_symbol="BBB.US",
                provider_exchange_code="US",
                data=bbb_data,
                payload_hash=storage.fundamentals_payload_hash(bbb_data),
                last_fetched_at="2026-03-30T00:00:00+00:00",
            ),
        ],
    )
    repo.upsert_many(
        "EODHD",
        [
            FundamentalsUpdate(
                security_id=tickers["AAA.US"].security_id,
                provider_symbol="AAA.US",
                provider_exchange_code="US",
                data=aaa_updated_data,
                payload_hash=storage.fundamentals_payload_hash(aaa_updated_data),
                last_fetched_at="2026-03-31T00:00:00+00:00",
            )
        ],
    )

    with sqlite3.connect(db_path) as conn:
        raw_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(fundamentals_raw)")
        }
        rows = conn.execute(
            """
            SELECT
                catalog.provider_symbol,
                catalog.security_id,
                catalog.provider_exchange_code,
                catalog.currency,
                fr.payload_hash,
                fr.last_fetched_at
            FROM fundamentals_raw fr
            JOIN provider_listing_catalog catalog
              ON catalog.provider_listing_id = fr.provider_listing_id
            ORDER BY catalog.provider_symbol
            """
        ).fetchall()

    assert "listing_id" not in raw_columns
    assert "security_id" not in raw_columns
    assert "currency" not in raw_columns
    assert "payload_id" not in raw_columns
    assert rows == [
        (
            "AAA.US",
            tickers["AAA.US"].security_id,
            "US",
            "USD",
            storage.fundamentals_payload_hash(aaa_updated_data),
            "2026-03-31T00:00:00+00:00",
        ),
        (
            "BBB.US",
            tickers["BBB.US"].security_id,
            "US",
            "USD",
            storage.fundamentals_payload_hash(bbb_data),
            "2026-03-30T00:00:00+00:00",
        ),
    ]
    # Reads go through a fresh resolution path, so undo the failing stub before
    # asserting the persisted payloads round-trip.
    monkeypatch.undo()
    aaa_payload = repo.fetch("EODHD", "AAA.US")
    bbb_payload = repo.fetch("EODHD", "BBB.US")
    assert aaa_payload is not None
    assert bbb_payload is not None
    assert aaa_payload["General"]["Name"] == "AAA Updated"
    assert bbb_payload["General"]["Name"] == "BBB"


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
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"PrimaryTicker": "AAA.US"}}, exchange="US"
    )
    fund_repo.upsert(
        "EODHD", "AAA.LSE", {"General": {"PrimaryTicker": "AAA.US"}}, exchange="LSE"
    )
    fund_repo.upsert(
        "EODHD", "BBB.LSE", {"General": {"Name": "BBB plc"}}, exchange="LSE"
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
    repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    repo.upsert(
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    repo.upsert(
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )

    assert ticker_repo.list_symbols_by_exchange("EODHD", "LSE") == [
        "AAA.LSE",
        "BBB.LSE",
    ]
    assert ticker_repo.list_symbols_by_exchange(
        "EODHD",
        "LSE",
        primary_only=True,
    ) == ["BBB.LSE"]
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
    fundamentals.upsert(
        "EODHD", "AAA.US", {"General": {"PrimaryTicker": "AAA.US"}}, exchange="US"
    )
    fundamentals.upsert(
        "EODHD", "AAA.LSE", {"General": {"PrimaryTicker": "AAA.US"}}, exchange="LSE"
    )
    fundamentals.upsert(
        "EODHD", "BBB.LSE", {"General": {"Name": "BBB plc"}}, exchange="LSE"
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
    rows = ticker_repo.list_for_exchange("EODHD", "US")
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

    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
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

    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
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

    id_aaa = repo._security_repo().resolve_id("AAA.US")
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
    id_aaa = sec_repo.resolve_id("AAA.US")
    id_bbb = sec_repo.resolve_id("BBB.US")
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

    from pyvalue.persistence.migrations import apply_migrations

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
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_from_listings("EODHD", "US", [_listing("AAA")])

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

    id_aaa = repo._security_repo().resolve_id("AAA.US")
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
    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
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
    id_aaa = repo._security_repo().resolve_id("AAA.US")
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

    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
    id_ccc = security_repo.resolve_id("CCC.US")
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
    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
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

    id_aaa = repo._security_repo().resolve_id("AAA.US")
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

    security_repo = repo._security_repo()
    id_aaa = security_repo.resolve_id("AAA.JSE")
    id_bbb = security_repo.resolve_id("BBB.TA")
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
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    fund_repo.upsert(
        "EODHD",
        "AAA.LSE",
        {"General": {"PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    fund_repo.upsert(
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

    security = repo.fetch_by_symbol("AAA.US")

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

    id_aaa = security_repo.resolve_id("AAA.US")
    id_bbb = security_repo.resolve_id("BBB.US")
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
    aaa = repo.ensure_from_symbol("AAA.US", entity_name="AAA Corp")
    bbb = repo.ensure_from_symbol(
        "BBB.US",
        entity_name="BBB Corp",
        description="BBB description",
    )

    updated = repo.upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=aaa.security_id,
                sector="Technology",
                industry="Software",
            ),
            SecurityMetadataUpdate(
                security_id=bbb.security_id,
                description="BBB refreshed",
                sector="Industrials",
            ),
        ]
    )

    assert updated == 2
    aaa_row = repo.fetch_by_symbol("AAA.US")
    bbb_row = repo.fetch_by_symbol("BBB.US")
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
    repo.upsert(
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
    repo.upsert("SEC", "AAA.US", {"entityName": "AAA SEC Name", "facts": {}})
    repo.upsert("SEC", "BBB.US", {"entityName": "BBB SEC Name", "facts": {}})

    security_repo = SecurityRepository(db_path)
    security_repo.ensure_from_symbol("CCC.US")
    security_ids = security_repo.resolve_ids_many(["AAA.US", "BBB.US", "CCC.US"])
    rows = repo.fetch_metadata_candidates(list(security_ids.values()))

    assert rows[security_ids["AAA.US"]].entity_name == "AAA Holdings"
    assert rows[security_ids["AAA.US"]].description == "AAA business"
    assert rows[security_ids["AAA.US"]].sector == "Technology"
    assert rows[security_ids["AAA.US"]].industry == "Software"
    assert rows[security_ids["BBB.US"]].entity_name == "BBB SEC Name"
    assert rows[security_ids["BBB.US"]].sector is None
    assert security_ids["CCC.US"] not in rows


def test_fundamentals_repository_fetch_many_returns_payloads_by_symbol(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals-fetch-many.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
    repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "Sector": "Technology"}},
        exchange="US",
    )
    repo.upsert(
        "EODHD",
        "BBB.US",
        {"General": {"Name": "BBB", "Sector": "Industrials"}},
        exchange="US",
    )

    rows = repo.fetch_many("EODHD", ["AAA.US", "BBB.US", "CCC.US"])

    assert rows["AAA.US"]["General"]["Sector"] == "Technology"
    assert rows["BBB.US"]["General"]["Sector"] == "Industrials"
    assert "CCC.US" not in rows


def test_replace_for_exchange_requires_seeded_exchange(tmp_path: Path) -> None:
    """The exchange catalog is owned by refresh-supported-exchanges.

    ``replace_for_exchange`` / ``replace_from_listings`` resolve the
    provider_exchange read-only and raise a clear error (rather than fabricating
    a stub) when the exchange has not been seeded -- the operator must run
    refresh-supported-exchanges first.
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
    with pytest.raises(ValueError, match="refresh-supported-exchanges"):
        repo.replace_from_listings("EODHD", "US", [_listing("AAA")])


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
    fundamentals_fetch_state, and market_data_fetch_state rows. This is why the
    CLI no longer calls ``delete_symbols`` separately; the dropped count is
    reported via ``SupportedTickerRefreshResult.removed``.
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
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.upsert("EODHD", "AAA.US", {"General": {}}, exchange="US")
    fund_repo.upsert("EODHD", "BBB.US", {"General": {}}, exchange="US")
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
    assert [row.symbol for row in repo.list_for_exchange("EODHD", "US")] == ["AAA.US"]
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
    assert [row.symbol for row in repo.list_for_exchange("EODHD", "US")] == [
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
    assert repo.fetch_currency("AAA.LSE", provider="EODHD") == "GBP"

    # Same ticker, different currency -> must fall through the skip check and
    # update rather than skip.
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "USD"}],
    )
    assert repo.fetch_currency("AAA.LSE", provider="EODHD") == "USD"


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
    security_repo = SecurityRepository(db_path)
    aaa = security_repo.ensure("AAA", "LSE", currency="GBP")
    bbb = security_repo.ensure("BBB", "LSE", currency="GBP")

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
                security_id=aaa.security_id,
                symbol="AAA.LSE",
                as_of="2025-01-02",
                price=10.0,
                volume=100,
                currency="GBP",
            ),
            MarketDataUpdate(
                security_id=aaa.security_id,
                symbol="AAA.LSE",
                as_of="2025-01-03",
                price=11.0,
                volume=120,
                currency="GBP",
            ),
            MarketDataUpdate(
                security_id=bbb.security_id,
                symbol="BBB.LSE",
                as_of="2025-01-02",
                price=20.0,
                volume=200,
                currency="GBP",
            ),
        ]
    )

    # Single metric read: the seeded value is returned; unknown metric is None.
    aaa_market_cap = metrics_repo.fetch_by_id(aaa.security_id, "market_cap")
    assert aaa_market_cap is not None
    assert aaa_market_cap.value == 1000.0
    assert metrics_repo.fetch_by_id(aaa.security_id, "missing") is None

    # Bulk metric read: the id-keyed map carries the seeded records, keyed by id.
    by_id = metrics_repo.fetch_many_by_ids(
        [aaa.security_id, bbb.security_id], ["market_cap", "current_ratio"]
    )
    assert by_id[aaa.security_id]["market_cap"].value == 1000.0
    assert by_id[aaa.security_id]["current_ratio"].value == 1.5
    assert by_id[bbb.security_id]["market_cap"].value == 2000.0

    # Latest snapshot: picks the most-recent as_of and rebuilds the canonical
    # symbol from ``listing ⋈ exchange``.
    rec_by_id = market_repo.latest_snapshot_record_by_id(aaa.security_id)
    assert rec_by_id is not None
    assert rec_by_id.as_of == "2025-01-03"
    assert rec_by_id.symbol == "AAA.LSE"
    assert rec_by_id.security_id == aaa.security_id

    snaps_by_id = market_repo.latest_snapshots_many_by_ids(
        [aaa.security_id, bbb.security_id]
    )
    assert snaps_by_id[aaa.security_id].as_of == "2025-01-03"
    assert snaps_by_id[bbb.security_id].as_of == "2025-01-02"

    # Listing currency: id form == symbol form (the symbol form is a kept edge).
    assert market_repo.ticker_currency_by_id(
        aaa.security_id
    ) == market_repo.ticker_currency("AAA.LSE")
    assert market_repo.ticker_currency_by_id(aaa.security_id) == "GBP"
