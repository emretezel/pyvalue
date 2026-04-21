import sqlite3
from datetime import date, timedelta

import pyvalue.storage as storage
from pyvalue.storage import (
    EntityMetadataRepository,
    ExchangeProviderRepository,
    ExchangeRepository,
    FXRateRecord,
    FXRatesRepository,
    FinancialFactsRefreshStateRepository,
    FundamentalsNormalizationStateRepository,
    FundamentalsUpdate,
    FundamentalsRepository,
    FundamentalsNormalizationCandidate,
    FinancialFactsRepository,
    FactRecord,
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
from pyvalue.marketdata import MarketDataUpdate
from pyvalue.marketdata.service import latest_share_count
from pyvalue.universe import Listing


def _listing(symbol: str, is_etf: bool = False) -> Listing:
    """Helper to instantiate listings in a compact way.

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
        currency=None,
    )


def test_supported_ticker_repository_replace_from_listings_persists_rows(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    inserted = repo.replace_from_listings(
        "SEC",
        "US",
        [_listing("AAA"), _listing("BBB", is_etf=True)],
    )

    assert inserted == 2

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            """
            SELECT p.provider_code, px.provider_exchange_code, pl.provider_symbol,
                   e.exchange_code, pl.currency
            FROM provider_listing pl
            JOIN provider p ON p.provider_id = pl.provider_id
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY pl.provider_symbol
            """
        ).fetchall()
        provider_listing_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(provider_listing)")
        }

    assert rows == [
        ("SEC", "US", "AAA", "US", None),
        ("SEC", "US", "BBB", "US", None),
    ]
    assert "security_type" not in provider_listing_columns


def test_supported_ticker_repository_replace_from_listings_overwrites_exchange_slice(
    tmp_path,
):
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    repo.replace_from_listings("SEC", "US", [_listing("AAA")])
    repo.replace_from_listings("SEC", "US", [_listing("CCC")])

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            "SELECT provider_symbol FROM supported_tickers ORDER BY provider_symbol"
        ).fetchall()

    assert rows == [("CCC.US",)]


def test_supported_ticker_repository_list_symbols_initializes_schema(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "universe.db")

    assert repo.list_symbols_by_exchange("SEC", "US") == []


def test_supported_ticker_repository_normalizes_exchange_and_fetches_currency(tmp_path):
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
    repo.replace_from_listings("EODHD", "LSE", [listing])

    assert repo.list_symbols_by_exchange("EODHD", "LSE") == ["FOO.LSE"]
    assert repo.list_symbols_by_exchange("eodhd", "lse") == ["FOO.LSE"]
    assert repo.fetch_currency("FOO.LSE", provider="EODHD") == "GBP"


def test_fundamentals_repository_normalizes_provider(tmp_path):
    repo = FundamentalsRepository(tmp_path / "funds.db")
    repo.initialize_schema()
    repo.upsert("eodhd", "FOO.LSE", payload={"bar": 1})

    assert repo.symbols("EODHD") == ["FOO.LSE"]
    assert repo.symbols("eodhd") == ["FOO.LSE"]


def test_fundamentals_repository_classifies_and_purges_secondary_listings(tmp_path):
    db_path = tmp_path / "listing-status.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB plc", "Type": "Common Stock"},
        ],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="GBP",
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
    MetricsRepository(db_path).upsert(
        "AAA.LSE",
        "market_cap",
        1000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )
    MetricComputeStatusRepository(db_path).upsert_many(
        [
            MetricComputeStatusRecord(
                symbol="AAA.LSE",
                metric_id="market_cap",
                status="success",
                attempted_at="2025-01-02T00:00:00+00:00",
                value_as_of="2025-01-02",
            )
        ]
    )
    FundamentalsNormalizationStateRepository(db_path).mark_success(
        "EODHD",
        "AAA.LSE",
        by_symbol["AAA.LSE"].security_id,
        "2025-01-01T00:00:00+00:00",
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
    assert reconciled == []

    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT provider_symbol, is_primary_listing, primary_provider_symbol, classification_basis
            FROM security_listing_status
            ORDER BY provider_symbol
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
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]
        market_state_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM market_data_fetch_state
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]

    assert statuses == [
        ("AAA.LSE", 0, "AAA.US", "different_primary_ticker"),
        ("AAA.US", 1, "AAA.US", "matched_primary_ticker"),
        ("BBB.LSE", 1, None, "missing_primary_ticker"),
    ]
    assert fact_rows == 0
    assert refresh_rows == 0
    assert market_rows == 0
    assert metric_rows == 0
    assert status_rows == 0
    assert normalization_rows == 0
    assert market_state_rows == 0


def test_exchange_provider_repository_replaces_rows_per_provider(tmp_path):
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


def test_exchange_provider_repository_fetch_normalizes_code(tmp_path):
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


def test_supported_ticker_repository_replaces_rows_per_exchange(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "supported-tickers.db")
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BRK.B", "Name": "Share Class", "Type": "Preferred Stock"},
        ],
    )
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "CCC", "Name": "CCC Inc", "Type": "Common Stock"}],
    )

    inserted = repo.replace_for_exchange(
        "eodhd",
        "lse",
        [{"Code": "AAA", "Name": "AAA plc refreshed", "Type": "Common Stock"}],
    )

    assert inserted == 1
    lse = repo.list_for_exchange("EODHD", "LSE")
    us = repo.list_for_exchange("EODHD", "US")
    assert [(row.symbol, row.security_name) for row in lse] == [
        ("AAA.LSE", "AAA plc refreshed")
    ]
    assert [(row.symbol, row.security_name) for row in us] == [("CCC.US", "CCC Inc")]

    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "BRK.B", "Name": "Berkshire B", "Type": "Common Stock"}],
    )

    us = repo.list_for_exchange("EODHD", "US")
    assert [(row.symbol, row.code) for row in us] == [("BRK.B.US", "BRK.B")]


def test_supported_ticker_repository_lists_eligible_symbols(tmp_path):
    db_path = tmp_path / "supported-tickers.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB plc", "Type": "Preferred Stock"},
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


def test_financial_facts_repository_replace_fact_rows_matches_replace_facts(tmp_path):
    db_path = tmp_path / "financial-facts.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    inserted = repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=100.0,
                currency="USD",
            )
        ],
    )

    assert inserted == 1

    replaced = repo.replace_fact_rows(
        "AAA.US",
        [
            (
                None,
                "Liabilities",
                "FY",
                "2024-12-31",
                "USD",
                40.0,
                None,
                None,
                None,
                None,
                None,
                "USD",
            )
        ],
    )

    assert replaced == 1

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
                SELECT s.canonical_symbol, ff.concept, ff.value, ff.source_provider
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.listing_id
                ORDER BY ff.concept
                """
        ).fetchall()

    assert rows == [("AAA.US", "Liabilities", 40.0, None)]


def test_financial_facts_repository_replace_facts_updates_refresh_state(tmp_path):
    db_path = tmp_path / "facts-refresh-state.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                end_date="2024-12-31",
                unit="USD",
                value=10.0,
            )
        ],
    )

    refresh_repo = FinancialFactsRefreshStateRepository(db_path)
    refresh_record = refresh_repo.fetch("AAA.US")

    assert refresh_record is not None
    assert refresh_record.symbol == "AAA.US"
    assert refresh_record.refreshed_at


def test_financial_facts_repository_replace_fact_rows_persists_source_provider(
    tmp_path,
):
    db_path = tmp_path / "financial-facts-source-provider.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    inserted = repo.replace_fact_rows(
        "AAA.US",
        [
            (
                None,
                "Assets",
                "FY",
                "2024-12-31",
                "USD",
                100.0,
                None,
                None,
                None,
                None,
                None,
                "USD",
            )
        ],
        source_provider="SEC",
    )

    assert inserted == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
                SELECT ff.source_provider
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.listing_id
                WHERE s.canonical_symbol = 'AAA.US'
                """
        ).fetchone()

    assert row == ("SEC",)


def test_fundamentals_repository_normalization_candidates_match_state_and_facts(
    tmp_path,
):
    db_path = tmp_path / "normalization-candidates.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    state_repo.initialize_schema()

    fund_repo.upsert("SEC", "AAA.US", {"entityName": "AAA", "facts": {}})
    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB", "facts": {}})
    fund_repo.upsert("SEC", "CCC.US", {"entityName": "CCC", "facts": {}})

    aaa_security_id = (
        fund_repo._security_repo().ensure_from_symbol("AAA.US").security_id
    )
    bbb_security_id = (
        fund_repo._security_repo().ensure_from_symbol("BBB.US").security_id
    )

    aaa_record = fund_repo.fetch_payload_with_fetched_at("SEC", "AAA.US")
    assert aaa_record is not None
    _, aaa_fetched_at = aaa_record
    bbb_record = fund_repo.fetch_payload_with_fetched_at("SEC", "BBB.US")
    assert bbb_record is not None
    _, bbb_fetched_at = bbb_record

    state_repo.mark_success("SEC", "AAA.US", aaa_security_id, aaa_fetched_at)
    state_repo.mark_success("SEC", "BBB.US", bbb_security_id, bbb_fetched_at)
    fact_repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=100.0,
            )
        ],
        source_provider="SEC",
    )
    fact_repo.replace_facts(
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=200.0,
            )
        ],
        source_provider="EODHD",
    )

    candidates = fund_repo.normalization_candidates(
        "SEC",
        ["AAA.US", "BBB.US", "CCC.US"],
    )

    assert candidates["AAA.US"] == FundamentalsNormalizationCandidate(
        provider_symbol="AAA.US",
        security_id=aaa_security_id,
        raw_fetched_at=aaa_fetched_at,
        normalized_raw_fetched_at=aaa_fetched_at,
        last_normalized_at=candidates["AAA.US"].last_normalized_at,
        current_source_provider="SEC",
    )
    assert candidates["BBB.US"].current_source_provider == "EODHD"
    assert candidates["BBB.US"].normalized_raw_fetched_at == bbb_fetched_at
    assert candidates["CCC.US"].normalized_raw_fetched_at is None


def test_fundamentals_repository_normalization_candidates_skip_facts_scan_without_state(
    tmp_path,
):
    db_path = tmp_path / "normalization-candidates-no-state.db"

    class _LoggingConnection:
        def __init__(self, conn, seen_sql):
            self._conn = conn
            self._seen_sql = seen_sql

        def __enter__(self):
            self._conn.__enter__()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return self._conn.__exit__(exc_type, exc_value, traceback)

        def execute(self, sql, parameters=()):
            self._seen_sql.append(" ".join(str(sql).split()))
            return self._conn.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    class LoggingFundamentalsRepository(FundamentalsRepository):
        def __init__(self, db_path, seen_sql):
            super().__init__(db_path)
            self._seen_sql = seen_sql

        def _connect(self):
            return _LoggingConnection(super()._connect(), self._seen_sql)

    seen_sql = []
    fund_repo = LoggingFundamentalsRepository(db_path, seen_sql)
    fund_repo.initialize_schema()
    fund_repo.upsert("EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US")
    fund_repo.upsert("EODHD", "BBB.US", {"General": {"Name": "BBB"}}, exchange="US")

    candidates = fund_repo.normalization_candidates("EODHD", ["AAA.US", "BBB.US"])

    assert sorted(candidates) == ["AAA.US", "BBB.US"]
    assert all(
        candidate.normalized_raw_fetched_at is None for candidate in candidates.values()
    )
    assert not any("FROM financial_facts" in sql for sql in seen_sql)


def test_financial_facts_repository_replace_fact_rows_replaces_symbol_slice(tmp_path):
    db_path = tmp_path / "financial-facts-replace.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()

    repo.replace_fact_rows(
        "AAA.US",
        [
            (
                None,
                "Assets",
                "FY",
                "2024-12-31",
                "USD",
                100.0,
                None,
                None,
                None,
                None,
                None,
                "USD",
            ),
            (
                None,
                "Liabilities",
                "FY",
                "2024-12-31",
                "USD",
                55.0,
                None,
                None,
                None,
                None,
                None,
                "USD",
            ),
        ],
    )

    repo.replace_fact_rows(
        "AAA.US",
        [
            (
                None,
                "StockholdersEquity",
                "FY",
                "2024-12-31",
                "USD",
                45.0,
                None,
                None,
                None,
                None,
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
    monkeypatch, tmp_path
):
    repo = FinancialFactsRepository(tmp_path / "locked-index.db")
    monkeypatch.setattr(storage, "apply_migrations", lambda db_path: None)
    monkeypatch.setattr(repo._security_repo(), "initialize_schema", lambda: None)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, params=()):
            if "idx_fin_facts_security_concept_latest" in sql:
                raise sqlite3.OperationalError("database is locked")
            return None

    monkeypatch.setattr(repo, "_connect", lambda: FakeConn())

    repo.initialize_schema()


def test_fundamentals_repository_upsert_marks_fetch_state_success(tmp_path):
    db_path = tmp_path / "fundamentals-fetch-state.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()

    repo.upsert("EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT last_fetched_at, last_status, attempts
            FROM fundamentals_fetch_state
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """
        ).fetchone()

    assert row is not None
    assert row[0] is not None
    assert row[1] == "ok"
    assert row[2] == 0


def test_fundamentals_repository_upsert_many_uses_resolved_metadata_and_overwrites(
    tmp_path,
):
    db_path = tmp_path / "fundamentals-batch.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
        ],
    )
    tickers = {row.symbol: row for row in ticker_repo.list_for_exchange("EODHD", "US")}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    original_resolve_security = repo._resolve_security
    repo._resolve_security = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("upsert_many should not resolve securities per symbol")
    )

    repo.upsert_many(
        "EODHD",
        [
            FundamentalsUpdate(
                security_id=tickers["AAA.US"].security_id,
                provider_symbol="AAA.US",
                provider_exchange_code="US",
                currency="USD",
                data='{"General": {"CurrencyCode": "USD", "Name": "AAA"}}',
                fetched_at="2026-03-30T00:00:00+00:00",
            ),
            FundamentalsUpdate(
                security_id=tickers["BBB.US"].security_id,
                provider_symbol="BBB.US",
                provider_exchange_code="US",
                currency="USD",
                data='{"General": {"CurrencyCode": "USD", "Name": "BBB"}}',
                fetched_at="2026-03-30T00:00:00+00:00",
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
                currency="USD",
                data='{"General": {"CurrencyCode": "USD", "Name": "AAA Updated"}}',
                fetched_at="2026-03-31T00:00:00+00:00",
            )
        ],
    )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT provider_symbol, security_id, provider_exchange_code, currency, fetched_at
            FROM fundamentals_raw
            ORDER BY provider_symbol
            """
        ).fetchall()

    assert rows == [
        (
            "AAA.US",
            tickers["AAA.US"].security_id,
            "US",
            "USD",
            "2026-03-31T00:00:00+00:00",
        ),
        (
            "BBB.US",
            tickers["BBB.US"].security_id,
            "US",
            "USD",
            "2026-03-30T00:00:00+00:00",
        ),
    ]
    repo._resolve_security = original_resolve_security
    assert repo.fetch("EODHD", "AAA.US")["General"]["Name"] == "AAA Updated"
    assert repo.fetch("EODHD", "BBB.US")["General"]["Name"] == "BBB"


def test_supported_ticker_repository_lists_market_data_symbols_missing_then_oldest(
    tmp_path,
):
    db_path = tmp_path / "supported-market-data.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
            {"Code": "CCC", "Name": "CCC Inc", "Type": "Common Stock"},
            {"Code": "DDD", "Name": "DDD Inc", "Type": "Common Stock"},
        ],
    )

    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price(
        "BBB.US", (date.today() - timedelta(days=1)).isoformat(), 10.0
    )
    market_repo.upsert_price(
        "CCC.US", (date.today() - timedelta(days=30)).isoformat(), 10.0
    )
    market_repo.upsert_price(
        "DDD.US", (date.today() - timedelta(days=12)).isoformat(), 10.0
    )

    rows = repo.list_eligible_for_market_data(
        "EODHD",
        exchange_codes=["US"],
        max_age_days=7,
    )

    assert [row.symbol for row in rows] == ["AAA.US", "CCC.US", "DDD.US"]


def test_supported_ticker_repository_primary_only_filters_secondary_listings(
    tmp_path,
):
    db_path = tmp_path / "supported-primary-only.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB plc", "Type": "Common Stock"},
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
    assert ticker_repo.list_canonical_symbols(primary_only=True) == [
        "AAA.US",
        "BBB.LSE",
    ]


def test_security_listing_status_repository_lists_missing_provider_symbols(tmp_path):
    db_path = tmp_path / "missing-listing-status.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB plc", "Type": "Common Stock"},
        ],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}

    status_repo = SecurityListingStatusRepository(db_path)
    status_repo.initialize_schema()
    status_repo.upsert_many(
        [
            SecurityListingStatusRecord(
                security_id=by_symbol["AAA.US"].security_id,
                source_provider="EODHD",
                provider_symbol="AAA.US",
                raw_fetched_at="2025-01-01T00:00:00+00:00",
                is_primary_listing=True,
                primary_provider_symbol="AAA.US",
                classification_basis="matched_primary_ticker",
            ),
            SecurityListingStatusRecord(
                security_id=by_symbol["BBB.LSE"].security_id,
                source_provider="EODHD",
                provider_symbol="BBB.LSE",
                raw_fetched_at="2025-01-01T00:00:00+00:00",
                is_primary_listing=True,
                primary_provider_symbol=None,
                classification_basis="missing_primary_ticker",
            ),
        ]
    )

    assert status_repo.list_missing_eodhd_provider_symbols() == ["AAA.LSE"]
    assert status_repo.list_missing_eodhd_provider_symbols(
        exchange_codes=["LSE"],
    ) == ["AAA.LSE"]
    assert (
        status_repo.list_missing_eodhd_provider_symbols(
            exchange_codes=["US"],
        )
        == []
    )
    assert status_repo.list_missing_eodhd_provider_symbols(
        provider_symbols=["AAA.LSE", "BBB.LSE"],
    ) == ["AAA.LSE"]


def test_market_data_fetch_state_repository_tracks_success_and_failure(tmp_path):
    db_path = tmp_path / "market-state.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"}],
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


def test_market_data_repository_upsert_prices_batches_rows(tmp_path):
    db_path = tmp_path / "market-data-batch.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
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

    assert repo.latest_snapshot("AAA.US").price == 10.0
    assert repo.latest_snapshot("BBB.US").price == 20.0


def test_market_data_fetch_state_repository_batch_methods(tmp_path):
    db_path = tmp_path / "market-state-batch.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
        ],
    )
    repo = MarketDataFetchStateRepository(db_path)
    repo.initialize_schema()

    repo.mark_failure_many(
        "EODHD",
        [("AAA.US", "boom"), ("BBB.US", "bang")],
        base_backoff_seconds=60,
    )
    assert repo.fetch("EODHD", "AAA.US")["last_status"] == "error"
    assert repo.fetch("EODHD", "BBB.US")["attempts"] == 1

    repo.mark_success_many("EODHD", ["AAA.US", "BBB.US"])
    assert repo.fetch("EODHD", "AAA.US")["last_status"] == "ok"
    assert repo.fetch("EODHD", "BBB.US")["attempts"] == 0


def test_market_data_repository_latest_snapshots_many_matches_single_lookup(tmp_path):
    db_path = tmp_path / "market-snapshots-many.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
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
                as_of="2026-03-28",
                price=9.0,
                volume=90,
                currency="USD",
                market_cap=900.0,
            ),
            MarketDataUpdate(
                security_id=by_symbol["AAA.US"].security_id,
                symbol="AAA.US",
                as_of="2026-03-29",
                price=10.0,
                volume=100,
                currency="USD",
                market_cap=1000.0,
            ),
            MarketDataUpdate(
                security_id=by_symbol["BBB.US"].security_id,
                symbol="BBB.US",
                as_of="2026-03-29",
                price=20.0,
                volume=200,
                currency="USD",
                market_cap=2000.0,
            ),
        ]
    )

    snapshots = repo.latest_snapshots_many(["AAA.US", "BBB.US", "CCC.US"])

    assert set(snapshots) == {"AAA.US", "BBB.US"}
    assert snapshots["AAA.US"].security_id == by_symbol["AAA.US"].security_id
    assert snapshots["AAA.US"].as_of == repo.latest_snapshot("AAA.US").as_of
    assert snapshots["AAA.US"].price == repo.latest_snapshot("AAA.US").price
    assert snapshots["AAA.US"].market_cap == repo.latest_snapshot("AAA.US").market_cap
    assert snapshots["BBB.US"].as_of == repo.latest_snapshot("BBB.US").as_of
    assert snapshots["BBB.US"].price == repo.latest_snapshot("BBB.US").price


def test_financial_facts_repository_latest_share_counts_many_matches_single_lookup(
    tmp_path,
):
    db_path = tmp_path / "share-counts-many.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="EntityCommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="shares",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="shares",
                value=222.0,
            ),
        ],
    )
    repo.replace_facts(
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="shares",
                value=300.0,
            ),
            FactRecord(
                symbol="BBB.US",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="shares",
                value=333.0,
            ),
        ],
    )

    counts = repo.latest_share_counts_many(["AAA.US", "BBB.US", "CCC.US"])
    security_ids = repo._security_repo().resolve_ids_many(
        ["AAA.US", "BBB.US", "CCC.US"]
    )
    counts_with_security_ids = repo.latest_share_counts_many(
        ["AAA.US", "BBB.US", "CCC.US"],
        security_ids_by_symbol=security_ids,
    )

    assert counts["AAA.US"] == latest_share_count("AAA.US", repo)
    assert counts["BBB.US"] == latest_share_count("BBB.US", repo)
    assert counts["AAA.US"] == 222.0
    assert counts["BBB.US"] == 333.0
    assert counts_with_security_ids == counts
    assert "CCC.US" not in counts


def test_financial_facts_repository_facts_for_symbols_many_matches_single_lookup(
    tmp_path,
):
    db_path = tmp_path / "facts-many.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="USD",
                value=101.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=11.0,
            ),
        ],
    )
    repo.replace_facts(
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=222.0,
            ),
            FactRecord(
                symbol="BBB.US",
                concept="Revenue",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=333.0,
            ),
        ],
    )

    facts = repo.facts_for_symbols_many(["AAA.US", "BBB.US", "CCC.US"], chunk_size=1)

    assert facts["AAA.US"] == repo.facts_for_symbol("AAA.US")
    assert facts["BBB.US"] == repo.facts_for_symbol("BBB.US")
    assert "CCC.US" not in facts


def test_financial_facts_repository_facts_for_symbols_many_concept_filter(tmp_path):
    """A non-empty ``concepts`` argument restricts the preload to that subset."""

    db_path = tmp_path / "facts-many-concepts.db"
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=111.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=11.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="Revenues",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit="USD",
                value=999.0,
            ),
        ],
    )

    filtered = repo.facts_for_symbols_many(
        ["AAA.US"],
        concepts=["AssetsCurrent", "LiabilitiesCurrent"],
    )
    concepts = {record.concept for record in filtered["AAA.US"]}
    assert concepts == {"AssetsCurrent", "LiabilitiesCurrent"}

    # Empty concepts list short-circuits to the unfiltered query.
    unfiltered = repo.facts_for_symbols_many(["AAA.US"], concepts=[])
    assert {r.concept for r in unfiltered["AAA.US"]} == {
        "AssetsCurrent",
        "LiabilitiesCurrent",
        "Revenues",
    }


def test_metrics_repository_upsert_many_with_external_connection(monkeypatch, tmp_path):
    """When a connection is supplied the persistence path reuses it."""

    from pyvalue.storage import StoredMetricRow

    db_path = tmp_path / "metrics-external-conn.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    # Pre-create the canonical securities so resolve_ids_many doesn't fall
    # through to ensure_from_symbol (which always opens its own connection).
    sec_repo = repo._security_repo()
    sec_repo.ensure_from_symbol("AAA.US", entity_name="AAA Corp")
    sec_repo.ensure_from_symbol("BBB.US", entity_name="BBB Corp")

    rows: list[StoredMetricRow] = [
        ("AAA.US", "dummy_metric", 1.0, "2024-01-01", "monetary", "USD", None),
        ("BBB.US", "dummy_metric", 2.0, "2024-01-01", "monetary", "USD", None),
    ]

    # Stub initialize_schema on both repos -- the table+migrations are already
    # in place from the warm-up above, so any further _connect() opens during
    # upsert_many can only come from the persistence path itself.
    monkeypatch.setattr(repo, "initialize_schema", lambda: None)
    monkeypatch.setattr(sec_repo, "initialize_schema", lambda: None)

    write_conn = repo.open_persistent_connection()
    try:
        opened: list[int] = []
        original_connect = repo._connect

        def tracking_connect(*args, **kwargs):
            opened.append(1)
            return original_connect(*args, **kwargs)

        original_sec_connect = sec_repo._connect
        sec_opened: list[int] = []

        def tracking_sec_connect(*args, **kwargs):
            sec_opened.append(1)
            return original_sec_connect(*args, **kwargs)

        monkeypatch.setattr(repo, "_connect", tracking_connect)
        monkeypatch.setattr(sec_repo, "_connect", tracking_sec_connect)
        persisted = repo.upsert_many(rows, connection=write_conn)
    finally:
        write_conn.close()

    assert persisted == len(rows)
    # With initialize_schema stubbed out, neither the upsert nor the resolve
    # path should have opened any new connection -- both share write_conn.
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


def test_sqlite_store_connect_applies_performance_pragmas(tmp_path):
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


def test_migration_029_creates_fin_facts_security_concept_latest_index(tmp_path):
    """Migration 029 ensures the composite preload index exists on existing DBs."""

    from pyvalue.migrations import apply_migrations

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

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name='idx_fin_facts_security_concept_latest'"
        ).fetchone()
    assert after is not None


def test_market_data_repository_update_market_caps_many_matches_single_update(tmp_path):
    db_path = tmp_path / "market-cap-updates-many.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
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
                as_of="2026-03-28",
                price=9.0,
                volume=90,
                currency="USD",
                market_cap=900.0,
            ),
            MarketDataUpdate(
                security_id=by_symbol["AAA.US"].security_id,
                symbol="AAA.US",
                as_of="2026-03-29",
                price=10.0,
                volume=100,
                currency="USD",
                market_cap=1000.0,
            ),
            MarketDataUpdate(
                security_id=by_symbol["BBB.US"].security_id,
                symbol="BBB.US",
                as_of="2026-03-29",
                price=20.0,
                volume=200,
                currency="USD",
                market_cap=2000.0,
            ),
        ]
    )

    updated = repo.update_market_caps_many(
        [
            (by_symbol["AAA.US"].security_id, "2026-03-29", 1500.0),
            (by_symbol["BBB.US"].security_id, "2026-03-29", 2500.0),
        ]
    )

    assert updated == 2
    assert repo.latest_snapshot("AAA.US").market_cap == 1500.0
    assert repo.latest_snapshot("BBB.US").market_cap == 2500.0
    with sqlite3.connect(db_path) as conn:
        historical = conn.execute(
            """
                SELECT market_cap
                FROM market_data md
                JOIN securities s ON s.security_id = md.listing_id
                WHERE s.canonical_symbol = 'AAA.US' AND md.as_of = '2026-03-28'
                """
        ).fetchone()[0]
    assert historical == 900.0


def test_latest_share_counts_many_prefers_best_same_date_share_fact(tmp_path):
    db_path = tmp_path / "share-count-selection.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_from_listings("EODHD", "US", [_listing("AAA")])

    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                cik=None,
                concept="EntityCommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit="USD",
                value=1_000_000.0,
                accn=None,
                filed="2026-03-27",
                frame="CY2025",
                start_date=None,
                accounting_standard=None,
                currency="USD",
            ),
            FactRecord(
                symbol="AAA.US",
                cik=None,
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit="shares",
                value=1_000.0,
                accn=None,
                filed=None,
                frame="CY2025",
                start_date=None,
                accounting_standard=None,
                currency=None,
            ),
        ],
    )

    counts = repo.latest_share_counts_many(["AAA.US"])

    assert counts == {"AAA.US": 1000.0}
    assert latest_share_count("AAA.US", repo) == 1000.0


def test_sqlite_store_connect_context_closes_connection(tmp_path):
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


def test_sqlite_store_enable_wal_mode(tmp_path):
    repo = MarketDataRepository(tmp_path / "wal-mode.db")
    repo.initialize_schema()

    mode = repo.enable_wal_mode()

    assert mode == "wal"
    assert repo.current_journal_mode() == "wal"


def test_metrics_repository_upsert_many_matches_single_upsert(tmp_path):
    db_path = tmp_path / "metrics-upsert-many.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()

    repo.upsert("AAA.US", "metric_one", 1.0, "2024-01-01")
    updated = repo.upsert_many(
        [
            ("AAA.US", "metric_one", 2.0, "2024-02-01"),
            ("AAA.US", "metric_two", 3.0, "2024-02-01"),
            ("BBB.US", "metric_one", 4.0, "2024-02-01"),
        ]
    )

    assert updated == 3
    assert repo.fetch("AAA.US", "metric_one") == (2.0, "2024-02-01")
    assert repo.fetch("AAA.US", "metric_two") == (3.0, "2024-02-01")
    assert repo.fetch("BBB.US", "metric_one") == (4.0, "2024-02-01")


def test_metrics_repository_upsert_many_retries_transient_locked_error(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "metrics-upsert-retry.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    monkeypatch.setattr(repo, "initialize_schema", lambda: None)

    original_connect = repo._connect
    attempts = {"count": 0}

    class LockedOnceConnection:
        def __enter__(self):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            self._conn = original_connect()
            return self._conn.__enter__()

        def __exit__(self, exc_type, exc_value, traceback):
            return self._conn.__exit__(exc_type, exc_value, traceback)

    monkeypatch.setattr(repo, "_connect", lambda: LockedOnceConnection())

    updated = repo.upsert_many([("AAA.US", "metric_one", 2.0, "2024-02-01")])

    assert attempts["count"] == 2
    assert updated == 1
    assert repo.fetch("AAA.US", "metric_one") == (2.0, "2024-02-01")


def test_metrics_repository_fetch_many_for_symbols_returns_requested_metrics(
    tmp_path,
):
    db_path = tmp_path / "metrics-fetch-many.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(
        [
            ("AAA.US", "metric_one", 1.0, "2024-01-01"),
            ("AAA.US", "metric_two", 2.0, "2024-01-02"),
            ("BBB.US", "metric_one", 3.0, "2024-01-03"),
            ("CCC.US", "metric_three", 4.0, "2024-01-04"),
        ]
    )

    fetched = repo.fetch_many_for_symbols(
        ["AAA.US", "BBB.US", "CCC.US", "DDD.US"],
        ["metric_one", "metric_two"],
        chunk_size=1,
    )

    assert fetched == {
        "AAA.US": {
            "metric_one": (1.0, "2024-01-01"),
            "metric_two": (2.0, "2024-01-02"),
        },
        "BBB.US": {
            "metric_one": (3.0, "2024-01-03"),
        },
    }


def test_metric_compute_status_repository_upsert_and_fetch_many(tmp_path):
    db_path = tmp_path / "metric-status.db"
    repo = MetricComputeStatusRepository(db_path)
    repo.initialize_schema()

    updated = repo.upsert_many(
        [
            MetricComputeStatusRecord(
                symbol="AAA.US",
                metric_id="metric_one",
                status="success",
                attempted_at="2024-01-02T00:00:00+00:00",
                value_as_of="2024-01-01",
            ),
            MetricComputeStatusRecord(
                symbol="BBB.US",
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
    single = repo.fetch("BBB.US", "metric_one")
    assert single is not None
    assert single.status == "failure"
    assert single.reason_code == "missing_data"

    fetched = repo.fetch_many_for_symbols(
        ["AAA.US", "BBB.US"],
        ["metric_one"],
        chunk_size=1,
    )

    assert fetched["AAA.US"]["metric_one"].status == "success"
    assert fetched["AAA.US"]["metric_one"].value_as_of == "2024-01-01"
    assert fetched["BBB.US"]["metric_one"].reason_detail == "Need more facts"


def test_metrics_repository_persists_unit_metadata(tmp_path):
    db_path = tmp_path / "metrics-metadata.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()

    repo.upsert(
        "AAA.US",
        "market_cap",
        100.0,
        "2024-01-01",
        unit_kind="monetary",
        currency="GBX",
        unit_label="money",
    )
    repo.upsert(
        "AAA.US",
        "earnings_yield",
        0.08,
        "2024-01-01",
        unit_kind="percent",
        currency="USD",
        unit_label="pct",
    )

    market_cap = repo.fetch("AAA.US", "market_cap")
    earnings_yield = repo.fetch("AAA.US", "earnings_yield")

    assert market_cap is not None
    assert market_cap.unit_kind == "monetary"
    assert market_cap.currency == "GBP"
    assert market_cap.unit_label == "money"
    assert earnings_yield is not None
    assert earnings_yield.unit_kind == "percent"
    assert earnings_yield.currency is None
    assert earnings_yield.unit_label == "pct"


def test_metrics_repository_normalizes_configured_subunit_currencies(tmp_path):
    db_path = tmp_path / "metrics-subunits.db"
    repo = MetricsRepository(db_path)
    repo.initialize_schema()

    repo.upsert(
        "AAA.JSE",
        "market_cap",
        237.5,
        "2024-01-01",
        unit_kind="monetary",
        currency="ZAC",
    )
    repo.upsert(
        "BBB.TA",
        "eps_ttm",
        12.34,
        "2024-01-01",
        unit_kind="per_share",
        currency="ILA",
    )

    market_cap = repo.fetch("AAA.JSE", "market_cap")
    eps_ttm = repo.fetch("BBB.TA", "eps_ttm")

    assert market_cap is not None
    assert market_cap.currency == "ZAR"
    assert eps_ttm is not None
    assert eps_ttm.currency == "ILS"


def test_fx_rates_repository_latest_on_or_before_and_discover_currencies(tmp_path):
    db_path = tmp_path / "fx-repo.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit="GBX",
                value=1000.0,
                currency="GBX",
            ),
            FactRecord(
                symbol="BBB.JSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit="ZAC",
                value=1000.0,
                currency="ZAC",
            ),
            FactRecord(
                symbol="CCC.TA",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit="ILA",
                value=1000.0,
                currency="ILA",
            ),
        ],
    )
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(
        [
            FXRateRecord(
                provider="FRANKFURTER",
                rate_date="2024-01-01",
                base_currency="USD",
                quote_currency="EUR",
                rate_text="0.8",
                fetched_at="2024-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="FRANKFURTER",
                rate_date="2024-01-10",
                base_currency="USD",
                quote_currency="EUR",
                rate_text="0.9",
                fetched_at="2024-01-10T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )

    record = repo.latest_on_or_before("FRANKFURTER", "USD", "EUR", "2024-01-05")

    assert record is not None
    assert record.rate_date == "2024-01-01"
    assert repo.discover_currencies() == ["GBP", "ILS", "ZAR"]
    assert (
        repo.fully_covered_quotes_for_window(
            "FRANKFURTER",
            "USD",
            ["EUR", "GBP"],
            date(2024, 1, 1),
            date(2024, 1, 10),
        )
        == set()
    )
    assert repo.fully_covered_quotes_for_window(
        "FRANKFURTER",
        "USD",
        ["EUR", "GBP"],
        date(2024, 1, 1),
        date(2024, 1, 1),
    ) == {"EUR"}


def test_fx_rates_repository_discover_currencies_excludes_secondary_supported_tickers(
    tmp_path,
):
    db_path = tmp_path / "fx-secondary-supported.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"}],
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

    assert repo.discover_currencies() == ["ZAR"]


def test_security_repository_upserts_sector_and_industry_metadata(tmp_path):
    repo = SecurityRepository(tmp_path / "security-metadata.db")
    repo.initialize_schema()

    repo.upsert_metadata(
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


def test_entity_metadata_repository_fetch_many_returns_security_records(tmp_path):
    repo = EntityMetadataRepository(tmp_path / "entity-metadata.db")
    repo.initialize_schema()
    repo.upsert("AAA.US", sector="Technology", industry="Software")
    repo.upsert("BBB.US", sector="Industrials", industry="Machinery")

    rows = repo.fetch_many(["AAA.US", "BBB.US"])

    assert rows["AAA.US"].sector == "Technology"
    assert rows["AAA.US"].industry == "Software"
    assert rows["BBB.US"].sector == "Industrials"
    assert rows["BBB.US"].industry == "Machinery"


def test_security_repository_upsert_metadata_many_updates_existing_rows(tmp_path):
    repo = SecurityRepository(tmp_path / "security-metadata-batch.db")
    repo.initialize_schema()
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


def test_fundamentals_repository_fetch_metadata_candidates_extracts_fields(tmp_path):
    db_path = tmp_path / "fundamentals-metadata-candidates.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
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


def test_fundamentals_repository_fetch_many_returns_payloads_by_symbol(tmp_path):
    db_path = tmp_path / "fundamentals-fetch-many.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
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
