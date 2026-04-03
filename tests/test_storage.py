import sqlite3
from datetime import date, timedelta

import pyvalue.storage as storage
from pyvalue.storage import (
    EntityMetadataRepository,
    FundamentalsNormalizationStateRepository,
    FundamentalsUpdate,
    FundamentalsRepository,
    FundamentalsNormalizationCandidate,
    FinancialFactsRepository,
    FactRecord,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricsRepository,
    SecurityMetadataUpdate,
    SecurityRepository,
    SupportedExchangeRepository,
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
            SELECT provider_symbol, provider_exchange_code, listing_exchange, security_type
            FROM supported_tickers
            ORDER BY provider_symbol
            """
        ).fetchall()

    assert rows == [
        ("AAA.US", "US", "NYSE", "Common Stock"),
        ("BBB.US", "US", "NYSE", "ETF"),
    ]


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


def test_supported_exchange_repository_replaces_rows_per_provider(tmp_path):
    repo = SupportedExchangeRepository(tmp_path / "supported-exchanges.db")
    repo.initialize_schema()
    repo.replace_for_provider(
        "EODHD",
        [
            {"Code": "LSE", "Name": "London Exchange"},
            {"Code": "US", "Name": "USA Stocks"},
        ],
    )
    repo.replace_for_provider(
        "OTHER",
        [{"Code": "TSX", "Name": "Toronto Exchange"}],
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
    other_rows = repo.list_all("OTHER")
    assert [(row.provider, row.code, row.name) for row in other_rows] == [
        ("OTHER", "TSX", "Toronto Exchange")
    ]


def test_supported_exchange_repository_fetch_normalizes_code(tmp_path):
    repo = SupportedExchangeRepository(tmp_path / "supported-exchanges.db")
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
            JOIN securities s ON s.security_id = ff.security_id
            ORDER BY ff.concept
            """
        ).fetchall()

    assert rows == [("AAA.US", "Liabilities", 40.0, None)]


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
            JOIN securities s ON s.security_id = ff.security_id
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
            JOIN securities s ON s.security_id = ff.security_id
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


def test_market_data_fetch_state_repository_tracks_success_and_failure(tmp_path):
    repo = MarketDataFetchStateRepository(tmp_path / "market-state.db")
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
    repo = MarketDataFetchStateRepository(tmp_path / "market-state-batch.db")
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
    assert counts["AAA.US"] == 111.0
    assert counts["BBB.US"] == 333.0
    assert counts_with_security_ids == counts
    assert "CCC.US" not in counts


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
            JOIN securities s ON s.security_id = md.security_id
            WHERE s.canonical_symbol = 'AAA.US' AND md.as_of = '2026-03-28'
            """
        ).fetchone()[0]
    assert historical == 900.0


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
