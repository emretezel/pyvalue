"""CLI-adjacent currency and FX tests.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
import sqlite3

import pyvalue.cli as cli
from pyvalue.fx import FXCatalogEntry
from pyvalue.screening import RankingDefinition, RankingMetric, ScreenDefinition
from pyvalue.storage import (
    EntityMetadataRepository,
    FXRateRecord,
    FXRatesRepository,
    FXRefreshStateRepository,
    FXSupportedPairsRepository,
    MetricsRepository,
)


def _ranking_definition(metric: RankingMetric) -> ScreenDefinition:
    return ScreenDefinition(
        criteria=[],
        ranking=RankingDefinition(
            peer_group="sector",
            min_sector_peers=10,
            winsor_lower_percentile=0.05,
            winsor_upper_percentile=0.95,
            metrics=(metric,),
            tie_breakers=(),
        ),
    )


def test_rank_screen_passers_skips_mixed_currency_metric_without_ranking_currency(
    tmp_path, caplog
):
    db_path = tmp_path / "ranking_skip.db"
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    metrics_repo.upsert(
        "AAA.US",
        "market_cap",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="USD",
    )
    metrics_repo.upsert(
        "BBB.US",
        "market_cap",
        90.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="EUR",
    )
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA")
    entity_repo.upsert("BBB.US", "BBB")

    definition = _ranking_definition(
        RankingMetric(metric_id="market_cap", weight=1.0, direction="higher")
    )

    with caplog.at_level("WARNING"):
        ordered, _ = cli._rank_screen_passers(
            definition,
            ["AAA.US", "BBB.US"],
            metrics_repo,
            entity_repo,
        )

    assert ordered == ["AAA.US", "BBB.US"]
    assert "mixed currencies without comparison currency" in caplog.text


def test_rank_screen_passers_normalizes_mixed_currency_metric_with_ranking_currency(
    tmp_path,
):
    db_path = tmp_path / "ranking_convert.db"
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    metrics_repo.upsert(
        "AAA.US",
        "market_cap",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="USD",
    )
    metrics_repo.upsert(
        "BBB.US",
        "market_cap",
        90.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="EUR",
    )
    fx_repo = FXRatesRepository(db_path)
    fx_repo.initialize_schema()
    fx_repo.upsert(
        FXRateRecord(
            provider="EODHD",
            rate_date="2023-12-31",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.5",
            fetched_at="2023-12-31T00:00:00+00:00",
            source_kind="provider",
        )
    )
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA")
    entity_repo.upsert("BBB.US", "BBB")

    definition = _ranking_definition(
        RankingMetric(
            metric_id="market_cap",
            weight=1.0,
            direction="higher",
            currency="USD",
        )
    )

    ordered, _ = cli._rank_screen_passers(
        definition,
        ["AAA.US", "BBB.US"],
        metrics_repo,
        entity_repo,
    )

    assert ordered == ["BBB.US", "AAA.US"]


class _BaseFakeEODHDFXProvider:
    provider_name = "EODHD"

    def __init__(self, api_key):
        self.api_key = api_key
        self.calls = []

    def list_catalog(self):
        return [
            FXCatalogEntry(
                symbol="EURUSD",
                canonical_symbol="EURUSD",
                base_currency="EUR",
                quote_currency="USD",
                name="EUR/USD",
                is_alias=False,
                is_refreshable=True,
            ),
            FXCatalogEntry(
                symbol="EUR",
                canonical_symbol="USDEUR",
                base_currency="USD",
                quote_currency="EUR",
                name="USD/EUR",
                is_alias=True,
                is_refreshable=False,
            ),
            FXCatalogEntry(
                symbol="USDARSB",
                canonical_symbol="USDARSB",
                base_currency=None,
                quote_currency=None,
                name="Odd",
                is_alias=False,
                is_refreshable=False,
            ),
        ]

    def fetch_history(self, *, canonical_symbol, start_date, end_date):
        raise NotImplementedError


def test_cmd_refresh_fx_rates_eodhd_syncs_catalog_and_skips_aliases(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx_eodhd.db"

    class FakeProvider(_BaseFakeEODHDFXProvider):
        last_instance = None

        def __init__(self, api_key):
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(self, *, canonical_symbol, start_date, end_date):
            self.calls.append(
                (canonical_symbol, start_date.isoformat(), end_date.isoformat())
            )
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-01",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate_text="1.09",
                    fetched_at="2024-01-01T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    monkeypatch.setattr(cli, "EODHDFXProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert rc == 0
    assert FakeProvider.last_instance is not None
    assert FakeProvider.last_instance.calls == [("EURUSD", "2024-01-01", "2024-01-01")]

    fx_repo = FXRatesRepository(db_path)
    assert fx_repo.fetch_pair_history("EODHD", "EUR", "USD") == [("2024-01-01", "1.09")]

    state = FXRefreshStateRepository(db_path).fetch("EODHD", "EURUSD")
    assert state is not None
    assert state.min_rate_date == "2024-01-01"
    assert state.max_rate_date == "2024-01-01"
    assert state.full_history_backfilled is False

    catalog_repo = FXSupportedPairsRepository(db_path)
    refreshable = catalog_repo.list_refreshable("EODHD")
    assert [row.canonical_symbol for row in refreshable] == ["EURUSD"]

    with sqlite3.connect(db_path) as conn:
        total_catalog_rows = conn.execute(
            "SELECT COUNT(*) FROM fx_supported_pairs WHERE provider = 'EODHD'"
        ).fetchone()[0]
    assert total_catalog_rows == 3

    output = capsys.readouterr().out
    assert "Syncing EODHD FOREX catalog" in output
    assert "canonical_pairs=1" in output
    assert "requested_range=2024-01-01..2024-01-01" in output
    assert "pair=EURUSD" in output


def test_cmd_refresh_fx_rates_eodhd_fetches_only_incremental_newer_history(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx_incremental.db"
    fx_repo = FXRatesRepository(db_path)
    fx_repo.initialize_schema()
    fx_repo.upsert_many(
        [
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-01",
                base_currency="EUR",
                quote_currency="USD",
                rate_text="1.09",
                fetched_at="2024-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-02",
                base_currency="EUR",
                quote_currency="USD",
                rate_text="1.10",
                fetched_at="2024-01-02T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )
    FXRefreshStateRepository(db_path).mark_success(
        "EODHD",
        "EURUSD",
        min_rate_date="2024-01-01",
        max_rate_date="2024-01-02",
        full_history_backfilled=True,
    )

    class FakeProvider(_BaseFakeEODHDFXProvider):
        last_instance = None

        def __init__(self, api_key):
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(self, *, canonical_symbol, start_date, end_date):
            self.calls.append(
                (canonical_symbol, start_date.isoformat(), end_date.isoformat())
            )
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-03",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate_text="1.11",
                    fetched_at="2024-01-03T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    monkeypatch.setattr(cli, "EODHDFXProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        end_date="2024-01-03",
    )

    assert rc == 0
    assert FakeProvider.last_instance is not None
    assert FakeProvider.last_instance.calls == [("EURUSD", "2024-01-03", "2024-01-03")]

    state = FXRefreshStateRepository(db_path).fetch("EODHD", "EURUSD")
    assert state is not None
    assert state.max_rate_date == "2024-01-03"
    assert state.full_history_backfilled is True
    output = capsys.readouterr().out
    assert "mode=auto-full-history requested_end=2024-01-03" in output
    assert "pair=EURUSD" in output


def test_cmd_refresh_fx_rates_eodhd_completes_old_history_after_bounded_backfill(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "refresh_fx_complete_old.db"
    fx_repo = FXRatesRepository(db_path)
    fx_repo.initialize_schema()
    fx_repo.upsert_many(
        [
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-02",
                base_currency="EUR",
                quote_currency="USD",
                rate_text="1.10",
                fetched_at="2024-01-02T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-03",
                base_currency="EUR",
                quote_currency="USD",
                rate_text="1.11",
                fetched_at="2024-01-03T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )
    FXRefreshStateRepository(db_path).mark_success(
        "EODHD",
        "EURUSD",
        min_rate_date="2024-01-02",
        max_rate_date="2024-01-03",
        full_history_backfilled=False,
    )

    class FakeProvider(_BaseFakeEODHDFXProvider):
        last_instance = None

        def __init__(self, api_key):
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(self, *, canonical_symbol, start_date, end_date):
            self.calls.append(
                (canonical_symbol, start_date.isoformat(), end_date.isoformat())
            )
            if end_date == date(2024, 1, 1):
                return [
                    FXRateRecord(
                        provider="EODHD",
                        rate_date="2024-01-01",
                        base_currency="EUR",
                        quote_currency="USD",
                        rate_text="1.09",
                        fetched_at="2024-01-01T00:00:00+00:00",
                        source_kind="provider",
                    )
                ]
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-04",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate_text="1.12",
                    fetched_at="2024-01-04T00:00:00+00:00",
                    source_kind="provider",
                ),
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-05",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate_text="1.13",
                    fetched_at="2024-01-05T00:00:00+00:00",
                    source_kind="provider",
                ),
            ]

    monkeypatch.setattr(cli, "EODHDFXProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        end_date="2024-01-05",
    )

    assert rc == 0
    assert FakeProvider.last_instance is not None
    assert FakeProvider.last_instance.calls == [
        ("EURUSD", "1900-01-01", "2024-01-01"),
        ("EURUSD", "2024-01-04", "2024-01-05"),
    ]

    state = FXRefreshStateRepository(db_path).fetch("EODHD", "EURUSD")
    assert state is not None
    assert state.min_rate_date == "2024-01-01"
    assert state.max_rate_date == "2024-01-05"
    assert state.full_history_backfilled is True


def test_cmd_refresh_fx_rates_eodhd_marks_failure_when_initial_fetch_returns_no_rows(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx_failure.db"

    class FakeProvider(_BaseFakeEODHDFXProvider):
        def fetch_history(self, *, canonical_symbol, start_date, end_date):
            return []

    monkeypatch.setattr(cli, "EODHDFXProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert rc == 0
    state = FXRefreshStateRepository(db_path).fetch("EODHD", "EURUSD")
    assert state is not None
    assert state.last_status == "error"
    assert state.attempts == 1
    assert "No FX history returned" in (state.last_error or "")
    output = capsys.readouterr().out
    assert "failed_pairs=1" in output
