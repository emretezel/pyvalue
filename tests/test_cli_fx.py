"""CLI-adjacent currency and FX tests.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3

import pytest

import pyvalue.cli as cli
from cli_test_helpers import patch_cli
from pyvalue.money.fx import FXCatalogEntry
from pyvalue.screening import RankingDefinition, RankingMetric, ScreenDefinition
from pyvalue.persistence.storage import (
    FXRateRecord,
    FXRatesRepository,
    FXRefreshStateRepository,
    FXSupportedPairsRepository,
    MetricsRepository,
    SecurityRepository,
    SupportedTickerRepository,
)
from pyvalue.persistence.storage.fx import _PAIR_COVERAGE_SQL

from conftest import seed_exchange


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


def _seed_listing(db_path: Path, symbol: str, *, currency: str = "USD") -> None:
    """Catalog a listing carrying a currency so metric/entity upserts (which
    materialize the listing) satisfy the NOT NULL listing.currency invariant.
    The metric's own currency is independent of the listing's quote currency.
    """

    ticker, _, suffix = symbol.partition(".")
    seed_exchange(db_path, suffix or "US", currency=currency)
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        suffix or "US",
        [{"Code": ticker, "Type": "Common Stock", "Currency": currency}],
    )


def test_rank_screen_passers_skips_mixed_currency_metric_without_ranking_currency(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "ranking_skip.db"
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
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
    security_repo = SecurityRepository(db_path)
    aaa = security_repo.resolve_id("AAA.US")
    bbb = security_repo.resolve_id("BBB.US")
    assert aaa is not None and bbb is not None

    definition = _ranking_definition(
        RankingMetric(metric_id="market_cap", weight=1.0, direction="higher")
    )

    with caplog.at_level("WARNING"):
        ordered, _ = cli._rank_screen_passers(
            definition,
            [(aaa, "AAA.US"), (bbb, "BBB.US")],
            metrics_repo,
            security_repo,
        )

    assert [{aaa: "AAA.US", bbb: "BBB.US"}[lid] for lid in ordered] == [
        "AAA.US",
        "BBB.US",
    ]
    assert "mixed currencies without comparison currency" in caplog.text


def test_rank_screen_passers_normalizes_mixed_currency_metric_with_ranking_currency(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ranking_convert.db"
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    _seed_listing(db_path, "AAA.US")
    _seed_listing(db_path, "BBB.US")
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
            rate=0.5,
            fetched_at="2023-12-31T00:00:00+00:00",
            source_kind="provider",
        )
    )
    security_repo = SecurityRepository(db_path)
    aaa = security_repo.resolve_id("AAA.US")
    bbb = security_repo.resolve_id("BBB.US")
    assert aaa is not None and bbb is not None

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
        [(aaa, "AAA.US"), (bbb, "BBB.US")],
        metrics_repo,
        security_repo,
    )

    assert [{aaa: "AAA.US", bbb: "BBB.US"}[lid] for lid in ordered] == [
        "BBB.US",
        "AAA.US",
    ]


class _BaseFakeEODHDFXProvider:
    """Base FX refresh provider fake.

    Mirrors the ``FXRefreshProvider`` protocol that ``cmd_refresh_fx_rates``
    drives. Concrete tests subclass it to record which history windows the CLI
    requested (``calls``) and to return canned rows from ``fetch_history``.
    """

    provider_name = "EODHD"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        # Each entry is ``(canonical_symbol, start_iso, end_iso)`` for one
        # history request, so tests can assert the exact windows fetched.
        self.calls: list[tuple[str, str, str]] = []

    def list_catalog(self) -> list[FXCatalogEntry]:
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

    def fetch_history(
        self, *, canonical_symbol: str, start_date: date, end_date: date
    ) -> list[FXRateRecord]:
        raise NotImplementedError


def test_cmd_refresh_fx_rates_eodhd_syncs_catalog_and_skips_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "refresh_fx_eodhd.db"

    class FakeProvider(_BaseFakeEODHDFXProvider):
        last_instance: FakeProvider | None = None

        def __init__(self, api_key: str) -> None:
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(
            self, *, canonical_symbol: str, start_date: date, end_date: date
        ) -> list[FXRateRecord]:
            self.calls.append(
                (canonical_symbol, start_date.isoformat(), end_date.isoformat())
            )
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-01",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate=1.09,
                    fetched_at="2024-01-01T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    patch_cli(monkeypatch, "EODHDFXProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert rc == 0
    assert FakeProvider.last_instance is not None
    assert FakeProvider.last_instance.calls == [("EURUSD", "2024-01-01", "2024-01-01")]

    fx_repo = FXRatesRepository(db_path)
    assert fx_repo.fetch_pair_history("EODHD", "EUR", "USD") == [("2024-01-01", 1.09)]

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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
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
                rate=1.09,
                fetched_at="2024-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-02",
                base_currency="EUR",
                quote_currency="USD",
                rate=1.10,
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
        last_instance: FakeProvider | None = None

        def __init__(self, api_key: str) -> None:
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(
            self, *, canonical_symbol: str, start_date: date, end_date: date
        ) -> list[FXRateRecord]:
            self.calls.append(
                (canonical_symbol, start_date.isoformat(), end_date.isoformat())
            )
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-03",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate=1.11,
                    fetched_at="2024-01-03T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    patch_cli(monkeypatch, "EODHDFXProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "secret")

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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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
                rate=1.10,
                fetched_at="2024-01-02T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-03",
                base_currency="EUR",
                quote_currency="USD",
                rate=1.11,
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
        last_instance: FakeProvider | None = None

        def __init__(self, api_key: str) -> None:
            super().__init__(api_key)
            FakeProvider.last_instance = self

        def fetch_history(
            self, *, canonical_symbol: str, start_date: date, end_date: date
        ) -> list[FXRateRecord]:
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
                        rate=1.09,
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
                    rate=1.12,
                    fetched_at="2024-01-04T00:00:00+00:00",
                    source_kind="provider",
                ),
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-05",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate=1.13,
                    fetched_at="2024-01-05T00:00:00+00:00",
                    source_kind="provider",
                ),
            ]

    patch_cli(monkeypatch, "EODHDFXProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "secret")

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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "refresh_fx_failure.db"

    class FakeProvider(_BaseFakeEODHDFXProvider):
        def fetch_history(
            self, *, canonical_symbol: str, start_date: date, end_date: date
        ) -> list[FXRateRecord]:
            return []

    patch_cli(monkeypatch, "EODHDFXProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "secret")

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


def test_pair_coverage_query_uses_split_index_seeks(tmp_path: Path) -> None:
    """Regression: ``pair_coverage`` must resolve MIN and MAX as two separate
    single-aggregate index-endpoint seeks, not one combined ``MIN(),MAX()``
    that scans the whole pair group in ``idx_fx_rates_pair_date``.

    A combined two-aggregate statement defeats SQLite's min/max optimization
    and degrades to a full covering-index scan of the pair (~500x slower on the
    largest stored pair). The split form shows up in the query plan as two
    independent SCALAR SUBQUERY nodes, each a covering-index search; the
    combined form shows a single search and no scalar subqueries.
    """

    db_path = tmp_path / "fx_coverage_plan.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    # Populate the pair so the planner is choosing over real rows.
    repo.upsert_many(
        [
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-01",
                base_currency="EUR",
                quote_currency="USD",
                rate=1.09,
                fetched_at="2024-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2024-01-02",
                base_currency="EUR",
                quote_currency="USD",
                rate=1.10,
                fetched_at="2024-01-02T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )

    with sqlite3.connect(db_path) as conn:
        plan_rows = conn.execute(
            "EXPLAIN QUERY PLAN " + _PAIR_COVERAGE_SQL,
            ("EODHD", "EUR", "USD", "EODHD", "EUR", "USD"),
        ).fetchall()
    plan = " | ".join(str(row[-1]) for row in plan_rows)

    assert plan.count("SCALAR SUBQUERY") == 2, plan
    assert plan.count("USING COVERING INDEX idx_fx_rates_pair_date") == 2, plan
    # And the split form still returns the correct coverage.
    assert repo.pair_coverage("EODHD", "EUR", "USD") == ("2024-01-01", "2024-01-02")


def test_refresh_fx_rates_widens_coverage_without_post_upsert_rescan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Regression: the refresh loop must read coverage once per pair (to plan
    ranges) and then widen it in-process from the upserted batch -- it must not
    re-query ``fx_rates`` coverage after every upsert.

    The post-upsert recompute was a redundant full-group MIN/MAX scan; a second
    ``pair_coverage`` call per refreshable pair means it has returned.
    """

    db_path = tmp_path / "refresh_fx_no_rescan.db"

    coverage_calls: list[tuple[str, str, str]] = []
    original_pair_coverage = FXRatesRepository.pair_coverage

    def counting_pair_coverage(
        self: FXRatesRepository,
        provider: str,
        base_currency: str,
        quote_currency: str,
    ) -> tuple[str | None, str | None]:
        coverage_calls.append((provider, base_currency, quote_currency))
        return original_pair_coverage(self, provider, base_currency, quote_currency)

    monkeypatch.setattr(FXRatesRepository, "pair_coverage", counting_pair_coverage)

    class FakeProvider(_BaseFakeEODHDFXProvider):
        def fetch_history(
            self, *, canonical_symbol: str, start_date: date, end_date: date
        ) -> list[FXRateRecord]:
            # Two rows in one window: the old code would have rescanned coverage
            # once after this single upsert; the new code derives it in-process.
            return [
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-01",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate=1.09,
                    fetched_at="2024-01-01T00:00:00+00:00",
                    source_kind="provider",
                ),
                FXRateRecord(
                    provider="EODHD",
                    rate_date="2024-01-02",
                    base_currency="EUR",
                    quote_currency="USD",
                    rate=1.10,
                    fetched_at="2024-01-02T00:00:00+00:00",
                    source_kind="provider",
                ),
            ]

    patch_cli(monkeypatch, "EODHDFXProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "secret")

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    assert rc == 0
    # Exactly one coverage read for the single refreshable pair (EURUSD): the
    # pre-plan read. No post-upsert rescan.
    assert coverage_calls == [("EODHD", "EUR", "USD")]

    # Coverage widened in-process must match what a fresh DB read reports.
    state = FXRefreshStateRepository(db_path).fetch("EODHD", "EURUSD")
    assert state is not None
    assert (state.min_rate_date, state.max_rate_date) == ("2024-01-01", "2024-01-02")
    assert FXRatesRepository(db_path).pair_coverage("EODHD", "EUR", "USD") == (
        "2024-01-01",
        "2024-01-02",
    )
