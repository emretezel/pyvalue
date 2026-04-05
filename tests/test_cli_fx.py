"""CLI-adjacent currency and FX tests.

Author: Emre Tezel
"""

from __future__ import annotations

import pyvalue.cli as cli
from pyvalue.screening import RankingDefinition, RankingMetric, ScreenDefinition
from pyvalue.storage import (
    EntityMetadataRepository,
    FXRateRecord,
    FXRatesRepository,
    FactRecord,
    FinancialFactsRepository,
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
            provider="FRANKFURTER",
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


def test_cmd_refresh_fx_rates_normalizes_discovered_currencies(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="GBX",
                value=1000.0,
                currency="GBX",
            ),
            FactRecord(
                symbol="BBB.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="EUR",
                value=100.0,
                currency="EUR",
            ),
            FactRecord(
                symbol="CCC.US",
                concept="Assets",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="USD",
                value=100.0,
                currency="USD",
            ),
        ],
    )

    calls = {}

    class FakeProvider:
        provider_name = "FRANKFURTER"

        def fetch_rates(self, *, base_currency, quote_currencies, start_date, end_date):
            calls["base_currency"] = base_currency
            calls["quote_currencies"] = list(quote_currencies)
            calls["start_date"] = start_date.isoformat()
            calls["end_date"] = end_date.isoformat()
            return [
                FXRateRecord(
                    provider="FRANKFURTER",
                    rate_date=end_date.isoformat(),
                    base_currency=base_currency,
                    quote_currency=quote_currencies[0],
                    rate_text="0.8",
                    fetched_at=f"{end_date.isoformat()}T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    class FakeService:
        def __init__(self, database, repository=None):
            self.provider = FakeProvider()
            self.provider_name = "FRANKFURTER"
            self.pivot_currency = "USD"
            self.repository = repository or FXRatesRepository(database)

    monkeypatch.setattr(cli, "FXService", FakeService)

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2023-12-31",
        end_date="2023-12-31",
    )

    assert rc == 0
    assert calls["base_currency"] == "USD"
    assert calls["quote_currencies"] == ["EUR", "GBP"]
    output = capsys.readouterr().out
    assert "Preparing FX refresh schema and indexes" in output
    assert "Stored FX rates" in output


def test_cmd_refresh_fx_rates_batches_history_and_reports_progress(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx_progress.db"

    calls: list[tuple[str, tuple[str, ...], str, str]] = []

    class FakeProvider:
        provider_name = "FRANKFURTER"

        def fetch_rates(self, *, base_currency, quote_currencies, start_date, end_date):
            calls.append(
                (
                    base_currency,
                    tuple(quote_currencies),
                    start_date.isoformat(),
                    end_date.isoformat(),
                )
            )
            return [
                FXRateRecord(
                    provider="FRANKFURTER",
                    rate_date=end_date.isoformat(),
                    base_currency=base_currency,
                    quote_currency=quote_currencies[0],
                    rate_text="0.8",
                    fetched_at=f"{end_date.isoformat()}T00:00:00+00:00",
                    source_kind="provider",
                )
            ]

    class FakeService:
        def __init__(self, database, repository=None):
            self.provider = FakeProvider()
            self.provider_name = "FRANKFURTER"
            self.pivot_currency = "USD"
            self.repository = repository or FXRatesRepository(database)

    monkeypatch.setattr(
        cli.FXRatesRepository,
        "discover_currencies",
        lambda self: ["USD", *[f"C{i:02d}" for i in range(30)]],
    )
    monkeypatch.setattr(cli, "FXService", FakeService)
    monkeypatch.setattr(cli, "FX_REFRESH_MAX_QUOTES_PER_REQUEST", 10)
    monkeypatch.setattr(cli, "FX_REFRESH_MAX_DAYS_PER_REQUEST", 365)

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2020-01-01",
        end_date="2021-12-31",
    )

    assert rc == 0
    assert len(calls) == 9
    assert calls[0] == (
        "USD",
        tuple(f"C{i:02d}" for i in range(10)),
        "2020-01-01",
        "2020-12-30",
    )
    assert calls[-1] == (
        "USD",
        tuple(f"C{i:02d}" for i in range(20, 30)),
        "2021-12-31",
        "2021-12-31",
    )
    output = capsys.readouterr().out
    assert "Refreshing FX rates" in output
    assert "Progress: [--------------------] 0/9 FX batches complete (0.0%)" in output
    assert "Progress: [####################] 9/9 FX batches complete (100.0%)" in output


def test_cmd_refresh_fx_rates_skips_fully_covered_currency_ranges(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "refresh_fx_skip_covered.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(
        [
            FXRateRecord(
                provider="FRANKFURTER",
                rate_date="2020-01-01",
                base_currency="USD",
                quote_currency="EUR",
                rate_text="0.9",
                fetched_at="2020-01-01T00:00:00+00:00",
                source_kind="provider",
            ),
            FXRateRecord(
                provider="FRANKFURTER",
                rate_date="2020-12-31",
                base_currency="USD",
                quote_currency="EUR",
                rate_text="0.8",
                fetched_at="2020-12-31T00:00:00+00:00",
                source_kind="provider",
            ),
        ]
    )

    calls = []

    class FakeProvider:
        provider_name = "FRANKFURTER"

        def fetch_rates(self, *, base_currency, quote_currencies, start_date, end_date):
            calls.append((base_currency, tuple(quote_currencies), start_date, end_date))
            return []

    class FakeService:
        def __init__(self, database, repository=None):
            self.provider = FakeProvider()
            self.provider_name = "FRANKFURTER"
            self.pivot_currency = "USD"
            self.repository = repository or FXRatesRepository(database)

    monkeypatch.setattr(
        cli.FXRatesRepository,
        "discover_currencies",
        lambda self: ["USD", "EUR"],
    )
    monkeypatch.setattr(cli, "FXService", FakeService)

    rc = cli.cmd_refresh_fx_rates(
        database=str(db_path),
        start_date="2020-01-01",
        end_date="2020-12-31",
    )

    assert rc == 0
    assert calls == []
    output = capsys.readouterr().out
    assert "skipped_currencies=1" in output
    assert "requests=0" in output
