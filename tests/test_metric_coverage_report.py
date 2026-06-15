"""Metric coverage report."""

import sqlite3
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pytest

from pyvalue.cli import cmd_report_metric_coverage
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.persistence.storage import (
    FactRecord,
    FinancialFactsRepository,
    MarketDataRepository,
    SecurityRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing

from conftest import seed_exchange, seed_facts, seed_price


def _seed_universe(db_path: Path) -> None:
    universe = SupportedTickerRepository(db_path)
    universe.initialize_schema()
    seed_exchange(db_path, "US", provider="SEC")
    universe.replace_from_listings(
        "SEC",
        "US",
        [
            Listing(
                symbol="AAA.US",
                security_name="AAA",
                exchange="NYSE",
                currency="USD",
            ),
            Listing(
                symbol="BBB.US",
                security_name="BBB",
                exchange="NYSE",
                currency="USD",
            ),
        ],
    )


def test_metric_coverage_counts_symbols(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "coverage.db"
    _seed_universe(db_path)
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    recent = date.today().isoformat()
    # AAA has full facts; BBB missing liabilities.
    seed_facts(
        db_path,
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="Q1",
                end_date=recent,
                unit_kind="monetary",
                currency="USD",
                value=100.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="Q1",
                end_date=recent,
                unit_kind="monetary",
                currency="USD",
                value=50.0,
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
                fiscal_period="Q1",
                end_date=recent,
                unit_kind="monetary",
                currency="USD",
                value=200.0,
            ),
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "AAA.US", recent, 10.0, currency="USD")
    seed_price(db_path, "BBB.US", recent, 10.0, currency="USD")

    exit_code = cmd_report_metric_coverage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[WorkingCapitalMetric.id, CurrentRatioMetric.id],
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "all metrics computed: 1/2" in output.lower()
    assert "- working_capital: 1/2" in output


def test_metric_coverage_carries_scope_listing_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """report-metric-coverage carries scope listing ids into the recompute.

    The scope resolver already holds each listing_id, so the batch recompute
    must not re-resolve symbol->listing_id. We count ``resolve_ids_many`` and
    assert it is never called (it would fire on the pre-fix symbol-scope path).

    Author: Emre Tezel
    """
    db_path = tmp_path / "coverage-carry-ids.db"
    _seed_universe(db_path)
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    recent = date.today().isoformat()
    for symbol in ("AAA.US", "BBB.US"):
        seed_facts(
            db_path,
            symbol,
            [
                FactRecord(
                    symbol=symbol,
                    concept="AssetsCurrent",
                    fiscal_period="Q1",
                    end_date=recent,
                    unit_kind="monetary",
                    currency="USD",
                    value=100.0,
                ),
                FactRecord(
                    symbol=symbol,
                    concept="LiabilitiesCurrent",
                    fiscal_period="Q1",
                    end_date=recent,
                    unit_kind="monetary",
                    currency="USD",
                    value=50.0,
                ),
            ],
        )

    calls = {"resolve_ids_many": 0}
    original_resolve_ids_many = SecurityRepository.resolve_ids_many

    def counting_resolve_ids_many(
        self: SecurityRepository,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        calls["resolve_ids_many"] += 1
        return original_resolve_ids_many(
            self, symbols, chunk_size=chunk_size, connection=connection
        )

    monkeypatch.setattr(
        SecurityRepository, "resolve_ids_many", counting_resolve_ids_many
    )

    exit_code = cmd_report_metric_coverage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[WorkingCapitalMetric.id],
    )

    assert exit_code == 0
    assert calls == {"resolve_ids_many": 0}
