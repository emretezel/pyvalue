"""Fact coverage reporting.

Author: Emre Tezel
"""

import sqlite3
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import pytest

from pyvalue.cli import cmd_report_fact_freshness
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.reporting import compute_fact_coverage
from pyvalue.persistence.storage import (
    FactRecord,
    FinancialFactsRepository,
    SecurityRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing

from conftest import seed_exchange


def _seed_universe(db_path: Path) -> None:
    universe = SupportedTickerRepository(db_path)
    universe.initialize_schema()
    seed_exchange(db_path, "US", provider="SEC")
    # Listings are non-nullable on currency and have no fallback; a Listing
    # without a currency is skipped entirely by the catalog, so the implicit
    # listing creation inside replace_facts would later raise. Seed both
    # ".US" listings with USD (US exchange convention) up front.
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


def _seed_facts(repo: FinancialFactsRepository) -> None:
    today = date.today()
    fresh_date = today.isoformat()
    stale_date = (today - timedelta(days=MAX_FACT_AGE_DAYS + 1)).isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="Q1",
                end_date=fresh_date,
                unit_kind="monetary",
                currency="USD",
                value=100.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="Q1",
                end_date=fresh_date,
                unit_kind="monetary",
                currency="USD",
                value=50.0,
            ),
        ],
    )
    repo.replace_facts(
        "BBB.US",
        [
            FactRecord(
                symbol="BBB.US",
                concept="AssetsCurrent",
                fiscal_period="Q1",
                end_date=stale_date,
                unit_kind="monetary",
                currency="USD",
                value=75.0,
            ),
        ],
    )


def test_compute_fact_coverage_counts_missing_and_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "facts.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    _seed_facts(fact_repo)

    # compute_fact_coverage now keys on the scope's listing ids; resolve the
    # symbols the way _resolve_canonical_scope_listings would for the CLI.
    listing_ids = list(
        SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"]).values()
    )
    report = compute_fact_coverage(
        fact_repo,
        listing_ids,
        [WorkingCapitalMetric],
    )

    assert len(report) == 1
    entry = report[0]
    assert entry.metric_id == "working_capital"
    assert entry.total_symbols == 2
    assert entry.fully_covered == 1
    coverage = {concept.concept: concept for concept in entry.concepts}
    assert coverage["AssetsCurrent"].stale == 1
    assert coverage["AssetsCurrent"].missing == 0
    assert coverage["LiabilitiesCurrent"].missing == 1
    assert coverage["LiabilitiesCurrent"].stale == 0


def test_cmd_report_fact_freshness_outputs_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    _seed_facts(fact_repo)

    exit_code = cmd_report_fact_freshness(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=["working_capital"],
        max_age_days=MAX_FACT_AGE_DAYS,
        output_csv=None,
        show_all=True,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "working_capital" in output
    assert "AssetsCurrent" in output
    assert "stale=1" in output
    assert "missing=1" in output


def test_cmd_report_fact_freshness_carries_scope_listing_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """report-fact-freshness carries scope listing ids into the bulk fact load.

    The scope resolver already holds each listing_id, so the single bulk
    ``facts_for_ids_many`` read seeks by id rather than re-resolving
    symbol->listing_id. We count ``resolve_ids_many`` and assert it never runs.

    Author: Emre Tezel
    """
    db_path = tmp_path / "freshness-carry-ids.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    _seed_facts(fact_repo)

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

    exit_code = cmd_report_fact_freshness(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=["working_capital"],
        max_age_days=MAX_FACT_AGE_DAYS,
        output_csv=None,
        show_all=True,
    )

    assert exit_code == 0
    assert calls == {"resolve_ids_many": 0}


def test_fact_report_counts_assets_current_from_components(tmp_path: Path) -> None:
    db_path = tmp_path / "components.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    today = date.today().isoformat()
    fact_repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="CashAndCashEquivalents",
                fiscal_period="Q1",
                end_date=today,
                unit_kind="monetary",
                currency="USD",
                value=10.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                fiscal_period="Q1",
                end_date=today,
                unit_kind="monetary",
                currency="USD",
                value=5.0,
            ),
        ],
    )

    listing_id = SecurityRepository(db_path).resolve_id("AAA.US")
    assert listing_id is not None
    report = compute_fact_coverage(fact_repo, [listing_id], [WorkingCapitalMetric])
    entry = report[0]
    coverage = {c.concept: c for c in entry.concepts}
    assert coverage["AssetsCurrent"].missing == 1
    assert coverage["AssetsCurrent"].stale == 0
    assert entry.fully_covered == 0


def test_fact_report_counts_liabilities_current_from_components(tmp_path: Path) -> None:
    db_path = tmp_path / "components2.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    today = date.today().isoformat()
    fact_repo.replace_facts(
        "AAA.US",
        [
            FactRecord(
                symbol="AAA.US",
                concept="AssetsCurrent",
                fiscal_period="Q1",
                end_date=today,
                unit_kind="monetary",
                currency="USD",
                value=50.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="AccountsPayableCurrent",
                fiscal_period="Q1",
                end_date=today,
                unit_kind="monetary",
                currency="USD",
                value=10.0,
            ),
            FactRecord(
                symbol="AAA.US",
                concept="AccruedLiabilitiesCurrent",
                fiscal_period="Q1",
                end_date=today,
                unit_kind="monetary",
                currency="USD",
                value=5.0,
            ),
        ],
    )

    listing_id = SecurityRepository(db_path).resolve_id("AAA.US")
    assert listing_id is not None
    report = compute_fact_coverage(fact_repo, [listing_id], [WorkingCapitalMetric])
    entry = report[0]
    coverage = {c.concept: c for c in entry.concepts}
    assert coverage["LiabilitiesCurrent"].missing == 1
    assert coverage["LiabilitiesCurrent"].stale == 0
    assert entry.fully_covered == 0
