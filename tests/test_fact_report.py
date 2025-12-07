"""Fact coverage reporting.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.cli import cmd_report_fact_freshness
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.reporting import compute_fact_coverage
from pyvalue.storage import FactRecord, FinancialFactsRepository, UniverseRepository
from pyvalue.universe import Listing


def _seed_universe(db_path):
    universe = UniverseRepository(db_path)
    universe.initialize_schema()
    universe.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB", exchange="NYSE"),
        ],
        region="US",
    )


def _seed_facts(repo: FinancialFactsRepository):
    today = date.today()
    fresh_date = today.isoformat()
    stale_date = (today - timedelta(days=400)).isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(symbol="AAA.US", concept="AssetsCurrent", fiscal_period="Q1", end_date=fresh_date, unit="USD", value=100.0),
            FactRecord(symbol="AAA.US", concept="LiabilitiesCurrent", fiscal_period="Q1", end_date=fresh_date, unit="USD", value=50.0),
        ],
    )
    repo.replace_facts(
        "BBB.US",
        [
            FactRecord(symbol="BBB.US", concept="AssetsCurrent", fiscal_period="Q1", end_date=stale_date, unit="USD", value=75.0),
        ],
    )


def test_compute_fact_coverage_counts_missing_and_stale(tmp_path):
    db_path = tmp_path / "facts.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    _seed_facts(fact_repo)

    report = compute_fact_coverage(fact_repo, ["AAA.US", "BBB.US"], [WorkingCapitalMetric], max_age_days=365)

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


def test_cmd_report_fact_freshness_outputs_counts(tmp_path, capsys):
    db_path = tmp_path / "report.db"
    _seed_universe(db_path)
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    _seed_facts(fact_repo)

    exit_code = cmd_report_fact_freshness(
        database=str(db_path),
        region="US",
        metric_ids=["working_capital"],
        max_age_days=365,
        output_csv=None,
        show_all=True,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "working_capital" in output
    assert "AssetsCurrent" in output
    assert "stale=1" in output
    assert "missing=1" in output
