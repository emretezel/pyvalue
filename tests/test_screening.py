"""Tests for screening configuration evaluation.

Author: Emre Tezel
"""
from pathlib import Path

from pyvalue.screening import Criterion, Term, evaluate_criterion
from pyvalue.storage import FinancialFactsRepository, MetricsRepository, FactRecord


def test_evaluate_criterion_uses_metrics_repo(tmp_path):
    db = tmp_path / "test.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    metrics_repo.upsert("AAPL.US", "working_capital", 100.0, "2023-09-30")
    metrics_repo.upsert("AAPL.US", "long_term_debt", 150.0, "2023-09-30")

    criterion = Criterion(
        name="Debt vs WC",
        left=Term(metric="long_term_debt"),
        operator="<=",
        right=Term(metric="working_capital", multiplier=1.75),
    )

    assert evaluate_criterion(criterion, "AAPL.US", metrics_repo, fact_repo) is True

def test_evaluate_criterion_supports_constant_terms(tmp_path):
    db = tmp_path / "test2.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    metrics_repo.upsert("AAPL.US", "earnings_yield", 0.05, "2023-09-30")

    criterion = Criterion(
        name="Positive earnings yield",
        left=Term(metric="earnings_yield"),
        operator=">",
        right=Term(value=0.0),
    )

    assert evaluate_criterion(criterion, "AAPL.US", metrics_repo, fact_repo) is True
