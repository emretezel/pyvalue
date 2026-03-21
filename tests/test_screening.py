"""Tests for screening configuration evaluation.

Author: Emre Tezel
"""

from pathlib import Path

from pyvalue.screening import Criterion, Term, evaluate_criterion
from pyvalue.screening import load_screen
from pyvalue.storage import FinancialFactsRepository, MetricsRepository


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


def test_evaluate_criterion_filters_when_metric_missing(tmp_path):
    db = tmp_path / "missing_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()

    criterion = Criterion(
        name="requires metric",
        left=Term(metric="working_capital"),
        operator=">",
        right=Term(value=0.0),
    )

    assert evaluate_criterion(criterion, "AAPL.US", metrics_repo, fact_repo) is False


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


def test_load_screen_parses_basic_value_example():
    screen_path = Path(__file__).resolve().parents[1] / "screeners" / "basic_value.yml"

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 4
    assert {criterion.operator for criterion in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "market_cap"
    assert definition.criteria[0].right.value == 750000000
    assert definition.criteria[3].left.metric == "long_term_debt"
    assert definition.criteria[3].right.metric == "working_capital"
    assert definition.criteria[3].right.multiplier == 1.75


def test_load_screen_parses_value_normalized_example():
    screen_path = (
        Path(__file__).resolve().parents[1] / "screeners" / "value_normalized.yml"
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 4
    assert {criterion.operator for criterion in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "market_cap"
    assert definition.criteria[0].right.value == 2000000000
    assert definition.criteria[1].left.metric == "oey_ev_norm"
    assert definition.criteria[2].left.metric == "ev_to_ebit"
    assert definition.criteria[3].left.metric == "graham_multiplier"


def test_load_screen_parses_quality_reasonable_price_example():
    screen_path = (
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price.yml"
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 7
    assert {criterion.operator for criterion in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "market_cap"
    assert definition.criteria[1].left.metric == "roic_10y_median"
    assert definition.criteria[2].left.metric == "opm_10y_min"
    assert definition.criteria[3].left.metric == "net_debt_to_ebitda"
    assert definition.criteria[4].left.metric == "cfo_to_ni_ttm"
    assert definition.criteria[5].left.metric == "oey_ev_norm"
    assert definition.criteria[6].left.metric == "share_count_cagr_10y"
