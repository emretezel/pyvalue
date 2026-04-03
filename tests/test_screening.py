"""Tests for screening configuration evaluation.

Author: Emre Tezel
"""

from pathlib import Path

from pyvalue.screening import (
    Criterion,
    Term,
    evaluate_criterion,
    evaluate_criterion_detail,
    load_screen,
    screen_metric_ids,
)
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


def test_evaluate_criterion_detail_reports_missing_metric_ids(tmp_path):
    db = tmp_path / "detail_missing.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()

    criterion = Criterion(
        name="requires two metrics",
        left=Term(metric="working_capital"),
        operator=">=",
        right=Term(metric="current_ratio"),
    )

    result = evaluate_criterion_detail(
        criterion,
        "AAPL.US",
        metrics_repo,
        fact_repo,
        log_missing_metrics=False,
    )

    assert result.passed is False
    assert result.failure_kind == "both_missing"
    assert result.missing_metric_ids == ("working_capital", "current_ratio")


def test_screen_metric_ids_dedupes_metrics_in_first_seen_order():
    definition = load_screen(
        Path(__file__).resolve().parents[1] / "screeners" / "value.yml"
    )

    assert screen_metric_ids(definition) == [
        "long_term_debt",
        "working_capital",
        "eps_streak",
        "graham_eps_10y_cagr_3y_avg",
        "graham_multiplier",
        "current_ratio",
        "earnings_yield",
        "roc_greenblatt_5y_avg",
        "roe_greenblatt_5y_avg",
        "price_to_fcf",
        "eps_ttm",
        "eps_6y_avg",
    ]


def test_load_screen_parses_basic_value_example():
    screen_path = Path(__file__).resolve().parents[1] / "screeners" / "basic_value.yml"

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 3
    assert {criterion.operator for criterion in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "current_ratio"
    assert definition.criteria[0].right.value == 1.25
    assert definition.criteria[2].left.metric == "long_term_debt"
    assert definition.criteria[2].right.metric == "working_capital"
    assert definition.criteria[2].right.multiplier == 1.75


def test_load_screen_parses_value_normalized_example():
    screen_path = (
        Path(__file__).resolve().parents[1] / "screeners" / "value_normalized.yml"
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 3
    assert {criterion.operator for criterion in definition.criteria} == {
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "oey_ev_norm"
    assert definition.criteria[1].left.metric == "ev_to_ebit"
    assert definition.criteria[2].left.metric == "graham_multiplier"


def test_load_screen_parses_quality_reasonable_price_example():
    screen_path = (
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price.yml"
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 6
    assert {criterion.operator for criterion in definition.criteria} == {
        ">=",
        "<=",
    }
    assert definition.criteria[0].left.metric == "roic_10y_median"
    assert definition.criteria[1].left.metric == "opm_10y_min"
    assert definition.criteria[2].left.metric == "net_debt_to_ebitda"
    assert definition.criteria[3].left.metric == "cfo_to_ni_ttm"
    assert definition.criteria[4].left.metric == "oey_ev_norm"
    assert definition.criteria[5].left.metric == "share_count_cagr_10y"
