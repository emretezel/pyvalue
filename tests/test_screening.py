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
    ranking_metric_ids,
    screen_metric_ids,
)
from pyvalue.storage import (
    FXRateRecord,
    FXRatesRepository,
    FinancialFactsRepository,
    MetricsRepository,
)


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


def test_evaluate_criterion_converts_monetary_constant_currency(tmp_path):
    db = tmp_path / "screen_fx_constant.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    metrics_repo.upsert(
        "AAPL.US",
        "working_capital",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="GBP",
    )
    fx_repo = FXRatesRepository(db)
    fx_repo.initialize_schema()
    fx_repo.upsert(
        FXRateRecord(
            provider="EODHD",
            rate_date="2023-12-31",
            base_currency="USD",
            quote_currency="GBP",
            rate_text="0.8",
            fetched_at="2023-12-31T00:00:00+00:00",
            source_kind="provider",
        )
    )

    criterion = Criterion(
        name="GBP threshold",
        left=Term(metric="working_capital"),
        operator=">=",
        right=Term(value=120.0, currency="USD"),
    )

    assert evaluate_criterion(criterion, "AAPL.US", metrics_repo, fact_repo) is True


def test_evaluate_criterion_converts_mixed_currency_metrics(tmp_path):
    db = tmp_path / "screen_fx_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    metrics_repo.upsert(
        "AAPL.US",
        "long_term_debt",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="USD",
    )
    metrics_repo.upsert(
        "AAPL.US",
        "working_capital",
        120.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="EUR",
    )
    fx_repo = FXRatesRepository(db)
    fx_repo.initialize_schema()
    fx_repo.upsert(
        FXRateRecord(
            provider="EODHD",
            rate_date="2023-12-31",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.9",
            fetched_at="2023-12-31T00:00:00+00:00",
            source_kind="provider",
        )
    )

    criterion = Criterion(
        name="Debt vs WC",
        left=Term(metric="long_term_debt"),
        operator="<=",
        right=Term(metric="working_capital"),
    )

    assert evaluate_criterion(criterion, "AAPL.US", metrics_repo, fact_repo) is True


def test_evaluate_criterion_normalizes_configured_subunit_metric_currencies(tmp_path):
    db = tmp_path / "screen_subunit_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    metrics_repo.upsert(
        "AAPL.US",
        "long_term_debt",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="ZAC",
    )
    metrics_repo.upsert(
        "AAPL.US",
        "working_capital",
        120.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="ILA",
    )
    fx_repo = FXRatesRepository(db)
    fx_repo.initialize_schema()
    fx_repo.upsert(
        FXRateRecord(
            provider="EODHD",
            rate_date="2023-12-31",
            base_currency="ZAR",
            quote_currency="ILS",
            rate_text="0.2",
            fetched_at="2023-12-31T00:00:00+00:00",
            source_kind="provider",
        )
    )

    criterion = Criterion(
        name="Debt vs WC",
        left=Term(metric="long_term_debt"),
        operator="<=",
        right=Term(metric="working_capital"),
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
    assert definition.ranking is not None
    assert definition.ranking.peer_group == "sector"
    assert definition.ranking.min_sector_peers == 10
    assert definition.ranking.winsor_lower_percentile == 0.05
    assert definition.ranking.winsor_upper_percentile == 0.95
    assert definition.ranking.metrics[0].metric_id == "oey_ev_norm"
    assert definition.ranking.metrics[0].weight == 0.30
    assert definition.ranking.metrics[0].direction == "higher"
    assert definition.ranking.metrics[4].metric_id == "cfo_to_ni_ttm"
    assert definition.ranking.metrics[4].cap == 1.5
    assert definition.ranking.tie_breakers[0].metric_id == "oey_ev_norm"
    assert definition.ranking.tie_breakers[1].direction == "ascending"
    assert definition.ranking.tie_breakers[2].metric_id == "canonical_symbol"


def test_ranking_metric_ids_preserve_first_seen_order():
    definition = load_screen(
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price.yml"
    )

    assert ranking_metric_ids(definition) == [
        "oey_ev_norm",
        "ev_to_ebit",
        "graham_multiplier",
        "roic_10y_median",
        "cfo_to_ni_ttm",
        "opm_10y_std",
        "net_debt_to_ebitda",
        "interest_coverage",
        "share_count_cagr_10y",
        "net_buyback_yield",
    ]
