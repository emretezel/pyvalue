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
from pyvalue.persistence.storage import (
    FXRateRecord,
    FXRatesRepository,
    FinancialFactsRepository,
    MetricsRepository,
    SupportedTickerRepository,
)

from conftest import resolve_listing_id, seed_exchange, seed_metric


def _evaluate(
    criterion: Criterion,
    db: Path,
    symbol: str,
    metrics_repo: MetricsRepository,
) -> bool:
    """Resolve ``symbol`` to its listing_id, then evaluate by natural identity.

    The screen evaluator keys on listing_id; the canonical symbol is only a log
    label. Tests address securities by symbol, so this resolves the id at the
    boundary exactly as the CLI does before evaluating.
    """

    listing_id = resolve_listing_id(db, symbol)
    assert listing_id is not None
    return evaluate_criterion(
        criterion, listing_id, metrics_repo, display_symbol=symbol
    )


def _seed_listing(db_path: Path, symbol: str, *, currency: str = "USD") -> None:
    """Catalog ``symbol`` carrying a quote ``currency`` before metrics land.

    ``listing.currency`` is NOT NULL with no fallback, so writing a metric for
    an uncataloged symbol would otherwise raise ``ValueError`` when the metrics
    repo tries to create the listing. Seeding a cataloged listing here gives the
    listing its currency up front; the per-metric ``currency`` kwargs used in
    the cross-currency tests are independent of the listing currency.
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


def test_evaluate_criterion_uses_metrics_repo(tmp_path: Path) -> None:
    db = tmp_path / "test.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    _seed_listing(db, "AAPL.US")
    seed_metric(db, "AAPL.US", "working_capital", 100.0, "2023-09-30")
    seed_metric(db, "AAPL.US", "long_term_debt", 150.0, "2023-09-30")

    criterion = Criterion(
        name="Debt vs WC",
        left=Term(metric="long_term_debt"),
        operator="<=",
        right=Term(metric="working_capital", multiplier=1.75),
    )

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is True


def test_evaluate_criterion_filters_when_metric_missing(tmp_path: Path) -> None:
    db = tmp_path / "missing_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    # Seed the listing (but not the metric) so the id resolves; the criterion
    # still fails because the metric value is absent.
    _seed_listing(db, "AAPL.US")

    criterion = Criterion(
        name="requires metric",
        left=Term(metric="working_capital"),
        operator=">",
        right=Term(value=0.0),
    )

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is False


def test_evaluate_criterion_supports_constant_terms(tmp_path: Path) -> None:
    db = tmp_path / "test2.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    _seed_listing(db, "AAPL.US")
    seed_metric(db, "AAPL.US", "earnings_yield", 0.05, "2023-09-30")

    criterion = Criterion(
        name="Positive earnings yield",
        left=Term(metric="earnings_yield"),
        operator=">",
        right=Term(value=0.0),
    )

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is True


def test_evaluate_criterion_converts_monetary_constant_currency(tmp_path: Path) -> None:
    db = tmp_path / "screen_fx_constant.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    _seed_listing(db, "AAPL.US")
    seed_metric(
        db,
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
            rate=0.8,
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

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is True


def test_evaluate_criterion_converts_mixed_currency_metrics(tmp_path: Path) -> None:
    db = tmp_path / "screen_fx_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    _seed_listing(db, "AAPL.US")
    seed_metric(
        db,
        "AAPL.US",
        "long_term_debt",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="USD",
    )
    seed_metric(
        db,
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
            rate=0.9,
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

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is True


def test_evaluate_criterion_normalizes_configured_subunit_metric_currencies(
    tmp_path: Path,
) -> None:
    db = tmp_path / "screen_subunit_metric.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()
    _seed_listing(db, "AAPL.US")
    seed_metric(
        db,
        "AAPL.US",
        "long_term_debt",
        100.0,
        "2023-12-31",
        unit_kind="monetary",
        currency="ZAC",
    )
    seed_metric(
        db,
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
            rate=0.2,
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

    assert _evaluate(criterion, db, "AAPL.US", metrics_repo) is True


def test_evaluate_criterion_detail_reports_missing_metric_ids(tmp_path: Path) -> None:
    db = tmp_path / "detail_missing.db"
    fact_repo = FinancialFactsRepository(db)
    fact_repo.initialize_schema()
    metrics_repo = MetricsRepository(db)
    metrics_repo.initialize_schema()

    _seed_listing(db, "AAPL.US")
    listing_id = resolve_listing_id(db, "AAPL.US")
    assert listing_id is not None

    criterion = Criterion(
        name="requires two metrics",
        left=Term(metric="working_capital"),
        operator=">=",
        right=Term(metric="current_ratio"),
    )

    result = evaluate_criterion_detail(
        criterion,
        listing_id,
        metrics_repo,
        display_symbol="AAPL.US",
        log_missing_metrics=False,
    )

    assert result.passed is False
    assert result.failure_kind == "both_missing"
    assert result.missing_metric_ids == ("working_capital", "current_ratio")


def test_screen_metric_ids_dedupes_metrics_in_first_seen_order() -> None:
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


def test_load_screen_parses_basic_value_example() -> None:
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


def test_load_screen_parses_value_normalized_example() -> None:
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


def test_load_screen_parses_quality_reasonable_price_example() -> None:
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
    assert definition.criteria[0].left.metric == "roic_7y_median"
    assert definition.criteria[1].left.metric == "opm_7y_min"
    assert definition.criteria[2].left.metric == "net_debt_to_ebitda"
    assert definition.criteria[3].left.metric == "cfo_to_ni_ttm"
    assert definition.criteria[4].left.metric == "oey_ev_norm"
    assert definition.criteria[5].left.metric == "share_count_cagr_5y"
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


def test_load_screen_parses_quality_reasonable_price_primary_example() -> None:
    screen_path = (
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price_primary.yml"
    )

    definition = load_screen(screen_path)

    # 18 hard filters: the 6-filter draft plus the reinvestment, gross-margin,
    # and full-cycle earnings-quality legs. The "==" operator is exercised by the
    # zero-loss-years criterion, which the original draft did not use.
    assert len(definition.criteria) == 18
    assert {criterion.operator for criterion in definition.criteria} == {
        ">=",
        "<=",
        "==",
    }
    # Spot-check a percent bound, a count bound, and the equality criterion.
    assert definition.criteria[0].left.metric == "roic_7y_median"
    assert definition.criteria[0].right.value == 0.12
    assert definition.criteria[1].left.metric == "roic_years_above_12pct"
    assert definition.criteria[1].right.value == 7
    assert definition.criteria[11].left.metric == "ni_loss_years_10y"
    assert definition.criteria[11].operator == "=="
    assert definition.criteria[11].right.value == 0
    assert definition.criteria[16].left.metric == "iroic_5y"
    assert definition.criteria[17].left.metric == "owner_earnings_cagr_10y"

    # Ranking block: same sector-relative shape as the draft, but a seven-metric
    # blend grouped by bucket -- quality/capital-efficiency 35% (roic_7y_median,
    # iroic_5y, gross_profit_to_assets_ttm), valuation 25% (oey_ev_norm,
    # ev_to_ebit), capital allocation 20% (shareholder_yield_ttm), earnings
    # stability 20% (opm_10y_std) -- with an iroic_5y cap.
    assert definition.ranking is not None
    assert definition.ranking.peer_group == "sector"
    assert definition.ranking.min_sector_peers == 10
    assert definition.ranking.winsor_lower_percentile == 0.05
    assert definition.ranking.winsor_upper_percentile == 0.95
    assert len(definition.ranking.metrics) == 7
    assert definition.ranking.metrics[0].metric_id == "roic_7y_median"
    assert definition.ranking.metrics[0].weight == 0.15
    assert definition.ranking.metrics[0].direction == "higher"
    assert definition.ranking.metrics[1].metric_id == "iroic_5y"
    assert definition.ranking.metrics[1].weight == 0.12
    assert definition.ranking.metrics[1].direction == "higher"
    assert definition.ranking.metrics[1].cap == 0.50
    assert definition.ranking.metrics[4].metric_id == "ev_to_ebit"
    assert definition.ranking.metrics[4].direction == "lower"
    assert definition.ranking.metrics[5].metric_id == "shareholder_yield_ttm"
    assert definition.ranking.metrics[5].weight == 0.20
    assert definition.ranking.metrics[6].metric_id == "opm_10y_std"
    assert definition.ranking.metrics[6].weight == 0.20
    assert definition.ranking.metrics[6].direction == "lower"
    # Weights sum to 1.00 across the four buckets (float-safe comparison).
    assert abs(sum(m.weight for m in definition.ranking.metrics) - 1.0) < 1e-9
    assert len(definition.ranking.tie_breakers) == 3
    assert definition.ranking.tie_breakers[0].metric_id == "oey_ev_norm"
    assert definition.ranking.tie_breakers[0].direction == "descending"
    assert definition.ranking.tie_breakers[2].metric_id == "canonical_symbol"


def test_load_screen_parses_deep_value_graham_example() -> None:
    screen_path = (
        Path(__file__).resolve().parents[1] / "screeners" / "deep_value_graham.yml"
    )

    definition = load_screen(screen_path)

    # 10 loose gates: structural-quality floors plus the composite-score and
    # investability criteria. Strict ">" is exercised here, which the QARP
    # screens do not use.
    assert len(definition.criteria) == 10
    assert {criterion.operator for criterion in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    # Spot-check the composite-score gates and the monetary constant. The
    # market-cap floor is the first currency-tagged constant used by a shipped
    # screener, so pin both the value and the currency.
    assert definition.criteria[0].left.metric == "roic_7y_median"
    assert definition.criteria[0].operator == ">"
    assert definition.criteria[0].right.value == 0
    # The stability gate uses the adaptive share metric so short-history
    # listings stay screenable; 0.40 preserves the old 4-of-10 tolerance.
    assert definition.criteria[4].left.metric == "ni_loss_year_share"
    assert definition.criteria[4].operator == "<="
    assert definition.criteria[4].right.value == 0.40
    assert definition.criteria[5].left.metric == "piotroski_f_score"
    assert definition.criteria[5].right.value == 5
    assert definition.criteria[6].left.metric == "altman_z"
    assert definition.criteria[6].right.value == 1.81
    assert definition.criteria[9].left.metric == "market_cap"
    assert definition.criteria[9].right.value == 150_000_000
    assert definition.criteria[9].right.currency == "USD"

    # Ranking block: cheapness-weighted blend -- valuation 45% (price_to_book,
    # ev_to_sales, ev_to_ebit), capital efficiency 25% (croic, roce, both
    # capped), composite health 30% (piotroski_f_score, altman_z).
    assert definition.ranking is not None
    assert definition.ranking.peer_group == "sector"
    assert definition.ranking.min_sector_peers == 10
    assert definition.ranking.winsor_lower_percentile == 0.05
    assert definition.ranking.winsor_upper_percentile == 0.95
    assert len(definition.ranking.metrics) == 7
    assert definition.ranking.metrics[0].metric_id == "price_to_book"
    assert definition.ranking.metrics[0].weight == 0.15
    assert definition.ranking.metrics[0].direction == "lower"
    assert definition.ranking.metrics[3].metric_id == "croic"
    assert definition.ranking.metrics[3].direction == "higher"
    assert definition.ranking.metrics[3].cap == 0.75
    assert definition.ranking.metrics[4].metric_id == "roce"
    assert definition.ranking.metrics[4].cap == 0.75
    assert definition.ranking.metrics[6].metric_id == "altman_z"
    assert definition.ranking.metrics[6].weight == 0.20
    assert definition.ranking.metrics[6].direction == "higher"
    # Weights sum to 1.00 across the three buckets (float-safe comparison).
    assert abs(sum(m.weight for m in definition.ranking.metrics) - 1.0) < 1e-9
    assert len(definition.ranking.tie_breakers) == 3
    assert definition.ranking.tie_breakers[0].metric_id == "altman_z"
    assert definition.ranking.tie_breakers[0].direction == "descending"
    assert definition.ranking.tie_breakers[1].metric_id == "price_to_book"
    assert definition.ranking.tie_breakers[1].direction == "ascending"
    assert definition.ranking.tie_breakers[2].metric_id == "canonical_symbol"


def test_ranking_metric_ids_preserve_first_seen_order() -> None:
    definition = load_screen(
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price.yml"
    )

    assert ranking_metric_ids(definition) == [
        "oey_ev_norm",
        "ev_to_ebit",
        "graham_multiplier",
        "roic_7y_median",
        "cfo_to_ni_ttm",
        "opm_10y_std",
        "net_debt_to_ebitda",
        "interest_coverage",
        "share_count_cagr_5y",
        "net_buyback_yield",
    ]
