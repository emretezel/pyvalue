"""Tests for screening configuration evaluation.

Author: Emre Tezel
"""

from pathlib import Path
from typing import Optional

import pytest

from pyvalue.screening import (
    Criterion,
    CriterionGroup,
    GroupEvaluation,
    Term,
    evaluate_criterion,
    evaluate_criterion_detail,
    evaluate_group,
    evaluate_group_detail,
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


def _sole(group: CriterionGroup) -> Criterion:
    """Return the single member of a one-member (bare-criterion) group.

    A bare screener criterion parses into a one-member group, so the shipped
    screeners' operators and terms live on ``group.members[0]``.
    """

    assert len(group.members) == 1
    return group.members[0]


def _evaluate_group(
    group: CriterionGroup,
    db: Path,
    symbol: str,
    metrics_repo: MetricsRepository,
) -> tuple[bool, Optional[float]]:
    """Resolve ``symbol`` then evaluate a group by natural identity (as the CLI does)."""

    listing_id = resolve_listing_id(db, symbol)
    assert listing_id is not None
    return evaluate_group(group, listing_id, metrics_repo, display_symbol=symbol)


def _group_detail(
    group: CriterionGroup,
    db: Path,
    symbol: str,
    metrics_repo: MetricsRepository,
) -> GroupEvaluation:
    """Resolve ``symbol`` then return the full per-member group evaluation."""

    listing_id = resolve_listing_id(db, symbol)
    assert listing_id is not None
    return evaluate_group_detail(group, listing_id, metrics_repo, display_symbol=symbol)


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


def test_screen_metric_ids_dedupes_metrics_in_first_seen_order(
    tmp_path: Path,
) -> None:
    # Inline screen: eps_ttm repeats across criteria and metrics also appear
    # as right-hand terms — the id list must keep first-seen order and include
    # each metric exactly once. (The checked-in screeners never repeat a
    # metric, so the dedupe semantics are pinned here instead.)
    screen_path = tmp_path / "dedupe.yml"
    screen_path.write_text(
        """
criteria:
  - name: "EPS positive"
    left:
      metric: eps_ttm
    operator: ">"
    right:
      value: 0
  - name: "Debt covered by working capital"
    left:
      metric: long_term_debt
    operator: "<="
    right:
      metric: working_capital
      multiplier: 1.75
  - name: "EPS above per-share book floor"
    left:
      metric: eps_ttm
    operator: ">="
    right:
      metric: book_value_per_share
""",
        encoding="utf-8",
    )

    definition = load_screen(screen_path)

    assert screen_metric_ids(definition) == [
        "eps_ttm",
        "long_term_debt",
        "working_capital",
        "book_value_per_share",
    ]


def test_load_screen_parses_metric_vs_metric_multiplier(tmp_path: Path) -> None:
    # Pins loader behaviours the checked-in screeners do not exercise: a
    # metric-vs-metric criterion with a right-hand multiplier and the strict
    # ">" operator (Graham's working-capital debt cover, formerly in the
    # deleted basic_value.yml example).
    screen_path = tmp_path / "multiplier.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Current ratio floor"
    left:
      metric: current_ratio
    operator: ">="
    right:
      value: 1.25
  - name: "Earnings yield strictly positive"
    left:
      metric: earnings_yield
    operator: ">"
    right:
      value: 0
  - name: "Debt covered by working capital"
    left:
      metric: long_term_debt
    operator: "<="
    right:
      metric: working_capital
      multiplier: 1.75
""",
        encoding="utf-8",
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 3
    # Bare criteria parse into one-member groups; operator/terms live on the member.
    assert {_sole(group).operator for group in definition.criteria} == {
        ">",
        ">=",
        "<=",
    }
    assert _sole(definition.criteria[0]).left.metric == "current_ratio"
    assert _sole(definition.criteria[0]).right.value == 1.25
    assert _sole(definition.criteria[2]).left.metric == "long_term_debt"
    assert _sole(definition.criteria[2]).right.metric == "working_capital"
    assert _sole(definition.criteria[2]).right.multiplier == 1.75


def test_load_screen_parses_quality_reasonable_price_primary_example() -> None:
    screen_path = (
        Path(__file__).resolve().parents[1]
        / "screeners"
        / "quality_reasonable_price_primary.yml"
    )

    definition = load_screen(screen_path)

    # 14 groups: 12 bare hard filters plus two OR groups that recover genuine
    # quality/value a single-lens AND gate over-excluded -- "Debt service"
    # (interest coverage OR already-low leverage) and "Reasonable price"
    # (owner-earnings OR EBIT OR FCF yield on EV). The count is unchanged because
    # each OR replaced one former bare gate in place. Deliberately absent gates:
    # sbc_to_fcf (EODHD SBC coverage is too sparse/unreliable; dilution is
    # policed by share_count_cagr_5y), eps_streak (EODHD EPS is analyst-adjusted
    # epsActual; GAAP stability is the zero-loss-years gate), roic_7y_median
    # (mathematically implied by roic_years_above_12pct >= 7) and accruals_ratio
    # (implied by cfo_to_ni_ttm >= 0.90 for any ROA <= 100%). The "==" operator
    # is exercised by the zero-loss-years criterion.
    assert len(definition.criteria) == 14
    all_members = [member for group in definition.criteria for member in group.members]
    assert {member.operator for member in all_members} == {">=", "<=", "=="}
    assert not any(
        member.left.metric
        in {"sbc_to_fcf", "eps_streak", "roic_7y_median", "accruals_ratio"}
        for member in all_members
    )
    # Spot-check a percent bound, a count bound, and the equality criterion.
    assert _sole(definition.criteria[0]).left.metric == "roic_years_above_12pct"
    assert _sole(definition.criteria[0]).right.value == 7
    assert _sole(definition.criteria[1]).left.metric == "roic_10y_min"
    assert _sole(definition.criteria[1]).right.value == 0.07
    assert _sole(definition.criteria[9]).left.metric == "ni_loss_years_10y"
    assert _sole(definition.criteria[9]).operator == "=="
    assert _sole(definition.criteria[9]).right.value == 0
    assert _sole(definition.criteria[12]).left.metric == "iroic_5y"
    assert _sole(definition.criteria[13]).left.metric == "owner_earnings_cagr_10y"

    # Debt-service OR (index 8): interest coverage >= 6x, or already-low leverage
    # (net debt / EBITDA <= 1.5x). min_pass == 1 (OR). The standalone
    # net_debt_to_ebitda <= 2.5x hard gate (index 7) still caps every listing, so
    # this only relaxes the 6x coverage demand for names below 1.5x leverage.
    assert _sole(definition.criteria[7]).left.metric == "net_debt_to_ebitda"
    assert _sole(definition.criteria[7]).right.value == 2.5
    debt_service = definition.criteria[8]
    assert debt_service.min_pass == 1
    assert [member.left.metric for member in debt_service.members] == [
        "interest_coverage",
        "net_debt_to_ebitda",
    ]
    assert [member.operator for member in debt_service.members] == [">=", "<="]
    assert [member.right.value for member in debt_service.members] == [6, 1.5]

    # Reasonable-price OR (index 11): owner-earnings, EBIT, or FCF yield on EV;
    # any one yield proves a reasonable price (min_pass == 1).
    reasonable_price = definition.criteria[11]
    assert reasonable_price.min_pass == 1
    assert [member.left.metric for member in reasonable_price.members] == [
        "oey_ev_norm",
        "ebit_yield_ev",
        "fcf_yield_ev",
    ]
    assert [member.operator for member in reasonable_price.members] == [
        ">=",
        ">=",
        ">=",
    ]
    assert [member.right.value for member in reasonable_price.members] == [
        0.05,
        0.0667,
        0.05,
    ]

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

    # 7 groups: three bare structural-quality floors (adaptive ROIC, cash
    # conversion, loss-year share), a >= 3-of-4 solvency scorecard, the Piotroski
    # floor, a >= 1-of-3 valuation OR (cheap on book, EV/EBIT, or FCF yield), and
    # the market-cap investability floor. Strict ">" is exercised here, which the
    # QARP screen does not use. The scorecard replaced four former bare distress
    # gates (leverage, coverage, Altman, FCF/EBITDA); the valuation OR replaced
    # the former bare price-to-book gate.
    assert len(definition.criteria) == 7
    all_members = [member for group in definition.criteria for member in group.members]
    assert {member.operator for member in all_members} == {">", ">=", "<="}
    # Bare structural floors. The adaptive ROIC median's 6-FY evidence bar
    # matches the screen's other adaptive gates (the strict roic_7y_median needed
    # 7 FY years); QARP keeps the strict median. The loss-share gate's 0.40
    # preserves the old 4-of-10 tolerance while staying screenable on short
    # histories.
    assert _sole(definition.criteria[0]).left.metric == "roic_10y_median_adaptive"
    assert _sole(definition.criteria[0]).operator == ">"
    assert _sole(definition.criteria[0]).right.value == 0
    assert _sole(definition.criteria[1]).left.metric == "cfo_to_ni_10y_median"
    assert _sole(definition.criteria[2]).left.metric == "ni_loss_year_share"
    assert _sole(definition.criteria[2]).operator == "<="
    assert _sole(definition.criteria[2]).right.value == 0.40
    assert _sole(definition.criteria[4]).left.metric == "piotroski_f_score"
    assert _sole(definition.criteria[4]).right.value == 5

    # Solvency scorecard (index 3): >= 3 of 4 going-concern signals, so a
    # deep-value cyclical may trip one benign signal and still pass while genuine
    # distress (fails two or more) is excluded.
    solvency = definition.criteria[3]
    assert solvency.min_pass == 3
    assert len(solvency.members) == 4
    assert [member.left.metric for member in solvency.members] == [
        "net_debt_to_ebitda",
        "interest_coverage",
        "altman_z",
        "fcf_to_ebitda",
    ]
    assert [member.operator for member in solvency.members] == ["<=", ">=", ">=", ">"]
    assert [member.right.value for member in solvency.members] == [5.0, 1.5, 1.81, 0]

    # Valuation OR (index 5): cheap on book, operating earnings, or free cash
    # flow (min_pass == 1). price_to_tangible_book is deliberately not an arm --
    # it is always >= price_to_book, so it could never rescue a P/B miss.
    valuation = definition.criteria[5]
    assert valuation.min_pass == 1
    assert [member.left.metric for member in valuation.members] == [
        "price_to_book",
        "ev_to_ebit",
        "fcf_yield_ev",
    ]
    assert [member.operator for member in valuation.members] == ["<=", "<=", ">="]
    assert [member.right.value for member in valuation.members] == [3.0, 12, 0.06]

    # Market-cap floor (index 6): the first currency-tagged constant used by a
    # shipped screener, so pin both the value and the currency.
    assert _sole(definition.criteria[6]).left.metric == "market_cap"
    assert _sole(definition.criteria[6]).right.value == 150_000_000
    assert _sole(definition.criteria[6]).right.currency == "USD"

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


def test_ranking_metric_ids_preserve_first_seen_order(tmp_path: Path) -> None:
    # A ranking block that repeats a metric id must yield each id once, in
    # first-seen order; tie-breakers contribute no ids. (The checked-in
    # screeners never repeat a ranking metric, so the dedupe semantics are
    # pinned here instead.)
    screen_path = tmp_path / "ranking.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Owner-earnings yield floor"
    left:
      metric: oey_ev_norm
    operator: ">="
    right:
      value: 0.05

ranking:
  peer_group: sector
  min_sector_peers: 10
  metrics:
    - metric: oey_ev_norm
      weight: 0.4
      direction: higher
    - metric: ev_to_ebit
      weight: 0.3
      direction: lower
    - metric: oey_ev_norm
      weight: 0.3
      direction: higher
  tie_breakers:
    - metric: roic_7y_median
      direction: descending
""",
        encoding="utf-8",
    )

    definition = load_screen(screen_path)

    assert ranking_metric_ids(definition) == ["oey_ev_norm", "ev_to_ebit"]


# --------------------------------------------------------------------------- #
# Criterion groups: OR / K-of-N parsing and evaluation.
# --------------------------------------------------------------------------- #


def _metrics_repo(db: Path) -> MetricsRepository:
    """Initialise the facts + metrics schemas and return a ready metrics repo."""

    FinancialFactsRepository(db).initialize_schema()
    repo = MetricsRepository(db)
    repo.initialize_schema()
    return repo


def _debt_service_group(min_pass: int = 1) -> CriterionGroup:
    """A two-arm debt-service group: interest coverage OR clean leverage.

    This is the canonical substitution case -- a debt-free issuer with no interest
    line can still clear the group on ``net_debt_to_ebitda``.
    """

    return CriterionGroup(
        name="Debt-service capacity",
        members=(
            Criterion(
                name="Interest coverage >= 6x",
                left=Term(metric="interest_coverage"),
                operator=">=",
                right=Term(value=6.0),
            ),
            Criterion(
                name="Net debt / EBITDA <= 2.5x",
                left=Term(metric="net_debt_to_ebitda"),
                operator="<=",
                right=Term(value=2.5),
            ),
        ),
        min_pass=min_pass,
    )


def test_load_screen_parses_any_of_group(tmp_path: Path) -> None:
    # A screen mixing a bare criterion, an OR group (default at_least), and a
    # K-of-N scorecard. Group names are the reportable units; screen_metric_ids
    # must union every arm's metrics in first-seen order.
    screen_path = tmp_path / "groups.yml"
    screen_path.write_text(
        """
criteria:
  - name: "ROIC floor"
    left:
      metric: roic_10y_min
    operator: ">="
    right:
      value: 0.07
  - name: "Debt-service capacity"
    any_of:
      - name: "Interest coverage >= 6x"
        left:
          metric: interest_coverage
        operator: ">="
        right:
          value: 6
      - name: "Net debt / EBITDA <= 2.5x"
        left:
          metric: net_debt_to_ebitda
        operator: "<="
        right:
          value: 2.5
  - name: "Quality scorecard (>=2 of 3)"
    at_least: 2
    any_of:
      - name: "GM"
        left:
          metric: gross_margin_ttm
        operator: ">="
        right:
          value: 0.35
      - name: "CFO/NI"
        left:
          metric: cfo_to_ni_ttm
        operator: ">="
        right:
          value: 0.9
      - name: "F-score"
        left:
          metric: piotroski_f_score
        operator: ">="
        right:
          value: 6
""",
        encoding="utf-8",
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 3
    bare, or_group, scorecard = definition.criteria

    assert bare.name == "ROIC floor"
    assert len(bare.members) == 1
    assert bare.min_pass == 1

    assert or_group.name == "Debt-service capacity"
    assert or_group.min_pass == 1  # OR is the default
    assert [member.left.metric for member in or_group.members] == [
        "interest_coverage",
        "net_debt_to_ebitda",
    ]

    assert scorecard.min_pass == 2
    assert len(scorecard.members) == 3

    # The metric-id union walks every member of every group, first-seen order.
    assert screen_metric_ids(definition) == [
        "roic_10y_min",
        "interest_coverage",
        "net_debt_to_ebitda",
        "gross_margin_ttm",
        "cfo_to_ni_ttm",
        "piotroski_f_score",
    ]


def test_load_screen_wraps_bare_criterion_as_one_member_group(tmp_path: Path) -> None:
    # Backward compatibility: a bare criterion becomes a one-member group whose
    # name is the criterion's name, so pre-group screeners parse unchanged.
    screen_path = tmp_path / "bare.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Current ratio floor"
    left:
      metric: current_ratio
    operator: ">="
    right:
      value: 1.25
""",
        encoding="utf-8",
    )

    definition = load_screen(screen_path)

    assert len(definition.criteria) == 1
    group = definition.criteria[0]
    assert group.name == "Current ratio floor"
    assert group.min_pass == 1
    assert _sole(group).left.metric == "current_ratio"


def test_load_screen_rejects_duplicate_group_names(tmp_path: Path) -> None:
    # Names are CSV columns / fallout labels, so duplicates would silently
    # collide in the per-group value dict -- reject them at parse time.
    screen_path = tmp_path / "dupe.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Same"
    left:
      metric: current_ratio
    operator: ">="
    right:
      value: 1
  - name: "Same"
    left:
      metric: earnings_yield
    operator: ">"
    right:
      value: 0
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate"):
        load_screen(screen_path)


def test_load_screen_rejects_at_least_out_of_range(tmp_path: Path) -> None:
    # at_least cannot exceed the member count (an unsatisfiable group).
    screen_path = tmp_path / "bad_at_least.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Impossible"
    at_least: 3
    any_of:
      - name: "A"
        left:
          metric: gross_margin_ttm
        operator: ">="
        right:
          value: 0.35
      - name: "B"
        left:
          metric: cfo_to_ni_ttm
        operator: ">="
        right:
          value: 0.9
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="at_least"):
        load_screen(screen_path)


def test_load_screen_rejects_group_without_name(tmp_path: Path) -> None:
    # An explicit group's name is its output column, so it is required.
    screen_path = tmp_path / "no_name.yml"
    screen_path.write_text(
        """
criteria:
  - any_of:
      - name: "A"
        left:
          metric: gross_margin_ttm
        operator: ">="
        right:
          value: 0.35
      - name: "B"
        left:
          metric: cfo_to_ni_ttm
        operator: ">="
        right:
          value: 0.9
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="name"):
        load_screen(screen_path)


def test_load_screen_rejects_empty_any_of(tmp_path: Path) -> None:
    screen_path = tmp_path / "empty_any_of.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Empty"
    any_of: []
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="any_of"):
        load_screen(screen_path)


def test_evaluate_group_or_passes_when_any_member_passes(tmp_path: Path) -> None:
    db = tmp_path / "group_or_pass.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # Coverage is weak (fails >= 6) but leverage is clean (passes <= 2.5): the OR
    # group still passes on its second arm.
    seed_metric(db, "AAPL.US", "interest_coverage", 3.0, "2023-12-31")
    seed_metric(db, "AAPL.US", "net_debt_to_ebitda", 1.0, "2023-12-31")

    passed, reported = _evaluate_group(
        _debt_service_group(), db, "AAPL.US", metrics_repo
    )

    assert passed is True
    # Reported value is the first passing arm's left value -- the leverage arm here.
    assert reported == 1.0


def test_evaluate_group_or_fails_when_all_members_fail(tmp_path: Path) -> None:
    db = tmp_path / "group_or_fail.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # Both arms have data and both miss their bars -> a genuine threshold fail.
    seed_metric(db, "AAPL.US", "interest_coverage", 3.0, "2023-12-31")
    seed_metric(db, "AAPL.US", "net_debt_to_ebitda", 4.0, "2023-12-31")

    passed, reported = _evaluate_group(
        _debt_service_group(), db, "AAPL.US", metrics_repo
    )
    assert passed is False
    assert reported is None

    detail = _group_detail(_debt_service_group(), db, "AAPL.US", metrics_repo)
    assert detail.passed is False
    assert detail.pass_count == 0
    assert detail.failure_kind == "comparison_failed"


def test_evaluate_group_k_of_n_requires_min_pass(tmp_path: Path) -> None:
    db = tmp_path / "group_kofn.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # Two of three arms pass.
    seed_metric(db, "AAPL.US", "interest_coverage", 8.0, "2023-12-31")  # passes >= 6
    seed_metric(db, "AAPL.US", "net_debt_to_ebitda", 1.0, "2023-12-31")  # passes <= 2.5
    seed_metric(db, "AAPL.US", "current_ratio", 0.5, "2023-12-31")  # fails >= 1.5

    members = (
        Criterion(
            name="Interest coverage >= 6x",
            left=Term(metric="interest_coverage"),
            operator=">=",
            right=Term(value=6.0),
        ),
        Criterion(
            name="Net debt / EBITDA <= 2.5x",
            left=Term(metric="net_debt_to_ebitda"),
            operator="<=",
            right=Term(value=2.5),
        ),
        Criterion(
            name="Current ratio >= 1.5x",
            left=Term(metric="current_ratio"),
            operator=">=",
            right=Term(value=1.5),
        ),
    )

    # 2 of 3 passing clears an at_least=2 gate...
    two_of_three = CriterionGroup(name="Scorecard", members=members, min_pass=2)
    assert _evaluate_group(two_of_three, db, "AAPL.US", metrics_repo)[0] is True

    # ...but not an at_least=3 (all) gate, since the current-ratio arm fails.
    all_three = CriterionGroup(name="Scorecard", members=members, min_pass=3)
    assert _evaluate_group(all_three, db, "AAPL.US", metrics_repo)[0] is False


def test_evaluate_group_detail_na_coverage_not_blamed(tmp_path: Path) -> None:
    db = tmp_path / "group_na_coverage.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # The coverage arm's metric is absent (NA), but the leverage arm passes: a
    # debt-free issuer clears the group without interest_coverage ever computing.
    seed_metric(db, "AAPL.US", "net_debt_to_ebitda", 1.0, "2023-12-31")

    detail = _group_detail(_debt_service_group(), db, "AAPL.US", metrics_repo)

    assert detail.passed is True
    assert detail.failure_kind is None
    assert detail.reported_value == 1.0


def test_evaluate_group_detail_threshold_failure_beats_missing_arm(
    tmp_path: Path,
) -> None:
    db = tmp_path / "group_threshold_over_na.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # Coverage is NA, leverage has data and genuinely misses the bar. The group
    # fails, but the failure is a real threshold miss -- not NA-blocked -- so the
    # missing coverage metric is NOT blamed for the exclusion (the coverage payoff).
    seed_metric(db, "AAPL.US", "net_debt_to_ebitda", 4.0, "2023-12-31")

    detail = _group_detail(_debt_service_group(), db, "AAPL.US", metrics_repo)

    assert detail.passed is False
    assert detail.failure_kind == "comparison_failed"


def test_evaluate_group_detail_all_arms_na_is_na_blocked(tmp_path: Path) -> None:
    db = tmp_path / "group_all_na.db"
    metrics_repo = _metrics_repo(db)
    _seed_listing(db, "AAPL.US")
    # Neither arm's metric was computed: the group is genuinely un-evaluable, so
    # both missing metrics are attributed to NA fallout.
    detail = _group_detail(_debt_service_group(), db, "AAPL.US", metrics_repo)

    assert detail.passed is False
    assert detail.failure_kind == "na_blocked"
    assert set(detail.missing_metric_ids) == {
        "interest_coverage",
        "net_debt_to_ebitda",
    }
