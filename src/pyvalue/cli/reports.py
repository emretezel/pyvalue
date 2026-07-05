"""CLI handlers for coverage/freshness/failure reports and metric recomputation.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import time
from collections import Counter
from dataclasses import dataclass
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.metrics import REGISTRY
from pyvalue.metrics.utils import is_recent_date
from pyvalue.reporting import MetricCoverage, compute_fact_coverage
from pyvalue.screening import (
    CriterionEvaluation,
    evaluate_criterion_detail,
    load_screen,
    screen_metric_ids,
)
from pyvalue.logging_utils import (
    suppress_console_metric_warnings,
)
from pyvalue.facts import RegionFactsRepository
from pyvalue.persistence.storage import (
    FinancialFactsRepository,
    MarketDataRepository,
    MetricComputeStatusRepository,
    MetricFailureReasonAggregate,
    MetricsRepository,
)

from ._common import (
    METRICS_COMPUTE_BATCH_SIZE,
    SCREEN_PROGRESS_INTERVAL_SECONDS,
    _CriterionFailureSummary,
    _MetricAttemptResult,
    _ScreenMetricImpactSummary,
    _prepare_output_csv_path,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
    _scope_label,
    _select_metric_classes,
)
from ._failure_analysis import (
    FailureTally,
    _FailureExample,
    _estimate_market_caps,
    _persist_metric_attempts,
    classify_availability_state,
    failure_example_console_suffix,
    failure_example_csv_cells,
)
from ._repos import (
    _PreloadedMetricsRepository,
    _SchemaReadyFinancialFactsRefreshStateRepository,
    _SchemaReadyFinancialFactsRepository,
    _SchemaReadyMarketDataRepository,
    _SchemaReadyMetricComputeStatusRepository,
    _SchemaReadyMetricsRepository,
    _StatusAwareMetricsRepository,
)
from .metrics import (
    _batch_listings,
    _compute_metric_batch_results,
    _initialize_metric_read_schema,
)


def _print_screen_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for screen-evaluation progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        f"Progress: [{bar}] {completed_symbols}/{total_symbols} symbols screened ({percent:.1f}%)",
        flush=True,
    )


def cmd_report_fact_freshness(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
    max_age_days: int,
    output_csv: Optional[str],
    show_all: bool,
) -> int:
    """Report missing or stale financial facts needed by metrics for a canonical scope."""

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    # The coverage report counts per metric/concept only; the scope listing ids
    # ride straight into the bulk fact read with no symbol resolution.
    listing_ids = [listing_id for listing_id, _ in scope_listings]

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    coverage = compute_fact_coverage(
        fact_repo,
        listing_ids,
        metric_classes,
        max_age_days=max_age_days,
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )

    print(
        f"Fact coverage for {scope_label} "
        f"({len(listing_ids)} symbols, max_age_days={max_age_days})"
    )
    for entry in coverage:
        missing_total = sum(c.missing for c in entry.concepts)
        stale_total = sum(c.stale for c in entry.concepts)
        print(
            f"- {entry.metric_id}: fully_fresh={entry.fully_covered}/{entry.total_symbols}, "
            f"missing={missing_total}, stale={stale_total}"
        )
        for concept in entry.concepts:
            if not show_all and concept.missing == 0 and concept.stale == 0:
                continue
            fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
            print(
                f"    {concept.concept}: fresh={fresh}, stale={concept.stale}, missing={concept.missing}"
            )

    # Concept coverage alone misses NAs caused by the market-data seam: any
    # uses_market_data metric also needs a stored price snapshot. Summarize that
    # side in one line whenever the selection includes such a metric.
    market_metric_count = sum(
        1
        for metric_cls in metric_classes
        if getattr(metric_cls, "uses_market_data", False)
    )
    if market_metric_count:
        snapshots_by_id = MarketDataRepository(db_path).latest_snapshots_many_by_ids(
            listing_ids
        )
        missing_snapshots = sum(
            1 for listing_id in listing_ids if listing_id not in snapshots_by_id
        )
        stale_snapshots = sum(
            1
            for listing_id in listing_ids
            if listing_id in snapshots_by_id
            and not is_recent_date(
                snapshots_by_id[listing_id].as_of, max_age_days=max_age_days
            )
        )
        fresh_snapshots = len(listing_ids) - missing_snapshots - stale_snapshots
        print(
            f"Market-data seam ({market_metric_count} selected metrics use price "
            f"snapshots): fresh={fresh_snapshots}/{len(listing_ids)}, "
            f"stale={stale_snapshots}, missing={missing_snapshots}"
        )

    if output_csv:
        _write_fact_report_csv(coverage, output_csv)
        print(f"Wrote concept-level coverage to {output_csv}")
    return 0


@dataclass(frozen=True)
class _MetricStatusSummary:
    """One metric's persisted-status tallies over the requested scope."""

    metric_id: str
    successes: int
    failures: int
    never_attempted: int
    na_share: float


def _write_metric_status_report_csv(
    summaries: Sequence[_MetricStatusSummary],
    reasons_by_metric: Mapping[str, Sequence[MetricFailureReasonAggregate]],
    symbol_by_id: Mapping[int, str],
    total_symbols: int,
    path: str,
) -> None:
    """Write metric status rows; with reasons, one row per (metric, reason).

    A single column schema for both verbosities: without ``--reasons`` each
    metric gets one row with empty reason cells; with ``--reasons`` the summary
    cells repeat on every reason row (mirroring the failure-report CSV style)
    so the file stays trivially filterable per metric.
    """

    target = _prepare_output_csv_path(path)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "total_symbols",
                "successes",
                "failures",
                "never_attempted",
                "na_share",
                "reason",
                "reason_count",
                "example_symbol",
            ]
        )
        for summary in summaries:
            base = [
                summary.metric_id,
                str(total_symbols),
                str(summary.successes),
                str(summary.failures),
                str(summary.never_attempted),
                f"{summary.na_share:.4f}",
            ]
            reasons = reasons_by_metric.get(summary.metric_id, [])
            if not reasons:
                writer.writerow(base + ["", "", ""])
                continue
            for reason in reasons:
                example_symbol = symbol_by_id.get(
                    reason.example_listing_id,
                    f"listing_id={reason.example_listing_id}",
                )
                writer.writerow(
                    base
                    + [
                        reason.reason_code or "(no reason recorded)",
                        str(reason.count),
                        example_symbol,
                    ]
                )


def cmd_report_metric_status(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
    config_path: Optional[str],
    show_reasons: bool,
    output_csv: Optional[str],
) -> int:
    """Summarize persisted metric compute status (NA rates, reasons) for a scope.

    A pure read of ``metric_compute_status`` -- nothing is recomputed and
    nothing is written -- so ranking a screen's metrics by NA share is cheap
    even at full-universe scale, but only as fresh as the last compute or
    report backfill. ``never_attempted`` counts scope listings with no
    persisted attempt at all for the metric, which is how a newly registered
    metric looks before its first ``compute-metrics`` run.
    """

    # --metrics and --config both select the metric set; accepting both would
    # silently prefer one, so reject the ambiguity outright.
    if metric_ids and config_path:
        raise SystemExit("Pass either --metrics or --config, not both.")

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    listing_ids = [listing_id for listing_id, _ in scope_listings]
    # The scope resolver already pairs each listing_id with its canonical
    # symbol, so failure examples resolve to display symbols with no DB lookup.
    symbol_by_id = {listing_id: symbol for listing_id, symbol in scope_listings}

    if config_path:
        # Screen scope: the criteria metrics are exactly the ones whose NA
        # excludes a symbol from the screen, so they are the set worth ranking.
        definition = load_screen(config_path)
        selected_metric_ids = screen_metric_ids(definition)
        # Fail loudly on a YAML typo: every screen metric must be registered.
        _select_metric_classes(selected_metric_ids)
    else:
        selected_metric_ids = [
            getattr(cls, "id", cls.__name__)
            for cls in _select_metric_classes(metric_ids)
        ]

    status_repo = MetricComputeStatusRepository(db_path)
    aggregates = status_repo.count_statuses_by_metric(listing_ids, selected_metric_ids)
    reasons_by_metric: Dict[str, List[MetricFailureReasonAggregate]] = {}
    if show_reasons:
        reasons_by_metric = status_repo.count_failure_reasons_by_metric(
            listing_ids, selected_metric_ids
        )

    total_symbols = len(scope_listings)
    summaries: List[_MetricStatusSummary] = []
    for metric_id in selected_metric_ids:
        aggregate = aggregates.get(metric_id)
        successes = aggregate.successes if aggregate else 0
        failures = aggregate.failures if aggregate else 0
        never_attempted = max(total_symbols - successes - failures, 0)
        # NA share is what a screen effectively sees: every scope listing whose
        # last attempt failed or that was never attempted has no usable value.
        na_share = (
            (failures + never_attempted) / total_symbols if total_symbols else 0.0
        )
        summaries.append(
            _MetricStatusSummary(
                metric_id=metric_id,
                successes=successes,
                failures=failures,
                never_attempted=never_attempted,
                na_share=na_share,
            )
        )
    # Worst NA share first so the screen-killing metrics lead the report.
    summaries.sort(key=lambda s: (-s.na_share, -s.failures, s.metric_id))

    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Metric status for {scope_label} "
        f"(symbols={total_symbols}, metrics={len(selected_metric_ids)}; "
        "persisted state only)"
    )
    for summary in summaries:
        print(
            f"- {summary.metric_id}: na_share={summary.na_share * 100:.1f}% "
            f"(failures={summary.failures}, "
            f"never_attempted={summary.never_attempted}, "
            f"successes={summary.successes} of {total_symbols})"
        )
        for reason in reasons_by_metric.get(summary.metric_id, []):
            example_symbol = symbol_by_id.get(
                reason.example_listing_id,
                f"listing_id={reason.example_listing_id}",
            )
            reason_label = reason.reason_code or "(no reason recorded)"
            print(f"    {reason_label}: {reason.count} (example={example_symbol})")
    if any(summary.never_attempted for summary in summaries):
        print(
            "Note: never_attempted = no persisted attempt for the metric; "
            "run compute-metrics to populate."
        )
    if output_csv:
        _write_metric_status_report_csv(
            summaries,
            reasons_by_metric,
            symbol_by_id,
            total_symbols,
            output_csv,
        )
        print(f"Wrote metric status summary to {output_csv}")
    return 0


def _write_metric_failure_report_csv(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, _FailureExample]],
    total_symbols: int,
    metric_order: Sequence[str],
    path: str,
) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "reason",
                "count",
                "total_symbols",
                "failure_rate",
                "example_symbol",
                "example_market_cap",
                "example_reason_detail",
            ]
        )
        for metric_id in metric_order:
            counter = failures.get(metric_id, Counter())
            if not counter:
                writer.writerow(
                    [metric_id, "", 0, total_symbols, 0.0]
                    + failure_example_csv_cells(None)
                )
                continue
            for reason, count in counter.most_common():
                rate = (count / total_symbols) if total_symbols else 0.0
                example = examples.get(metric_id, {}).get(reason)
                writer.writerow(
                    [metric_id, reason, count, total_symbols, rate]
                    + failure_example_csv_cells(example)
                )


def _write_screen_failure_report_csv(
    impacts: Sequence[_ScreenMetricImpactSummary],
    path: str,
) -> None:
    # Criterion-fallout columns only: per-reason root causes belong to
    # ``report-metric-status --reasons``, so this CSV stays screen-shaped.
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "missing_symbols",
                "affected_criteria_count",
                "affected_criteria",
            ]
        )
        for impact in impacts:
            criteria = sorted(impact.affected_criteria)
            writer.writerow(
                [
                    impact.metric_id,
                    len(impact.missing_symbols),
                    len(criteria),
                    "; ".join(criteria),
                ]
            )


def _print_screen_metric_na_impact(
    impacts: Sequence[_ScreenMetricImpactSummary],
) -> None:
    # Impact counts only: which metric gaps hurt which criteria. The per-reason
    # root causes live with the persisted-status survey (report-metric-status
    # --reasons); the caller prints a drill-down hint after this section.
    print("Metric NA impact")
    if not impacts:
        print("- none")
        return
    for impact in impacts:
        print(
            f"- {impact.metric_id}: missing={len(impact.missing_symbols)} symbols, "
            f"affects={len(impact.affected_criteria)} criteria"
        )


def _print_screen_criterion_fallout(
    summaries: Sequence[_CriterionFailureSummary],
    total_symbols: int,
) -> None:
    print("Criterion fallout")
    if not summaries:
        print("- none")
        return
    for summary in summaries:
        print(
            f"- {summary.criterion.name}: fails={summary.fail_count}/{total_symbols}, "
            f"na_fails={summary.na_fail_count}, "
            f"threshold_fails={summary.threshold_fail_count}"
        )
        metric_details: List[str] = []
        if summary.criterion.left.metric:
            metric_details.append(f"left_metric={summary.criterion.left.metric}")
        if summary.criterion.right.metric:
            metric_details.append(f"right_metric={summary.criterion.right.metric}")
        if metric_details:
            print(f"    {', '.join(metric_details)}")
        if summary.missing_metric_symbols:
            missing_counts = sorted(
                (
                    (metric_id, len(symbols))
                    for metric_id, symbols in summary.missing_metric_symbols.items()
                ),
                key=lambda item: (-item[1], item[0]),
            )
            display = ", ".join(
                f"{metric_id}={count}" for metric_id, count in missing_counts
            )
            print(f"    missing_metrics: {display}")


def cmd_report_metric_failures(
    database: str,
    metric_ids: Optional[Sequence[str]],
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
) -> int:
    """Summarize warning reasons for metric computation failures in a canonical scope."""

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    # Keep the scope's (listing_id, symbol) pairs: reads/writes/market-cap key on
    # the listing_id, the display symbol feeds the failure-example output only.
    listing_ids = [listing_id for listing_id, _ in scope_listings]

    metric_classes = _select_metric_classes(metric_ids)
    metric_id_order = [getattr(cls, "id", cls.__name__) for cls in metric_classes]
    include_market_data = any(
        getattr(metric_cls, "uses_market_data", False) for metric_cls in metric_classes
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    availability_repo = _StatusAwareMetricsRepository(
        db_path,
        raw_metrics_repo=metrics_repo,
        status_repo=status_repo,
        facts_refresh_repo=_SchemaReadyFinancialFactsRefreshStateRepository(db_path),
        market_repo=market_repo,
    )

    metric_id_set = [getattr(cls, "id", cls.__name__) for cls in metric_classes]
    totals: Dict[str, int] = {metric_id: 0 for metric_id in metric_id_set}
    tally = FailureTally(
        metric_id_set,
        _estimate_market_caps(
            fact_repo,
            market_repo.latest_snapshots_many_by_ids(
                listing_ids,
                chunk_size=METRICS_COMPUTE_BATCH_SIZE,
            ),
        ),
    )

    for metric_cls in metric_classes:
        metric_id = getattr(metric_cls, "id", metric_cls.__name__)
        states_by_id = availability_repo.states_many_by_ids(
            listing_ids,
            [metric_id],
            chunk_size=METRICS_COMPUTE_BATCH_SIZE,
        )
        pending_listings: List[Tuple[int, str]] = []
        for listing_id, symbol in scope_listings:
            state = states_by_id.get(listing_id, {}).get(metric_id)
            verdict = classify_availability_state(state)
            if verdict == "usable":
                continue
            if verdict == "fresh_failure":
                assert state is not None and state.status_record is not None
                totals[metric_id] += 1
                tally.record(
                    metric_id=metric_id,
                    reason=state.status_record.reason_code or "no warning emitted",
                    listing_id=listing_id,
                    symbol=symbol,
                    reason_detail=state.status_record.reason_detail,
                )
                continue
            pending_listings.append((listing_id, symbol))

        if not pending_listings:
            continue

        batch_market_repo = (
            market_repo if getattr(metric_cls, "uses_market_data", False) else None
        )
        for listing_batch in _batch_listings(
            pending_listings,
            METRICS_COMPUTE_BATCH_SIZE,
        ):
            batch_results = _compute_metric_batch_results(
                listing_batch,
                [metric_id],
                fact_repo,
                batch_market_repo,
                suppress_metric_warnings=True,
            )
            batch_attempts: List[_MetricAttemptResult] = []
            for result in batch_results:
                batch_attempts.extend(result.attempts)
                for attempt in result.attempts:
                    if attempt.status != "failure":
                        continue
                    totals[metric_id] += 1
                    tally.record(
                        metric_id=metric_id,
                        reason=attempt.reason_code or "no warning emitted",
                        listing_id=attempt.listing_id,
                        symbol=attempt.symbol,
                        reason_detail=attempt.reason_detail,
                    )
            _persist_metric_attempts(
                metrics_repo,
                status_repo,
                batch_attempts,
            )

    total_symbols = len(scope_listings)
    metric_order = sorted(
        metric_id_order,
        key=lambda current_metric_id: (
            -totals.get(current_metric_id, 0),
            current_metric_id,
        ),
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Metric failure reasons for {scope_label} (symbols={total_symbols}, metrics={len(metric_classes)})"
    )
    for metric_id in metric_order:
        total_failures = totals.get(metric_id, 0)
        print(f"- {metric_id}: failures={total_failures}/{total_symbols}")
        counter = tally.failures.get(metric_id)
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = tally.examples.get(metric_id, {}).get(reason)
            suffix = failure_example_console_suffix(example)
            print(f"    {reason}: {count}{suffix}")

    if output_csv:
        _write_metric_failure_report_csv(
            tally.failures, tally.examples, total_symbols, metric_order, output_csv
        )
        print(f"Wrote metric failure reasons to {output_csv}")
    return 0


def cmd_report_screen_failures(
    config_path: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
) -> int:
    """Rank which criteria and missing metrics eliminate the most symbols.

    A pure read: criteria are evaluated against stored metric values shadowed
    by persisted attempt status — nothing is recomputed and nothing is written.
    This report owns the criterion-level analytics only screen evaluation can
    produce (threshold vs NA fallout, metric-to-criteria linkage); per-reason
    NA root causes live in ``report-metric-status --reasons``, which the
    console output points at.
    """

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    total_symbols = len(scope_listings)
    completed_symbols = 0
    last_progress_at = time.monotonic()
    last_reported_completed = 0 if total_symbols > 0 else -1

    if total_symbols > 0:
        _print_screen_progress_bar(0, total_symbols)

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0 or completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SCREEN_PROGRESS_INTERVAL_SECONDS:
            return
        _print_screen_progress_bar(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    definition = load_screen(config_path)
    metric_ids = screen_metric_ids(definition)
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    metrics_repo = _StatusAwareMetricsRepository(
        db_path,
        market_repo=market_repo,
    )
    listing_ids = [listing_id for listing_id, _ in scope_listings]
    evaluation_metrics_repo = _PreloadedMetricsRepository(
        db_path,
        metrics_repo.fetch_many_by_ids(listing_ids, metric_ids),
    )

    criterion_summaries = [
        _CriterionFailureSummary(index=index, criterion=criterion)
        for index, criterion in enumerate(definition.criteria)
    ]
    metric_impacts: Dict[str, _ScreenMetricImpactSummary] = {
        metric_id: _ScreenMetricImpactSummary(metric_id=metric_id)
        for metric_id in metric_ids
    }
    passed_all = 0

    with suppress_console_metric_warnings(True):
        for listing_id, symbol in scope_listings:
            symbol_passed = True
            for summary in criterion_summaries:
                evaluation: CriterionEvaluation = evaluate_criterion_detail(
                    summary.criterion,
                    listing_id,
                    evaluation_metrics_repo,
                    display_symbol=symbol,
                    log_missing_metrics=False,
                )
                if evaluation.passed:
                    continue
                symbol_passed = False
                summary.fail_count += 1
                if evaluation.failure_kind == "comparison_failed":
                    summary.threshold_fail_count += 1
                    continue
                summary.na_fail_count += 1
                for metric_id in evaluation.missing_metric_ids:
                    impact = metric_impacts.setdefault(
                        metric_id,
                        _ScreenMetricImpactSummary(metric_id=metric_id),
                    )
                    impact.missing_symbols.add(symbol)
                    impact.affected_criteria.add(summary.label)
                    summary.missing_metric_symbols.setdefault(metric_id, set()).add(
                        symbol
                    )
            if symbol_passed:
                passed_all += 1
            completed_symbols += 1
            maybe_report_progress(False)

        maybe_report_progress(True)

    ordered_impacts = sorted(
        (impact for impact in metric_impacts.values() if impact.missing_symbols),
        key=lambda impact: (-len(impact.missing_symbols), impact.metric_id),
    )
    ordered_criteria = sorted(
        criterion_summaries,
        key=lambda summary: (-summary.fail_count, summary.index),
    )

    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Screen failure analysis for {scope_label} "
        f"(symbols={total_symbols}, criteria={len(definition.criteria)}, "
        f"unique_metrics={len(metric_ids)})"
    )
    print(f"Passed all criteria: {passed_all}/{total_symbols}")
    print(
        f"Failed at least one criterion: {total_symbols - passed_all}/{total_symbols}"
    )
    _print_screen_metric_na_impact(ordered_impacts)
    if ordered_impacts:
        # Same drill-down style as run-screen's explain-metric hint: name the
        # exact command that explains WHY the missing metrics are NA.
        print(f"hint: pyvalue report-metric-status --config {config_path} --reasons")
    _print_screen_criterion_fallout(ordered_criteria, total_symbols)

    if output_csv:
        _write_screen_failure_report_csv(ordered_impacts, output_csv)
        print(f"Wrote screen failure reasons to {output_csv}")

    return 0


def _write_fact_report_csv(report: Sequence[MetricCoverage], path: str) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "concept",
                "missing",
                "stale",
                "fresh",
                "fully_covered",
                "total_symbols",
            ]
        )
        for entry in report:
            if not entry.concepts:
                writer.writerow(
                    [
                        entry.metric_id,
                        "",
                        0,
                        0,
                        entry.total_symbols,
                        entry.fully_covered,
                        entry.total_symbols,
                    ]
                )
                continue
            for concept in entry.concepts:
                fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
                writer.writerow(
                    [
                        entry.metric_id,
                        concept.concept,
                        concept.missing,
                        concept.stale,
                        fresh,
                        entry.fully_covered,
                        entry.total_symbols,
                    ]
                )
