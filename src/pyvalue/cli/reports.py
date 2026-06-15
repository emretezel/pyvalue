"""CLI handlers for coverage/freshness/failure reports and metric recomputation.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import time
from collections import Counter
from typing import (
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.metrics import REGISTRY
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
    MarketSnapshotRecord,
    MetricComputeStatusRepository,
    MetricsRepository,
)

from ._common import (
    METRICS_COMPUTE_BATCH_SIZE,
    SCREEN_PROGRESS_INTERVAL_SECONDS,
    _CriterionFailureSummary,
    _MetricAttemptResult,
    _ScreenMetricImpactSummary,
    _batch_values,
    _format_value,
    _metric_status_rows_from_attempts,
    _prepare_output_csv_path,
    _print_symbol_progress,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
    _scope_label,
    _select_metric_classes,
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
    _compute_metric_batch_results,
    _initialize_metric_read_schema,
)


def _make_symbol_progress_reporter(
    total_symbols: int,
    interval_seconds: float,
    printer: Optional[Callable[[int, int], None]] = None,
    start_immediately: bool = False,
) -> Callable[[int, bool], None]:
    """Return a throttled symbol-progress reporter."""

    progress_printer = printer or _print_symbol_progress
    last_progress_at = time.monotonic()
    last_reported_completed = -1
    if start_immediately and total_symbols > 0:
        progress_printer(0, total_symbols)
        last_reported_completed = 0

    def maybe_report_progress(completed_symbols: int, force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0 or completed_symbols == last_reported_completed:
            return
        now = time.monotonic()
        elapsed = now - last_progress_at
        if not force and elapsed < interval_seconds:
            return
        progress_printer(completed_symbols, total_symbols)
        last_progress_at = now
        last_reported_completed = completed_symbols

    return maybe_report_progress


def _persist_metric_attempts(
    metrics_repo: MetricsRepository,
    status_repo: MetricComputeStatusRepository,
    attempts: Sequence[_MetricAttemptResult],
    *,
    ids_by_symbol: Optional[Mapping[str, int]] = None,
) -> None:
    # The recomputed attempts are for symbols already in the caller's scope, so
    # the writers reuse the scope-resolved listing ids instead of re-resolving.
    metric_rows = [
        attempt.stored_row for attempt in attempts if attempt.stored_row is not None
    ]
    if metric_rows:
        metrics_repo.upsert_many(metric_rows, ids_by_symbol=ids_by_symbol)
    status_rows = _metric_status_rows_from_attempts(attempts)
    if status_rows:
        status_repo.upsert_many(status_rows, ids_by_symbol=ids_by_symbol)


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


def _print_recompute_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for missing-metric root-cause analysis progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        "Progress: "
        f"[{bar}] {completed_symbols}/{total_symbols} missing symbols analyzed "
        f"({percent:.1f}%)",
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
    selected_symbols = [symbol for _, symbol in scope_listings]
    security_ids_by_symbol = {
        symbol: listing_id for listing_id, symbol in scope_listings
    }

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    coverage = compute_fact_coverage(
        fact_repo,
        selected_symbols,
        metric_classes,
        max_age_days=max_age_days,
        security_ids_by_symbol=security_ids_by_symbol,
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )

    print(
        f"Fact coverage for {scope_label} "
        f"({len(selected_symbols)} symbols, max_age_days={max_age_days})"
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
    if output_csv:
        _write_fact_report_csv(coverage, output_csv)
        print(f"Wrote concept-level coverage to {output_csv}")
    return 0


def cmd_report_metric_coverage(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
) -> int:
    """Count symbols that can compute all requested metrics for a canonical scope."""

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    selected_symbols = [symbol for _, symbol in scope_listings]
    security_ids_by_symbol = {
        symbol: listing_id for listing_id, symbol in scope_listings
    }

    metric_classes = _select_metric_classes(metric_ids)
    metric_id_order = [getattr(cls, "id", cls.__name__) for cls in metric_classes]
    include_market_data = any(
        getattr(metric_cls, "uses_market_data", False) for metric_cls in metric_classes
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)

    per_metric_success: Dict[str, int] = {metric_id: 0 for metric_id in metric_id_order}
    all_success = 0

    # Recompute via the shared batch engine (same path compute-metrics uses): it
    # carries the scope-resolved listing ids straight into the facts / snapshot
    # preloads -- no per-symbol symbol->id resolution -- and binds one FX service
    # per batch. A symbol "covers" a metric when its attempt succeeds.
    for symbol_batch in _batch_values(selected_symbols, METRICS_COMPUTE_BATCH_SIZE):
        batch_ids = {
            symbol: security_ids_by_symbol[symbol]
            for symbol in symbol_batch
            if symbol in security_ids_by_symbol
        }
        batch_results = _compute_metric_batch_results(
            symbol_batch,
            metric_id_order,
            fact_repo,
            market_repo,
            suppress_metric_warnings=True,
            security_ids_by_symbol=batch_ids,
        )
        for result in batch_results:
            success_metric_ids = {
                attempt.metric_id
                for attempt in result.attempts
                if attempt.status == "success"
            }
            for metric_id in success_metric_ids:
                if metric_id in per_metric_success:
                    per_metric_success[metric_id] += 1
            if metric_id_order and len(success_metric_ids) == len(metric_id_order):
                all_success += 1

    total_symbols = len(selected_symbols)
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Metric coverage for {scope_label} "
        f"(symbols={total_symbols}, metrics={len(metric_classes)})"
    )
    print(f"Symbols where all metrics computed: {all_success}/{total_symbols}")
    ordered = sorted(per_metric_success.items(), key=lambda item: (item[1], item[0]))
    for metric_id, count in ordered:
        print(f"- {metric_id}: {count}/{total_symbols} symbols")
    return 0


def _write_metric_failure_report_csv(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
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
            ]
        )
        for metric_id in metric_order:
            counter = failures.get(metric_id, Counter())
            if not counter:
                writer.writerow([metric_id, "", 0, total_symbols, 0.0, "", ""])
                continue
            for reason, count in counter.most_common():
                rate = (count / total_symbols) if total_symbols else 0.0
                example = examples.get(metric_id, {}).get(reason)
                example_symbol = example[0] if example else ""
                example_cap = example[1] if example else None
                writer.writerow(
                    [
                        metric_id,
                        reason,
                        count,
                        total_symbols,
                        rate,
                        example_symbol,
                        example_cap or "",
                    ]
                )


def _write_screen_failure_report_csv(
    impacts: Sequence[_ScreenMetricImpactSummary],
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    path: str,
) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "missing_symbols",
                "affected_criteria_count",
                "affected_criteria",
                "root_cause",
                "root_cause_count",
                "example_symbol",
                "example_market_cap",
            ]
        )
        for impact in impacts:
            counter = failures.get(impact.metric_id, Counter())
            criteria = sorted(impact.affected_criteria)
            if not counter:
                writer.writerow(
                    [
                        impact.metric_id,
                        len(impact.missing_symbols),
                        len(criteria),
                        "; ".join(criteria),
                        "",
                        0,
                        "",
                        "",
                    ]
                )
                continue
            for reason, count in counter.most_common():
                example = examples.get(impact.metric_id, {}).get(reason)
                example_symbol = example[0] if example else ""
                example_cap = example[1] if example else None
                writer.writerow(
                    [
                        impact.metric_id,
                        len(impact.missing_symbols),
                        len(criteria),
                        "; ".join(criteria),
                        reason,
                        count,
                        example_symbol,
                        example_cap if example_cap is not None else "",
                    ]
                )


def _estimate_market_caps(
    fact_repo: FinancialFactsRepository,
    snapshots_by_symbol: Mapping[str, MarketSnapshotRecord],
) -> Dict[str, Optional[float]]:
    """Estimate market caps (latest shares x latest price) for report examples.

    Market cap is no longer a stored column; this is a diagnostic-only sizing
    heuristic used to pick a representative (large) failing example. It pairs the
    latest share count with the latest price rather than the share-count-dated
    price the metrics use -- close enough for ranking examples by size, and cheap
    (one bulk share-count read over the already-loaded snapshots).
    """

    estimates: Dict[str, Optional[float]] = {}
    if not snapshots_by_symbol:
        return estimates
    share_counts = fact_repo.latest_share_counts_many(
        list(snapshots_by_symbol.keys()),
        security_ids_by_symbol={
            symbol: snapshot.security_id
            for symbol, snapshot in snapshots_by_symbol.items()
        },
    )
    for symbol, snapshot in snapshots_by_symbol.items():
        shares = share_counts.get(symbol)
        if (
            shares is None
            or shares <= 0
            or snapshot.price is None
            or snapshot.price <= 0
        ):
            continue
        estimates[symbol] = snapshot.price * shares
    return estimates


def _metric_market_cap(
    market_caps: Dict[str, Optional[float]],
    symbol: str,
) -> Optional[float]:
    return market_caps.get(symbol)


def _record_failure_example(
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    metric_id: str,
    reason: str,
    symbol: str,
    market_cap: Optional[float],
) -> None:
    current = examples[metric_id].get(reason)
    if current is None:
        examples[metric_id][reason] = (symbol, market_cap)
        return
    current_cap = current[1]
    if market_cap is not None and (current_cap is None or market_cap > current_cap):
        examples[metric_id][reason] = (symbol, market_cap)


def _record_metric_failure_reason(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    market_caps: Dict[str, Optional[float]],
    *,
    metric_id: str,
    reason: str,
    symbol: str,
) -> None:
    failures[metric_id][reason] += 1
    cap = _metric_market_cap(market_caps, symbol)
    _record_failure_example(examples, metric_id, reason, symbol, cap)


def _recompute_missing_screen_metrics(
    metric_impacts: Mapping[str, _ScreenMetricImpactSummary],
    fact_repo: FinancialFactsRepository,
    market_repo: MarketDataRepository,
    progress_interval_seconds: Optional[float] = None,
    *,
    security_ids_by_symbol: Optional[Mapping[str, int]] = None,
) -> tuple[Dict[str, Counter], Dict[str, Dict[str, tuple[str, Optional[float]]]]]:
    failures: Dict[str, Counter] = {
        metric_id: Counter() for metric_id in metric_impacts.keys()
    }
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        metric_id: {} for metric_id in metric_impacts.keys()
    }
    metric_ids_by_symbol: Dict[str, List[str]] = {}
    for metric_id, impact in metric_impacts.items():
        for symbol in impact.missing_symbols:
            metric_ids_by_symbol.setdefault(symbol, []).append(metric_id)

    symbols_to_recompute = sorted(metric_ids_by_symbol.keys())
    if not symbols_to_recompute:
        return failures, examples

    # Narrow the scope-wide id map to just the symbols we recompute so the
    # snapshot / availability reads carry the listing_id instead of re-resolving.
    recompute_ids: Optional[Dict[str, int]] = (
        {
            symbol: security_ids_by_symbol[symbol]
            for symbol in symbols_to_recompute
            if symbol in security_ids_by_symbol
        }
        if security_ids_by_symbol is not None
        else None
    )

    db_path = fact_repo.db_path
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    availability_repo = _StatusAwareMetricsRepository(
        db_path,
        raw_metrics_repo=metrics_repo,
        status_repo=status_repo,
        facts_refresh_repo=_SchemaReadyFinancialFactsRefreshStateRepository(db_path),
        market_repo=market_repo,
    )
    maybe_report_progress = (
        _make_symbol_progress_reporter(
            len(symbols_to_recompute),
            progress_interval_seconds,
            printer=_print_recompute_progress_bar,
            start_immediately=True,
        )
        if progress_interval_seconds is not None
        else None
    )

    snapshots_by_symbol = market_repo.latest_snapshots_many(
        symbols_to_recompute,
        security_ids_by_symbol=recompute_ids,
    )
    market_caps = _estimate_market_caps(fact_repo, snapshots_by_symbol)

    availability_states = availability_repo.states_many(
        symbols_to_recompute,
        tuple(metric_impacts.keys()),
        chunk_size=METRICS_COMPUTE_BATCH_SIZE,
        security_ids_by_symbol=recompute_ids,
    )
    pending_metric_ids_by_symbol: Dict[str, List[str]] = {}
    completed_symbols = 0

    for symbol in symbols_to_recompute:
        pending_metric_ids: List[str] = []
        for metric_id in metric_ids_by_symbol[symbol]:
            if metric_id not in REGISTRY:
                _record_metric_failure_reason(
                    failures,
                    examples,
                    market_caps,
                    metric_id=metric_id,
                    reason="unknown_metric_id",
                    symbol=symbol,
                )
                continue
            state = availability_states.get(symbol, {}).get(metric_id)
            if (
                state is not None
                and state.status_record is not None
                and not state.stale
                and state.status_record.status == "failure"
            ):
                _record_metric_failure_reason(
                    failures,
                    examples,
                    market_caps,
                    metric_id=metric_id,
                    reason=state.status_record.reason_code or "no warning emitted",
                    symbol=symbol,
                )
                continue
            pending_metric_ids.append(metric_id)
        if pending_metric_ids:
            pending_metric_ids_by_symbol[symbol] = pending_metric_ids
            continue
        completed_symbols += 1
        if maybe_report_progress is not None:
            maybe_report_progress(completed_symbols, False)

    symbols_by_metric_group: Dict[Tuple[str, ...], List[str]] = {}
    for symbol, pending_metric_ids in pending_metric_ids_by_symbol.items():
        metric_group = tuple(pending_metric_ids)
        symbols_by_metric_group.setdefault(metric_group, []).append(symbol)

    for metric_group, group_symbols in symbols_by_metric_group.items():
        for symbol_batch in _batch_values(
            group_symbols,
            METRICS_COMPUTE_BATCH_SIZE,
        ):
            batch_ids: Optional[Dict[str, int]] = (
                {
                    symbol: recompute_ids[symbol]
                    for symbol in symbol_batch
                    if symbol in recompute_ids
                }
                if recompute_ids is not None
                else None
            )
            batch_results = _compute_metric_batch_results(
                symbol_batch,
                metric_group,
                fact_repo,
                market_repo,
                suppress_metric_warnings=True,
                preloaded_snapshots_by_symbol=snapshots_by_symbol,
                security_ids_by_symbol=batch_ids,
            )
            batch_attempts: List[_MetricAttemptResult] = []
            for result in batch_results:
                batch_attempts.extend(result.attempts)
                for attempt in result.attempts:
                    reason = (
                        "stored_missing_but_computable_now"
                        if attempt.status == "success"
                        else attempt.reason_code or "no warning emitted"
                    )
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_caps,
                        metric_id=attempt.metric_id,
                        reason=reason,
                        symbol=attempt.symbol,
                    )
                completed_symbols += 1
                if maybe_report_progress is not None:
                    maybe_report_progress(completed_symbols, False)
            _persist_metric_attempts(
                metrics_repo,
                status_repo,
                batch_attempts,
                ids_by_symbol=batch_ids,
            )

    if maybe_report_progress is not None:
        maybe_report_progress(len(symbols_to_recompute), True)
    return failures, examples


def _print_screen_metric_na_impact(
    impacts: Sequence[_ScreenMetricImpactSummary],
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
) -> None:
    print("Metric NA impact")
    if not impacts:
        print("- none")
        return
    for impact in impacts:
        print(
            f"- {impact.metric_id}: missing={len(impact.missing_symbols)} symbols, "
            f"affects={len(impact.affected_criteria)} criteria"
        )
        counter = failures.get(impact.metric_id, Counter())
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = examples.get(impact.metric_id, {}).get(reason)
            if example:
                example_symbol, example_cap = example
                cap_display = (
                    _format_value(example_cap) if example_cap is not None else "N/A"
                )
                print(
                    f"    {reason}: {count} "
                    f"(example={example_symbol}, market_cap={cap_display})"
                )
            else:
                print(f"    {reason}: {count}")


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
    selected_symbols = [symbol for _, symbol in scope_listings]
    security_ids_by_symbol = {
        symbol: listing_id for listing_id, symbol in scope_listings
    }

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

    failures: Dict[str, Counter] = {
        getattr(cls, "id", cls.__name__): Counter() for cls in metric_classes
    }
    totals: Dict[str, int] = {
        getattr(cls, "id", cls.__name__): 0 for cls in metric_classes
    }
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        getattr(cls, "id", cls.__name__): {} for cls in metric_classes
    }
    market_caps = _estimate_market_caps(
        fact_repo,
        market_repo.latest_snapshots_many(
            selected_symbols,
            chunk_size=METRICS_COMPUTE_BATCH_SIZE,
            security_ids_by_symbol=security_ids_by_symbol,
        ),
    )

    for metric_cls in metric_classes:
        metric_id = getattr(metric_cls, "id", metric_cls.__name__)
        states_by_symbol = availability_repo.states_many(
            selected_symbols,
            [metric_id],
            chunk_size=METRICS_COMPUTE_BATCH_SIZE,
            security_ids_by_symbol=security_ids_by_symbol,
        )
        pending_symbols: List[str] = []
        for symbol in selected_symbols:
            state = states_by_symbol.get(symbol, {}).get(metric_id)
            if state is None:
                pending_symbols.append(symbol)
                continue
            if (
                state.status_record is None
                and state.record is not None
                and not state.stale
            ):
                continue
            if state.status_record is not None and not state.stale:
                if state.status_record.status == "failure":
                    reason = state.status_record.reason_code or "no warning emitted"
                    totals[metric_id] += 1
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_caps,
                        metric_id=metric_id,
                        reason=reason,
                        symbol=symbol,
                    )
                    continue
                if state.record is not None:
                    continue
            pending_symbols.append(symbol)

        if not pending_symbols:
            continue

        batch_market_repo = (
            market_repo if getattr(metric_cls, "uses_market_data", False) else None
        )
        for symbol_batch in _batch_values(
            pending_symbols,
            METRICS_COMPUTE_BATCH_SIZE,
        ):
            batch_ids = {
                symbol: security_ids_by_symbol[symbol]
                for symbol in symbol_batch
                if symbol in security_ids_by_symbol
            }
            batch_results = _compute_metric_batch_results(
                symbol_batch,
                [metric_id],
                fact_repo,
                batch_market_repo,
                suppress_metric_warnings=True,
                security_ids_by_symbol=batch_ids,
            )
            batch_attempts: List[_MetricAttemptResult] = []
            for result in batch_results:
                batch_attempts.extend(result.attempts)
                for attempt in result.attempts:
                    if attempt.status != "failure":
                        continue
                    totals[metric_id] += 1
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_caps,
                        metric_id=metric_id,
                        reason=attempt.reason_code or "no warning emitted",
                        symbol=attempt.symbol,
                    )
            _persist_metric_attempts(
                metrics_repo,
                status_repo,
                batch_attempts,
                ids_by_symbol=batch_ids,
            )

    total_symbols = len(selected_symbols)
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
        counter = failures.get(metric_id)
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = examples.get(metric_id, {}).get(reason)
            if example:
                example_symbol, example_cap = example
                cap_display = (
                    _format_value(example_cap) if example_cap is not None else "N/A"
                )
                print(
                    f"    {reason}: {count} (example={example_symbol}, market_cap={cap_display})"
                )
            else:
                print(f"    {reason}: {count}")

    if output_csv:
        _write_metric_failure_report_csv(
            failures, examples, total_symbols, metric_order, output_csv
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
    """Rank which criteria and missing metrics eliminate the most symbols."""

    db_path = _resolve_database_path(database)
    scope_listings, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    # Scope symbols are already uppercase canonical tickers; keep the listing_id
    # the scope join produced so the screen reads carry it instead of re-resolving.
    selected_symbols = [symbol for _, symbol in scope_listings]
    security_ids_by_symbol = {
        symbol: listing_id for listing_id, symbol in scope_listings
    }
    total_symbols = len(selected_symbols)
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
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    metrics_repo = _StatusAwareMetricsRepository(
        db_path,
        market_repo=market_repo,
    )
    evaluation_metrics_repo = _PreloadedMetricsRepository(
        db_path,
        metrics_repo.fetch_many_for_symbols(
            selected_symbols,
            metric_ids,
            security_ids_by_symbol=security_ids_by_symbol,
        ),
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
        for symbol in selected_symbols:
            symbol_passed = True
            for summary in criterion_summaries:
                evaluation: CriterionEvaluation = evaluate_criterion_detail(
                    summary.criterion,
                    symbol,
                    evaluation_metrics_repo,
                    fact_repo,
                    market_repo,
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

        failures, examples = _recompute_missing_screen_metrics(
            metric_impacts,
            fact_repo,
            market_repo,
            progress_interval_seconds=SCREEN_PROGRESS_INTERVAL_SECONDS,
            security_ids_by_symbol=security_ids_by_symbol,
        )

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
    _print_screen_metric_na_impact(ordered_impacts, failures, examples)
    _print_screen_criterion_fallout(ordered_criteria, total_symbols)

    if output_csv:
        _write_screen_failure_report_csv(
            ordered_impacts,
            failures,
            examples,
            output_csv,
        )
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
