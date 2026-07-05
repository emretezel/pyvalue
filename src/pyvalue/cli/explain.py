"""CLI handler explaining why a metric is (or is not) computable per symbol.

``explain-metric`` is the microscope next to the survey reports: for a small
set of named symbols it shows, per metric, the persisted attempt state
(including the otherwise-buried ``reason_detail``), what each required fact
concept actually holds, the market-data seam, and a live write-free recompute
whose warnings are printed *untemplated* (real dates and counts, not the
``<n>``/``<date>`` placeholders the persisted ``reason_code`` carries).

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence

from pyvalue.metrics import REGISTRY
from pyvalue.persistence.storage import MetricsRepository
from pyvalue.reporting import ConceptDetail, compute_fact_detail
from pyvalue.screening import load_screen, screen_metric_ids
from pyvalue.logging_utils import suppress_console_metric_warnings

from ._common import (
    _MetricWarningCollector,
    _format_value,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
    _select_metric_classes,
)
from ._repos import (
    _SchemaReadyFinancialFactsRepository,
    _SchemaReadyMarketDataRepository,
    _StatusAwareMetricsRepository,
)
from .metrics import _compute_metrics_for_symbol, _initialize_metric_read_schema


def _print_persisted_state(
    availability_repo: _StatusAwareMetricsRepository,
    listing_id: int,
    metric_id: str,
) -> None:
    """Print the persisted attempt state for one (listing, metric) pair."""

    state = availability_repo.state_by_id(listing_id, metric_id)
    status_record = state.status_record
    if status_record is None and state.record is None:
        print("  persisted: none (never attempted; run compute-metrics)")
        return
    stale_note = " [stale: inputs changed since this attempt]" if state.stale else ""
    if status_record is None:
        # Legacy stored row without status tracking.
        assert state.record is not None
        print(
            f"  persisted: value={_format_value(state.record.value)} "
            f"(as_of {state.record.as_of}, no status row){stale_note}"
        )
        return
    if status_record.status == "success" and state.record is not None:
        print(
            f"  persisted: value={_format_value(state.record.value)} "
            f"(as_of {state.record.as_of}, attempted {status_record.attempted_at})"
            f"{stale_note}"
        )
        return
    print(
        f"  persisted: {status_record.status} "
        f"(attempted {status_record.attempted_at}){stale_note}"
    )
    if status_record.reason_code:
        print(f"    reason_code: {status_record.reason_code}")
    if status_record.reason_detail:
        print(f"    reason_detail: {status_record.reason_detail}")


def _print_concept_details(details: Sequence[ConceptDetail]) -> None:
    """Print one line per required concept: latest point, freshness, depth."""

    if not details:
        print("  inputs: metric declares no required fact concepts")
        return
    print("  inputs:")
    for detail in details:
        if not detail.present:
            print(f"    {detail.concept}: MISSING (no stored facts)")
            continue
        freshness = "fresh" if detail.fresh else "STALE"
        value_display = (
            _format_value(detail.latest_value)
            if detail.latest_value is not None
            else "N/A"
        )
        currency_suffix = f" {detail.latest_currency}" if detail.latest_currency else ""
        filed_display = detail.latest_filed or "unknown"
        print(
            f"    {detail.concept}: latest {detail.latest_end_date} "
            f"({detail.latest_fiscal_period}, filed {filed_display}), {freshness}, "
            f"value={value_display}{currency_suffix}, "
            f"rows: FY={detail.fy_rows} Q={detail.quarterly_rows} "
            f"total={detail.total_rows}"
        )


def _print_market_seam(
    market_repo: _SchemaReadyMarketDataRepository,
    listing_id: int,
) -> None:
    """Print the latest stored price snapshot (market-data metrics only)."""

    snapshot = market_repo.latest_snapshot_record_by_id(listing_id)
    if snapshot is None:
        print("  market data: no price snapshot stored")
        return
    currency_suffix = f" {snapshot.currency}" if snapshot.currency else ""
    print(
        f"  market data: price={_format_value(snapshot.price)}{currency_suffix} "
        f"(as_of {snapshot.as_of})"
    )


def cmd_explain_metric(
    database: str,
    symbols: Sequence[str],
    metric_ids: Optional[Sequence[str]],
    config_path: Optional[str],
    max_age_days: int,
) -> int:
    """Explain per (symbol, metric) why the metric computes or comes out NA.

    Deliberately symbol-scoped (a microscope, not a survey — use the report-*
    commands for scope-wide rankings) and **write-free**: the live recompute
    never persists its attempts, so running it mid-investigation cannot change
    what screens or reports see.
    """

    if bool(metric_ids) == bool(config_path):
        raise SystemExit("Pass exactly one of --metrics or --config.")

    db_path = _resolve_database_path(database)
    scope_listings, _, _ = _resolve_canonical_scope_listings(
        str(db_path),
        symbols,
        None,
        False,
    )

    if config_path:
        # A screen's criteria metrics are exactly the ones whose NA excludes a
        # symbol, so they are the default explanation set for screen tuning.
        selected_metric_ids = screen_metric_ids(load_screen(config_path))
        _select_metric_classes(selected_metric_ids)
    else:
        selected_metric_ids = [
            getattr(cls, "id", cls.__name__)
            for cls in _select_metric_classes(metric_ids)
        ]

    include_market_data = any(
        getattr(REGISTRY[metric_id], "uses_market_data", False)
        for metric_id in selected_metric_ids
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    availability_repo = _StatusAwareMetricsRepository(db_path, market_repo=market_repo)

    # Metric guards emit their diagnostics as LOGGER.warning records; collect
    # them off the root logger so the *untemplated* messages (real listing ids,
    # dates, counts) can be printed per metric. Console emission is suppressed
    # to avoid printing every warning twice.
    collector = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(collector)
    try:
        with suppress_console_metric_warnings(True):
            for listing_id, symbol in scope_listings:
                for metric_id in selected_metric_ids:
                    metric_cls = REGISTRY[metric_id]
                    print(f"== {symbol} / {metric_id} ==")
                    _print_persisted_state(availability_repo, listing_id, metric_id)
                    _print_concept_details(
                        compute_fact_detail(
                            fact_repo,
                            listing_id,
                            metric_cls,
                            max_age_days=max_age_days,
                        )
                    )
                    if getattr(metric_cls, "uses_market_data", False):
                        _print_market_seam(market_repo, listing_id)

                    # One metric per call so the collector holds exactly this
                    # metric's warnings. Attempts are NOT persisted.
                    result = _compute_metrics_for_symbol(
                        symbol,
                        listing_id,
                        [metric_id],
                        fact_repo,
                        market_repo,
                        warning_collector=collector,
                    )
                    attempt = result.attempts[0]
                    warnings: List[str] = [
                        record.getMessage() for record in collector.records
                    ]
                    if attempt.status == "success":
                        assert attempt.stored_row is not None
                        value = attempt.stored_row[2]
                        value_as_of = attempt.stored_row[3]
                        print(
                            f"  live recompute: SUCCESS value={_format_value(value)} "
                            f"(as_of {value_as_of}; not persisted — run "
                            "compute-metrics to store)"
                        )
                    else:
                        print("  live recompute: FAILURE")
                        if attempt.reason_code:
                            print(f"    reason_code: {attempt.reason_code}")
                        if attempt.reason_detail:
                            print(f"    reason_detail: {attempt.reason_detail}")
                    for warning in warnings:
                        print(f"    warning: {warning}")
    finally:
        root_logger.removeHandler(collector)
    return 0
