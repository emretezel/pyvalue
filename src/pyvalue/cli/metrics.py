"""CLI handlers and workers for computing metrics over the screened universe.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import (
    Future,
    as_completed,
)
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import logging
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from pyvalue.currency import (
    metric_currency_or_none,
)
from pyvalue.marketdata import PriceData
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import (
    Metric,
    MetricCurrencyInvariantError,
    MetricResult,
    consume_metric_currency_invariant_error,
    metadata_for_metric,
)
from pyvalue.metrics.utils import reuse_or_bind_metric_fx_service
from pyvalue.logging_utils import (
    suppress_console_metric_warnings,
)
from pyvalue.persistence.storage import (
    FinancialFactsRepository,
    FinancialFactsRefreshStateRecord,
    FinancialFactsRefreshStateRepository,
    FactRecord,
    IdKeyedStoredMetricRow,
    MarketDataRepository,
    MarketSnapshotRecord,
    MetricComputeStatusRepository,
    MetricsRepository,
    MetricsWriteSession,
)

from ._common import (
    LOGGER,
    METRICS_COMPUTE_BATCH_SIZE,
    METRICS_MAX_WORKERS,
    METRICS_PROGRESS_INTERVAL_SECONDS,
    METRICS_WRITE_BATCH_INTERVAL_SECONDS,
    METRICS_WRITE_BATCH_SIZE,
    _ComputedMetricsResult,
    _MetricAttemptResult,
    _MetricComputationFailure,
    _MetricWarningCollector,
    _ProfiledComputedMetricsBatchResult,
    _metric_status_rows_from_attempts,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
)
from ._batch import (
    _cancel_cli_command,
    _create_process_pool_executor,
)
from ._repos import (
    _CachedMarketDataRepository,
    _CachedRegionFactsRepository,
    _SchemaReadyFinancialFactsRefreshStateRepository,
    _SchemaReadyFinancialFactsRepository,
    _SchemaReadyMarketDataRepository,
    _SchemaReadyMetricComputeStatusRepository,
    _SchemaReadyMetricsRepository,
)


_PRELOADED_MARKET_SNAPSHOT_MISSING = object()


_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _price_data_from_snapshot_record(record: MarketSnapshotRecord) -> PriceData:
    """Convert a stored latest-snapshot row into the PriceData interface."""

    return PriceData(
        symbol=record.symbol,
        price=record.price,
        as_of=record.as_of,
        currency=record.currency,
        volume=record.volume,
    )


def _validated_metric_ids(metric_ids: Optional[Sequence[str]]) -> List[str]:
    ids_to_compute = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    if not ids_to_compute:
        raise SystemExit("No metrics specified.")
    unknown = [metric_id for metric_id in ids_to_compute if metric_id not in REGISTRY]
    if unknown:
        if len(unknown) == 1:
            raise SystemExit(f"Unknown metric id: {unknown[0]}")
        raise SystemExit(f"Unknown metric ids: {', '.join(unknown)}")
    return ids_to_compute


def _metrics_use_market_data(metric_ids: Sequence[str]) -> bool:
    return any(
        getattr(REGISTRY[metric_id], "uses_market_data", False)
        for metric_id in metric_ids
    )


def _metrics_use_financial_facts(metric_ids: Sequence[str]) -> bool:
    return any(
        getattr(REGISTRY[metric_id], "uses_financial_facts", True)
        for metric_id in metric_ids
    )


def _prefetch_metric_facts_for_ids(
    fact_repo: FinancialFactsRepository,
    listing_ids: Sequence[int],
    metric_ids: Sequence[str],
    *,
    chunk_size: int,
) -> Dict[int, List[FactRecord]]:
    """Bulk-load only the facts required by the requested metrics, keyed by id.

    The natural-identity prefetch: the scope already holds each ``listing_id``,
    so the read seeks by id with no symbol resolution. Unknown metric ids are
    ignored so investigative paths can still classify them without aborting the
    whole batch. When every known metric is market-data-only, the preload is
    skipped entirely.
    """

    known_metric_ids = tuple(
        metric_id for metric_id in metric_ids if metric_id in REGISTRY
    )
    if not known_metric_ids or not _metrics_use_financial_facts(known_metric_ids):
        return {}
    required_concepts = _required_concepts_for_metric_ids(known_metric_ids)
    return fact_repo.facts_for_ids_many(
        listing_ids,
        concepts=required_concepts or None,
        chunk_size=chunk_size,
    )


@lru_cache(maxsize=None)
def _required_concepts_for_metric_ids(metric_ids: Tuple[str, ...]) -> Tuple[str, ...]:
    """Return the deduplicated union of ``required_concepts`` across metrics.

    Used by ``_compute_metric_batch_results`` to restrict the fact preload to
    only the concepts the requested metric set actually consumes. The result
    is memoised because every batch in a run uses the same metric_ids tuple
    and the audit cost (resolving each metric class's declaration) is fixed.

    Returns an empty tuple if any metric in the set both uses financial facts
    AND declares an empty ``required_concepts`` — that signals a "wildcard"
    consumer, so the caller must fall back to the unfiltered preload to avoid
    silently dropping rows the metric depends on. Today the only metric with
    empty ``required_concepts`` (``MarketCapitalizationMetric``) also sets
    ``uses_financial_facts = False``, so it never triggers the fallback.
    """

    seen: set[str] = set()
    ordered: List[str] = []
    for metric_id in metric_ids:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            continue
        if not getattr(metric_cls, "uses_financial_facts", True):
            continue
        declared = tuple(getattr(metric_cls, "required_concepts", ()) or ())
        if not declared:
            # Wildcard fact consumer — disable the filter for the whole batch.
            return ()
        for concept in declared:
            if concept and concept not in seen:
                seen.add(concept)
                ordered.append(concept)
    return tuple(ordered)


def _metric_worker_count(total_symbols: int) -> int:
    """Return an automatic worker count for bulk metric computation."""

    if total_symbols <= 0:
        return 1
    cpu_bound = max(os.cpu_count() or 1, 1)
    return max(1, min(total_symbols, min(cpu_bound, METRICS_MAX_WORKERS)))


def _initialize_metric_read_schema(
    db_path: Path,
    include_market_data: bool,
) -> None:
    """Ensure worker-read tables exist before process workers start reading."""

    FinancialFactsRepository(db_path).initialize_schema()
    FinancialFactsRefreshStateRepository(db_path).initialize_schema()
    MetricComputeStatusRepository(db_path).initialize_schema()
    if include_market_data:
        MarketDataRepository(db_path).initialize_schema()


def _flush_metric_write_batch(
    pending_rows: List[IdKeyedStoredMetricRow],
    pending_attempts: List[_MetricAttemptResult],
    writer: MetricsWriteSession,
    profile_state: Optional["_MetricComputationProfile"] = None,
) -> None:
    """Persist one buffered metric batch by natural ``listing_id`` identity.

    Rows are id-led (``IdKeyedStoredMetricRow``) and the status records carry
    their ``listing_id``, so the writer selects on the id the scope already
    resolved -- no symbol->id resolution happens here. The persistent connection
    and the metrics+status transaction live inside ``MetricsWriteSession``; this
    only times the flush for the profiler.
    """

    if not pending_rows and not pending_attempts:
        return
    status_rows = _metric_status_rows_from_attempts(pending_attempts)
    row_count = len(pending_rows)

    if profile_state is not None and profile_state.enabled:
        flush_start = time.perf_counter()
        writer.flush(pending_rows, status_rows)
        profile_state.write_seconds += time.perf_counter() - flush_start
        profile_state.write_flush_count += 1
        profile_state.write_row_count += row_count
    else:
        writer.flush(pending_rows, status_rows)
    pending_rows.clear()
    pending_attempts.clear()


def _print_metric_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for metric-computation progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        f"Progress: [{bar}] {completed_symbols}/{total_symbols} symbols complete ({percent:.1f}%)",
        flush=True,
    )


def _metric_attempt_success(
    metric_id: str,
    metric: Metric,
    result: MetricResult,
    *,
    symbol: str,
    listing_id: int,
    attempted_at: str,
    facts_refreshed_at: Optional[str],
    market_snapshot_record: Optional[MarketSnapshotRecord],
) -> _MetricAttemptResult:
    metadata = metadata_for_metric(metric_id, metric)
    unit_kind = metadata.unit_kind if result.unit_kind == "other" else result.unit_kind
    unit_label = result.unit_label or metadata.unit_label
    currency = metric_currency_or_none(unit_kind, result.currency)
    # Id-led stored row: the metric layer keys on listing_id, so the row written
    # back carries it (the canonical symbol stays purely as a display label).
    stored_row: IdKeyedStoredMetricRow = (
        listing_id,
        result.metric_id,
        result.value,
        result.as_of,
        unit_kind,
        currency,
        unit_label,
    )
    return _MetricAttemptResult(
        symbol=symbol,
        listing_id=listing_id,
        metric_id=metric_id,
        status="success",
        attempted_at=attempted_at,
        stored_row=stored_row,
        value_as_of=result.as_of,
        facts_refreshed_at=facts_refreshed_at,
        market_data_as_of=(
            market_snapshot_record.as_of if market_snapshot_record is not None else None
        ),
        market_data_updated_at=(
            market_snapshot_record.updated_at
            if market_snapshot_record is not None
            else None
        ),
    )


def _metric_attempt_failure(
    *,
    symbol: str,
    listing_id: int,
    metric_id: str,
    attempted_at: str,
    reason_code: str,
    reason_detail: Optional[str],
    facts_refreshed_at: Optional[str],
    market_snapshot_record: Optional[MarketSnapshotRecord],
    persist_status: bool = True,
) -> _MetricAttemptResult:
    return _MetricAttemptResult(
        symbol=symbol,
        listing_id=listing_id,
        metric_id=metric_id,
        status="failure",
        attempted_at=attempted_at,
        reason_code=reason_code,
        reason_detail=reason_detail,
        facts_refreshed_at=facts_refreshed_at,
        market_data_as_of=(
            market_snapshot_record.as_of if market_snapshot_record is not None else None
        ),
        market_data_updated_at=(
            market_snapshot_record.updated_at
            if market_snapshot_record is not None
            else None
        ),
        persist_status=persist_status,
    )


def _compute_metrics_for_symbol(
    symbol: str,
    listing_id: int,
    metric_ids: Sequence[str],
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    preloaded_facts: Optional[Sequence[FactRecord]] = None,
    preloaded_market_snapshot: object = _PRELOADED_MARKET_SNAPSHOT_MISSING,
    preloaded_market_snapshot_record: Optional[MarketSnapshotRecord] = None,
    facts_refreshed_at: Optional[str] = None,
    warning_collector: Optional["_MetricWarningCollector"] = None,
) -> _ComputedMetricsResult:
    """Compute one symbol's metrics under the batch FX-conversion context.

    Metric inputs are converted to the listing currency by the shared seam
    (``metrics.utils.require_metric_money``), which reads the active FX service.
    The batch driver (:func:`_compute_metric_batch_results`) binds one service
    for the whole symbol loop; this function reuses it via
    ``reuse_or_bind_metric_fx_service`` and only binds its own when called
    standalone with no active binding. Because the batch binding is established
    inside each worker process, the context var is visible to the seam under the
    multiprocessing workers.
    """

    with reuse_or_bind_metric_fx_service(fact_repo, market_repo):
        return _compute_metrics_for_symbol_inner(
            symbol,
            listing_id,
            metric_ids,
            fact_repo,
            market_repo,
            preloaded_facts=preloaded_facts,
            preloaded_market_snapshot=preloaded_market_snapshot,
            preloaded_market_snapshot_record=preloaded_market_snapshot_record,
            facts_refreshed_at=facts_refreshed_at,
            warning_collector=warning_collector,
        )


def _compute_metrics_for_symbol_inner(
    symbol: str,
    listing_id: int,
    metric_ids: Sequence[str],
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    preloaded_facts: Optional[Sequence[FactRecord]] = None,
    preloaded_market_snapshot: object = _PRELOADED_MARKET_SNAPSHOT_MISSING,
    preloaded_market_snapshot_record: Optional[MarketSnapshotRecord] = None,
    facts_refreshed_at: Optional[str] = None,
    warning_collector: Optional["_MetricWarningCollector"] = None,
) -> _ComputedMetricsResult:
    # The compute orchestration is the boundary that holds both identities: the
    # ``listing_id`` keys the metric layer (cache, compute, fact reads) while the
    # display ``symbol`` is retained only for the symbol-keyed write path and
    # status/attempt records the worker emits.
    symbol_upper = symbol.strip().upper()
    records = (
        list(preloaded_facts)
        if preloaded_facts is not None
        else fact_repo.facts_for_id(listing_id)
    )
    cached_fact_repo = _CachedRegionFactsRepository(
        fact_repo,
        listing_id,
        records,
    )
    snapshot_loaded = (
        preloaded_market_snapshot is not _PRELOADED_MARKET_SNAPSHOT_MISSING
    )
    if (
        preloaded_market_snapshot is _PRELOADED_MARKET_SNAPSHOT_MISSING
        and preloaded_market_snapshot_record is not None
    ):
        preloaded_market_snapshot = _price_data_from_snapshot_record(
            preloaded_market_snapshot_record
        )
        snapshot_loaded = True
    snapshot = (
        None
        if preloaded_market_snapshot is _PRELOADED_MARKET_SNAPSHOT_MISSING
        else cast(Optional[PriceData], preloaded_market_snapshot)
    )
    cached_market_repo = (
        _CachedMarketDataRepository(
            market_repo,
            listing_id,
            snapshot=snapshot,
            snapshot_loaded=snapshot_loaded,
        )
        if market_repo is not None
        else None
    )

    rows: List[IdKeyedStoredMetricRow] = []
    failures: List[_MetricComputationFailure] = []
    attempts: List[_MetricAttemptResult] = []
    computed = 0
    for metric_id in metric_ids:
        if warning_collector is not None:
            warning_collector.clear()
        metric_cls = REGISTRY.get(metric_id)
        attempted_at = datetime.now(timezone.utc).isoformat()
        if metric_cls is None:
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code="unknown_metric_id",
                    reason_detail="Metric id not found in registry",
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                    persist_status=False,
                )
            )
            continue
        metric = metric_cls()
        try:
            if getattr(metric, "uses_market_data", False):
                if cached_market_repo is None:  # pragma: no cover - defensive
                    raise RuntimeError(
                        f"Metric {metric_id} requires market data but no market repo was provided."
                    )
                result = metric.compute(
                    listing_id, cached_fact_repo, cached_market_repo
                )
            else:
                result = metric.compute(listing_id, cached_fact_repo)
        except MetricCurrencyInvariantError as exc:
            failures.append(
                _MetricComputationFailure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    reason=exc.summary_reason,
                    message=str(exc),
                )
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=exc.summary_reason,
                    reason_detail=str(exc),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        except Exception as exc:  # pragma: no cover - metric errors
            LOGGER.error("Metric %s failed for %s: %s", metric_id, symbol_upper, exc)
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=_metric_failure_reason_from_exception(exc),
                    reason_detail=_metric_failure_message(exc),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        invariant_error = consume_metric_currency_invariant_error(metric)
        if invariant_error is not None:
            failures.append(
                _MetricComputationFailure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    reason=invariant_error.summary_reason,
                    message=str(invariant_error),
                )
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=invariant_error.summary_reason,
                    reason_detail=str(invariant_error),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        if result is None:
            LOGGER.warning(
                "Metric %s could not be computed for %s", metric_id, symbol_upper
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    listing_id=listing_id,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=_format_failure_reason(
                        warning_collector.records if warning_collector else (),
                        symbol_upper,
                    ),
                    reason_detail=None,
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        attempt = _metric_attempt_success(
            metric_id,
            metric,
            result,
            symbol=symbol_upper,
            listing_id=listing_id,
            attempted_at=attempted_at,
            facts_refreshed_at=facts_refreshed_at,
            market_snapshot_record=preloaded_market_snapshot_record,
        )
        rows.append(cast(IdKeyedStoredMetricRow, attempt.stored_row))
        attempts.append(attempt)
        computed += 1

    return _ComputedMetricsResult(
        symbol=symbol_upper,
        listing_id=listing_id,
        rows=tuple(rows),
        computed_count=computed,
        failures=tuple(failures),
        attempts=tuple(attempts),
    )


def _compute_metric_batch_results(
    listings: Sequence[Tuple[int, str]],
    metric_ids: Sequence[str],
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    suppress_metric_warnings: bool = True,
    profile_state: Optional["_MetricComputationProfile"] = None,
    preloaded_snapshots_by_id: Optional[Mapping[int, MarketSnapshotRecord]] = None,
    preloaded_facts_refresh_rows: Optional[
        Mapping[int, FinancialFactsRefreshStateRecord]
    ] = None,
) -> Tuple[_ComputedMetricsResult, ...]:
    # The batch is driven by ``(listing_id, display_symbol)`` pairs the scope
    # resolution already produced. The metric layer keys entirely on listing_id;
    # the display symbol is carried only for log/status labels and the result.
    selected_listings = [
        (int(listing_id), symbol.strip().upper()) for listing_id, symbol in listings
    ]
    if not selected_listings:
        return ()
    selected_ids = [listing_id for listing_id, _ in selected_listings]

    known_metric_ids = tuple(
        metric_id for metric_id in metric_ids if metric_id in REGISTRY
    )
    uses_financial_facts = bool(known_metric_ids) and _metrics_use_financial_facts(
        known_metric_ids
    )
    uses_market_data = (
        market_repo is not None
        and bool(known_metric_ids)
        and any(
            getattr(REGISTRY[metric_id], "uses_market_data", False)
            for metric_id in known_metric_ids
        )
    )
    profile_enabled = profile_state is not None and profile_state.enabled
    facts_by_id: Dict[int, List[FactRecord]] = {}
    facts_refresh_rows = dict(preloaded_facts_refresh_rows or {})
    snapshots_by_id: Dict[int, MarketSnapshotRecord] = {}
    if preloaded_snapshots_by_id is not None:
        selected_id_set = set(selected_ids)
        snapshots_by_id = {
            listing_id: snapshot
            for listing_id, snapshot in preloaded_snapshots_by_id.items()
            if listing_id in selected_id_set
        }
    needs_market_snapshot_load = uses_market_data and preloaded_snapshots_by_id is None
    needs_read_prefetch = uses_financial_facts or needs_market_snapshot_load
    if needs_read_prefetch:
        read_start = time.perf_counter() if profile_enabled else 0.0
        # Each prefetch opens its own short-lived connection inside the repo --
        # the compute phase is CPU-bound per symbol, so a few connection opens per
        # batch are negligible and the CLI never holds a sqlite connection.
        if uses_financial_facts:
            facts_by_id = _prefetch_metric_facts_for_ids(
                fact_repo,
                selected_ids,
                known_metric_ids,
                chunk_size=METRICS_COMPUTE_BATCH_SIZE,
            )
            if not facts_refresh_rows:
                facts_refresh_rows = _SchemaReadyFinancialFactsRefreshStateRepository(
                    fact_repo.db_path
                ).fetch_many_by_ids(
                    selected_ids,
                    chunk_size=METRICS_COMPUTE_BATCH_SIZE,
                )
        if needs_market_snapshot_load:
            assert market_repo is not None
            snapshots_by_id = market_repo.latest_snapshots_many_by_ids(
                selected_ids,
                chunk_size=METRICS_COMPUTE_BATCH_SIZE,
            )
        if profile_enabled:
            assert profile_state is not None
            profile_state.read_seconds += time.perf_counter() - read_start

    results: List[_ComputedMetricsResult] = []
    warning_collector = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(warning_collector)
    try:
        with suppress_console_metric_warnings(suppress_metric_warnings):
            # Bind one FX service for the whole batch; each symbol's compute
            # reuses it (one rate cache + one schema check per batch instead of
            # per symbol). This runs inside the worker process, so the context
            # var is visible to require_metric_money under multiprocessing.
            with reuse_or_bind_metric_fx_service(fact_repo, market_repo):
                for listing_id, display_symbol in selected_listings:
                    snapshot_record = snapshots_by_id.get(listing_id)
                    facts_refresh_row = facts_refresh_rows.get(listing_id)
                    results.append(
                        _compute_metrics_for_symbol(
                            display_symbol,
                            listing_id,
                            metric_ids,
                            fact_repo,
                            market_repo,
                            preloaded_facts=(
                                facts_by_id.get(listing_id, ())
                                if uses_financial_facts
                                else ()
                            ),
                            preloaded_market_snapshot_record=snapshot_record,
                            facts_refreshed_at=(
                                facts_refresh_row.refreshed_at
                                if facts_refresh_row is not None
                                else None
                            ),
                            warning_collector=warning_collector,
                        )
                    )
    finally:
        root_logger.removeHandler(warning_collector)
    return tuple(results)


def _compute_metrics_for_symbol_worker(
    database: Union[str, Path],
    listing: Tuple[int, str],
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> _ComputedMetricsResult:
    """Compute all requested metrics for one ``(listing_id, symbol)`` listing.

    The payload is the picklable ``(listing_id, display_symbol)`` pair the scope
    resolution produced, so the worker keys on the id with no symbol resolution.
    """

    fact_repo = _SchemaReadyFinancialFactsRepository(database)
    market_repo = (
        _SchemaReadyMarketDataRepository(database)
        if _metrics_use_market_data(metric_ids)
        else None
    )
    results = _compute_metric_batch_results(
        [listing],
        metric_ids,
        fact_repo,
        market_repo,
        suppress_metric_warnings=suppress_metric_warnings,
    )
    return results[0]


def _compute_metrics_for_symbol_batch_worker(
    database: Union[str, Path],
    listings: Sequence[Tuple[int, str]],
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> Tuple[_ComputedMetricsResult, ...]:
    """Compute requested metrics for a batch of ``(listing_id, symbol)`` listings."""

    fact_repo = _SchemaReadyFinancialFactsRepository(database)
    market_repo = (
        _SchemaReadyMarketDataRepository(database)
        if _metrics_use_market_data(metric_ids)
        else None
    )
    return _compute_metric_batch_results(
        listings,
        metric_ids,
        fact_repo,
        market_repo,
        suppress_metric_warnings=suppress_metric_warnings,
    )


def _compute_metrics_for_symbol_batch_worker_profiled(
    database: Union[str, Path],
    listings: Sequence[Tuple[int, str]],
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> _ProfiledComputedMetricsBatchResult:
    """Compute one worker batch and return worker-side timing breakdowns."""

    profile_state = _MetricComputationProfile(enabled=True)
    results = _compute_metric_batch_results(
        listings,
        metric_ids,
        _SchemaReadyFinancialFactsRepository(database),
        (
            _SchemaReadyMarketDataRepository(database)
            if _metrics_use_market_data(metric_ids)
            else None
        ),
        suppress_metric_warnings=suppress_metric_warnings,
        profile_state=profile_state,
    )
    return _ProfiledComputedMetricsBatchResult(
        results=results,
        read_seconds=profile_state.read_seconds,
        compute_seconds=profile_state.compute_seconds,
    )


@dataclass
class _MetricComputationProfile:
    """Wall-clock accumulator for compute-metrics phases."""

    enabled: bool = False
    read_seconds: float = 0.0
    compute_seconds: float = 0.0
    write_seconds: float = 0.0
    total_seconds: float = 0.0
    write_flush_count: int = 0
    write_row_count: int = 0


def _batch_listings(
    listings: Sequence[Tuple[int, str]],
    size: int,
) -> List[List[Tuple[int, str]]]:
    """Split ``(listing_id, display_symbol)`` pairs into stable ordered batches."""

    if size <= 0:
        raise ValueError("size must be positive")
    return [list(listings[idx : idx + size]) for idx in range(0, len(listings), size)]


def _run_metric_computation(
    database: str,
    listings: Sequence[Tuple[int, str]],
    metric_ids: Sequence[str],
    cancelled_message: str,
    suppress_metric_warnings: bool = True,
    profile: bool = False,
) -> int:
    db_path = _resolve_database_path(database)
    # The scope resolution already produced ``(listing_id, display_symbol)`` pairs;
    # the metric layer keys on the id end-to-end and the symbol stays a display
    # label only. Worker payloads carry the pairs directly (they are picklable).
    selected_listings = [
        (int(listing_id), symbol.upper()) for listing_id, symbol in listings
    ]
    total_symbols = len(selected_listings)

    profile_state = _MetricComputationProfile(enabled=profile)
    run_start_at = time.perf_counter()

    include_market_data = _metrics_use_market_data(metric_ids)
    base_metrics_repo = MetricsRepository(db_path)
    base_metrics_repo.initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    journal_mode = base_metrics_repo.ensure_wal_mode()
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    # One persistent writer session drives every flush in this run, so pragma
    # setup happens once and the SQLite page cache stays warm across the ~tens of
    # flushes a large universe produces. The session owns the connection +
    # per-flush transaction; close it on every exit path below.
    writer = MetricsWriteSession(metrics_repo, status_repo)

    print(
        f"Computing metrics for {total_symbols} symbols ({len(metric_ids)} metrics each)"
    )

    workers = _metric_worker_count(total_symbols)
    if workers > 1 and journal_mode != "wal":
        print(
            f"SQLite journal mode is {journal_mode or 'unknown'}; "
            "falling back to serial metric computation to avoid lock contention.",
            flush=True,
        )
        workers = 1

    pending_rows: List[IdKeyedStoredMetricRow] = []
    pending_attempts: List[_MetricAttemptResult] = []
    metric_failures: List[_MetricComputationFailure] = []
    buffered_symbols = 0
    last_flush_at = time.monotonic()
    completed_symbols = 0
    last_progress_at = time.monotonic()
    last_reported_completed = -1

    def buffer_result(result: _ComputedMetricsResult) -> None:
        nonlocal buffered_symbols, last_flush_at
        pending_rows.extend(result.rows)
        pending_attempts.extend(result.attempts)
        buffered_symbols += 1
        elapsed = time.monotonic() - last_flush_at
        if (
            buffered_symbols < METRICS_WRITE_BATCH_SIZE
            and elapsed < METRICS_WRITE_BATCH_INTERVAL_SECONDS
        ):
            return
        _flush_metric_write_batch(
            pending_rows,
            pending_attempts,
            writer,
            profile_state,
        )
        buffered_symbols = 0
        last_flush_at = time.monotonic()

    def flush_pending(force: bool = False) -> None:
        nonlocal buffered_symbols, last_flush_at
        if not pending_rows and not pending_attempts:
            return
        if not force:
            elapsed = time.monotonic() - last_flush_at
            if (
                buffered_symbols < METRICS_WRITE_BATCH_SIZE
                and elapsed < METRICS_WRITE_BATCH_INTERVAL_SECONDS
            ):
                return
        _flush_metric_write_batch(
            pending_rows,
            pending_attempts,
            writer,
            profile_state,
        )
        buffered_symbols = 0
        last_flush_at = time.monotonic()

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0:
            if force and last_reported_completed != 0:
                _print_metric_progress_bar(0, 0)
                last_reported_completed = 0
                last_progress_at = time.monotonic()
            return
        if completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < METRICS_PROGRESS_INTERVAL_SECONDS:
            return
        _print_metric_progress_bar(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    if workers <= 1:
        fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
        market_repo = (
            _SchemaReadyMarketDataRepository(db_path) if include_market_data else None
        )
        try:
            for listing_batch in _batch_listings(
                selected_listings,
                METRICS_COMPUTE_BATCH_SIZE,
            ):
                if profile_state.enabled:
                    batch_wall_start = time.perf_counter()
                    read_seconds_before = profile_state.read_seconds
                else:
                    batch_wall_start = 0.0
                    read_seconds_before = 0.0
                for result in _compute_metric_batch_results(
                    listing_batch,
                    metric_ids,
                    fact_repo,
                    market_repo,
                    suppress_metric_warnings=suppress_metric_warnings,
                    profile_state=profile_state if profile_state.enabled else None,
                ):
                    buffer_result(result)
                    metric_failures.extend(result.failures)
                    completed_symbols += 1
                    maybe_report_progress()
                if profile_state.enabled:
                    # Compute = batch wall time minus the read phase, which
                    # _compute_metric_batch_results already credited to
                    # profile_state.read_seconds.
                    batch_wall = time.perf_counter() - batch_wall_start
                    read_delta = profile_state.read_seconds - read_seconds_before
                    profile_state.compute_seconds += max(0.0, batch_wall - read_delta)
        except KeyboardInterrupt:
            return _cancel_cli_command(
                cancelled_message,
                flushers=[
                    lambda: flush_pending(force=True),
                    lambda: maybe_report_progress(force=True),
                    writer.close,
                ],
            )
        flush_pending(force=True)
        maybe_report_progress(force=True)
        _print_metric_invariant_failure_summary(metric_failures)
        print(f"Computed metrics for {total_symbols} symbols in {db_path}")
        if profile_state.enabled:
            profile_state.total_seconds = time.perf_counter() - run_start_at
            _print_metric_computation_profile(profile_state)
        writer.close()
        return 0

    executor = _create_process_pool_executor(workers)
    interrupted = False
    try:
        if total_symbols <= METRICS_COMPUTE_BATCH_SIZE:
            if profile_state.enabled:
                # The future is keyed by the display symbol used only for the
                # worker-crash log line; the worker payload is the picklable
                # ``(listing_id, symbol)`` pair.
                single_profile_futures: Dict[
                    Future[_ProfiledComputedMetricsBatchResult], str
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker_profiled,
                        str(db_path),
                        (listing,),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): listing[1]
                    for listing in selected_listings
                }
                for profiled_future in as_completed(single_profile_futures):
                    symbol = single_profile_futures[profiled_future]
                    try:
                        profiled_result_batch = profiled_future.result()
                        profile_state.read_seconds += profiled_result_batch.read_seconds
                        profile_state.compute_seconds += (
                            profiled_result_batch.compute_seconds
                        )
                        for result in profiled_result_batch.results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %s: %s", symbol, exc
                        )
                    completed_symbols += 1
                    maybe_report_progress()
            else:
                single_result_futures: Dict[Future[_ComputedMetricsResult], str] = {
                    executor.submit(
                        _compute_metrics_for_symbol_worker,
                        str(db_path),
                        listing,
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): listing[1]
                    for listing in selected_listings
                }
                for result_future in as_completed(single_result_futures):
                    symbol = single_result_futures[result_future]
                    try:
                        computed_result = result_future.result()
                        buffer_result(computed_result)
                        metric_failures.extend(computed_result.failures)
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %s: %s", symbol, exc
                        )
                    completed_symbols += 1
                    maybe_report_progress()
        else:
            if profile_state.enabled:
                batch_profile_futures: Dict[
                    Future[_ProfiledComputedMetricsBatchResult],
                    Tuple[Tuple[int, str], ...],
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker_profiled,
                        str(db_path),
                        tuple(listing_batch),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): tuple(listing_batch)
                    for listing_batch in _batch_listings(
                        selected_listings,
                        METRICS_COMPUTE_BATCH_SIZE,
                    )
                }
                for profiled_batch_future in as_completed(batch_profile_futures):
                    batch_listings = batch_profile_futures[profiled_batch_future]
                    try:
                        profiled_result_batch = profiled_batch_future.result()
                        profile_state.read_seconds += profiled_result_batch.read_seconds
                        profile_state.compute_seconds += (
                            profiled_result_batch.compute_seconds
                        )
                        for result in profiled_result_batch.results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                            completed_symbols += 1
                            maybe_report_progress()
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %d-symbol batch starting at %s: %s",
                            len(batch_listings),
                            batch_listings[0][1],
                            exc,
                        )
                        completed_symbols += len(batch_listings)
                        maybe_report_progress()
            else:
                batch_result_futures: Dict[
                    Future[Tuple[_ComputedMetricsResult, ...]],
                    Tuple[Tuple[int, str], ...],
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker,
                        str(db_path),
                        tuple(listing_batch),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): tuple(listing_batch)
                    for listing_batch in _batch_listings(
                        selected_listings,
                        METRICS_COMPUTE_BATCH_SIZE,
                    )
                }
                for result_batch_future in as_completed(batch_result_futures):
                    batch_listings = batch_result_futures[result_batch_future]
                    try:
                        computed_batch_results = result_batch_future.result()
                        for result in computed_batch_results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                            completed_symbols += 1
                            maybe_report_progress()
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %d-symbol batch starting at %s: %s",
                            len(batch_listings),
                            batch_listings[0][1],
                            exc,
                        )
                        completed_symbols += len(batch_listings)
                        maybe_report_progress()
    except KeyboardInterrupt:
        interrupted = True
        return _cancel_cli_command(
            cancelled_message,
            executors=[executor],
            flushers=[
                lambda: flush_pending(force=True),
                lambda: maybe_report_progress(force=True),
                writer.close,
            ],
        )
    finally:
        if not interrupted:
            executor.shutdown(wait=True)

    flush_pending(force=True)
    maybe_report_progress(force=True)
    _print_metric_invariant_failure_summary(metric_failures)
    print(f"Computed metrics for {total_symbols} symbols in {db_path}")
    if profile_state.enabled:
        profile_state.total_seconds = time.perf_counter() - run_start_at
        _print_metric_computation_profile(profile_state)
    writer.close()
    return 0


def _print_metric_computation_profile(
    profile_state: "_MetricComputationProfile",
) -> None:
    """Emit a one-line wall-clock summary of compute-metrics phases."""

    parent_phase_seconds = (
        profile_state.read_seconds
        + profile_state.compute_seconds
        + profile_state.write_seconds
    )
    other_seconds = max(0.0, profile_state.total_seconds - parent_phase_seconds)
    print(
        "Profile: "
        f"read={profile_state.read_seconds:.2f}s "
        f"compute={profile_state.compute_seconds:.2f}s "
        f"write={profile_state.write_seconds:.2f}s "
        f"other={other_seconds:.2f}s "
        f"total={profile_state.total_seconds:.2f}s "
        f"flushes={profile_state.write_flush_count} "
        f"rows_written={profile_state.write_row_count}",
        flush=True,
    )


def cmd_compute_metrics_stage(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
    show_metric_warnings: bool = False,
    profile: bool = False,
) -> int:
    """Unified metric computation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    # Resolve the scope to (listing_id, canonical_symbol) pairs so the natural
    # listing_id rides through reads and writes -- compute-metrics never
    # re-resolves symbol->listing_id from the DB. The pairs are carried straight
    # into the compute as worker payloads.
    scope_listings, _explicit_symbols, _resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    ids_to_compute = _validated_metric_ids(metric_ids)
    return _run_metric_computation(
        database=str(db_path),
        listings=scope_listings,
        metric_ids=ids_to_compute,
        cancelled_message="\nMetric computation cancelled by user.",
        suppress_metric_warnings=not show_metric_warnings,
        profile=profile,
    )


def _format_failure_reason(records: Sequence[logging.LogRecord], symbol: str) -> str:
    if not records:
        return "no warning emitted"

    record = records[0]
    msg = record.msg if isinstance(record.msg, str) else str(record.msg)
    args = record.args
    if not args:
        return msg

    def transform(arg: object) -> object:
        if isinstance(arg, str):
            if arg.upper() == symbol.upper():
                return "<symbol>"
            if _DATE_PATTERN.match(arg):
                return "<date>"
            return arg
        if isinstance(arg, (int, float)):
            return "<n>"
        return arg

    try:
        if isinstance(args, dict):
            transformed_map = {key: transform(value) for key, value in args.items()}
            return msg % transformed_map
        if not isinstance(args, tuple):
            args = (args,)
        transformed_args = tuple(transform(value) for value in args)
        return msg % transformed_args
    except Exception:
        return record.getMessage()


def _metric_failure_reason_from_exception(exc: Exception) -> str:
    """Return a compact failure reason for one metric exception."""

    if isinstance(exc, MetricCurrencyInvariantError):
        return exc.summary_reason
    return f"exception: {exc.__class__.__name__}"


def _metric_failure_message(exc: Exception) -> str:
    """Return the detailed user/log message for one metric exception."""

    return str(exc)


def _print_metric_invariant_failure_summary(
    failures: Sequence[_MetricComputationFailure],
    *,
    symbol: Optional[str] = None,
) -> None:
    """Print a compact grouped summary for currency-invariant metric failures."""

    invariant_failures = [
        failure
        for failure in failures
        if failure.reason.startswith("currency invariant:")
    ]
    if not invariant_failures:
        return

    grouped = Counter(
        (failure.metric_id, failure.reason) for failure in invariant_failures
    )
    examples: Dict[tuple[str, str], _MetricComputationFailure] = {}
    for failure in invariant_failures:
        examples.setdefault((failure.metric_id, failure.reason), failure)

    if symbol is not None:
        print(f"Metric currency invariant failures for {symbol}:")
    else:
        print(
            f"Metric currency invariant failures: {len(invariant_failures)} across {len(grouped)} grouped reasons"
        )

    ordered = sorted(
        grouped.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1]),
    )
    for (metric_id, reason), count in ordered:
        example = examples[(metric_id, reason)]
        if symbol is not None:
            print(f"- {metric_id}: {reason} ({example.message})")
            continue
        print(f"- {metric_id}: {reason} ({count}, example={example.symbol})")
