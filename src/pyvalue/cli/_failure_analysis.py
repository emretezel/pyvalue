"""Shared failure-analysis engine for the metric/screen failure reports.

``report-metric-failures`` and ``report-screen-failures`` iterate differently
(per metric over the whole scope vs per listing over a screen's missing
metrics), but everything downstream of the iteration is identical: deciding
whether a persisted status can be trusted, bucketing failures by reason,
choosing a representative example, persisting recomputed attempts, and
rendering the example cells. That shared core lives here exactly once so the
two commands stay thin front-ends and cannot drift apart again.

Author: Emre Tezel
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Mapping, Optional, Sequence

from pyvalue.persistence.storage import (
    FinancialFactsRepository,
    MarketSnapshotRecord,
    MetricComputeStatusRepository,
    MetricsRepository,
)

from ._common import (
    _MetricAttemptResult,
    _MetricAvailabilityState,
    _format_value,
    _metric_status_rows_from_attempts,
)

# Long ``reason_detail`` payloads (currency-invariant dumps, exception text)
# would drown the console report; the CSV keeps the full text.
REASON_DETAIL_CONSOLE_MAX_CHARS = 120


@dataclass(frozen=True)
class _FailureExample:
    """Representative failing listing for one (metric, reason) bucket.

    ``reason_detail`` is the untemplated detail of *this* example's attempt
    (persisted or recomputed), so symbol and detail always describe the same
    listing.
    """

    symbol: str
    market_cap: Optional[float]
    reason_detail: Optional[str] = None


class FailureTally:
    """Accumulates failure counts and representative examples per metric.

    Both failure reports feed one tally from their persisted-status and
    recompute paths, so the bucketing and example-selection policy exists in
    one place. The example kept for a bucket is the largest-market-cap listing
    seen so far: diagnostics read better anchored to a well-known name than to
    whichever micro-cap happened to be scanned first.
    """

    def __init__(
        self,
        metric_ids: Iterable[str],
        market_caps: Mapping[int, Optional[float]],
    ) -> None:
        self.failures: Dict[str, Counter[str]] = {
            metric_id: Counter() for metric_id in metric_ids
        }
        self.examples: Dict[str, Dict[str, _FailureExample]] = {
            metric_id: {} for metric_id in self.failures
        }
        self._market_caps = market_caps

    def record(
        self,
        *,
        metric_id: str,
        reason: str,
        listing_id: int,
        symbol: str,
        reason_detail: Optional[str] = None,
    ) -> None:
        """Count one failure and keep the biggest-cap example for its bucket."""

        self.failures.setdefault(metric_id, Counter())[reason] += 1
        examples = self.examples.setdefault(metric_id, {})
        market_cap = self._market_caps.get(listing_id)
        current = examples.get(reason)
        if current is None or (
            market_cap is not None
            and (current.market_cap is None or market_cap > current.market_cap)
        ):
            examples[reason] = _FailureExample(
                symbol=symbol,
                market_cap=market_cap,
                reason_detail=reason_detail,
            )


def classify_availability_state(
    state: Optional[_MetricAvailabilityState],
) -> Literal["fresh_failure", "usable", "pending"]:
    """Decide how a persisted availability state feeds the failure analysis.

    - ``fresh_failure``: the last attempt failed and its freshness watermarks
      still match the current inputs -- count it directly, no recompute.
    - ``usable``: a fresh stored value exists (with or without a status row) --
      nothing to report for this pair.
    - ``pending``: no state, stale state, or fresh success whose stored row is
      gone -- the pair must be recomputed to learn its current outcome.
    """

    if state is None:
        return "pending"
    if state.status_record is not None and not state.stale:
        if state.status_record.status == "failure":
            return "fresh_failure"
        if state.record is not None:
            return "usable"
        return "pending"
    if state.status_record is None and state.record is not None and not state.stale:
        return "usable"
    return "pending"


def _estimate_market_caps(
    fact_repo: FinancialFactsRepository,
    snapshots_by_id: Mapping[int, MarketSnapshotRecord],
) -> Dict[int, Optional[float]]:
    """Estimate market caps (latest shares x latest price) for report examples.

    Market cap is no longer a stored column; this is a diagnostic-only sizing
    heuristic used to pick a representative (large) failing example. It pairs the
    latest share count with the latest price rather than the share-count-dated
    price the metrics use -- close enough for ranking examples by size, and cheap
    (one bulk share-count read by listing_id over the already-loaded snapshots).
    The result is keyed by ``listing_id``.
    """

    estimates: Dict[int, Optional[float]] = {}
    if not snapshots_by_id:
        return estimates
    share_counts = fact_repo.latest_share_counts_many_by_ids(
        list(snapshots_by_id.keys()),
    )
    for listing_id, snapshot in snapshots_by_id.items():
        shares = share_counts.get(listing_id)
        if (
            shares is None
            or shares <= 0
            or snapshot.price is None
            or snapshot.price <= 0
        ):
            continue
        estimates[listing_id] = snapshot.price * shares
    return estimates


def _persist_metric_attempts(
    metrics_repo: MetricsRepository,
    status_repo: MetricComputeStatusRepository,
    attempts: Sequence[_MetricAttemptResult],
) -> None:
    # The recomputed attempts carry the scope-resolved listing_id, so the writers
    # select by id with no symbol->id resolution. ``stored_row`` is already the
    # id-led row shape consumed by ``upsert_many_by_id``.
    metric_rows = [
        attempt.stored_row for attempt in attempts if attempt.stored_row is not None
    ]
    if metric_rows:
        metrics_repo.upsert_many_by_id(metric_rows)
    status_rows = _metric_status_rows_from_attempts(attempts)
    if status_rows:
        status_repo.upsert_many_by_id(status_rows)


def failure_example_console_suffix(example: Optional[_FailureExample]) -> str:
    """Render the parenthetical example suffix for one console reason line.

    Empty when there is no example; the ``detail=`` segment appears only when
    the example's attempt carried an untemplated ``reason_detail`` (truncated
    to keep the console scannable -- the CSV holds the full text).
    """

    if example is None:
        return ""
    cap_display = (
        _format_value(example.market_cap) if example.market_cap is not None else "N/A"
    )
    suffix = f" (example={example.symbol}, market_cap={cap_display}"
    if example.reason_detail:
        detail = example.reason_detail
        if len(detail) > REASON_DETAIL_CONSOLE_MAX_CHARS:
            detail = detail[: REASON_DETAIL_CONSOLE_MAX_CHARS - 1] + "…"
        suffix += f", detail={detail}"
    return suffix + ")"


def failure_example_csv_cells(example: Optional[_FailureExample]) -> List[object]:
    """Render the shared trailing CSV cells for one (metric, reason) row.

    Column order: example_symbol, example_market_cap, example_reason_detail.
    Both failure-report CSV writers append these so the example columns cannot
    diverge between the two files.
    """

    if example is None:
        return ["", "", ""]
    return [
        example.symbol,
        example.market_cap if example.market_cap is not None else "",
        example.reason_detail or "",
    ]
