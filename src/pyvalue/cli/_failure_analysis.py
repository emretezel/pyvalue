"""Failure analysis over persisted metric attempt state.

This is the read-only engine behind ``report-metric-status --reasons``: it
classifies each (listing, metric) pair's persisted state, buckets the
non-usable ones by reason, and picks a representative (largest-market-cap)
example per bucket. Nothing here recomputes metrics or writes to the database
-- ``compute-metrics`` is the only writer of metric state, and these
diagnostics simply explain what it last stored.

Author: Emre Tezel
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Mapping, Optional, Tuple

from pyvalue.persistence.storage import (
    FinancialFactsRepository,
    MarketSnapshotRecord,
)

from ._common import (
    _MetricAvailabilityState,
    _format_value,
)

# Long ``reason_detail`` payloads (currency-invariant dumps, exception text)
# would drown the console report; the CSV keeps the full text.
REASON_DETAIL_CONSOLE_MAX_CHARS = 120

# Bucket labels for pairs with no trustworthy persisted failure reason. Both
# carry the remedy in the label because the report is read-only by design: the
# user reruns the pipeline command instead of the report recomputing anything.
STALE_INPUTS_REASON = "stale_inputs (run compute-metrics)"
NEVER_ATTEMPTED_REASON = "never_attempted (run compute-metrics)"


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
) -> Literal["fresh_failure", "usable", "stale", "never_attempted"]:
    """Decide how a persisted availability state feeds the failure analysis.

    - ``fresh_failure``: the last attempt failed and its freshness watermarks
      still match the current inputs -- its reason_code is trustworthy.
    - ``usable``: a fresh stored value exists (with or without a status row) --
      nothing to report for this pair.
    - ``stale``: persisted state exists but no longer matches the current
      inputs (moved watermarks, or a fresh success whose stored row vanished)
      -- only ``compute-metrics`` can refresh the verdict.
    - ``never_attempted``: no persisted state at all for the pair.
    """

    if state is None:
        return "never_attempted"
    if state.status_record is not None and not state.stale:
        if state.status_record.status == "failure":
            return "fresh_failure"
        if state.record is not None:
            return "usable"
        # A fresh success whose stored metric row is gone: the status no
        # longer describes reality, so treat it like moved inputs.
        return "stale"
    if state.status_record is None and state.record is None:
        return "never_attempted"
    if state.status_record is None and state.record is not None and not state.stale:
        return "usable"
    return "stale"


def tally_persisted_states(
    tally: FailureTally,
    metric_id: str,
    scope_listings: Iterable[Tuple[int, str]],
    states_by_id: Mapping[int, Mapping[str, _MetricAvailabilityState]],
) -> None:
    """Bucket every non-usable (listing, metric) persisted state into ``tally``.

    Fresh failures bucket under their persisted ``reason_code`` and carry the
    untemplated ``reason_detail`` on the example; stale and never-attempted
    pairs land in explicit remedy buckets, with the stale example's detail
    summarizing the last (now untrustworthy) attempt so the console still says
    what the pair looked like before its inputs moved.
    """

    for listing_id, symbol in scope_listings:
        state = states_by_id.get(listing_id, {}).get(metric_id)
        verdict = classify_availability_state(state)
        if verdict == "usable":
            continue
        if verdict == "fresh_failure":
            assert state is not None and state.status_record is not None
            tally.record(
                metric_id=metric_id,
                reason=state.status_record.reason_code or "no warning emitted",
                listing_id=listing_id,
                symbol=symbol,
                reason_detail=state.status_record.reason_detail,
            )
            continue
        if verdict == "never_attempted":
            tally.record(
                metric_id=metric_id,
                reason=NEVER_ATTEMPTED_REASON,
                listing_id=listing_id,
                symbol=symbol,
            )
            continue
        detail: Optional[str] = None
        if state is not None and state.status_record is not None:
            detail = f"last attempt: {state.status_record.status}"
            if state.status_record.reason_code:
                detail += f", {state.status_record.reason_code}"
        elif state is not None and state.record is not None:
            detail = f"stored value as_of {state.record.as_of}, no status row"
        tally.record(
            metric_id=metric_id,
            reason=STALE_INPUTS_REASON,
            listing_id=listing_id,
            symbol=symbol,
            reason_detail=detail,
        )


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
