"""Shared failure-analysis engine primitives.

Author: Emre Tezel
"""

from __future__ import annotations

import pytest

from pyvalue.cli._common import _MetricAvailabilityState
from pyvalue.cli._failure_analysis import (
    NEVER_ATTEMPTED_REASON,
    REASON_DETAIL_CONSOLE_MAX_CHARS,
    STALE_INPUTS_REASON,
    FailureTally,
    _FailureExample,
    classify_availability_state,
    failure_example_console_suffix,
    failure_example_csv_cells,
    tally_persisted_states,
)
from pyvalue.persistence.storage import MetricComputeStatusRecord, MetricRecord


def _record(value: float = 1.0, as_of: str = "2026-03-31") -> MetricRecord:
    return MetricRecord(
        value=value, as_of=as_of, unit_kind="other", currency=None, unit_label=None
    )


def _status(status: str, value_as_of: str | None = None) -> MetricComputeStatusRecord:
    assert status in ("success", "failure")
    return MetricComputeStatusRecord(
        metric_id="m",
        status="success" if status == "success" else "failure",
        attempted_at="2026-07-04T00:00:00Z",
        reason_code="m: guard tripped",
        reason_detail="inputs: a=1 b=0",
        value_as_of=value_as_of,
    )


def test_tally_counts_and_prefers_larger_market_cap_example() -> None:
    tally = FailureTally(["m"], {1: 10.0, 2: 99.0, 3: None})

    tally.record(
        metric_id="m", reason="r", listing_id=1, symbol="AAA.US", reason_detail="d1"
    )
    # Larger cap replaces the example (and carries its own detail with it).
    tally.record(
        metric_id="m", reason="r", listing_id=2, symbol="BBB.US", reason_detail="d2"
    )
    # Unknown cap never replaces an existing example.
    tally.record(
        metric_id="m", reason="r", listing_id=3, symbol="CCC.US", reason_detail="d3"
    )
    # A different reason gets its own bucket, even for a cap-less listing.
    tally.record(metric_id="m", reason="other", listing_id=3, symbol="CCC.US")

    assert tally.failures["m"]["r"] == 3
    assert tally.failures["m"]["other"] == 1
    assert tally.examples["m"]["r"] == _FailureExample(
        symbol="BBB.US", market_cap=99.0, reason_detail="d2"
    )
    assert tally.examples["m"]["other"] == _FailureExample(
        symbol="CCC.US", market_cap=None, reason_detail=None
    )


def test_tally_accepts_unseeded_metric_ids() -> None:
    """unknown_metric_id buckets arrive for metrics outside the initial set."""

    tally = FailureTally([], {})
    tally.record(
        metric_id="ghost", reason="unknown_metric_id", listing_id=1, symbol="AAA.US"
    )
    assert tally.failures["ghost"]["unknown_metric_id"] == 1


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        # No persisted knowledge at all.
        (None, "never_attempted"),
        # Fresh persisted failure -> its reason_code is trustworthy.
        (
            _MetricAvailabilityState(
                metric_id="m",
                record=None,
                status_record=_status("failure"),
                stale=False,
            ),
            "fresh_failure",
        ),
        # Stale persisted failure -> only compute-metrics can refresh it.
        (
            _MetricAvailabilityState(
                metric_id="m",
                record=None,
                status_record=_status("failure"),
                stale=True,
            ),
            "stale",
        ),
        # Fresh success with its stored row -> nothing to report.
        (
            _MetricAvailabilityState(
                metric_id="m",
                record=_record(),
                status_record=_status("success", value_as_of="2026-03-31"),
                stale=False,
            ),
            "usable",
        ),
        # Fresh success whose stored row vanished -> status no longer real.
        (
            _MetricAvailabilityState(
                metric_id="m",
                record=None,
                status_record=_status("success", value_as_of="2026-03-31"),
                stale=False,
            ),
            "stale",
        ),
        # Legacy stored row without any status -> usable as-is.
        (
            _MetricAvailabilityState(
                metric_id="m", record=_record(), status_record=None, stale=False
            ),
            "usable",
        ),
        # Legacy stored row whose inputs moved -> stale.
        (
            _MetricAvailabilityState(
                metric_id="m", record=_record(), status_record=None, stale=True
            ),
            "stale",
        ),
        # An empty state object (no row, no status) -> never attempted.
        (
            _MetricAvailabilityState(
                metric_id="m", record=None, status_record=None, stale=False
            ),
            "never_attempted",
        ),
    ],
)
def test_classify_availability_state(
    state: _MetricAvailabilityState | None, expected: str
) -> None:
    assert classify_availability_state(state) == expected


def test_tally_persisted_states_buckets_by_verdict() -> None:
    """Fresh failures keep reason_code; stale/never-attempted get remedy buckets."""

    tally = FailureTally(["m"], {})
    states = {
        1: {
            "m": _MetricAvailabilityState(
                metric_id="m",
                record=None,
                status_record=_status("failure"),
                stale=False,
            )
        },
        2: {
            "m": _MetricAvailabilityState(
                metric_id="m",
                record=None,
                status_record=_status("failure"),
                stale=True,
            )
        },
        # Listing 3 has no state at all -> never attempted.
        4: {
            "m": _MetricAvailabilityState(
                metric_id="m",
                record=_record(),
                status_record=_status("success", value_as_of="2026-03-31"),
                stale=False,
            )
        },
        # Legacy stale stored row without a status row.
        5: {
            "m": _MetricAvailabilityState(
                metric_id="m", record=_record(), status_record=None, stale=True
            )
        },
    }
    scope = [(1, "AAA.US"), (2, "BBB.US"), (3, "CCC.US"), (4, "DDD.US"), (5, "EEE.US")]

    tally_persisted_states(tally, "m", scope, states)

    assert tally.failures["m"]["m: guard tripped"] == 1
    assert tally.failures["m"][STALE_INPUTS_REASON] == 2
    assert tally.failures["m"][NEVER_ATTEMPTED_REASON] == 1
    # The usable pair (listing 4) contributes nothing.
    assert sum(tally.failures["m"].values()) == 4
    # Fresh failures surface their untemplated persisted detail...
    assert tally.examples["m"]["m: guard tripped"].reason_detail == "inputs: a=1 b=0"
    # ...while stale examples summarize the last, now untrustworthy, attempt
    # (first stale listing wins the example: no market caps were supplied).
    assert tally.examples["m"][STALE_INPUTS_REASON].reason_detail == (
        "last attempt: failure, m: guard tripped"
    )
    assert tally.examples["m"][NEVER_ATTEMPTED_REASON].symbol == "CCC.US"


def test_console_suffix_formats_example_and_truncates_detail() -> None:
    assert failure_example_console_suffix(None) == ""
    assert (
        failure_example_console_suffix(
            _FailureExample(symbol="AAA.US", market_cap=None)
        )
        == " (example=AAA.US, market_cap=N/A)"
    )
    long_detail = "x" * (REASON_DETAIL_CONSOLE_MAX_CHARS + 20)
    suffix = failure_example_console_suffix(
        _FailureExample(
            symbol="AAA.US", market_cap=2_000_000.0, reason_detail=long_detail
        )
    )
    assert "example=AAA.US" in suffix
    assert "detail=" in suffix
    assert suffix.endswith("…)")
    # Truncated payload: max-chars minus one for the ellipsis marker.
    assert ("x" * (REASON_DETAIL_CONSOLE_MAX_CHARS - 1) + "…") in suffix
    assert ("x" * REASON_DETAIL_CONSOLE_MAX_CHARS) not in suffix


def test_csv_cells_keep_full_detail() -> None:
    assert failure_example_csv_cells(None) == ["", "", ""]
    assert failure_example_csv_cells(
        _FailureExample(symbol="AAA.US", market_cap=5.0, reason_detail="full detail")
    ) == ["AAA.US", 5.0, "full detail"]
    assert failure_example_csv_cells(
        _FailureExample(symbol="AAA.US", market_cap=None, reason_detail=None)
    ) == ["AAA.US", "", ""]
