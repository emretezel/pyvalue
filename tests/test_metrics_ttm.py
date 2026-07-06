"""Tests for the cadence-aware TTM window resolver.

Author: Emre Tezel
"""

from datetime import date, timedelta

from hypothesis import given
from hypothesis import strategies as st

from pyvalue.metrics.ttm import (
    FAILURE_LATEST_QUARTER_TOO_OLD,
    FAILURE_NO_TTM_CADENCE,
    FAILURE_TOO_FEW_QUARTERLY_RECORDS,
    QUARTERLY_GAP_DAYS,
    SEMI_ANNUAL_GAP_DAYS,
    paired_records,
    resolve_ttm_window,
)
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS
from pyvalue.persistence.storage import FactRecord
from test_metrics import fact

# All window tests pin the clock so fixtures are absolute dates, not
# today-relative arithmetic (the resolver threads reference_date through to
# the freshness gate for exactly this purpose).
REFERENCE = date(2026, 7, 6)

# Contiguous calendar quarters, newest-first (gaps 90/92/92 days).
QUARTERLY_DATES = ("2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30")
# Contiguous half-years, newest-first (gaps 184/181/184 days).
SEMI_ANNUAL_DATES = ("2025-12-31", "2025-06-30", "2024-12-31", "2024-06-30")


def _rows(
    dates: tuple[str, ...],
    *,
    periods: tuple[str, ...] | None = None,
    concept: str = "OperatingIncomeLoss",
) -> list[FactRecord]:
    resolved_periods = periods or ("Q4", "Q3", "Q2", "Q1") * 2
    return [
        fact(
            concept=concept,
            fiscal_period=period,
            end_date=end_date,
            value=float(index + 1),
        )
        for index, (end_date, period) in enumerate(zip(dates, resolved_periods))
    ]


def test_clean_quarterly_resolves_four_row_window() -> None:
    resolution = resolve_ttm_window(_rows(QUARTERLY_DATES), reference_date=REFERENCE)
    window = resolution.window
    assert resolution.failure is None
    assert window is not None
    assert window.cadence == "quarterly"
    assert [record.end_date for record in window.records] == list(QUARTERLY_DATES)
    assert window.as_of == QUARTERLY_DATES[0]


def test_clean_semi_annual_resolves_two_row_window() -> None:
    resolution = resolve_ttm_window(
        _rows(SEMI_ANNUAL_DATES, periods=("Q4", "Q2", "Q4", "Q2")),
        reference_date=REFERENCE,
    )
    window = resolution.window
    assert resolution.failure is None
    assert window is not None
    assert window.cadence == "semi_annual"
    # Two half-year rows cover twelve months; the older halves stay out.
    assert [record.end_date for record in window.records] == [
        "2025-12-31",
        "2025-06-30",
    ]
    assert window.as_of == "2025-12-31"


def test_semi_annual_pair_without_third_row_is_accepted() -> None:
    # History-boundary case: a young semi-annual listing has exactly two rows.
    resolution = resolve_ttm_window(
        _rows(SEMI_ANNUAL_DATES[:2], periods=("Q4", "Q2")),
        reference_date=REFERENCE,
    )
    assert resolution.window is not None
    assert resolution.window.cadence == "semi_annual"


def test_hole_below_anchor_refuses_window() -> None:
    # 184/91 day gaps: either a missing quarter right below the anchor or a
    # cadence transition -- both would sum to something that is not a TTM.
    dates = ("2025-12-31", "2025-06-30", "2025-03-31", "2024-12-31")
    resolution = resolve_ttm_window(
        _rows(dates, periods=("Q4", "Q2", "Q1", "Q4")), reference_date=REFERENCE
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_NO_TTM_CADENCE


def test_quarterly_window_ignores_older_hole() -> None:
    # The newest four rows are contiguous; a gap further back is irrelevant.
    dates = QUARTERLY_DATES + ("2024-06-30",)
    resolution = resolve_ttm_window(
        _rows(dates, periods=("Q1", "Q4", "Q3", "Q2", "Q2")),
        reference_date=REFERENCE,
    )
    assert resolution.window is not None
    assert resolution.window.cadence == "quarterly"
    assert len(resolution.window.records) == 4


def test_three_quarterly_rows_are_too_few() -> None:
    resolution = resolve_ttm_window(
        _rows(QUARTERLY_DATES[:3]), reference_date=REFERENCE
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_TOO_FEW_QUARTERLY_RECORDS


def test_single_row_is_too_few() -> None:
    resolution = resolve_ttm_window(
        _rows(QUARTERLY_DATES[:1]), reference_date=REFERENCE
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_TOO_FEW_QUARTERLY_RECORDS


def test_stale_anchor_refuses_window() -> None:
    stale_reference = date.fromisoformat(QUARTERLY_DATES[0]) + timedelta(days=401)
    resolution = resolve_ttm_window(
        _rows(QUARTERLY_DATES), reference_date=stale_reference
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_LATEST_QUARTER_TOO_OLD


def test_dedupe_keeps_first_record_per_end_date() -> None:
    # Repositories return newest-filed first; the first row seen per end_date
    # must win so amendments beat originals exactly as they did pre-refactor.
    original = fact(
        concept="OperatingIncomeLoss",
        fiscal_period="Q1",
        end_date=QUARTERLY_DATES[0],
        value=999.0,
    )
    amended = fact(
        concept="OperatingIncomeLoss",
        fiscal_period="Q1",
        end_date=QUARTERLY_DATES[0],
        value=111.0,
    )
    rows = [amended, original, *_rows(QUARTERLY_DATES[1:])]
    resolution = resolve_ttm_window(rows, reference_date=REFERENCE)
    assert resolution.window is not None
    assert resolution.window.records[0].value == 111.0


def test_unsorted_input_resolves_same_window() -> None:
    rows = _rows(QUARTERLY_DATES)
    resolution_sorted = resolve_ttm_window(rows, reference_date=REFERENCE)
    resolution_shuffled = resolve_ttm_window(
        list(reversed(rows)), reference_date=REFERENCE
    )
    assert resolution_sorted.window is not None
    assert resolution_shuffled.window is not None
    assert [record.end_date for record in resolution_sorted.window.records] == [
        record.end_date for record in resolution_shuffled.window.records
    ]


def test_non_quarterly_periods_are_excluded() -> None:
    # An FY row newer than every quarter must not enter (or anchor) the window.
    fy_row = fact(
        concept="OperatingIncomeLoss",
        fiscal_period="FY",
        end_date="2026-04-30",
        value=400.0,
    )
    resolution = resolve_ttm_window(
        [fy_row, *_rows(QUARTERLY_DATES)], reference_date=REFERENCE
    )
    assert resolution.window is not None
    assert resolution.window.as_of == QUARTERLY_DATES[0]


def test_lowercase_fiscal_period_is_accepted() -> None:
    resolution = resolve_ttm_window(
        _rows(QUARTERLY_DATES, periods=("q1", "q4", "q3", "q2")),
        reference_date=REFERENCE,
    )
    assert resolution.window is not None


def test_unparseable_end_date_rows_are_dropped() -> None:
    # A garbage end_date must neither anchor the window (it would string-sort
    # above real ISO dates) nor break gap arithmetic: the row is dropped, and
    # the remaining three quarters are simply too few for a window.
    rows = _rows(QUARTERLY_DATES)
    rows[2] = fact(
        concept="OperatingIncomeLoss",
        fiscal_period="Q3",
        end_date="not-a-date",
        value=3.0,
    )
    resolution = resolve_ttm_window(rows, reference_date=REFERENCE)
    assert resolution.window is None
    assert resolution.failure == FAILURE_TOO_FEW_QUARTERLY_RECORDS


def test_bad_date_in_deep_history_is_ignored() -> None:
    rows = _rows(QUARTERLY_DATES) + [
        fact(
            concept="OperatingIncomeLoss",
            fiscal_period="Q1",
            end_date="1900-bad",
            value=5.0,
        )
    ]
    resolution = resolve_ttm_window(rows, reference_date=REFERENCE)
    assert resolution.window is not None


def test_paired_records_matches_every_window_row() -> None:
    resolution = resolve_ttm_window(_rows(QUARTERLY_DATES), reference_date=REFERENCE)
    assert resolution.window is not None
    candidates = _rows(QUARTERLY_DATES, concept="DepreciationDepletionAndAmortization")
    pairs = paired_records(resolution.window, candidates)
    assert pairs is not None
    assert [(left.end_date, right.end_date) for left, right in pairs] == [
        (end_date, end_date) for end_date in QUARTERLY_DATES
    ]


def test_paired_records_fails_on_any_missing_quarter() -> None:
    resolution = resolve_ttm_window(_rows(QUARTERLY_DATES), reference_date=REFERENCE)
    assert resolution.window is not None
    candidates = _rows(
        QUARTERLY_DATES[:3], concept="DepreciationDepletionAndAmortization"
    )
    assert paired_records(resolution.window, candidates) is None


def test_paired_records_ignores_fy_row_sharing_quarter_end_date() -> None:
    # An FY D&A row often shares Q4's end_date; matching it would sum an
    # annual amount into a quarterly chain.
    resolution = resolve_ttm_window(_rows(QUARTERLY_DATES), reference_date=REFERENCE)
    assert resolution.window is not None
    candidates = _rows(
        QUARTERLY_DATES[1:], concept="DepreciationDepletionAndAmortization"
    ) + [
        fact(
            concept="DepreciationDepletionAndAmortization",
            fiscal_period="FY",
            end_date=QUARTERLY_DATES[0],
            value=400.0,
        )
    ]
    assert paired_records(resolution.window, candidates) is None


def _fy_row(
    end_date: str, *, value: float = 100.0, concept: str = "OperatingIncomeLoss"
) -> FactRecord:
    return fact(
        concept=concept,
        fiscal_period="FY",
        end_date=end_date,
        value=value,
    )


def test_annual_opt_in_resolves_a_lone_fresh_fy_row() -> None:
    # An annual-only filer: a single fresh FY row and no quarters. With the
    # opt-in it forms a one-row "annual" window.
    resolution = resolve_ttm_window(
        [_fy_row("2025-12-31")],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    window = resolution.window
    assert resolution.failure is None
    assert window is not None
    assert window.cadence == "annual"
    assert [record.end_date for record in window.records] == ["2025-12-31"]
    assert window.as_of == "2025-12-31"


def test_annual_is_off_without_the_opt_in() -> None:
    # The default (annual_max_age_days=None) must not change any behaviour:
    # a lone FY row still fails exactly as before.
    resolution = resolve_ttm_window([_fy_row("2025-12-31")], reference_date=REFERENCE)
    assert resolution.window is None
    assert resolution.failure == FAILURE_TOO_FEW_QUARTERLY_RECORDS


def test_quarterly_still_wins_over_an_available_fy_row() -> None:
    # Opting in must not disturb a listing that has a clean quarterly window.
    resolution = resolve_ttm_window(
        [_fy_row("2026-04-30"), *_rows(QUARTERLY_DATES)],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is not None
    assert resolution.window.cadence == "quarterly"
    assert resolution.window.as_of == QUARTERLY_DATES[0]


def test_stale_fy_row_is_not_resolved_even_when_opted_in() -> None:
    # 2024-12-31 is >480 days before the reference: an annual data gap, not a
    # usable window.
    resolution = resolve_ttm_window(
        [_fy_row("2024-12-31")],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_TOO_FEW_QUARTERLY_RECORDS


def test_annual_fallback_preserves_the_sub_annual_failure_reason() -> None:
    # Stale quarters plus no fresh FY row: the diagnostic quarterly failure
    # (not a generic annual one) must survive for metric_compute_status.
    stale_reference = date.fromisoformat(QUARTERLY_DATES[0]) + timedelta(days=401)
    resolution = resolve_ttm_window(
        _rows(QUARTERLY_DATES),
        reference_date=stale_reference,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is None
    assert resolution.failure == FAILURE_LATEST_QUARTER_TOO_OLD


def test_annual_picks_the_latest_fresh_fy_row() -> None:
    resolution = resolve_ttm_window(
        [_fy_row("2024-12-31", value=1.0), _fy_row("2025-12-31", value=2.0)],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is not None
    assert resolution.window.as_of == "2025-12-31"
    assert resolution.window.records[0].value == 2.0


def test_paired_records_on_annual_window_matches_the_fy_companion() -> None:
    resolution = resolve_ttm_window(
        [_fy_row("2025-12-31")],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is not None
    companion = _fy_row(
        "2025-12-31", value=40.0, concept="DepreciationDepletionAndAmortization"
    )
    pairs = paired_records(resolution.window, [companion])
    assert pairs is not None
    assert len(pairs) == 1
    assert pairs[0][1].value == 40.0


def test_paired_records_on_annual_window_ignores_a_quarterly_companion() -> None:
    # On an annual window the companion must be the FY row, not a quarter that
    # happens to share the end_date.
    resolution = resolve_ttm_window(
        [_fy_row("2025-12-31")],
        reference_date=REFERENCE,
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    assert resolution.window is not None
    quarter_companion = fact(
        concept="DepreciationDepletionAndAmortization",
        fiscal_period="Q4",
        end_date="2025-12-31",
        value=40.0,
    )
    assert paired_records(resolution.window, [quarter_companion]) is None


@given(
    anchor_age_days=st.integers(min_value=0, max_value=500),
    gaps=st.lists(st.integers(min_value=40, max_value=260), min_size=0, max_size=7),
)
def test_property_any_emitted_window_is_a_true_ttm(
    anchor_age_days: int, gaps: list[int]
) -> None:
    """Whatever the spacing, an emitted window always spans ~12 months.

    Invariants: a window is either 4 rows with every adjacent gap inside the
    quarterly band, or 2 rows with the gap inside the semi-annual band; rows
    are strictly newest-first; the anchor is fresh. Otherwise the resolution
    carries one of the documented failure reasons.
    """

    anchor = REFERENCE - timedelta(days=anchor_age_days)
    dates = [anchor]
    for gap in gaps:
        dates.append(dates[-1] - timedelta(days=gap))
    rows = [
        fact(
            concept="OperatingIncomeLoss",
            fiscal_period="Q1",
            end_date=day.isoformat(),
            value=1.0,
        )
        for day in dates
    ]

    resolution = resolve_ttm_window(rows, reference_date=REFERENCE)
    window = resolution.window
    if window is None:
        assert resolution.failure in {
            FAILURE_TOO_FEW_QUARTERLY_RECORDS,
            FAILURE_LATEST_QUARTER_TOO_OLD,
            FAILURE_NO_TTM_CADENCE,
        }
        return

    assert anchor_age_days <= 400
    parsed = [date.fromisoformat(record.end_date) for record in window.records]
    assert parsed == sorted(parsed, reverse=True)
    observed_gaps = [
        (parsed[index] - parsed[index + 1]).days for index in range(len(parsed) - 1)
    ]
    if window.cadence == "quarterly":
        assert len(window.records) == 4
        assert all(
            QUARTERLY_GAP_DAYS[0] <= gap <= QUARTERLY_GAP_DAYS[1]
            for gap in observed_gaps
        )
    else:
        assert len(window.records) == 2
        assert all(
            SEMI_ANNUAL_GAP_DAYS[0] <= gap <= SEMI_ANNUAL_GAP_DAYS[1]
            for gap in observed_gaps
        )
