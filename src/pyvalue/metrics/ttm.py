"""Cadence-aware trailing-twelve-month (TTM) window resolution.

Author: Emre Tezel

Every quarterly-TTM metric used to sum the "latest 4 quarterly rows". EODHD
stores half-yearly reporters (Australia, the UK, France, ...) as Q2/Q4 rows in
the quarterly table, so for those listings the naive latest-4 sum silently
covered *two years* of flows (~5.3k listings in the 2026-07 universe, ~2x
inflation on every additive TTM: EBITDA, CFO, revenue, dividends, ...).

This module is the single shared window builder that replaces the per-metric
copies. It infers the reporting cadence from the spacing of the newest
end dates and returns a window that actually spans ~12 months:

- four consecutive quarterly rows (adjacent end-date gaps 70-110 days), or
- two consecutive half-year rows (gap 150-220 days).

Histories that form neither shape -- a reporting hole right below the anchor,
a cadence transition, irregular spacing -- resolve to a failure instead of a
window: a wrong-but-plausible sum is strictly worse than an honest NA, and the
affected listing becomes computable again as soon as a clean window re-forms.

Known limitation (accepted): a quarterly reporter whose *newest* row is its
first half-year statement (fresh quarterly->semi-annual switch) shows the same
~91-day end-date gaps as a clean quarterly history, because the half-year
period overlaps the quarter before it. End dates alone cannot expose that
overlap. The shape requires a cadence switch inside the last twelve months,
and it self-heals one period later when the 150-220 day gap appears.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Generic, Literal, Optional, Sequence, TypeVar

from pyvalue.facts import FactView
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_date

Cadence = Literal["quarterly", "semi_annual"]

# Fiscal periods that may participate in a TTM window. FY/INSTANT rows are
# excluded up front: an FY row often shares its end_date with Q4, and letting
# it into the window (or into a paired lookup) would silently sum an annual
# amount into a quarterly chain.
QUARTERLY_PERIODS: frozenset[str] = frozenset({"Q1", "Q2", "Q3", "Q4"})

# Adjacent end-date gap bands, in days. Quarterly tolerates 13/14-week fiscal
# quarters and month-end drift; semi-annual tolerates 26+/--week halves. The
# bands are deliberately disjoint so a gap can never satisfy both cadences.
QUARTERLY_GAP_DAYS: tuple[int, int] = (70, 110)
SEMI_ANNUAL_GAP_DAYS: tuple[int, int] = (150, 220)

# Row counts per cadence: what "twelve months" means in rows.
_QUARTERLY_WINDOW_ROWS = 4
_SEMI_ANNUAL_WINDOW_ROWS = 2

# Failure reasons, as plain phrases callers interpolate into their existing
# metric-scoped log messages (same convention as roic_fy_series.FAILURE_*).
# Keeping the stale/short/mixed split intact preserves the diagnostic value of
# persisted failure reasons in metric_compute_status.
FAILURE_TOO_FEW_QUARTERLY_RECORDS = "too few quarterly records"
FAILURE_LATEST_QUARTER_TOO_OLD = "latest quarterly record too old"
FAILURE_NO_TTM_CADENCE = "quarterly records do not form a trailing-twelve-month window"

WindowFactT = TypeVar("WindowFactT", bound=FactView)
PairedFactT = TypeVar("PairedFactT", bound=FactView)


@dataclass(frozen=True)
class TTMWindow(Generic[WindowFactT]):
    """A resolved trailing-twelve-month window of quarterly-table rows.

    ``records`` is newest-first -- 4 rows for a quarterly reporter, 2 for a
    semi-annual one -- and ``as_of`` is the newest end_date (the staleness
    clock, exactly as the legacy per-metric builders stamped it). Consumers
    sum the records through their own currency seam; this type carries no
    amounts of its own.
    """

    records: tuple[WindowFactT, ...]
    as_of: str
    cadence: Cadence


@dataclass(frozen=True)
class TTMWindowResolution(Generic[WindowFactT]):
    """Outcome of a window resolution: exactly one of window/failure is set."""

    window: Optional[TTMWindow[WindowFactT]]
    failure: Optional[str]


def resolve_ttm_window(
    records: Sequence[WindowFactT],
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
    reference_date: Optional[date] = None,
) -> TTMWindowResolution[WindowFactT]:
    """Resolve the newest ~12-month window from quarterly-table rows.

    ``records`` may arrive in any order and may mix fiscal periods; rows are
    filtered to Q1..Q4, deduped by end_date (first record seen per end_date
    wins, preserving the repositories' newest-filed-first contract for
    amendments), and sorted newest-first explicitly -- several legacy builders
    trusted repository ordering, which this closes off as a latent bug.

    Freshness (newest end_date within ``max_age_days``) is checked before
    cadence, mirroring the legacy builders' staleness gate. For a
    clean-quarterly history the resolved window is exactly the legacy
    ``quarterly[:4]`` selection in the same order, so downstream sums stay
    bit-identical.
    """

    dated = _quarterly_rows(records)
    if len(dated) < _SEMI_ANNUAL_WINDOW_ROWS:
        return TTMWindowResolution(
            window=None, failure=FAILURE_TOO_FEW_QUARTERLY_RECORDS
        )
    ordered = [record for _, record in dated]

    if not is_recent_date(
        ordered[0].end_date,
        max_age_days=max_age_days,
        reference_date=reference_date,
    ):
        return TTMWindowResolution(window=None, failure=FAILURE_LATEST_QUARTER_TOO_OLD)

    # Only the newest rows can enter a window, so only their spacing matters:
    # irregular spacing in deep history must not void a clean window.
    newest = dated[:_QUARTERLY_WINDOW_ROWS]
    gaps = [
        (newest[index][0] - newest[index + 1][0]).days
        for index in range(len(newest) - 1)
    ]

    if len(ordered) >= _QUARTERLY_WINDOW_ROWS and all(
        _within(QUARTERLY_GAP_DAYS, gap) for gap in gaps[: _QUARTERLY_WINDOW_ROWS - 1]
    ):
        return _window(ordered, _QUARTERLY_WINDOW_ROWS, "quarterly")

    if _within(SEMI_ANNUAL_GAP_DAYS, gaps[0]) and (
        len(ordered) == _SEMI_ANNUAL_WINDOW_ROWS
        or _within(SEMI_ANNUAL_GAP_DAYS, gaps[1])
    ):
        # Two half-year rows cover twelve months. A lone pair (no third row)
        # is accepted on the history boundary: demanding confirmation from a
        # third half-year would keep young semi-annual listings NA for six
        # extra months for no informational gain.
        return _window(ordered, _SEMI_ANNUAL_WINDOW_ROWS, "semi_annual")

    if _within(QUARTERLY_GAP_DAYS, gaps[0]) and len(ordered) < _QUARTERLY_WINDOW_ROWS:
        # Quarter-spaced but fewer than four rows: a young quarterly listing,
        # not a cadence problem.
        return TTMWindowResolution(
            window=None, failure=FAILURE_TOO_FEW_QUARTERLY_RECORDS
        )

    # Mixed spacing: a hole directly below the anchor, a cadence transition,
    # or an irregular calendar. Any sum built here would cover more or less
    # than twelve months (the pre-refactor 2x-inflation bug), so refuse.
    return TTMWindowResolution(window=None, failure=FAILURE_NO_TTM_CADENCE)


def paired_records(
    window: TTMWindow[WindowFactT],
    candidates: Sequence[PairedFactT],
) -> Optional[list[tuple[WindowFactT, PairedFactT]]]:
    """Match a second concept's rows onto every window row by end_date.

    The alignment idiom shared by EBITDA (EBIT + same-quarter D&A), gross
    profit (revenue + same-quarter COGS) and interest coverage (EBIT +
    same-quarter interest): each window row must have a companion row with the
    same end_date, otherwise the whole pairing fails (``None``) -- a partially
    paired sum would mix window lengths.

    Candidates are filtered to Q1..Q4 and deduped by end_date (first seen
    wins) for the same reason the window itself is: an FY row sharing Q4's
    end_date must never stand in for the quarterly amount.
    """

    by_end_date: dict[str, PairedFactT] = {}
    for candidate in candidates:
        period = (candidate.fiscal_period or "").upper()
        if period not in QUARTERLY_PERIODS:
            continue
        by_end_date.setdefault(candidate.end_date, candidate)

    pairs: list[tuple[WindowFactT, PairedFactT]] = []
    for record in window.records:
        match = by_end_date.get(record.end_date)
        if match is None:
            return None
        pairs.append((record, match))
    return pairs


def _quarterly_rows(
    records: Sequence[WindowFactT],
) -> list[tuple[date, WindowFactT]]:
    """Filter to quarterly periods, dedupe by end_date, sort newest-first.

    Rows whose end_date does not parse as an ISO date are dropped outright:
    they can never anchor a freshness check or participate in gap arithmetic,
    and letting one string-sort above real dates would crown garbage as the
    window anchor.
    """

    deduped: dict[str, tuple[date, WindowFactT]] = {}
    for record in records:
        period = (record.fiscal_period or "").upper()
        if period not in QUARTERLY_PERIODS:
            continue
        if record.end_date in deduped:
            # First record seen per end_date wins, preserving the
            # repositories' newest-filed-first contract for amendments.
            continue
        try:
            end_date = date.fromisoformat(record.end_date)
        except ValueError:
            continue
        deduped[record.end_date] = (end_date, record)
    return sorted(deduped.values(), key=lambda item: item[0], reverse=True)


def _within(bounds: tuple[int, int], gap: int) -> bool:
    return bounds[0] <= gap <= bounds[1]


def _window(
    ordered: Sequence[WindowFactT], rows: int, cadence: Cadence
) -> TTMWindowResolution[WindowFactT]:
    selected = tuple(ordered[:rows])
    return TTMWindowResolution(
        window=TTMWindow(
            records=selected,
            as_of=selected[0].end_date,
            cadence=cadence,
        ),
        failure=None,
    )


__all__ = [
    "Cadence",
    "FAILURE_LATEST_QUARTER_TOO_OLD",
    "FAILURE_NO_TTM_CADENCE",
    "FAILURE_TOO_FEW_QUARTERLY_RECORDS",
    "QUARTERLY_GAP_DAYS",
    "QUARTERLY_PERIODS",
    "SEMI_ANNUAL_GAP_DAYS",
    "TTMWindow",
    "TTMWindowResolution",
    "paired_records",
    "resolve_ttm_window",
]
