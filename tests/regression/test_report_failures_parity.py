"""Regression: diagnostic-report outputs over persisted state only.

The failure diagnostics were made pure reads: ``report-metric-failures`` was
merged into ``report-metric-status --reasons`` (persisted-status bucketing
with the ``reason_detail`` example column) and ``report-screen-failures`` was
slimmed to criterion fallout, deferring root causes to that survey. These
tests pin the console lines, CSV layouts, and read-only guarantee end to end
so any future drift -- bucketing, example selection, columns, or a
reintroduced DB write -- fails loudly.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import List, Tuple

import pytest

from pyvalue.cli import cmd_report_metric_status, cmd_report_screen_failures
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.persistence.storage import MetricComputeStatusRecord
from pyvalue.universe import Listing

from conftest import (
    seed_exchange,
    seed_metric,
    seed_metric_status,
    seed_supported_listings,
)

CURRENT_RATIO = CurrentRatioMetric.id
REASON = f"{CURRENT_RATIO}: guard tripped"
DETAIL = "AssetsCurrent=5 LiabilitiesCurrent=0 on 2026-03-31"


def _seed_fixture(db_path: Path) -> None:
    """Three listings, all with deterministic persisted state (no recompute).

    AAA/BBB carry fresh persisted failures (same reason, one with detail);
    CCC carries a fresh persisted success plus its stored metric row. With no
    facts-refresh state and no market snapshots seeded, every status row is
    fresh, so both commands run purely off persisted state.
    """

    seed_exchange(db_path, "US", provider="EODHD")
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US", security_name="AAA", exchange="NYSE", currency="USD"
            ),
            Listing(
                symbol="BBB.US", security_name="BBB", exchange="NYSE", currency="USD"
            ),
            Listing(
                symbol="CCC.US", security_name="CCC", exchange="NYSE", currency="USD"
            ),
        ],
    )
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            metric_id=CURRENT_RATIO,
            status="failure",
            attempted_at="2026-07-04T00:00:00Z",
            reason_code=REASON,
            reason_detail=DETAIL,
            symbol="AAA.US",
        ),
        MetricComputeStatusRecord(
            metric_id=CURRENT_RATIO,
            status="failure",
            attempted_at="2026-07-04T00:00:00Z",
            reason_code=REASON,
            reason_detail="other detail",
            symbol="BBB.US",
        ),
        MetricComputeStatusRecord(
            metric_id=CURRENT_RATIO,
            status="success",
            attempted_at="2026-07-04T00:00:00Z",
            value_as_of="2026-03-31",
            symbol="CCC.US",
        ),
    )
    seed_metric(db_path, "CCC.US", CURRENT_RATIO, 2.0, "2026-03-31")


def _dump_rows(db_path: Path, table: str, columns: str) -> List[Tuple[object, ...]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT {columns} FROM {table} ORDER BY listing_id, metric_id"
        ).fetchall()
    return [tuple(row) for row in rows]


def test_metric_status_reasons_persisted_output_and_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "parity-metric.db"
    _seed_fixture(db_path)
    before_status = _dump_rows(
        db_path, "metric_compute_status", "listing_id, metric_id, status, reason_code"
    )
    before_metrics = _dump_rows(db_path, "metrics", "listing_id, metric_id, value")
    output_csv = tmp_path / "status.csv"

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CURRENT_RATIO],
        config_path=None,
        show_reasons=True,
        output_csv=str(output_csv),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert (
        f"- {CURRENT_RATIO}: na_share=66.7% "
        "(failures=2, never_attempted=0, successes=1 of 3)" in output
    )
    # The example is the first fresh failure seen (no market caps seeded), and
    # its untemplated reason_detail rides along on the console line.
    assert (
        f"    {REASON}: 2 (example=AAA.US, market_cap=N/A, detail={DETAIL})" in output
    )

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows == [
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
            "example_market_cap",
            "example_reason_detail",
        ],
        [
            CURRENT_RATIO,
            "3",
            "1",
            "2",
            "0",
            "0.6667",
            REASON,
            "2",
            "AAA.US",
            "",
            DETAIL,
        ],
    ]
    # Purely persisted path: the report must not have rewritten any state.
    assert (
        _dump_rows(
            db_path,
            "metric_compute_status",
            "listing_id, metric_id, status, reason_code",
        )
        == before_status
    )
    assert (
        _dump_rows(db_path, "metrics", "listing_id, metric_id, value") == before_metrics
    )


def test_screen_failures_persisted_path_output_and_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "parity-screen.db"
    _seed_fixture(db_path)
    before_status = _dump_rows(
        db_path,
        "metric_compute_status",
        "listing_id, metric_id, status, reason_code, attempted_at",
    )
    before_metrics = _dump_rows(db_path, "metrics", "listing_id, metric_id, value")
    config = tmp_path / "screen.yml"
    config.write_text(
        "criteria:\n"
        '  - name: "Current ratio >= 1"\n'
        "    left:\n"
        f"      metric: {CURRENT_RATIO}\n"
        '    operator: ">="\n'
        "    right:\n"
        "      value: 1\n",
        encoding="utf-8",
    )
    output_csv = tmp_path / "screen_failures.csv"

    exit_code = cmd_report_screen_failures(
        config_path=str(config),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(output_csv),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    # CCC passes on its stored row; AAA/BBB are NA via their fresh failures.
    assert "Passed all criteria: 1/3" in output
    assert f"- {CURRENT_RATIO}: missing=2 symbols, affects=1 criteria" in output
    # Root-cause buckets moved to report-metric-status --reasons: the report
    # prints the drill-down hint instead of inlining persisted reasons.
    assert f"hint: pyvalue report-metric-status --config {config} --reasons" in output
    assert REASON not in output
    assert "- Current ratio >= 1: fails=2/3, na_fails=2, threshold_fails=0" in output

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows == [
        [
            "metric_id",
            "missing_symbols",
            "affected_criteria_count",
            "affected_criteria",
        ],
        [
            CURRENT_RATIO,
            "2",
            "1",
            # Criterion labels carry their 1-based index prefix in the CSV.
            "1. Current ratio >= 1",
        ],
    ]
    # Read-only: the report must not have rewritten metric or status state.
    assert (
        _dump_rows(
            db_path,
            "metric_compute_status",
            "listing_id, metric_id, status, reason_code, attempted_at",
        )
        == before_status
    )
    assert (
        _dump_rows(db_path, "metrics", "listing_id, metric_id, value") == before_metrics
    )


def test_screen_failures_or_group_fallout_and_na_coverage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Pins the OR-group fallout contract: the fallout label is the GROUP name
    # (index-prefixed), and NA fallout is attributed only when the group is
    # NA-blocked. AAA fails on a real leverage-arm threshold miss, so its missing
    # interest_coverage is NOT blamed; BBB is NA on both arms, so both metrics are
    # blamed, once each. Any future drift in these counts fails loudly.
    db_path = tmp_path / "parity-or-group.db"
    seed_exchange(db_path, "US", provider="EODHD")
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US", security_name="AAA", exchange="NYSE", currency="USD"
            ),
            Listing(
                symbol="BBB.US", security_name="BBB", exchange="NYSE", currency="USD"
            ),
        ],
    )
    seed_metric(db_path, "AAA.US", "net_debt_to_ebitda", 4.0, "2026-03-31")

    config = tmp_path / "or-screen.yml"
    config.write_text(
        "criteria:\n"
        '  - name: "Debt-service capacity"\n'
        "    any_of:\n"
        '      - name: "Interest coverage >= 6x"\n'
        "        left:\n"
        "          metric: interest_coverage\n"
        '        operator: ">="\n'
        "        right:\n"
        "          value: 6\n"
        '      - name: "Net debt / EBITDA <= 2.5x"\n'
        "        left:\n"
        "          metric: net_debt_to_ebitda\n"
        '        operator: "<="\n'
        "        right:\n"
        "          value: 2.5\n",
        encoding="utf-8",
    )
    output_csv = tmp_path / "or_screen_failures.csv"

    exit_code = cmd_report_screen_failures(
        config_path=str(config),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(output_csv),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Passed all criteria: 0/2" in output
    assert "- Debt-service capacity: fails=2/2, na_fails=1, threshold_fails=1" in output
    # interest_coverage is blamed only for BBB (the NA-blocked issuer), not AAA.
    assert "- interest_coverage: missing=1 symbols, affects=1 criteria" in output
    assert "- net_debt_to_ebitda: missing=1 symbols, affects=1 criteria" in output

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows == [
        [
            "metric_id",
            "missing_symbols",
            "affected_criteria_count",
            "affected_criteria",
        ],
        # The fallout label is the GROUP name with its 1-based index prefix.
        ["interest_coverage", "1", "1", "1. Debt-service capacity"],
        ["net_debt_to_ebitda", "1", "1", "1. Debt-service capacity"],
    ]
