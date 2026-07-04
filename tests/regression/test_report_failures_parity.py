"""Regression: failure-report outputs across the shared-engine consolidation.

The metric- and screen-failure reports were rewired onto one shared engine
(``cli/_failure_analysis.py``) and grew a ``reason_detail`` example column.
These tests pin the persisted-status path end to end -- console lines and CSV
rows -- so any future drift in bucketing, example selection, or column layout
fails loudly.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import List, Tuple

import pytest

from pyvalue.cli import cmd_report_metric_failures, cmd_report_screen_failures
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


def test_metric_failures_persisted_path_output_and_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "parity-metric.db"
    _seed_fixture(db_path)
    before_status = _dump_rows(
        db_path, "metric_compute_status", "listing_id, metric_id, status, reason_code"
    )
    output_csv = tmp_path / "failures.csv"

    exit_code = cmd_report_metric_failures(
        database=str(db_path),
        metric_ids=[CURRENT_RATIO],
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(output_csv),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert f"- {CURRENT_RATIO}: failures=2/3" in output
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
            "reason",
            "count",
            "total_symbols",
            "failure_rate",
            "example_symbol",
            "example_market_cap",
            "example_reason_detail",
        ],
        [
            CURRENT_RATIO,
            REASON,
            "2",
            "3",
            str(2 / 3),
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


def test_screen_failures_persisted_path_output_and_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "parity-screen.db"
    _seed_fixture(db_path)
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
    assert (
        f"    {REASON}: 2 (example=AAA.US, market_cap=N/A, detail={DETAIL})" in output
    )
    assert "- Current ratio >= 1: fails=2/3, na_fails=2, threshold_fails=0" in output

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows == [
        [
            "metric_id",
            "missing_symbols",
            "affected_criteria_count",
            "affected_criteria",
            "root_cause",
            "root_cause_count",
            "example_symbol",
            "example_market_cap",
            "example_reason_detail",
        ],
        [
            CURRENT_RATIO,
            "2",
            "1",
            # Criterion labels carry their 1-based index prefix in the CSV.
            "1. Current ratio >= 1",
            REASON,
            "2",
            "AAA.US",
            "",
            DETAIL,
        ],
    ]
