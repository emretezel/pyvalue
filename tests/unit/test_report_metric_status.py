"""Persisted metric-status report (NA-share ranking and failure reasons).

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

import pytest

from pyvalue.cli import cmd_report_metric_status
from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.persistence.storage import (
    MetricComputeStatusRecord,
    MetricComputeStatusRepository,
    SecurityRepository,
)
from pyvalue.universe import Listing

from conftest import (
    resolve_listing_id,
    seed_exchange,
    seed_metric_status,
    seed_supported_listings,
)

CURRENT_RATIO = CurrentRatioMetric.id
WORKING_CAPITAL = WorkingCapitalMetric.id


def _seed_universe(db_path: Path) -> None:
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


def _status(
    symbol: str,
    metric_id: str,
    status: str,
    reason_code: str | None = None,
) -> MetricComputeStatusRecord:
    assert status in ("success", "failure")
    return MetricComputeStatusRecord(
        metric_id=metric_id,
        status="success" if status == "success" else "failure",
        attempted_at="2026-07-04T00:00:00Z",
        reason_code=reason_code,
        symbol=symbol,
    )


def _dump_status_rows(db_path: Path) -> List[Tuple[object, ...]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, metric_id, status, reason_code, reason_detail,
                   attempted_at
            FROM metric_compute_status
            ORDER BY listing_id, metric_id
            """
        ).fetchall()
    return [tuple(row) for row in rows]


def test_metric_status_ranks_by_na_share(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """failures + never-attempted make the NA share; worst metric prints first."""

    db_path = tmp_path / "status.db"
    _seed_universe(db_path)
    # current_ratio: 1 success, 1 failure, 1 never-attempted -> na_share 2/3.
    # working_capital: no rows at all -> na_share 3/3, must rank first.
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "success"),
        _status("BBB.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
    )

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CURRENT_RATIO, WORKING_CAPITAL],
        config_path=None,
        show_reasons=False,
        output_csv=None,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    working_capital_line = (
        f"- {WORKING_CAPITAL}: na_share=100.0% "
        "(failures=0, never_attempted=3, successes=0 of 3)"
    )
    current_ratio_line = (
        f"- {CURRENT_RATIO}: na_share=66.7% "
        "(failures=1, never_attempted=1, successes=1 of 3)"
    )
    assert working_capital_line in output
    assert current_ratio_line in output
    assert output.index(working_capital_line) < output.index(current_ratio_line)
    assert "run compute-metrics to populate" in output


def test_metric_status_reason_breakdown_and_csv(tmp_path: Path) -> None:
    """--reasons groups failures by reason_code with a stable example symbol."""

    db_path = tmp_path / "status-reasons.db"
    _seed_universe(db_path)
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "failure", "current_ratio: other guard"),
        _status("BBB.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
        _status("CCC.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
    )
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
    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == [
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
    ]
    # Both reason rows repeat the metric summary; the bigger bucket comes first
    # and, with no market caps derivable, each bucket keeps its first-seen
    # scope listing as the example (BBB before CCC for the shared reason).
    assert rows[1] == [
        CURRENT_RATIO,
        "3",
        "0",
        "3",
        "0",
        "1.0000",
        "current_ratio: guard tripped",
        "2",
        "BBB.US",
        "",
        "",
    ]
    assert rows[2] == [
        CURRENT_RATIO,
        "3",
        "0",
        "3",
        "0",
        "1.0000",
        "current_ratio: other guard",
        "1",
        "AAA.US",
        "",
        "",
    ]


def test_metric_status_config_restricts_to_screen_metrics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--config expands to the screen's criteria metrics only."""

    db_path = tmp_path / "status-config.db"
    _seed_universe(db_path)
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

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=None,
        config_path=str(config),
        show_reasons=False,
        output_csv=None,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert f"- {CURRENT_RATIO}:" in output
    assert WORKING_CAPITAL not in output
    assert "metrics=1" in output


def test_metric_status_rejects_metrics_and_config(tmp_path: Path) -> None:
    """--metrics and --config are mutually exclusive selection modes."""

    db_path = tmp_path / "status-exclusive.db"
    _seed_universe(db_path)

    with pytest.raises(SystemExit, match="either --metrics or --config"):
        cmd_report_metric_status(
            database=str(db_path),
            symbols=None,
            exchange_codes=["US"],
            all_supported=False,
            metric_ids=[CURRENT_RATIO],
            config_path="screen.yml",
            show_reasons=False,
            output_csv=None,
        )


def test_metric_status_is_read_only(tmp_path: Path) -> None:
    """The report never writes: status rows are byte-identical after a run."""

    db_path = tmp_path / "status-readonly.db"
    _seed_universe(db_path)
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "success"),
        _status("BBB.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
    )
    before = _dump_status_rows(db_path)

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CURRENT_RATIO, WORKING_CAPITAL],
        config_path=None,
        show_reasons=True,
        output_csv=None,
    )

    assert exit_code == 0
    assert _dump_status_rows(db_path) == before


def test_metric_status_reasons_buckets_stale_and_never_attempted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Non-usable pairs without a trustworthy failure land in remedy buckets."""

    db_path = tmp_path / "status-stale.db"
    _seed_universe(db_path)
    # AAA: fresh persisted failure -> reason bucket. BBB: fresh SUCCESS whose
    # stored metrics row is absent -> stale bucket (the status no longer
    # describes reality). CCC: no persisted state -> never-attempted bucket.
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
        _status("BBB.US", CURRENT_RATIO, "success"),
    )

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CURRENT_RATIO],
        config_path=None,
        show_reasons=True,
        output_csv=None,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "    current_ratio: guard tripped: 1 (example=AAA.US" in output
    assert (
        "    stale_inputs (run compute-metrics): 1 "
        "(example=BBB.US, market_cap=N/A, detail=last attempt: success)" in output
    )
    assert (
        "    never_attempted (run compute-metrics): 1 "
        "(example=CCC.US, market_cap=N/A)" in output
    )


def test_metric_status_reasons_never_computes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--reasons only reads persisted state; metric compute must never run."""

    db_path = tmp_path / "status-nocompute.db"
    _seed_universe(db_path)

    class CachedMetric:
        id = "cached_metric"
        required_concepts = ()
        uses_market_data = False
        uses_financial_facts = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            raise AssertionError("report-metric-status must never compute metrics")

    monkeypatch.setitem(REGISTRY, CachedMetric.id, CachedMetric)
    seed_metric_status(
        db_path,
        _status("AAA.US", CachedMetric.id, "failure", "cached_failure"),
    )

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CachedMetric.id],
        config_path=None,
        show_reasons=True,
        output_csv=None,
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "cached_failure: 1 (example=AAA.US" in output


def test_metric_status_reasons_carries_scope_listing_ids(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """All reads key on the scope's listing ids; symbols never re-resolve."""

    db_path = tmp_path / "status-carry-ids.db"
    _seed_universe(db_path)
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
    )

    calls = {"resolve_ids_many": 0}
    original_resolve_ids_many = SecurityRepository.resolve_ids_many

    def counting_resolve_ids_many(
        self: SecurityRepository,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        calls["resolve_ids_many"] += 1
        return original_resolve_ids_many(
            self, symbols, chunk_size=chunk_size, connection=connection
        )

    monkeypatch.setattr(
        SecurityRepository, "resolve_ids_many", counting_resolve_ids_many
    )

    exit_code = cmd_report_metric_status(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=[CURRENT_RATIO],
        config_path=None,
        show_reasons=True,
        output_csv=None,
    )

    assert exit_code == 0
    assert calls == {"resolve_ids_many": 0}
    assert "symbols=3" in capsys.readouterr().out


def test_count_statuses_chunking_matches_unchunked(tmp_path: Path) -> None:
    """Chunked aggregation (chunk_size=1) equals the single-query result."""

    db_path = tmp_path / "status-chunks.db"
    _seed_universe(db_path)
    seed_metric_status(
        db_path,
        _status("AAA.US", CURRENT_RATIO, "success"),
        _status("BBB.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
        _status("CCC.US", CURRENT_RATIO, "failure", "current_ratio: guard tripped"),
        _status("AAA.US", WORKING_CAPITAL, "failure", "working_capital: no facts"),
    )
    listing_ids = [
        resolve_listing_id(db_path, symbol) for symbol in ("AAA.US", "BBB.US", "CCC.US")
    ]
    repo = MetricComputeStatusRepository(db_path)
    metric_ids = [CURRENT_RATIO, WORKING_CAPITAL]

    default_counts = repo.count_statuses_by_metric(listing_ids, metric_ids)
    chunked_counts = repo.count_statuses_by_metric(
        listing_ids, metric_ids, chunk_size=1
    )

    assert chunked_counts == default_counts
    assert default_counts[CURRENT_RATIO].successes == 1
    assert default_counts[CURRENT_RATIO].failures == 2
