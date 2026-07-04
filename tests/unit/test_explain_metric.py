"""explain-metric: per-symbol NA root-cause command.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Tuple

import pytest

from pyvalue.cli import cmd_explain_metric
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.market_capitalization import MarketCapitalizationMetric
from pyvalue.persistence.storage import FactRecord, MetricComputeStatusRecord
from pyvalue.universe import Listing

from conftest import (
    resolve_listing_id,
    seed_exchange,
    seed_facts,
    seed_metric_status,
    seed_supported_listings,
)

CURRENT_RATIO = CurrentRatioMetric.id
MARKET_CAP = MarketCapitalizationMetric.id


def _seed_listing(db_path: Path) -> int:
    seed_exchange(db_path, "US", provider="EODHD")
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US", security_name="AAA", exchange="NYSE", currency="USD"
            )
        ],
    )
    return resolve_listing_id(db_path, "AAA.US")


def _fact(concept: str, end_date: str, value: float) -> FactRecord:
    return FactRecord(
        symbol="AAA.US",
        concept=concept,
        fiscal_period="Q1",
        end_date=end_date,
        unit_kind="monetary",
        currency="USD",
        value=value,
    )


def _dump_tables(db_path: Path) -> List[Tuple[object, ...]]:
    with sqlite3.connect(db_path) as conn:
        metrics = conn.execute(
            "SELECT listing_id, metric_id, value FROM metrics ORDER BY 1, 2"
        ).fetchall()
        statuses = conn.execute(
            "SELECT listing_id, metric_id, status, reason_code "
            "FROM metric_compute_status ORDER BY 1, 2"
        ).fetchall()
    return [tuple(row) for row in metrics] + [tuple(row) for row in statuses]


def _explain(
    db_path: Path,
    metric_ids: List[str] | None = None,
    screen_config: str | None = None,
) -> int:
    return cmd_explain_metric(
        database=str(db_path),
        symbols=["AAA.US"],
        metric_ids=metric_ids,
        screen_config=screen_config,
        max_age_days=400,
    )


def test_explain_shows_missing_concept_and_untemplated_warning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "explain-missing.db"
    listing_id = _seed_listing(db_path)
    recent = date.today().isoformat()
    seed_facts(db_path, "AAA.US", [_fact("AssetsCurrent", recent, 100.0)])

    assert _explain(db_path, metric_ids=[CURRENT_RATIO]) == 0
    output = capsys.readouterr().out
    assert f"== AAA.US / {CURRENT_RATIO} ==" in output
    assert "persisted: none (never attempted" in output
    assert "AssetsCurrent: latest" in output
    assert "LiabilitiesCurrent: MISSING (no stored facts)" in output
    assert "live recompute: FAILURE" in output
    # The reason_code keeps the templated persisted form...
    assert (
        "reason_code: current_ratio: missing assets/liabilities for listing_id=<n>"
        in output
    )
    # ...while the warning is the raw log record: real listing_id, untemplated.
    assert (
        f"warning: current_ratio: missing assets/liabilities for listing_id={listing_id}"
        in output
    )


def test_explain_flags_stale_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "explain-stale.db"
    _seed_listing(db_path)
    seed_facts(
        db_path,
        "AAA.US",
        [
            _fact("AssetsCurrent", "2019-03-31", 100.0),
            _fact("LiabilitiesCurrent", "2019-03-31", 50.0),
        ],
    )

    assert _explain(db_path, metric_ids=[CURRENT_RATIO]) == 0
    output = capsys.readouterr().out
    assert output.count("STALE") >= 2


def test_explain_reports_market_seam_absence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "explain-market.db"
    _seed_listing(db_path)

    assert _explain(db_path, metric_ids=[MARKET_CAP]) == 0
    output = capsys.readouterr().out
    assert "market data: no price snapshot stored" in output
    assert "live recompute: FAILURE" in output


def test_explain_expands_screen_config(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "explain-screen.db"
    _seed_listing(db_path)
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

    assert _explain(db_path, screen_config=str(config)) == 0
    output = capsys.readouterr().out
    assert f"== AAA.US / {CURRENT_RATIO} ==" in output
    assert output.count("== AAA.US /") == 1


def test_explain_requires_exactly_one_selection_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "explain-exclusive.db"
    _seed_listing(db_path)

    with pytest.raises(SystemExit, match="exactly one of --metrics or --screen"):
        _explain(db_path)
    with pytest.raises(SystemExit, match="exactly one of --metrics or --screen"):
        _explain(db_path, metric_ids=[CURRENT_RATIO], screen_config="screen.yml")


def test_explain_is_write_free_and_shows_persisted_detail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Neither a failing nor a succeeding recompute persists anything."""

    db_path = tmp_path / "explain-writefree.db"
    _seed_listing(db_path)
    recent = date.today().isoformat()
    # Computable inputs -> the live recompute SUCCEEDS but must not be stored.
    seed_facts(
        db_path,
        "AAA.US",
        [
            _fact("AssetsCurrent", recent, 100.0),
            _fact("LiabilitiesCurrent", recent, 50.0),
        ],
    )
    # A persisted failure with detail -> the persisted block must surface it.
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            metric_id=CURRENT_RATIO,
            status="failure",
            attempted_at="2026-07-04T00:00:00Z",
            reason_code="current_ratio: guard tripped",
            reason_detail="AssetsCurrent=100 LiabilitiesCurrent=0",
            symbol="AAA.US",
        ),
    )
    before = _dump_tables(db_path)

    assert _explain(db_path, metric_ids=[CURRENT_RATIO]) == 0
    output = capsys.readouterr().out
    assert "reason_detail: AssetsCurrent=100 LiabilitiesCurrent=0" in output
    assert "live recompute: SUCCESS value=2" in output
    assert "not persisted" in output
    assert _dump_tables(db_path) == before
