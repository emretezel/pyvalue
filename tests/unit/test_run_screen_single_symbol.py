"""run-screen single-symbol branch: NA reasons and explain-metric hint.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyvalue.cli import cmd_run_screen_stage
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


def _seed_listing(db_path: Path) -> None:
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


def _write_screen(tmp_path: Path) -> Path:
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
    return config


def _run(db_path: Path, config: Path) -> int:
    return cmd_run_screen_stage(
        config_path=str(config),
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
        show_metric_warnings=False,
    )


def _write_or_screen(tmp_path: Path) -> Path:
    """A single two-arm OR group, to exercise the multi-member detail view."""

    config = tmp_path / "or_screen.yml"
    config.write_text(
        "criteria:\n"
        '  - name: "Value or quality"\n'
        "    any_of:\n"
        '      - name: "Current ratio >= 1"\n'
        "        left:\n"
        f"          metric: {CURRENT_RATIO}\n"
        '        operator: ">="\n'
        "        right:\n"
        "          value: 1\n"
        '      - name: "Earnings yield > 0.10"\n'
        "        left:\n"
        "          metric: earnings_yield\n"
        '        operator: ">"\n'
        "        right:\n"
        "          value: 0.10\n",
        encoding="utf-8",
    )
    return config


def test_single_symbol_na_shows_never_attempted_and_hint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-na.db"
    _seed_listing(db_path)
    config = _write_screen(tmp_path)

    exit_code = _run(db_path, config)

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "Current ratio >= 1: FAIL (value=N/A)" in output
    assert f"    {CURRENT_RATIO} NA: never attempted; run compute-metrics" in output
    assert (
        f"hint: pyvalue explain-metric --symbols AAA.US --metrics {CURRENT_RATIO}"
        in output
    )


def test_single_symbol_na_shows_persisted_failure_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-reason.db"
    _seed_listing(db_path)
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            metric_id=CURRENT_RATIO,
            status="failure",
            attempted_at="2026-07-04T00:00:00Z",
            reason_code=f"{CURRENT_RATIO}: guard tripped",
            symbol="AAA.US",
        ),
    )
    config = _write_screen(tmp_path)

    exit_code = _run(db_path, config)

    assert exit_code == 1
    output = capsys.readouterr().out
    assert f"    {CURRENT_RATIO} NA: {CURRENT_RATIO}: guard tripped" in output
    assert "hint: pyvalue explain-metric" in output


def test_single_symbol_pass_prints_no_na_lines(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-pass.db"
    _seed_listing(db_path)
    seed_metric(db_path, "AAA.US", CURRENT_RATIO, 2.0, "2026-03-31")
    config = _write_screen(tmp_path)

    exit_code = _run(db_path, config)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Current ratio >= 1: PASS (value=2)" in output
    assert " NA: " not in output
    assert "hint:" not in output


def test_single_symbol_or_group_shows_member_breakdown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-or.db"
    _seed_listing(db_path)
    seed_metric(db_path, "AAA.US", CURRENT_RATIO, 2.0, "2026-03-31")  # passes >= 1
    seed_metric(db_path, "AAA.US", "earnings_yield", 0.05, "2026-03-31")  # fails > 0.10
    config = _write_or_screen(tmp_path)

    exit_code = _run(db_path, config)

    assert exit_code == 0  # the OR group passes on its first arm
    output = capsys.readouterr().out
    assert "Value or quality: PASS (1/2 passed, need 1)" in output
    assert "    Current ratio >= 1: PASS (value=2)" in output
    assert "    Earnings yield > 0.10: FAIL (value=0.05)" in output
    # Both arms had data, so nothing is NA.
    assert " NA: " not in output


def test_single_symbol_or_group_na_arm_silent_when_other_passes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-or-na.db"
    _seed_listing(db_path)
    # Only the first arm has data; the second arm's metric is NA. The group still
    # passes, and the missing metric is NOT reported (the OR coverage payoff).
    seed_metric(db_path, "AAA.US", CURRENT_RATIO, 2.0, "2026-03-31")
    config = _write_or_screen(tmp_path)

    exit_code = _run(db_path, config)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Value or quality: PASS (1/2 passed, need 1)" in output
    assert "    Earnings yield > 0.10: FAIL (value=N/A)" in output
    assert " NA: " not in output
    assert "hint:" not in output
