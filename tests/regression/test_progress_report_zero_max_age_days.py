"""Regression: `--max-age-days 0` means zero, not the fallback default.

Both progress reporters resolved their freshness window with a falsy-or --
`max_age_days or 30` in the fundamentals reporter and `max_age_days or 7` in the
market-data one (whose fallback did not even match its own argparse default of
30). An explicit `--max-age-days 0` is falsy, so it was silently rewritten to
the fallback and the report described a window the user had not asked for:
data fetched moments ago counted as fresh under a window that should have
declared everything stale. Both parsers already guarantee an int, so the
fallbacks were unreachable except for the one value they corrupted.

These tests fail on the falsy-or code (they see `freshness(30d)` / `freshness(7d)`
and `Stale: 0`).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from cli_test_helpers import patch_cli
from conftest import seed_exchange, seed_price, seed_raw_fundamentals
from pyvalue import cli
from pyvalue.persistence.storage import SupportedTickerRepository

_AAA_ROW = {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}


def _seed_us_catalog(db_path: Path) -> None:
    """Catalogue a single US ticker so the progress scans have a scope."""

    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "US", provider="EODHD")
    repo.replace_for_exchange("EODHD", "US", [_AAA_ROW])


def _patch_config_without_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    """Silence the quota snapshot -- these tests assert on the window only."""

    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_market_data_daily_buffer_calls=0,
        ),
    )


def test_fundamentals_progress_honours_a_zero_day_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "progress-zero-fundamentals.db"
    _seed_us_catalog(db_path)
    # Seeded with "now", which is strictly before the cutoff the report computes
    # a moment later, so a genuine 0-day window must call it stale.
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"Name": "AAA"}}, exchange="US"
    )
    _patch_config_without_quota(monkeypatch)

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=0,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Mode: freshness(0d)" in output
    assert "Stale: 1" in output
    assert "Fresh: 0" in output


def test_market_data_progress_honours_a_zero_day_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "progress-zero-market-data.db"
    _seed_us_catalog(db_path)
    # Market-data freshness compares dates, so today's snapshot sits exactly on
    # a 0-day cutoff and counts as stale.
    seed_price(db_path, "AAA.US", date.today().isoformat(), 10.0)
    _patch_config_without_quota(monkeypatch)

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=0,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Mode: freshness(0d)" in output
    assert "Stale: 1" in output
    assert "Fresh: 0" in output
