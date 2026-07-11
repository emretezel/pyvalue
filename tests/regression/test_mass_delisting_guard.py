"""Regression: an anomalous payload must not mass-prune an exchange's mappings.

On 2026-07-11 EODHD answered a plan-dropped exchange (BE) with HTTP 200 and a
truncated 30-symbol payload; the refresh trusted it and removed 2,835 provider
listings in one committed transaction. `replace_for_exchange` now refuses a
removal that is both >= 20 mappings AND more than half of the exchange's
existing mappings (`MassDelistingError`), rolling back the whole slice --
including the same payload's upserts -- unless the operator passes
``allow_mass_delisting`` / ``--allow-mass-delisting``. These tests fail on the
unguarded code, which silently prunes.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cli_test_helpers import patch_cli
from conftest import seed_exchange
from pyvalue import cli
from pyvalue.persistence.storage import (
    MassDelistingError,
    SupportedTickerRepository,
)


def _rows(count: int, prefix: str = "T") -> list[dict[str, object]]:
    """Build ``count`` distinct common-stock payload rows."""

    return [
        {
            "Code": f"{prefix}{index:02d}",
            "Name": f"{prefix}{index:02d} Inc",
            "Type": "Common Stock",
            "Currency": "USD",
        }
        for index in range(count)
    ]


def _provider_listing_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM provider_listing").fetchone()[0])


def _seed_forty(db_path: Path) -> SupportedTickerRepository:
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange("EODHD", "US", _rows(40))
    return ticker_repo


def test_mass_delisting_is_blocked_and_rolled_back(tmp_path: Path) -> None:
    db_path = tmp_path / "mass-delisting-guard.db"
    ticker_repo = _seed_forty(db_path)

    # A 5-of-40 payload would remove 35 mappings (>= 20 and > 50%): blocked.
    with pytest.raises(MassDelistingError) as excinfo:
        ticker_repo.replace_for_exchange("EODHD", "US", _rows(5))

    assert excinfo.value.exchange_code == "US"
    assert excinfo.value.existing == 40
    assert excinfo.value.removed == 35
    assert "--allow-mass-delisting" in str(excinfo.value)
    assert _provider_listing_count(db_path) == 40

    # The raise unwinds the whole slice: an upsert arriving in the same
    # anomalous payload (NEW) must be discarded with it -- "blocked" means the
    # exchange is byte-identical to before the call. NEW makes the slice 41
    # rows at prune time, hence existing == 41 here.
    with pytest.raises(MassDelistingError) as excinfo:
        ticker_repo.replace_for_exchange(
            "EODHD",
            "US",
            _rows(5)
            + [
                {
                    "Code": "NEW",
                    "Name": "New Inc",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ],
        )

    assert excinfo.value.existing == 41
    assert excinfo.value.removed == 35
    assert _provider_listing_count(db_path) == 40
    with sqlite3.connect(db_path) as conn:
        new_rows = conn.execute(
            "SELECT COUNT(*) FROM provider_listing WHERE provider_symbol = 'NEW'"
        ).fetchone()[0]
        new_listings = conn.execute(
            "SELECT COUNT(*) FROM listing WHERE symbol = 'NEW'"
        ).fetchone()[0]
    assert new_rows == 0
    assert new_listings == 0


def test_allow_mass_delisting_bypasses_guard(tmp_path: Path) -> None:
    db_path = tmp_path / "mass-delisting-bypass.db"
    ticker_repo = _seed_forty(db_path)

    result = ticker_repo.replace_for_exchange(
        "EODHD", "US", _rows(5), allow_mass_delisting=True
    )

    assert result.removed == 35
    assert result.orphaned_listings == 35
    assert _provider_listing_count(db_path) == 5
    # Retention flip: even an allowed mass prune only removes the provider
    # layer -- every canonical listing row survives.
    with sqlite3.connect(db_path) as conn:
        listing_rows = conn.execute("SELECT COUNT(*) FROM listing").fetchone()[0]
    assert listing_rows == 40


def test_small_exchange_full_turnover_not_blocked(tmp_path: Path) -> None:
    db_path = tmp_path / "small-turnover.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange("EODHD", "US", _rows(10))

    # 10 removals is 100% of the exchange but under the absolute floor: a tiny
    # exchange's legitimate full turnover must keep flowing.
    result = ticker_repo.replace_for_exchange("EODHD", "US", _rows(10, prefix="U"))

    assert result.removed == 10
    assert result.inserted == 10


def test_cmd_refresh_supported_tickers_guard_skips_and_mentions_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "mass-delisting-cli.db"
    _seed_forty(db_path)

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def list_symbols(self, exchange_code: str) -> list[dict[str, object]]:
            return _rows(5)

        def list_exchanges(self) -> list[dict[str, object]]:
            raise AssertionError("Should not refresh supported exchanges on cache hit")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=["US"],
        all_supported=False,
    )

    out = capsys.readouterr().out
    assert rc == 1
    assert "WARNING: US refresh blocked" in out
    assert "--allow-mass-delisting" in out
    assert "mass-delisting guard: US" in out
    assert _provider_listing_count(db_path) == 40
