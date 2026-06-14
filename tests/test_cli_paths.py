"""CLI path resolution helpers.

Author: Emre Tezel
"""

from pathlib import Path

import pytest

from pyvalue.cli import (
    _resolve_canonical_scope_symbols,
    _resolve_database_path,
    _resolve_provider_scope,
    _validate_scope_selector,
)
from pyvalue.persistence.storage import SupportedTickerRepository
from pyvalue.universe import Listing

from conftest import seed_exchange


def test_resolve_database_path_falls_back_to_repo_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = _resolve_database_path("data/pyvalue.db")

    assert resolved.name == "pyvalue.db"
    assert resolved.exists()
    assert Path("data/pyvalue.db").resolve() != resolved


def test_validate_scope_selector_defaults_to_full_universe() -> None:
    symbol_filters, exchange_filters = _validate_scope_selector(None, None, False)

    assert symbol_filters is None
    assert exchange_filters is None


def test_validate_scope_selector_rejects_multiple_explicit_selectors() -> None:
    try:
        _validate_scope_selector(["AAPL.US"], ["US"], False)
    except SystemExit as exc:
        assert "At most one scope selector may be provided" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected SystemExit for conflicting selectors")


def test_resolve_canonical_scope_symbols_defaults_to_all_supported(
    tmp_path: Path,
) -> None:
    repo = SupportedTickerRepository(tmp_path / "scope.db")
    repo.initialize_schema()
    seed_exchange(tmp_path / "scope.db", "US")
    repo.replace_from_listings(
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US",
                security_name="AAA Inc",
                exchange="NYSE",
                market_category="N",
                is_etf=False,
                is_test_issue=False,
                status="N",
                round_lot_size=100,
                source="test",
                isin=None,
                currency="USD",
            ),
            Listing(
                symbol="BBB.US",
                security_name="BBB Inc",
                exchange="NYSE",
                market_category="N",
                is_etf=False,
                is_test_issue=False,
                status="N",
                round_lot_size=100,
                source="test",
                isin=None,
                currency="USD",
            ),
        ],
    )

    selected, explicit, exchanges = _resolve_canonical_scope_symbols(
        str(tmp_path / "scope.db"),
        symbols=None,
        exchange_codes=None,
        all_supported=False,
    )

    assert selected == ["AAA.US", "BBB.US"]
    assert explicit is None
    assert exchanges is None


def test_resolve_provider_scope_defaults_to_all_supported(tmp_path: Path) -> None:
    repo = SupportedTickerRepository(tmp_path / "provider-scope.db")
    repo.initialize_schema()
    seed_exchange(tmp_path / "provider-scope.db", "US")
    repo.replace_from_listings(
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US",
                security_name="AAA Inc",
                exchange="NYSE",
                market_category="N",
                is_etf=False,
                is_test_issue=False,
                status="N",
                round_lot_size=100,
                source="test",
                isin=None,
                currency="USD",
            )
        ],
    )

    count, symbols, exchanges = _resolve_provider_scope(
        str(tmp_path / "provider-scope.db"),
        provider="EODHD",
        symbols=None,
        exchange_codes=None,
        all_supported=False,
    )

    assert count == 1
    assert symbols is None
    assert exchanges is None
