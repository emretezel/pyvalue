"""CLI path resolution helpers.

Author: Emre Tezel
"""

from pathlib import Path

from pyvalue.cli import (
    _resolve_canonical_scope_symbols,
    _resolve_database_path,
    _resolve_provider_scope_rows,
    _validate_scope_selector,
)
from pyvalue.storage import SupportedTickerRepository
from pyvalue.universe import Listing


def test_resolve_database_path_falls_back_to_repo_data(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    resolved = _resolve_database_path("data/pyvalue.db")

    assert resolved.name == "pyvalue.db"
    assert resolved.exists()
    assert Path("data/pyvalue.db").resolve() != resolved


def test_validate_scope_selector_defaults_to_full_universe():
    symbol_filters, exchange_filters = _validate_scope_selector(None, None, False)

    assert symbol_filters is None
    assert exchange_filters is None


def test_validate_scope_selector_rejects_multiple_explicit_selectors():
    try:
        _validate_scope_selector(["AAPL.US"], ["US"], False)
    except SystemExit as exc:
        assert "At most one scope selector may be provided" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected SystemExit for conflicting selectors")


def test_resolve_canonical_scope_symbols_defaults_to_all_supported(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "scope.db")
    repo.initialize_schema()
    repo.replace_from_listings(
        "SEC",
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
                currency=None,
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
                currency=None,
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


def test_resolve_provider_scope_rows_defaults_to_all_supported(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "provider-scope.db")
    repo.initialize_schema()
    repo.replace_from_listings(
        "SEC",
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
                currency=None,
            )
        ],
    )

    rows, symbols, exchanges = _resolve_provider_scope_rows(
        str(tmp_path / "provider-scope.db"),
        provider="SEC",
        symbols=None,
        exchange_codes=None,
        all_supported=False,
    )

    assert [row.symbol for row in rows] == ["AAA.US"]
    assert symbols is None
    assert exchanges is None
