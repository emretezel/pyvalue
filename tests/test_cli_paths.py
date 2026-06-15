"""CLI path resolution helpers.

Author: Emre Tezel
"""

from pathlib import Path

import pytest

from pyvalue.cli import (
    _resolve_canonical_scope_listings,
    _resolve_database_path,
    _resolve_provider_scope,
    _validate_scope_selector,
)
from pyvalue.persistence.storage import SecurityRepository, SupportedTickerRepository
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


def test_resolve_canonical_scope_listings_defaults_to_all_supported(
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

    listings, explicit, exchanges = _resolve_canonical_scope_listings(
        str(tmp_path / "scope.db"),
        symbols=None,
        exchange_codes=None,
        all_supported=False,
    )

    # The id-bearing scope carries (listing_id, canonical_symbol) pairs; ids are
    # real integers from the listing table, not re-derived downstream.
    assert [symbol for _, symbol in listings] == ["AAA.US", "BBB.US"]
    assert all(isinstance(listing_id, int) for listing_id, _ in listings)
    assert explicit is None
    assert exchanges is None


def test_resolve_canonical_scope_listings_symbols_uses_targeted_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``--symbols`` scope seeks only the requested tickers.

    The id-bearing scope already holds each listing_id; for an explicit
    ``--symbols`` request it must do a targeted seek, not materialise the whole
    supported universe. We make the whole-universe read raise and assert the
    symbol scope still resolves -- proving it took the targeted path.

    Author: Emre Tezel
    """
    db_path = tmp_path / "symbol-scope.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, "US")
    repo.replace_from_listings(
        "EODHD",
        "US",
        [
            Listing(
                symbol="AAA.US",
                security_name="AAA Inc",
                exchange="NYSE",
                currency="USD",
            ),
            Listing(
                symbol="BBB.US",
                security_name="BBB Inc",
                exchange="NYSE",
                currency="USD",
            ),
        ],
    )

    monkeypatch.setattr(
        SecurityRepository,
        "list_supported_listings",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("--symbols scope must not load the whole supported universe")
        ),
    )

    listings, explicit, exchanges = _resolve_canonical_scope_listings(
        str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert [symbol for _, symbol in listings] == ["AAA.US"]
    assert all(isinstance(listing_id, int) for listing_id, _ in listings)
    assert explicit == ["AAA.US"]
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
