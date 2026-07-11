"""Regression: the catalog sync drops dead venues' provider layer, nothing more.

Before 2026-07-11, `replace_for_provider` deleted a provider exchange absent
from the provider's list only when it had no `provider_listing` children --
so a venue dropped from the EODHD plan could never leave the catalog (emptying
it required a successful symbol-list call, which 404s), and every later ticker
refresh kept tripping over it. The sync now cascade-purges the dropped venue's
provider layer (mappings + raw fundamentals + fetch/normalization state, then
the `provider_exchange` row) inside one transaction, guarded against truncated
payloads by `MassExchangeDropError` (>= 5 drops and > 50% of the catalog).
Canonical rows (`exchange`/`listing`/`issuer`) and canonical data are
provider-independent and are never deleted. These tests fail on the
childless-only code, which silently keeps the dropped venue.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from conftest import resolve_listing_id, seed_facts, seed_raw_fundamentals
from pyvalue.persistence.storage import (
    DroppedProviderExchange,
    ExchangeProviderRepository,
    FactRecord,
    MassExchangeDropError,
    SecurityRepository,
    SupportedTickerRepository,
)


def _exchange_row(code: str) -> dict[str, object]:
    return {
        "Code": code,
        "Name": f"{code} Exchange",
        "Country": "Testland",
        "Currency": "USD",
    }


def _ticker_row(code: str) -> dict[str, object]:
    return {
        "Code": code,
        "Name": f"{code} Inc",
        "Type": "Common Stock",
        "Currency": "USD",
    }


def _catalog_codes(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {
            row[0]
            for row in conn.execute(
                """
                SELECT px.provider_exchange_code
                FROM provider_exchange px
                JOIN provider p ON p.provider_id = px.provider_id
                WHERE p.provider_code = 'EODHD'
                """
            )
        }


def test_dropped_exchange_cascades_provider_layer_only(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog-cascade.db"
    exchanges = ExchangeProviderRepository(db_path)
    exchanges.initialize_schema()
    exchanges.replace_for_provider("EODHD", [_exchange_row("AA"), _exchange_row("BB")])
    tickers = SupportedTickerRepository(db_path)
    tickers.replace_for_exchange("EODHD", "AA", [_ticker_row("AAA")])
    tickers.replace_for_exchange("EODHD", "BB", [_ticker_row("BBB")])
    seed_raw_fundamentals(db_path, "EODHD", "BBB.BB", {"General": {}}, exchange="BB")
    seed_facts(
        db_path,
        "BBB.BB",
        [
            FactRecord(
                symbol="BBB.BB",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="USD",
            )
        ],
    )
    aaa_id = resolve_listing_id(db_path, "AAA.AA")
    bbb_id = resolve_listing_id(db_path, "BBB.BB")

    # EODHD's exchange list no longer contains BB (e.g. a plan change).
    result = exchanges.replace_for_provider("EODHD", [_exchange_row("AA")])

    assert result.stored == 1
    assert result.dropped == (
        DroppedProviderExchange(code="BB", purged_provider_listings=1),
    )
    assert _catalog_codes(db_path) == {"AA"}

    with sqlite3.connect(db_path) as conn:
        # Provider layer gone: BB's mapping and its raw payload.
        bbb_mappings = conn.execute(
            "SELECT COUNT(*) FROM provider_listing WHERE listing_id = ?",
            (bbb_id,),
        ).fetchone()[0]
        raw_rows = conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0]
        # Canonical layer retained: the exchange row, the listing, its facts.
        exchange_rows = conn.execute(
            "SELECT COUNT(*) FROM \"exchange\" WHERE exchange_code = 'BB'"
        ).fetchone()[0]
        listing_rows = conn.execute(
            "SELECT COUNT(*) FROM listing WHERE listing_id = ?", (bbb_id,)
        ).fetchone()[0]
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?", (bbb_id,)
        ).fetchone()[0]
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert bbb_mappings == 0
    assert raw_rows == 0
    assert exchange_rows == 1
    assert listing_rows == 1
    assert fact_rows == 1
    assert fk_violations == []
    # The orphaned listing is unreachable through the provider-joined scopes.
    assert SecurityRepository(db_path).list_supported_listings() == [(aaa_id, "AAA.AA")]


def test_mass_exchange_drop_blocked_and_rolled_back(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog-mass-drop.db"
    exchanges = ExchangeProviderRepository(db_path)
    exchanges.initialize_schema()
    codes = [f"E{index}" for index in range(10)]
    exchanges.replace_for_provider("EODHD", [_exchange_row(code) for code in codes])
    tickers = SupportedTickerRepository(db_path)
    tickers.replace_for_exchange("EODHD", "E9", [_ticker_row("TTT")])

    # A payload shrinking 10 exchanges to 2 (8 dropped: >= 5 and > 50%) looks
    # like a truncated exchanges-list response: blocked, nothing committed.
    with pytest.raises(MassExchangeDropError) as excinfo:
        exchanges.replace_for_provider(
            "EODHD", [_exchange_row("E0"), _exchange_row("E1")]
        )

    assert excinfo.value.existing == 10
    assert excinfo.value.dropped == 8
    assert excinfo.value.dropped_codes == tuple(f"E{index}" for index in range(2, 10))
    assert "--allow-mass-drop" in str(excinfo.value)
    assert _catalog_codes(db_path) == set(codes)
    with sqlite3.connect(db_path) as conn:
        mapping_rows = conn.execute("SELECT COUNT(*) FROM provider_listing").fetchone()[
            0
        ]
    assert mapping_rows == 1


def test_allow_mass_drop_bypasses_guard(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog-mass-drop-bypass.db"
    exchanges = ExchangeProviderRepository(db_path)
    exchanges.initialize_schema()
    codes = [f"E{index}" for index in range(10)]
    exchanges.replace_for_provider("EODHD", [_exchange_row(code) for code in codes])
    tickers = SupportedTickerRepository(db_path)
    tickers.replace_for_exchange("EODHD", "E9", [_ticker_row("TTT")])
    ttt_id = resolve_listing_id(db_path, "TTT.E9")

    result = exchanges.replace_for_provider(
        "EODHD",
        [_exchange_row("E0"), _exchange_row("E1")],
        allow_mass_drop=True,
    )

    assert result.stored == 2
    assert len(result.dropped) == 8
    assert _catalog_codes(db_path) == {"E0", "E1"}
    with sqlite3.connect(db_path) as conn:
        mapping_rows = conn.execute("SELECT COUNT(*) FROM provider_listing").fetchone()[
            0
        ]
        listing_rows = conn.execute(
            "SELECT COUNT(*) FROM listing WHERE listing_id = ?", (ttt_id,)
        ).fetchone()[0]
    # Even the explicit bypass purges only the provider layer.
    assert mapping_rows == 0
    assert listing_rows == 1


def test_drop_under_absolute_floor_passes(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog-small-drop.db"
    exchanges = ExchangeProviderRepository(db_path)
    exchanges.initialize_schema()
    codes = [f"E{index}" for index in range(6)]
    exchanges.replace_for_provider("EODHD", [_exchange_row(code) for code in codes])

    # 4 of 6 dropped exceeds the fraction but stays under the 5-drop floor --
    # the shape of a real small plan change (BE/IC/TA was 3 of 73).
    result = exchanges.replace_for_provider(
        "EODHD", [_exchange_row("E0"), _exchange_row("E1")]
    )

    assert result.stored == 2
    assert len(result.dropped) == 4
    assert _catalog_codes(db_path) == {"E0", "E1"}
