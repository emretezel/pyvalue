"""Tests for shared currency helpers and FX lookup behavior.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from pyvalue.currency import normalize_monetary_amount, resolve_eodhd_currency
from pyvalue.fx import FXService
from pyvalue.storage import FXRateRecord, FXRatesRepository


class _DummyProvider:
    provider_name = "FRANKFURTER"

    def fetch_rates(self, **kwargs):  # pragma: no cover - defensive fallback
        return []


def _service_with_rates(tmp_path, *records: FXRateRecord) -> FXService:
    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(list(records))
    service = FXService(db_path, repository=repo, provider=_DummyProvider())
    service.lazy_fetch = False
    return service


def test_resolve_eodhd_currency_uses_explicit_precedence():
    resolution = resolve_eodhd_currency(
        {"currency_symbol": "EUR", "currency": "USD"},
        statement_currency="GBP",
        payload_currency="JPY",
        fallback_currency="CHF",
    )

    assert resolution.currency_code == "USD"
    assert resolution.source == "entry:currency"

    statement_resolution = resolve_eodhd_currency(
        {},
        statement_currency="GBX",
        payload_currency="USD",
        fallback_currency="EUR",
    )
    assert statement_resolution.currency_code == "GBP"
    assert statement_resolution.source == "statement"


def test_normalize_monetary_amount_converts_gbx_to_gbp():
    amount, currency = normalize_monetary_amount(Decimal("1250"), "GBX")

    assert amount == Decimal("12.5")
    assert currency == "GBP"


def test_fx_service_same_currency_returns_identity(tmp_path):
    service = _service_with_rates(tmp_path)

    quote = service.get_fx_rate("GBP0.01", "GBP", "2024-01-10")
    converted = service.convert_amount(Decimal("250"), "GBX", "GBP", "2024-01-10")

    assert quote is not None
    assert quote.rate == Decimal("1")
    assert converted == Decimal("2.5")


def test_fx_service_direct_inverse_and_on_or_before_lookup(tmp_path):
    service = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.80",
            fetched_at="2024-01-01T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2024-01-10",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.90",
            fetched_at="2024-01-10T00:00:00+00:00",
            source_kind="provider",
        ),
    )

    direct = service.get_fx_rate("USD", "EUR", "2024-01-05")
    inverse = service.get_fx_rate("EUR", "USD", "2024-01-05")

    assert direct is not None
    assert direct.rate_date.isoformat() == "2024-01-01"
    assert direct.rate == Decimal("0.80")
    assert inverse is not None
    assert inverse.rate == Decimal("1.25")


def test_fx_service_triangulates_through_pivot_currency(tmp_path):
    service = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2024-02-01",
            base_currency="USD",
            quote_currency="CAD",
            rate_text="1.30",
            fetched_at="2024-02-01T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2024-02-01",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.80",
            fetched_at="2024-02-01T00:00:00+00:00",
            source_kind="provider",
        ),
    )

    quote = service.get_fx_rate("CAD", "EUR", "2024-02-03")

    assert quote is not None
    assert quote.via_currency == "USD"
    assert quote.rate == pytest.approx(Decimal("0.6153846153846153846153846154"))


def test_fx_service_missing_rate_warns_and_returns_none(tmp_path, caplog):
    service = _service_with_rates(tmp_path)

    with caplog.at_level("WARNING"):
        quote = service.get_fx_rate("JPY", "CHF", "2024-03-01")

    assert quote is None
    assert "Missing FX rate" in caplog.text
