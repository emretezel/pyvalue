"""Tests for shared currency helpers and FX lookup behavior.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pyvalue.currency import (
    normalize_currency_code,
    normalize_monetary_amount,
    resolve_eodhd_currency,
)
from pyvalue.fx import FXService, FrankfurterProvider
from pyvalue.storage import FXRateRecord, FXRatesRepository


class _DummyProvider:
    provider_name = "FRANKFURTER"

    def fetch_rates(self, **kwargs):  # pragma: no cover - defensive fallback
        return []


class _FakeResponse:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = (
            payload if isinstance(payload, str) else __import__("json").dumps(payload)
        )

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {}), timeout))
        return self.responses.pop(0)


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
    zac_amount, zac_currency = normalize_monetary_amount(Decimal("250"), "ZAC")
    ila_amount, ila_currency = normalize_monetary_amount(Decimal("250"), "ILA")

    assert amount == Decimal("12.5")
    assert currency == "GBP"
    assert normalize_currency_code("ZAC") == "ZAR"
    assert zac_amount == Decimal("2.5")
    assert zac_currency == "ZAR"
    assert normalize_currency_code("ILA") == "ILS"
    assert ila_amount == Decimal("2.5")
    assert ila_currency == "ILS"


def test_fx_service_same_currency_returns_identity(tmp_path):
    service = _service_with_rates(tmp_path)

    quote = service.get_fx_rate("GBP0.01", "GBP", "2024-01-10")
    converted = service.convert_amount(Decimal("250"), "GBX", "GBP", "2024-01-10")
    ila_quote = service.get_fx_rate("ILA", "ILS", "2024-01-10")
    zac_converted = service.convert_amount(Decimal("250"), "ZAC", "ZAR", "2024-01-10")

    assert quote is not None
    assert quote.rate == Decimal("1")
    assert converted == Decimal("2.5")
    assert ila_quote is not None
    assert ila_quote.rate == Decimal("1")
    assert zac_converted == Decimal("2.5")


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


def test_fx_service_prefers_fresher_inverse_over_older_direct(tmp_path):
    service = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2022-12-31",
            base_currency="EUR",
            quote_currency="GBP",
            rate_text="0.88561",
            fetched_at="2022-12-31T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2024-06-30",
            base_currency="GBP",
            quote_currency="EUR",
            rate_text="1.1815",
            fetched_at="2024-06-30T00:00:00+00:00",
            source_kind="provider",
        ),
    )

    quote = service.get_fx_rate("EUR", "GBP", "2025-12-31")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2024-06-30"
    assert quote.source_kind == "inverse"
    assert quote.rate == pytest.approx(Decimal("0.8463817177316970038087177317"))


def test_fx_service_prefers_fresher_triangulation_over_older_direct(tmp_path):
    service = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2022-12-31",
            base_currency="EUR",
            quote_currency="GBP",
            rate_text="0.88561",
            fetched_at="2022-12-31T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2025-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate_text="1.1751",
            fetched_at="2025-12-31T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="FRANKFURTER",
            rate_date="2025-12-31",
            base_currency="GBP",
            quote_currency="USD",
            rate_text="1.2648",
            fetched_at="2025-12-31T00:00:00+00:00",
            source_kind="provider",
        ),
    )

    quote = service.get_fx_rate("EUR", "GBP", "2025-12-31")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2025-12-31"
    assert quote.source_kind == "triangulated"
    assert quote.via_currency == "USD"
    assert quote.rate == pytest.approx(Decimal("0.9290796963946869070208728653"))


def test_fx_service_missing_rate_warns_and_returns_none(tmp_path, caplog):
    service = _service_with_rates(tmp_path)

    with caplog.at_level("WARNING"):
        quote = service.get_fx_rate("JPY", "CHF", "2024-03-01")

    assert quote is None
    assert "Missing FX rate" in caplog.text


def test_frankfurter_provider_retries_without_unsupported_quotes():
    session = _FakeSession(
        [
            _FakeResponse(
                422,
                {"status": 422, "message": "invalid currency: BEF"},
            ),
            _FakeResponse(
                200,
                [
                    {
                        "date": "2024-01-10",
                        "base": "USD",
                        "quote": "EUR",
                        "rate": 0.91,
                    },
                    {
                        "date": "2024-01-10",
                        "base": "USD",
                        "quote": "CNY",
                        "rate": 7.12,
                    },
                ],
                headers={"Date": "Wed, 10 Jan 2024 00:00:00 GMT"},
            ),
        ]
    )
    provider = FrankfurterProvider(session=session)

    rows = provider.fetch_rates(
        base_currency="USD",
        quote_currencies=["EUR", "CNY", "BEF"],
        start_date=date(2024, 1, 10),
        end_date=date(2024, 1, 10),
    )

    assert [row.quote_currency for row in rows] == ["EUR", "CNY"]
    assert session.calls[0][1]["quotes"] == "BEF,CNY,EUR"
    assert session.calls[1][1]["quotes"] == "CNY,EUR"


def test_frankfurter_provider_normalizes_subunit_currencies_before_request():
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                [
                    {
                        "date": "2024-01-10",
                        "base": "ZAR",
                        "quote": "ILS",
                        "rate": 0.20,
                    },
                    {
                        "date": "2024-01-10",
                        "base": "ZAR",
                        "quote": "USD",
                        "rate": 0.05,
                    },
                ],
                headers={"Date": "Wed, 10 Jan 2024 00:00:00 GMT"},
            )
        ]
    )
    provider = FrankfurterProvider(session=session)

    rows = provider.fetch_rates(
        base_currency="ZAC",
        quote_currencies=["ILA", "USD"],
        start_date=date(2024, 1, 10),
        end_date=date(2024, 1, 10),
    )

    assert session.calls[0][1]["base"] == "ZAR"
    assert session.calls[0][1]["quotes"] == "ILS,USD"
    assert [row.base_currency for row in rows] == ["ZAR", "ZAR"]
    assert [row.quote_currency for row in rows] == ["ILS", "USD"]
