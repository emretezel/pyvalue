"""Tests for shared currency helpers and FX lookup behavior.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from datetime import date
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import TypeVar, overload
import json
import multiprocessing as mp
import pickle
import re

from hypothesis import given, strategies as st
import pytest
import requests

from pyvalue.config import DEFAULT_FX_PIVOT_CURRENCIES, Config
from pyvalue.currency import (
    canonical_trading_currency,
    normalize_currency_code,
    normalize_monetary_amount,
    resolve_eodhd_currency,
    shaped_currency_code,
)
from pyvalue.money import fx_service_for_context
from pyvalue.money.fx import (
    EODHDFXProvider,
    EURO_LEGACY_FIXED_RATES,
    FXService,
    MissingFXRateError,
    parse_eodhd_fx_catalog_entry,
)
from pyvalue.persistence.storage import FXRateRecord, FXRatesRepository

# A repository subclass that the typed factory can return concretely; bound so
# ``_service_with_rates`` preserves the exact class passed via ``repository_cls``.
RepoT = TypeVar("RepoT", bound=FXRatesRepository)


class _ExplodingProvider:
    """FX provider stand-in whose history fetch must never be reached.

    ``FXService`` accepts ``provider: object`` and only reads ``provider_name``
    off it, so this plain class satisfies the parameter without subclassing.
    """

    provider_name = "EODHD"

    def fetch_history(self, **kwargs: object) -> list[FXRateRecord]:
        # pragma: no cover - defensive fallback
        raise AssertionError("FXService must never fetch from the network at runtime")


class _FakeResponse:
    """Declarative spec for a canned HTTP response (status, body, headers).

    ``_FakeSession`` converts each spec into a genuine ``requests.Response`` so
    the provider's real ``json``/``raise_for_status``/``headers`` access works.
    """

    def __init__(
        self,
        status_code: int,
        payload: object,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.payload = payload
        self.headers = headers or {}


class _FakeSession(requests.Session):
    """A ``requests.Session`` returning queued canned responses in order.

    Subclasses the real session to satisfy ``EODHDFXProvider``'s ``Session``
    parameter type. ``request`` -- the funnel every verb delegates to -- pops
    the next spec and materialises a real ``requests.Response`` so downstream
    parsing is exercised unchanged. ``__init__`` skips ``super().__init__`` so
    no real adapters/pools are built.
    """

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)
        # Records ``(url, params)`` per call so tests can assert on the query.
        self.calls: list[tuple[str, dict[str, object]]] = []

    def request(
        self,
        method: str | bytes,
        url: str | bytes,
        *args: object,
        **kwargs: object,
    ) -> requests.Response:
        raw_params = kwargs.get("params")
        params: dict[str, object] = (
            dict(raw_params) if isinstance(raw_params, dict) else {}
        )
        self.calls.append((str(url), params))
        spec = self.responses.pop(0)
        response = requests.Response()
        response.status_code = spec.status_code
        response._content = json.dumps(spec.payload).encode("utf-8")
        response.headers.update(spec.headers)
        return response


class _CountingFXRatesRepository(FXRatesRepository):
    """Repository that counts how the FX service hits the database.

    Used to assert the service's caching: how many times it loads the whole
    canonical table versus individual direct pairs, and which pairs it asked
    for.
    """

    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path)
        self.fetch_all_calls = 0
        self.fetch_pair_history_calls = 0
        self.fetch_pair_history_pairs: list[tuple[str, str]] = []

    def fetch_all(self) -> list[tuple[str, str, str, float]]:
        self.fetch_all_calls += 1
        return super().fetch_all()

    def fetch_pair_history(
        self, base_currency: str, quote_currency: str
    ) -> list[tuple[str, float]]:
        self.fetch_pair_history_calls += 1
        self.fetch_pair_history_pairs.append((str(base_currency), str(quote_currency)))
        return super().fetch_pair_history(base_currency, quote_currency)


def _raise_missing_fx_rate_error() -> None:
    raise MissingFXRateError(
        provider="EODHD",
        base_currency="NLG",
        quote_currency="EUR",
        as_of="2000-06-30",
    )


@overload
def _service_with_rates(
    tmp_path: Path,
    *records: FXRateRecord,
    provider_name: str = ...,
    preload_all: bool = ...,
) -> tuple[FXService, FXRatesRepository]: ...


@overload
def _service_with_rates(
    tmp_path: Path,
    *records: FXRateRecord,
    provider_name: str = ...,
    preload_all: bool = ...,
    repository_cls: type[RepoT],
) -> tuple[FXService, RepoT]: ...


def _service_with_rates(
    tmp_path: Path,
    *records: FXRateRecord,
    provider_name: str = "EODHD",
    preload_all: bool = False,
    repository_cls: type[FXRatesRepository] = FXRatesRepository,
) -> tuple[FXService, FXRatesRepository]:
    # Overloaded so callers passing ``_CountingFXRatesRepository`` get that exact
    # type back (and can read its call counters) without a downcast, while the
    # default-arg path stays plain ``FXRatesRepository``.
    db_path = tmp_path / "fx.db"
    repo = repository_cls(db_path)
    repo.initialize_schema()
    repo.upsert_many(list(records))
    service = FXService(
        db_path,
        repository=repo,
        provider_name=provider_name,
        preload_all=preload_all,
    )
    return service, repo


def test_resolve_eodhd_currency_uses_explicit_precedence() -> None:
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


def test_normalize_monetary_amount_converts_gbx_to_gbp() -> None:
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


def test_shaped_currency_code_keeps_only_three_letter_codes() -> None:
    # The schema-shape gate: 3 uppercase ASCII letters or nothing. Provider
    # placeholders ('Unknown') and the 7-char GBP0.01 subunit alias must map
    # to None; 3-letter subunit codes remain valid listing currencies.
    assert shaped_currency_code(None) is None
    assert shaped_currency_code("") is None
    assert shaped_currency_code("   ") is None
    assert shaped_currency_code("Unknown") is None
    assert shaped_currency_code(" eur ") == "EUR"
    assert shaped_currency_code("USD") == "USD"
    assert shaped_currency_code("GBX") == "GBX"
    assert shaped_currency_code("GBP0.01") is None
    assert shaped_currency_code("US") is None
    assert shaped_currency_code("USDT") is None


@given(st.text())
def test_shaped_currency_code_matches_shape_spec(raw: str) -> None:
    # Total spec: the helper is exactly "strip+uppercase, keep iff the result
    # is three ASCII uppercase letters" -- and it is idempotent on its output.
    shaped = shaped_currency_code(raw)
    normalized = raw.strip().upper()
    if re.fullmatch(r"[A-Z]{3}", normalized):
        assert shaped == normalized
    else:
        assert shaped is None
    if shaped is not None:
        assert shaped_currency_code(shaped) == shaped


def test_fx_service_same_currency_returns_identity(tmp_path: Path) -> None:
    service, _ = _service_with_rates(tmp_path)

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


def test_fx_service_direct_inverse_and_on_or_before_lookup(tmp_path: Path) -> None:
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.80,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-10",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.90,
            fetched_at="2024-01-10T00:00:00+00:00",
        ),
    )

    direct = service.get_fx_rate("USD", "EUR", "2024-01-05")
    inverse = service.get_fx_rate("EUR", "USD", "2024-01-05")

    assert direct is not None
    assert direct.rate_date.isoformat() == "2024-01-01"
    assert direct.rate == Decimal("0.80")
    assert inverse is not None
    assert inverse.rate == Decimal("1.25")


def test_fx_service_triangulates_through_pivot_currency(tmp_path: Path) -> None:
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-02-01",
            base_currency="USD",
            quote_currency="CAD",
            rate=1.30,
            fetched_at="2024-02-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-02-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.80,
            fetched_at="2024-02-01T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("CAD", "EUR", "2024-02-03")

    assert quote is not None
    assert quote.via_currency == "USD"
    assert quote.rate == pytest.approx(Decimal("0.6153846153846153846153846154"))


def test_fx_service_prefers_fresher_inverse_over_older_direct(tmp_path: Path) -> None:
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2022-12-31",
            base_currency="EUR",
            quote_currency="GBP",
            rate=0.88561,
            fetched_at="2022-12-31T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-06-30",
            base_currency="GBP",
            quote_currency="EUR",
            rate=1.1815,
            fetched_at="2024-06-30T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("EUR", "GBP", "2025-12-31")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2024-06-30"
    assert quote.source_kind == "inverse"
    assert quote.rate == pytest.approx(Decimal("0.8463817177316970038087177317"))


def test_fx_service_prefers_fresher_triangulation_over_older_direct(
    tmp_path: Path,
) -> None:
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2022-12-31",
            base_currency="EUR",
            quote_currency="GBP",
            rate=0.88561,
            fetched_at="2022-12-31T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2025-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.1751,
            fetched_at="2025-12-31T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2025-12-31",
            base_currency="GBP",
            quote_currency="USD",
            rate=1.2648,
            fetched_at="2025-12-31T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("EUR", "GBP", "2025-12-31")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2025-12-31"
    assert quote.source_kind == "triangulated"
    assert quote.via_currency == "USD"
    assert quote.rate == pytest.approx(Decimal("0.9290796963946869070208728653"))


def test_fx_service_triangulates_through_gbp_when_only_gbp_legs_exist(
    tmp_path: Path,
) -> None:
    # Mirrors the production PGK shape: EODHD stores deep GBP->PGK and
    # GBP->AUD histories but no USD/EUR legs for PGK before 2024, so only the
    # third pivot (GBP) can bridge PGK->AUD for historical dates.
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2017-06-01",
            base_currency="GBP",
            quote_currency="PGK",
            rate=4.0,
            fetched_at="2017-06-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2017-06-01",
            base_currency="GBP",
            quote_currency="AUD",
            rate=1.7,
            fetched_at="2017-06-01T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("PGK", "AUD", "2017-06-30")

    assert quote is not None
    assert quote.source_kind == "triangulated"
    assert quote.via_currency == "GBP"
    # (PGK->GBP inverse 1/4) / (AUD->GBP inverse 1/1.7) = 1.7/4.
    assert quote.rate == pytest.approx(Decimal("0.425"))


def test_fx_service_prefers_earlier_pivot_at_equal_freshness(tmp_path: Path) -> None:
    # All three pivots can bridge SEK->NOK on the same rate date but with
    # deliberately different cross rates; the configured order (USD before
    # EUR before GBP) must decide the tie.
    def _leg(base: str, quote: str, rate: float) -> FXRateRecord:
        return FXRateRecord(
            provider="EODHD",
            rate_date="2024-05-01",
            base_currency=base,
            quote_currency=quote,
            rate=rate,
            fetched_at="2024-05-01T00:00:00+00:00",
        )

    service, _ = _service_with_rates(
        tmp_path,
        _leg("USD", "SEK", 10.0),
        _leg("USD", "NOK", 11.0),
        _leg("EUR", "SEK", 12.0),
        _leg("EUR", "NOK", 12.6),
        _leg("GBP", "SEK", 13.0),
        _leg("GBP", "NOK", 13.26),
    )

    quote = service.get_fx_rate("SEK", "NOK", "2024-05-02")

    assert quote is not None
    assert quote.via_currency == "USD"
    assert quote.rate == pytest.approx(Decimal("1.1"))

    eur_gbp_dir = tmp_path / "eur_gbp"
    eur_gbp_dir.mkdir()
    eur_gbp_service, _ = _service_with_rates(
        eur_gbp_dir,
        _leg("EUR", "SEK", 12.0),
        _leg("EUR", "NOK", 12.6),
        _leg("GBP", "SEK", 13.0),
        _leg("GBP", "NOK", 13.26),
    )

    eur_quote = eur_gbp_service.get_fx_rate("SEK", "NOK", "2024-05-02")

    assert eur_quote is not None
    assert eur_quote.via_currency == "EUR"
    assert eur_quote.rate == pytest.approx(Decimal("1.05"))


def test_fx_service_pivot_freshness_still_dominates_pivot_order(
    tmp_path: Path,
) -> None:
    # A fresher GBP bridge must beat a staler USD bridge: rate-date freshness
    # outranks pivot position, exactly as it already outranked source kind.
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="SEK",
            rate=10.0,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="NOK",
            rate=11.0,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-06-01",
            base_currency="GBP",
            quote_currency="SEK",
            rate=13.0,
            fetched_at="2024-06-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-06-01",
            base_currency="GBP",
            quote_currency="NOK",
            rate=13.26,
            fetched_at="2024-06-01T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("SEK", "NOK", "2024-06-15")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2024-06-01"
    assert quote.via_currency == "GBP"
    assert quote.rate == pytest.approx(Decimal("1.02"))


def test_config_fx_pivot_currencies_parses_comma_list(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('[fx]\npivot_currencies = "usd, gbp , USD, eur"\n')

    assert Config(config_path).fx_pivot_currencies == ("USD", "GBP", "EUR")


def test_config_fx_pivot_currencies_defaults_when_absent_or_empty(
    tmp_path: Path,
) -> None:
    missing = Config(tmp_path / "missing.toml")
    assert missing.fx_pivot_currencies == DEFAULT_FX_PIVOT_CURRENCIES

    empty_path = tmp_path / "empty.toml"
    empty_path.write_text('[fx]\npivot_currencies = " , "\n')
    assert Config(empty_path).fx_pivot_currencies == DEFAULT_FX_PIVOT_CURRENCIES

    # The two non-fetching stand-in configs must stay locked to the same
    # default so every FXService construction path agrees on the pivot chain.
    assert FXService(":memory:").pivot_currencies == DEFAULT_FX_PIVOT_CURRENCIES
    assert fx_service_for_context().pivot_currencies == DEFAULT_FX_PIVOT_CURRENCIES


def test_statutory_nlg_to_eur_conversion(tmp_path: Path) -> None:
    # No market rates at all: the statutory table alone must convert a
    # guilder-era filing (1 EUR = 2.20371 NLG, Council Regulation 2866/98).
    service, _ = _service_with_rates(tmp_path)

    quote = service.get_fx_rate("NLG", "EUR", "2000-06-30")
    converted = service.convert_amount(Decimal("220371"), "NLG", "EUR", "2000-06-30")

    assert quote is not None
    assert quote.source_kind == "statutory"
    assert quote.via_currency is None
    assert quote.rate_date == date(2000, 6, 30)
    # This direction is the 28-digit reciprocal of the fixed figure, so the
    # product is approx-equal, not bit-equal, to the clean 100000.
    assert converted is not None
    assert converted == pytest.approx(Decimal("100000"))


def test_statutory_eur_to_nlg_inverse_is_exact(tmp_path: Path) -> None:
    service, _ = _service_with_rates(tmp_path)

    quote = service.get_fx_rate("EUR", "NLG", "2001-03-31")

    assert quote is not None
    assert quote.source_kind == "statutory"
    # This orientation serves the legislated figure itself -- exact.
    assert quote.rate == Decimal("2.20371")


def test_statutory_frf_to_dem_cross_via_eur(tmp_path: Path) -> None:
    # Legacy->legacy composes two statutory legs through the EUR pivot; the
    # cross is exact arithmetic on the legislated figures.
    service, _ = _service_with_rates(tmp_path)

    quote = service.get_fx_rate("FRF", "DEM", "2000-12-31")

    assert quote is not None
    assert quote.source_kind == "triangulated"
    assert quote.via_currency == "EUR"
    assert quote.rate == pytest.approx(Decimal("1.95583") / Decimal("6.55957"))


def test_statutory_iep_to_gbp_composes_with_market_eur_gbp(tmp_path: Path) -> None:
    # The audit's IEP->GBP shape: fixed IEP->EUR statutory leg composed with
    # a market EUR<->GBP leg through the EUR pivot.
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="1999-06-28",
            base_currency="EUR",
            quote_currency="GBP",
            rate=0.65,
            fetched_at="1999-06-28T00:00:00+00:00",
        ),
    )

    quote = service.get_fx_rate("IEP", "GBP", "1999-06-30")

    assert quote is not None
    assert quote.source_kind == "triangulated"
    assert quote.via_currency == "EUR"
    # (IEP->EUR = 1/0.787564) / (GBP->EUR = 1/0.65) = 0.65/0.787564; the
    # composed rate date is the market leg's (the statutory leg is as-of).
    assert quote.rate_date == date(1999, 6, 28)
    assert quote.rate == pytest.approx(Decimal("0.65") / Decimal("0.787564"))


def test_statutory_refused_before_effective_date(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # Before adoption the currency still floated: the fixed figure is not a
    # valid historical rate, so the lookup must fail rather than fabricate.
    service, _ = _service_with_rates(tmp_path)

    with caplog.at_level("WARNING"):
        pre_euro = service.get_fx_rate("DEM", "EUR", "1998-12-31")
    pre_adoption_grd = service.get_fx_rate("GRD", "EUR", "2000-06-30")

    assert pre_euro is None
    assert "Missing FX rate" in caplog.text
    # Greece adopted in 2001; mid-2000 is before GRD's effective date even
    # though the first-wave currencies were already fixed.
    assert pre_adoption_grd is None


def test_statutory_quote_never_warns_stale(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    service, _ = _service_with_rates(tmp_path)

    with caplog.at_level("WARNING"):
        quote = service.get_fx_rate("NLG", "EUR", "2000-06-30")

    assert quote is not None
    # rate_date == as_of by construction, so the staleness check sees age 0.
    assert "Stale FX rate" not in caplog.text


@lru_cache(maxsize=1)
def _statutory_only_service() -> FXService:
    # One shared empty in-memory service for the Hypothesis properties:
    # statutory quotes never touch the rates tables, and schema setup runs
    # the full migration chain -- far too slow to repeat per example.
    return FXService(":memory:")


@given(
    amount=st.decimals(
        min_value=Decimal("0.01"),
        max_value=Decimal("1000000000"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
    scale=st.integers(min_value=1, max_value=10_000),
)
def test_statutory_conversion_scale_invariance(amount: Decimal, scale: int) -> None:
    # Statutory conversion is a pure multiplication, so it must commute with
    # scaling up to the last ulp of the 28-digit Decimal context.
    service = _statutory_only_service()

    single = service.convert_amount(amount, "NLG", "EUR", "2000-06-30")
    scaled = service.convert_amount(amount * scale, "NLG", "EUR", "2000-06-30")

    assert single is not None
    assert scaled is not None
    assert scaled == pytest.approx(single * scale, rel=Decimal("1e-24"))


@given(
    currency=st.sampled_from(sorted(EURO_LEGACY_FIXED_RATES)),
    amount=st.decimals(
        min_value=Decimal("0.01"),
        max_value=Decimal("1000000000"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
)
def test_statutory_round_trip_within_decimal_tolerance(
    currency: str, amount: Decimal
) -> None:
    # legacy -> EUR -> legacy multiplies by 1/u then u; the round trip must
    # reproduce the input up to the last ulp of the reciprocal.
    service = _statutory_only_service()
    as_of = EURO_LEGACY_FIXED_RATES[currency].effective

    to_eur = service.convert_amount(amount, currency, "EUR", as_of)
    assert to_eur is not None
    back = service.convert_amount(to_eur, "EUR", currency, as_of)
    assert back is not None
    assert back == pytest.approx(amount, rel=Decimal("1e-24"))


def test_fx_service_missing_rate_warns_and_returns_none_without_network_fetch(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    service = FXService(db_path, repository=repo, provider=_ExplodingProvider())

    with caplog.at_level("WARNING"):
        quote = service.get_fx_rate("JPY", "CHF", "2024-03-01")

    assert quote is None
    assert "Missing FX rate" in caplog.text


def test_fx_service_preload_all_loads_provider_table_once(tmp_path: Path) -> None:
    service, repo = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.90,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="GBP",
            rate=0.80,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        preload_all=True,
        repository_cls=_CountingFXRatesRepository,
    )

    direct = service.get_fx_rate("USD", "EUR", "2024-01-02")
    inverse = service.get_fx_rate("EUR", "USD", "2024-01-02")
    cross = service.get_fx_rate("EUR", "GBP", "2024-01-02")

    assert direct is not None
    assert inverse is not None
    assert cross is not None
    assert repo.fetch_all_calls == 1
    assert repo.fetch_pair_history_calls == 0


def test_fx_service_lazy_pair_cache_loads_each_direct_leg_once(tmp_path: Path) -> None:
    service, repo = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.90,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
        repository_cls=_CountingFXRatesRepository,
    )

    first = service.get_fx_rate("USD", "EUR", "2024-01-02")
    repeated = service.get_fx_rate("USD", "EUR", "2024-01-02")
    inverse = service.get_fx_rate("EUR", "USD", "2024-01-02")
    inverse_repeat = service.get_fx_rate("EUR", "USD", "2024-01-03")

    assert first is not None
    assert repeated is not None
    assert inverse is not None
    assert inverse_repeat is not None
    assert repo.fetch_all_calls == 0
    # Six one-time loads, never refetched: the USD<->EUR legs plus the GBP
    # pivot probes (USD/GBP on the first lookup; EUR/GBP on the inverse
    # lookup -- the first lookup's empty USD/GBP leg short-circuits its
    # inner quote-leg loop before EUR/GBP is ever consulted).
    assert repo.fetch_pair_history_calls == 6
    assert repo.fetch_pair_history_pairs == [
        ("USD", "EUR"),
        ("EUR", "USD"),
        ("USD", "GBP"),
        ("GBP", "USD"),
        ("EUR", "GBP"),
        ("GBP", "EUR"),
    ]


def test_fx_service_stale_rate_logs_warning_but_returns_quote(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    service, _ = _service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.90,
            fetched_at="2024-01-01T00:00:00+00:00",
        ),
    )

    with caplog.at_level("WARNING"):
        quote = service.get_fx_rate("USD", "EUR", "2024-01-10")

    assert quote is not None
    assert quote.rate_date.isoformat() == "2024-01-01"
    assert quote.rate == Decimal("0.90")
    assert "Stale FX rate used" in caplog.text


def test_missing_fx_rate_error_roundtrips_via_pickle() -> None:
    exc = MissingFXRateError(
        provider="EODHD",
        base_currency="NLG",
        quote_currency="EUR",
        as_of="2000-06-30",
    )

    restored = pickle.loads(pickle.dumps(exc))

    assert isinstance(restored, MissingFXRateError)
    assert restored.provider == "EODHD"
    assert restored.base_currency == "NLG"
    assert restored.quote_currency == "EUR"
    assert restored.as_of == "2000-06-30"


def test_missing_fx_rate_error_crosses_spawn_process_pool() -> None:
    ctx = mp.get_context("spawn")

    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as executor:
        future = executor.submit(_raise_missing_fx_rate_error)
        with pytest.raises(MissingFXRateError) as exc_info:
            future.result()

    exc = exc_info.value
    assert exc.provider == "EODHD"
    assert exc.base_currency == "NLG"
    assert exc.quote_currency == "EUR"
    assert exc.as_of == "2000-06-30"


def test_parse_eodhd_fx_catalog_entry_parses_canonical_alias_and_odd_symbols() -> None:
    canonical = parse_eodhd_fx_catalog_entry({"Code": "EURUSD", "Name": "EUR/USD"})
    alias = parse_eodhd_fx_catalog_entry({"Code": "EUR", "Name": "USD/EUR"})
    odd = parse_eodhd_fx_catalog_entry({"Code": "USDARSB", "Name": "odd"})

    assert canonical is not None
    assert canonical.canonical_symbol == "EURUSD"
    assert canonical.base_currency == "EUR"
    assert canonical.quote_currency == "USD"
    assert canonical.is_alias is False
    assert canonical.is_refreshable is True

    assert alias is not None
    assert alias.canonical_symbol == "USDEUR"
    assert alias.base_currency == "USD"
    assert alias.quote_currency == "EUR"
    assert alias.is_alias is True
    assert alias.is_refreshable is False

    assert odd is not None
    assert odd.canonical_symbol == "USDARSB"
    assert odd.base_currency is None
    assert odd.is_refreshable is False


def test_eodhd_provider_lists_catalog_entries() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                [
                    {"Code": "EURUSD", "Name": "EUR/USD"},
                    {"Code": "EUR", "Name": "USD/EUR"},
                    {"Code": "USDARSB", "Name": "Odd"},
                ],
            )
        ]
    )
    provider = EODHDFXProvider(api_key="secret", session=session)

    entries = provider.list_catalog()

    assert [entry.symbol for entry in entries] == ["EURUSD", "EUR", "USDARSB"]
    assert session.calls[0][0].endswith("/exchange-symbol-list/FOREX")
    assert session.calls[0][1] == {"api_token": "secret", "fmt": "json"}


def test_eodhd_provider_fetch_history_uses_close_rate() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                [
                    {
                        "date": "2024-01-02",
                        "open": 1.09,
                        "high": 1.10,
                        "low": 1.08,
                        "close": 1.095,
                    }
                ],
                headers={"Date": "Tue, 02 Jan 2024 00:00:00 GMT"},
            )
        ]
    )
    provider = EODHDFXProvider(api_key="secret", session=session)

    rows = provider.fetch_history(
        canonical_symbol="EURUSD",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )

    assert len(rows) == 1
    assert rows[0].provider == "EODHD"
    assert rows[0].base_currency == "EUR"
    assert rows[0].quote_currency == "USD"
    assert rows[0].rate == 1.095
    assert rows[0].provider_symbol == "EURUSD"
    assert session.calls[0][0].endswith("/eod/EURUSD.FOREX")
    assert session.calls[0][1] == {
        "api_token": "secret",
        "fmt": "json",
        "from": "2024-01-01",
        "to": "2024-01-02",
        "order": "a",
    }


def test_canonical_trading_currency_normalizes_subunits() -> None:
    assert canonical_trading_currency("GBX") == "GBP"
    assert canonical_trading_currency("ZAC") == "ZAR"
    assert canonical_trading_currency("ILA") == "ILS"
    assert canonical_trading_currency("GBP0.01") == "GBP"


def test_canonical_trading_currency_passes_through_normal_codes() -> None:
    assert canonical_trading_currency("USD") == "USD"
    assert canonical_trading_currency("EUR") == "EUR"
    assert canonical_trading_currency("GBP") == "GBP"
    assert canonical_trading_currency("ZAR") == "ZAR"
    assert canonical_trading_currency("ILS") == "ILS"


def test_canonical_trading_currency_returns_none_for_missing() -> None:
    assert canonical_trading_currency(None) is None
    assert canonical_trading_currency("") is None
