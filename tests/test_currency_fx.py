"""Tests for shared currency helpers and FX lookup behavior.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TypeVar, overload
import json
import multiprocessing as mp
import pickle

import pytest
import requests

from pyvalue.currency import (
    canonical_trading_currency,
    normalize_currency_code,
    normalize_monetary_amount,
    resolve_eodhd_currency,
)
from pyvalue.money.fx import (
    EODHDFXProvider,
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
    provider table versus individual direct pairs, and which pairs it asked for.
    """

    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path)
        self.fetch_all_calls = 0
        self.fetch_pair_history_calls = 0
        self.fetch_pair_history_pairs: list[tuple[str, str, str]] = []

    def fetch_all_for_provider(
        self, provider: str
    ) -> list[tuple[str, str, str, float]]:
        self.fetch_all_calls += 1
        return super().fetch_all_for_provider(provider)

    def fetch_pair_history(
        self, provider: str, base_currency: str, quote_currency: str
    ) -> list[tuple[str, float]]:
        self.fetch_pair_history_calls += 1
        self.fetch_pair_history_pairs.append(
            (str(provider), str(base_currency), str(quote_currency))
        )
        return super().fetch_pair_history(provider, base_currency, quote_currency)


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
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-10",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.90,
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
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-02-01",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.80,
            fetched_at="2024-02-01T00:00:00+00:00",
            source_kind="provider",
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
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-06-30",
            base_currency="GBP",
            quote_currency="EUR",
            rate=1.1815,
            fetched_at="2024-06-30T00:00:00+00:00",
            source_kind="provider",
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
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2025-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.1751,
            fetched_at="2025-12-31T00:00:00+00:00",
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2025-12-31",
            base_currency="GBP",
            quote_currency="USD",
            rate=1.2648,
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
            source_kind="provider",
        ),
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-01-01",
            base_currency="USD",
            quote_currency="GBP",
            rate=0.80,
            fetched_at="2024-01-01T00:00:00+00:00",
            source_kind="provider",
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
            source_kind="provider",
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
    assert repo.fetch_pair_history_calls == 2
    assert repo.fetch_pair_history_pairs == [
        ("EODHD", "USD", "EUR"),
        ("EODHD", "EUR", "USD"),
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
            source_kind="provider",
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
    assert rows[0].meta_json is not None and "EURUSD" in rows[0].meta_json
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
