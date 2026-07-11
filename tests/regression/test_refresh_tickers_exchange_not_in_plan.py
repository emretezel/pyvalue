"""Regression: a plan-dropped exchange must not kill the multi-exchange refresh.

On 2026-07-11 the user's renewed EODHD subscription stopped covering three
exchanges; `exchange-symbol-list/IC` answered HTTP 404 and the raw
``raise_for_status()`` HTTPError aborted `refresh-supported-tickers` at
exchange 24 of 73 (the traceback also printed the request URL with the
``api_token``). The client now maps that 404 to a typed
`ExchangeNotInPlanError` and the CLI warns, skips the exchange with its stored
data untouched, and finishes the run (exit 0 for pure not-in-plan skips; other
provider errors are warned, skipped, and surface as exit 1). These tests fail
on the crashing code.

Author: Emre Tezel
"""

from __future__ import annotations

import json as json_lib
from pathlib import Path

import pytest
import requests

from cli_test_helpers import patch_cli
from conftest import seed_exchange
from pyvalue import cli
from pyvalue.ingestion import EODHDFundamentalsClient, ExchangeNotInPlanError
from pyvalue.persistence.storage import SupportedTickerRepository

_ICE_ROW = {"Code": "ICE", "Name": "Ice hf", "Type": "Common Stock", "Currency": "ISK"}
_KEEP_ROW = {
    "Code": "KEEP",
    "Name": "Keep plc",
    "Type": "Common Stock",
    "Currency": "GBP",
}


class _RoutingSession(requests.Session):
    """A ``requests.Session`` routing each exchange to a canned status/payload.

    Modeled on ``DummyEODSession`` (tests/test_market_data.py): subclassing the
    real ``Session`` keeps the client's parameter type satisfied, and returning
    a genuine ``requests.Response`` means ``raise_for_status``/``json`` run the
    production code paths. The response ``url`` re-merges the query parameters
    exactly as requests would, so an error message embeds the ``api_token`` --
    reproducing the leak the sanitizer must scrub.
    """

    def __init__(self, routes: dict[str, tuple[int, object]]) -> None:
        # Deliberately no super().__init__(): no real network machinery needed.
        self.routes = routes
        self.calls: list[str] = []

    def request(
        self,
        method: str | bytes,
        url: str | bytes,
        *args: object,
        **kwargs: object,
    ) -> requests.Response:
        url_text = str(url)
        exchange_code = url_text.rstrip("/").rsplit("/", 1)[-1]
        self.calls.append(exchange_code)
        status, payload = self.routes[exchange_code]
        raw_params = kwargs.get("params")
        params = raw_params if isinstance(raw_params, dict) else {}
        query = "&".join(f"{key}={value}" for key, value in params.items())
        response = requests.Response()
        response.status_code = status
        response.reason = "Not Found" if status == 404 else "Error"
        response.url = f"{url_text}?{query}" if query else url_text
        response._content = json_lib.dumps(payload).encode("utf-8")
        return response


def _patch_routing_client(
    monkeypatch: pytest.MonkeyPatch, session: _RoutingSession
) -> None:
    """Patch the CLI's client class with one bound to the routing session."""

    class RoutingClient(EODHDFundamentalsClient):
        def __init__(self, api_key: str) -> None:
            super().__init__(api_key=api_key, session=session)

    patch_cli(monkeypatch, "EODHDFundamentalsClient", RoutingClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "SECRET")


def _seed_catalog_with_ic_ticker(db_path: Path) -> SupportedTickerRepository:
    """Catalog IC + LSE and give IC one supported ticker to protect."""

    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "IC", "LSE")
    ticker_repo.replace_for_exchange("EODHD", "IC", [_ICE_ROW])
    return ticker_repo


def test_list_symbols_404_maps_to_typed_error_without_token() -> None:
    session = _RoutingSession({"IC": (404, {"error": "not found"})})
    client = EODHDFundamentalsClient(api_key="SECRET", session=session)

    with pytest.raises(ExchangeNotInPlanError) as excinfo:
        client.list_symbols("IC")

    assert excinfo.value.exchange_code == "IC"
    assert "SECRET" not in str(excinfo.value)
    # The chained HTTPError is what tracebacks print below the typed error --
    # it must carry the redacted URL, never the real token.
    cause = excinfo.value.__cause__
    assert isinstance(cause, requests.HTTPError)
    assert "SECRET" not in str(cause)
    assert "api_token=REDACTED" in str(cause)


def test_refresh_continues_past_not_in_plan_exchange(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "not-in-plan.db"
    ticker_repo = _seed_catalog_with_ic_ticker(db_path)
    session = _RoutingSession({"IC": (404, {}), "LSE": (200, [_KEEP_ROW])})
    _patch_routing_client(monkeypatch, session)

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=True,
    )

    out = capsys.readouterr().out
    # The run finished: IC (alphabetically first) 404ed, LSE still refreshed.
    assert rc == 0
    assert session.calls == ["IC", "LSE"]
    assert "WARNING: IC is not covered by the current EODHD plan" in out
    assert "refresh-supported-exchanges" in out
    assert "Skipped 1 exchange(s) not covered by the current EODHD plan: IC" in out
    assert "SECRET" not in out
    lse_rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    assert [row.symbol for row in lse_rows] == ["KEEP.LSE"]
    # Skipped means untouched: IC's stored catalog survives the 404.
    ic_rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["IC"])
    assert [row.symbol for row in ic_rows] == ["ICE.IC"]


def test_refresh_other_provider_errors_exit_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "provider-error.db"
    ticker_repo = _seed_catalog_with_ic_ticker(db_path)
    session = _RoutingSession(
        {"IC": (500, {"error": "boom"}), "LSE": (200, [_KEEP_ROW])}
    )
    _patch_routing_client(monkeypatch, session)

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=True,
    )

    out = capsys.readouterr().out
    # A transient provider error is skipped but must reach the exit code.
    assert rc == 1
    assert "WARNING: IC refresh failed with a provider error" in out
    assert "Failed to refresh 1 exchange(s) on provider errors: IC" in out
    assert "api_token=REDACTED" in out
    assert "SECRET" not in out
    lse_rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    assert [row.symbol for row in lse_rows] == ["KEEP.LSE"]
    ic_rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["IC"])
    assert [row.symbol for row in ic_rows] == ["ICE.IC"]
