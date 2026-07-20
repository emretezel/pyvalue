"""Regression: the corrupt WMT.MX 2017-07-31 provider period never normalizes.

The 2026-07 normalize-fundamentals log audit found that EODHD's WMT.MX
(Walmart de México) quarterly statements contain a period ending 2017-07-31
that is actually Walmart Inc's (US parent) fiscal Q2-FY2018 balance sheet in
USD -- totalAssets 201,566,000,000, a fiscal quarter end Walmex does not have
-- mislabeled ``currency_symbol="PGK"`` (verified against ``fundamentals_raw``
on 2026-07-20). Until the GBP pivot landed, the missing PGK->MXN rate
accidentally blocked conversion of the monetary fields, but the FX-free
concepts (share counts, EPS) still leaked into ``financial_facts`` (purged by
migration 085).

The headline test seeds a *USD*-pivot FX path (PGK->USD, USD->MXN) so the
corrupt period converts cleanly on pre-quarantine code -- exactly the hazard
the production GBP pivot introduces -- and asserts the quarantine drops every
fact of the period while the legitimate MXN sibling survives.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from pyvalue.money.fx import FXService
from pyvalue.normalization.eodhd import EODHDFactsNormalizer
from pyvalue.persistence.storage import FXRateRecord, FXRatesRepository


def _fx_service_with_usd_pivot_rates(tmp_path: Path) -> FXService:
    """FX service whose only paths bridge PGK->MXN through the USD pivot."""

    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(
        [
            FXRateRecord(
                provider="EODHD",
                rate_date="2017-07-28",
                base_currency="USD",
                quote_currency="PGK",
                rate=3.2,
                fetched_at="2017-07-28T00:00:00+00:00",
            ),
            FXRateRecord(
                provider="EODHD",
                rate_date="2017-07-28",
                base_currency="USD",
                quote_currency="MXN",
                rate=17.7,
                fetched_at="2017-07-28T00:00:00+00:00",
            ),
        ]
    )
    return FXService(db_path, repository=repo, provider_name="EODHD")


def _wmt_mx_payload() -> Dict:
    """A minimal WMT.MX-shaped payload: one legit MXN quarter + the corrupt one."""

    return {
        "Financials": {
            "Balance_Sheet": {
                "quarterly": [
                    {
                        "date": "2017-04-30",
                        "totalAssets": 291_000_000_000.0,
                        "currency_symbol": "MXN",
                    },
                    {
                        # Walmart Inc's USD balance sheet mislabeled as PGK.
                        "date": "2017-07-31",
                        "totalAssets": 201_566_000_000.0,
                        "currency_symbol": "PGK",
                    },
                ]
            }
        },
        "General": {"CurrencyCode": "MXN"},
    }


def test_quarantined_pgk_period_never_converts_into_canonical_facts(
    tmp_path: Path,
) -> None:
    fx = _fx_service_with_usd_pivot_rates(tmp_path)
    normalizer = EODHDFactsNormalizer(fx_service=fx)

    records = normalizer.normalize(
        _wmt_mx_payload(), symbol="WMT.MX", target_currency="MXN"
    )

    # Pre-fix, the corrupt period converts via the seeded USD pivot and lands
    # in the output; the quarantine must drop it entirely.
    assert [r for r in records if r.end_date == "2017-07-31"] == []

    survivors = [r for r in records if r.concept == "Assets"]
    assert [r.end_date for r in survivors] == ["2017-04-30"]
    assert survivors[0].currency == "MXN"
    assert survivors[0].value == 291_000_000_000.0


def test_quarantine_is_symbol_scoped(tmp_path: Path) -> None:
    # The same period under a different symbol is untouched: the registry
    # must never over-drop legitimate 2017-07-31 filings elsewhere.
    fx = _fx_service_with_usd_pivot_rates(tmp_path)
    normalizer = EODHDFactsNormalizer(fx_service=fx)

    records = normalizer.normalize(
        _wmt_mx_payload(), symbol="TEST.MX", target_currency="MXN"
    )

    end_dates = {r.end_date for r in records if r.concept == "Assets"}
    assert end_dates == {"2017-04-30", "2017-07-31"}
