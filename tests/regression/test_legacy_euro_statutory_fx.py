"""Regression: legacy-euro statement currencies convert via statutory rates.

The 2026-07 normalize-fundamentals log audit counted ~4,151 "Missing FX rate"
warnings for euro legacy currencies (NLG, DEM, FRF, ESP, FIM, PTE, BEF, GRD ->
EUR and IEP -> GBP), all for 1999-2002 transition-era filings -- e.g. AALB.AS
(Aalberts) 2000 annual reports in Dutch guilders. EODHD's FOREX catalog has no
pairs for dead currencies, so no refresh can ever supply them; the fix serves
the irrevocable Council Regulation (EC) No 2866/98 conversion rates from code
(``EURO_LEGACY_FIXED_RATES``).

This test replays the audit shape end-to-end through the normalizer with an
*empty* market-FX database: pre-fix the NLG balance-sheet fact is dropped with
a missing-FX warning; post-fix it converts exactly at 1 EUR = 2.20371 NLG with
no warning.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from pyvalue.money.fx import FXService
from pyvalue.normalization.eodhd import EODHDFactsNormalizer
from pyvalue.persistence.storage import FXRatesRepository


def test_aalb_as_nlg_balance_sheet_converts_with_no_market_fx(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    fx = FXService(db_path, repository=repo, provider_name="EODHD")
    normalizer = EODHDFactsNormalizer(fx_service=fx)

    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2000-06-30",
                        "totalAssets": 1_000_000.0,
                        "currency_symbol": "NLG",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    with caplog.at_level("WARNING"):
        records = normalizer.normalize(payload, symbol="AALB.AS", target_currency="EUR")

    assets = [r for r in records if r.concept == "Assets"]
    assert assets, "the NLG fact must survive conversion, not be dropped"
    assert assets[0].currency == "EUR"
    assert assets[0].value == pytest.approx(
        float(Decimal("1000000") / Decimal("2.20371"))
    )
    assert "Missing FX rate" not in caplog.text
