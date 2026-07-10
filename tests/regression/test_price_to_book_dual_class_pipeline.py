"""Regression: dual-class P/B end-to-end (raw payload -> normalize -> metric).

Replays the GOOGL P1 defect through the *production* path on a temp DB: a
GOOGL-shaped raw EODHD payload (class-scoped ``SharesStats``, total-count
balance-sheet/outstandingShares history, ``Highlights.MarketCapitalization``
anchor) is normalized by the real bulk command, and ``price_to_book`` is then
computed from the real repositories. On the pre-resolver code the stored
metric came out 4.4477 (Class-A share basis); the arbitrated resolver must
price the company total and produce ~9.34.

No network: normalization reads ``fundamentals_raw``; prices are seeded.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from conftest import (
    resolve_listing_id,
    seed_exchange,
    seed_price,
    seed_raw_fundamentals,
    seed_supported_listings,
)
from pyvalue.cli.normalize import cmd_normalize_eodhd_fundamentals_bulk
from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.price_to_book import PriceToBookMetric
from pyvalue.persistence.storage import FinancialFactsRepository, MarketDataRepository
from pyvalue.universe import Listing

_TODAY = date.today()
UPDATED_AT = (_TODAY - timedelta(days=5)).isoformat()
LATEST_PRICE_DATE = (_TODAY - timedelta(days=1)).isoformat()
# A quarter-end-ish balance-sheet date well inside the 400-day recency window.
PERIOD_END = (_TODAY - timedelta(days=100)).isoformat()

EQUITY = 415_265e6
TOTAL_SHARES = 12_228e6
CLASS_A_SHARES = 5_822e6
PROVIDER_MARKET_CAP = 3_318_691e6
NEAR_ANCHOR_CLOSE = 273.50
LATEST_CLOSE = 317.24


def _googl_shaped_payload() -> dict[str, object]:
    return {
        "General": {"CurrencyCode": "USD", "UpdatedAt": UPDATED_AT},
        # The poison source: SharesStats counts only the listed class.
        "SharesStats": {"SharesOutstanding": CLASS_A_SHARES},
        # The company-total anchor EODHD computes as close x total shares.
        "Highlights": {"MarketCapitalization": PROVIDER_MARKET_CAP},
        "outstandingShares": {
            "quarterly": {
                "0": {
                    "date": "2025-Q4",
                    "dateFormatted": PERIOD_END,
                    "shares": TOTAL_SHARES,
                }
            }
        },
        "Financials": {
            "Balance_Sheet": {
                "quarterly": [
                    {
                        "date": PERIOD_END,
                        "commonStockSharesOutstanding": TOTAL_SHARES,
                        "totalStockholderEquity": EQUITY,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
    }


def test_price_to_book_pipeline_prices_the_company_total(tmp_path: Path) -> None:
    db_path = tmp_path / "dual-class.db"

    seed_exchange(db_path)
    seed_supported_listings(
        db_path,
        "EODHD",
        "US",
        [
            Listing(
                symbol="GOOGL.US",
                security_name="Alphabet Inc Class A",
                exchange="US",
                currency="USD",
            )
        ],
    )
    seed_raw_fundamentals(db_path, "EODHD", "GOOGL.US", _googl_shaped_payload())
    # Two closes: one at the anchor's own date (the implied-shares divisor)
    # and the fresher one the metric prices with.
    seed_price(db_path, "GOOGL.US", UPDATED_AT, NEAR_ANCHOR_CLOSE)
    seed_price(db_path, "GOOGL.US", LATEST_PRICE_DATE, LATEST_CLOSE)

    rc = cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path), symbols=["GOOGL.US"], force=False
    )
    assert rc == 0

    listing_id = resolve_listing_id(db_path, "GOOGL.US")
    facts_repo = FinancialFactsRepository(db_path)

    # The normalizer must have persisted both the poison snapshot and the
    # anchor -- the fix arbitrates them, it does not suppress them.
    snapshot_rows = facts_repo.facts_for_concept(
        listing_id, "CommonStockSharesOutstanding", fiscal_period="INSTANT"
    )
    assert [row.value for row in snapshot_rows] == [CLASS_A_SHARES]
    assert snapshot_rows[0].end_date == UPDATED_AT
    anchor_rows = facts_repo.facts_for_concept(
        listing_id, "ProviderMarketCapitalization"
    )
    assert [row.value for row in anchor_rows] == [PROVIDER_MARKET_CAP]
    assert anchor_rows[0].currency == "USD"

    result = PriceToBookMetric().compute(
        listing_id,
        RegionFactsRepository(facts_repo),
        MarketDataRepository(db_path),
    )

    assert result is not None
    expected = LATEST_CLOSE / (EQUITY / TOTAL_SHARES)
    assert result.value == pytest.approx(expected, rel=1e-9)
    assert result.value == pytest.approx(9.3411, rel=1e-3)
