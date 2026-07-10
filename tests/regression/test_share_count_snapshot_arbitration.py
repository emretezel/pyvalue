"""Regression: share-basis metrics price the arbitrated company-total count.

The 2026-07 metric-verification audit found GOOGL.US ``price_to_book`` = 4.45
vs a recomputed 9.34 (docs/research/qarp-dvg-metric-verification-2026-07.md,
P1): the EODHD SharesStats snapshot -- normalized as the newest
``CommonStockSharesOutstanding`` INSTANT row -- counts only the listed class
for dual-class issuers, and P/B's latest-by-date read always picked it. The
inverse shape also exists: for TSLA/C the snapshot is the *only correct*
count, while the filing-based periodic rows carry weighted-average /
issued-incl-treasury figures.

These fixtures replay the four verified real-world shapes through the shared
resolver. The GOOGL case is the headline: it fails on the pre-resolver code
(P/B = 4.4477) and passes with the arbitrated total (P/B = 9.3411).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pytest

from pyvalue.currency import MetricUnitKind
from pyvalue.facts import RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.price_to_book import PriceToBookMetric
from pyvalue.metrics.share_resolver import (
    PROVIDER_MARKET_CAP_CONCEPT,
    resolve_current_share_count,
)
from pyvalue.metrics.utils import market_cap_money
from pyvalue.persistence.storage import FactRecord, MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()

# All fixture dates sit well inside the 400-day recency window so the metric
# gates never interfere with what these tests pin (the share *basis*).
SNAPSHOT_DATE = (_TODAY - timedelta(days=5)).isoformat()
PERIOD_END = (_TODAY - timedelta(days=100)).isoformat()
LATEST_PRICE_DATE = (_TODAY - timedelta(days=1)).isoformat()


class _FakeFactsRepo(RegionFactsRepository):
    """In-memory fact source keyed by concept, newest-first like storage."""

    def __init__(self, records_by_concept: dict[str, list[FactRecord]]) -> None:
        super().__init__(self)
        self._records_by_concept = records_by_concept

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        records = list(self._records_by_concept.get(concept, []))
        if fiscal_period:
            period = fiscal_period.upper()
            records = [
                record
                for record in records
                if (record.fiscal_period or "").upper() == period
            ]
        if limit is not None:
            return records[:limit]
        return records

    def latest_fact(self, listing_id: int, concept: str) -> Optional[FactRecord]:
        records = self.facts_for_concept(listing_id, concept)
        if not records:
            return None
        return max(records, key=lambda record: record.end_date)

    def ticker_currency_by_id(self, listing_id: int) -> Optional[str]:
        return "USD"


class _FakeMarketRepo(MarketDataRepository):
    # Nominal MarketDataRepository subtype whose __init__ skips super() so no
    # SQLite DB is opened; serves the latest close plus the anchor-dated close
    # the resolver's implied-shares division needs.
    def __init__(self, latest_price: float, near_price: Optional[float]) -> None:
        self._latest_price = latest_price
        self._near_price = near_price

    def latest_snapshot_by_id(self, listing_id: int) -> Optional[PriceData]:
        return PriceData(
            symbol="TEST.US",
            price=self._latest_price,
            as_of=LATEST_PRICE_DATE,
            currency="USD",
        )

    def snapshot_near_date_by_id(
        self,
        listing_id: int,
        as_of: str,
        *,
        max_distance_days: int,
    ) -> Optional[PriceData]:
        if self._near_price is None:
            return None
        return PriceData(
            symbol="TEST.US",
            price=self._near_price,
            as_of=as_of,
            currency="USD",
        )

    def ticker_currency_by_id(self, listing_id: int) -> Optional[str]:
        return "USD"


def _fact(
    concept: str,
    value: float,
    *,
    fiscal_period: str,
    end_date: str,
    unit_kind: MetricUnitKind,
    currency: Optional[str] = None,
) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind=unit_kind,
        value=value,
        filed=None,
        currency=currency,
    )


def _share_fixture(
    *,
    snapshot_shares: float,
    periodic_shares: float,
    provider_market_cap: float,
    equity: Optional[float] = None,
) -> _FakeFactsRepo:
    records: dict[str, list[FactRecord]] = {
        "CommonStockSharesOutstanding": [
            _fact(
                "CommonStockSharesOutstanding",
                snapshot_shares,
                fiscal_period="INSTANT",
                end_date=SNAPSHOT_DATE,
                unit_kind="count",
            ),
            _fact(
                "CommonStockSharesOutstanding",
                periodic_shares,
                fiscal_period="Q4",
                end_date=PERIOD_END,
                unit_kind="count",
            ),
        ],
        "EntityCommonStockSharesOutstanding": [
            _fact(
                "EntityCommonStockSharesOutstanding",
                periodic_shares,
                fiscal_period="Q4",
                end_date=PERIOD_END,
                unit_kind="count",
            ),
        ],
        PROVIDER_MARKET_CAP_CONCEPT: [
            _fact(
                PROVIDER_MARKET_CAP_CONCEPT,
                provider_market_cap,
                fiscal_period="INSTANT",
                end_date=SNAPSHOT_DATE,
                unit_kind="monetary",
                currency="USD",
            ),
        ],
    }
    if equity is not None:
        records["CommonStockholdersEquity"] = [
            _fact(
                "CommonStockholdersEquity",
                equity,
                fiscal_period="Q4",
                end_date=PERIOD_END,
                unit_kind="monetary",
                currency="USD",
            ),
        ]
    return _FakeFactsRepo(records)


def test_googl_price_to_book_uses_the_company_total() -> None:
    """The headline P1 case: dual-class P/B must price all 12.228B shares.

    On the pre-resolver code the INSTANT SharesStats row (Class A only,
    5.822B) wins latest-by-date and P/B computes 4.4477; the anchor
    (3,318,691M / 273.50 = 12,134M implied shares) rejects it.
    """

    repo = _share_fixture(
        snapshot_shares=5.822e9,
        periodic_shares=12.228e9,
        provider_market_cap=3_318_691e6,
        equity=415_265e6,
    )
    market = _FakeMarketRepo(latest_price=317.24, near_price=273.50)

    result = PriceToBookMetric().compute(LISTING_ID, repo, market)

    assert result is not None
    expected = 317.24 / (415_265e6 / 12.228e9)
    assert result.value == pytest.approx(expected, rel=1e-9)
    assert result.value == pytest.approx(9.3411, rel=1e-3)
    # The buggy Class-A basis would halve the multiple; make the failure mode
    # explicit for the next reader.
    assert result.value != pytest.approx(4.4477, rel=1e-2)
    assert result.as_of == PERIOD_END  # still stamped by the equity date


def test_googl_market_cap_matches_the_same_share_basis() -> None:
    repo = _share_fixture(
        snapshot_shares=5.822e9,
        periodic_shares=12.228e9,
        provider_market_cap=3_318_691e6,
    )
    market = _FakeMarketRepo(latest_price=317.24, near_price=273.50)

    cap = market_cap_money(
        LISTING_ID,
        repo=repo,
        market_repo=market,
        metric_id="market_cap",
        target_currency="USD",
    )

    assert cap is not None
    assert cap.money.amount == pytest.approx(12.228e9 * 317.24, rel=1e-9)
    assert cap.as_of == LATEST_PRICE_DATE


def test_pltr_periodic_total_beats_the_class_scoped_snapshot() -> None:
    # 11% class gap -- inside any plausible drift band, so only the anchor
    # (342,153M / 132.96 = 2,573M implied) can resolve it correctly.
    repo = _share_fixture(
        snapshot_shares=2_291_470_751.0,
        periodic_shares=2_573_497_000.0,
        provider_market_cap=342_153e6,
    )
    market = _FakeMarketRepo(latest_price=128.06, near_price=132.96)

    record = resolve_current_share_count(LISTING_ID, repo, market)

    assert record is not None
    assert record.value == 2_573_497_000.0
    assert record.fiscal_period != "INSTANT"


def test_tsla_true_snapshot_beats_the_weighted_average_periodic() -> None:
    # The mirror image: the periodic rows are the artifact (weighted-average
    # 3,539M vs the true period-end 3,752M) and the anchor endorses the
    # snapshot -- market_cap deliberately moves off the old Entity basis.
    repo = _share_fixture(
        snapshot_shares=3_752_431_984.0,
        periodic_shares=3.539e9,
        provider_market_cap=1_357_742e6,
    )
    market = _FakeMarketRepo(latest_price=348.95, near_price=361.83)

    record = resolve_current_share_count(LISTING_ID, repo, market)
    assert record is not None
    assert record.value == 3_752_431_984.0
    assert record.fiscal_period == "INSTANT"

    cap = market_cap_money(
        LISTING_ID,
        repo=repo,
        market_repo=market,
        metric_id="market_cap",
        target_currency="USD",
    )
    assert cap is not None
    assert cap.money.amount == pytest.approx(3_752_431_984.0 * 348.95, rel=1e-9)


def test_citi_true_snapshot_beats_the_issued_incl_treasury_periodic() -> None:
    # 6.5% issued-vs-outstanding gap; the anchor factorizes on the snapshot to
    # 0.1% (187,842M / 107.38 = 1,749M).
    repo = _share_fixture(
        snapshot_shares=1_749_319_009.0,
        periodic_shares=1_862_600_000.0,
        provider_market_cap=187_842e6,
    )
    market = _FakeMarketRepo(latest_price=124.39, near_price=107.38)

    record = resolve_current_share_count(LISTING_ID, repo, market)

    assert record is not None
    assert record.value == 1_749_319_009.0
    assert record.fiscal_period == "INSTANT"
