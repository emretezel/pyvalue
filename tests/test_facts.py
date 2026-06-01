"""Tests for the typed fact read layer (MonetaryFact / ScalarFact accessors).

These cover the Phase 5a read boundary: monetary/per-share facts are minted into
currency-carrying ``Money`` (no bare-float ``value`` reachable), scalar/count
facts stay plain floats, and asking for the wrong kind raises rather than
silently coercing.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pytest

from pyvalue.currency import MetricUnitKind
from pyvalue.facts import (
    FactReader,
    MonetaryFact,
    RegionFactsRepository,
    ScalarFact,
    to_monetary_fact,
    to_scalar_fact,
)
from pyvalue.money import Money
from pyvalue.storage import FactRecord


def _fact(
    concept: str,
    unit_kind: MetricUnitKind,
    value: float,
    currency: Optional[str] = None,
    end_date: str = "2023-12-31",
    fiscal_period: str = "FY",
) -> FactRecord:
    """Build a stored fact record for the fake source."""

    return FactRecord(
        symbol="X.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind=unit_kind,
        value=value,
        currency=currency,
    )


class _FakeSource:
    """In-memory raw fact source satisfying ``RawFactSource``."""

    def __init__(self, facts_by_concept: Dict[str, List[FactRecord]]) -> None:
        self._facts = facts_by_concept

    def latest_fact(self, symbol: str, concept: str) -> Optional[FactRecord]:
        records = self._facts.get(concept, [])
        return records[0] if records else None

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        records = [
            record
            for record in self._facts.get(concept, [])
            if fiscal_period is None or record.fiscal_period == fiscal_period
        ]
        return records[:limit] if limit is not None else records


def _region(facts_by_concept: Dict[str, List[FactRecord]]) -> RegionFactsRepository:
    return RegionFactsRepository(_FakeSource(facts_by_concept))


def test_region_repository_satisfies_fact_reader() -> None:
    region = _region({})
    assert isinstance(region, FactReader)


def test_latest_monetary_fact_mints_money() -> None:
    region = _region({"Revenues": [_fact("Revenues", "monetary", 5_000_000.0, "USD")]})
    fact = region.latest_monetary_fact("X.US", "Revenues")
    assert fact is not None
    assert fact.money == Money.of(5_000_000.0, "USD")
    assert fact.end_date == "2023-12-31"
    # The whole point of the boundary: the bare float is not reachable.
    assert not hasattr(fact, "value")


def test_per_share_fact_is_monetary() -> None:
    # EPS / DPS carry a currency, so they are MonetaryFact (per-share is money).
    region = _region(
        {"EarningsPerShare": [_fact("EarningsPerShare", "per_share", 3.5, "USD")]}
    )
    fact = region.latest_monetary_fact("X.US", "EarningsPerShare")
    assert fact is not None
    assert fact.money == Money.of(3.5, "USD")


def test_latest_scalar_fact_keeps_float() -> None:
    region = _region(
        {
            "EntityCommonStockSharesOutstanding": [
                _fact("EntityCommonStockSharesOutstanding", "count", 1_000.0)
            ]
        }
    )
    fact = region.latest_scalar_fact("X.US", "EntityCommonStockSharesOutstanding")
    assert fact is not None
    assert fact.value == 1_000.0
    assert fact.unit_kind == "count"


def test_monetary_facts_for_concept_maps_every_row() -> None:
    region = _region(
        {
            "Revenues": [
                _fact("Revenues", "monetary", 3.0, "USD", end_date="2023-12-31"),
                _fact("Revenues", "monetary", 2.0, "USD", end_date="2022-12-31"),
            ]
        }
    )
    facts = region.monetary_facts_for_concept("X.US", "Revenues")
    assert [f.money.amount for f in facts] == [3.0, 2.0]
    assert all(isinstance(f, MonetaryFact) for f in facts)


def test_scalar_facts_for_concept_maps_every_row() -> None:
    region = _region(
        {
            "Shares": [
                _fact("Shares", "count", 10.0),
                _fact("Shares", "count", 20.0),
            ]
        }
    )
    facts = region.scalar_facts_for_concept("X.US", "Shares")
    assert [f.value for f in facts] == [10.0, 20.0]
    assert all(isinstance(f, ScalarFact) for f in facts)


def test_subunit_currency_collapses_to_major_at_boundary() -> None:
    # Defensive: a subunit code should never reach a stored fact post-071, but if
    # it does the Money mint must collapse pence -> pounds, not inflate 100x.
    region = _region({"Revenues": [_fact("Revenues", "monetary", 2_500.0, "GBX")]})
    fact = region.latest_monetary_fact("X.US", "Revenues")
    assert fact is not None
    assert fact.money == Money.of(25.0, "GBP")


def test_latest_monetary_fact_rejects_scalar_concept() -> None:
    region = _region({"Shares": [_fact("Shares", "count", 1_000.0)]})
    with pytest.raises(ValueError):
        region.latest_monetary_fact("X.US", "Shares")


def test_latest_scalar_fact_rejects_monetary_concept() -> None:
    region = _region({"Revenues": [_fact("Revenues", "monetary", 1.0, "USD")]})
    with pytest.raises(ValueError):
        region.latest_scalar_fact("X.US", "Revenues")


def test_to_scalar_fact_rejects_monetary_record() -> None:
    with pytest.raises(ValueError):
        to_scalar_fact(_fact("Revenues", "monetary", 1.0, "USD"))


def test_to_monetary_fact_drops_currencyless_monetary_row() -> None:
    # A monetary fact without a currency is a normalization bug: drop + warn,
    # do not crash the batch.
    assert to_monetary_fact(_fact("Revenues", "monetary", 1.0, currency=None)) is None


def test_missing_fact_returns_none_or_empty() -> None:
    region = _region({})
    assert region.latest_monetary_fact("X.US", "Revenues") is None
    assert region.latest_scalar_fact("X.US", "Shares") is None
    assert region.monetary_facts_for_concept("X.US", "Revenues") == []
    assert region.scalar_facts_for_concept("X.US", "Shares") == []
