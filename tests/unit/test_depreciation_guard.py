"""Unit tests for the D&A sign guard (``pyvalue.metrics.depreciation``).

The guard drops negative Depreciation & Amortization facts (an EODHD
data-quality artifact -- sign errors on operating companies, net
accretion-of-discount mislabels and scale blow-ups on financials) while leaving
every other concept untouched, because capex and net income are legitimately
negative and must not be filtered.

Author: Emre Tezel
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.depreciation import (
    DA_CONCEPTS,
    DA_FALLBACK_CONCEPT,
    DA_PRIMARY_CONCEPT,
    guarded_monetary_facts,
)

LISTING_ID = 1
# SuRo/SSSS raw cash-flow depreciation: a real EODHD row, absurd by ~7 orders of
# magnitude. abs() would turn this into a +$87T add-back; the guard drops it.
SURO_SCALE_ERROR = -87_445_149_000_000.0
CAPEX_CONCEPT = "CapitalExpenditures"


class _FakeFactsRepo(RegionFactsRepository):
    """In-memory fact source keyed by concept, mirroring the read path."""

    def __init__(self, records_by_concept: dict[str, list[FactRecord]]) -> None:
        super().__init__(self)
        self._records_by_concept = records_by_concept

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: str | None = None,
        limit: int | None = None,
    ) -> list[FactRecord]:
        records = list(self._records_by_concept.get(concept, []))
        if limit is not None:
            return records[:limit]
        return records

    def ticker_currency_by_id(self, listing_id: int) -> str | None:
        return "USD"


def _rec(concept: str, value: float) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date="2025-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _amounts(concept: str, values: list[float]) -> list[float]:
    repo = _FakeFactsRepo({concept: [_rec(concept, value) for value in values]})
    return [
        fact.money.amount for fact in guarded_monetary_facts(repo, LISTING_ID, concept)
    ]


def test_drops_negative_da_keeps_zero_and_positive_in_order() -> None:
    # A genuine no-D&A period (0.0) is a valid add-back of nothing and is kept;
    # the sign error (-5) and the scale error are both dropped.
    assert _amounts(DA_PRIMARY_CONCEPT, [10.0, -5.0, 0.0, SURO_SCALE_ERROR, 20.0]) == [
        10.0,
        0.0,
        20.0,
    ]


def test_scale_error_dropped_for_cashflow_da_concept() -> None:
    assert _amounts(DA_FALLBACK_CONCEPT, [SURO_SCALE_ERROR]) == []


def test_non_da_concept_passes_negatives_through() -> None:
    # Capex is a real negative cash outflow; it must never be sign-guarded.
    assert _amounts(CAPEX_CONCEPT, [-50.0, -10.0, 5.0]) == [-50.0, -10.0, 5.0]


def test_da_concepts_membership() -> None:
    assert DA_PRIMARY_CONCEPT in DA_CONCEPTS
    assert DA_FALLBACK_CONCEPT in DA_CONCEPTS
    assert CAPEX_CONCEPT not in DA_CONCEPTS


_FINITE = st.floats(
    allow_nan=False, allow_infinity=False, min_value=-1e15, max_value=1e15
)


@given(values=st.lists(_FINITE))
def test_property_da_result_has_no_negatives_and_keeps_the_rest(
    values: list[float],
) -> None:
    result = _amounts(DA_PRIMARY_CONCEPT, values)
    assert all(amount >= 0 for amount in result)
    assert len(result) == sum(1 for value in values if value >= 0)


@given(values=st.lists(_FINITE))
def test_property_non_da_concept_is_untouched(values: list[float]) -> None:
    assert _amounts(CAPEX_CONCEPT, values) == list(values)
