"""Regression: ``LongTermDebt`` must never be a non-current-liabilities proxy.

Until 2026-08-10 the EODHD normalizer substituted
``totalLiabilities - totalCurrentLiabilities`` for ``LongTermDebt`` whenever
every raw long-term-debt field was null. That quantity is *all* non-current
liabilities -- deferred revenue, deferred tax, pensions, provisions -- and it
landed in ``financial_facts`` under a concept name asserting it was borrowed
money. Nothing downstream could distinguish it from a reported figure, so it
flowed into enterprise value, net debt and invested capital as if measured.

It affected **46% of the universe** (24,467 of 53,026 listings carrying a
``LongTermDebt`` fact had it exactly equal to ``Liabilities -
LiabilitiesCurrent``); where a vendor debt rollup also existed the substituted
value exceeded it by more than 2x on 21% of them. This is the overstating half
of the 2026-08 debt audit -- see ``docs/research/msft-qarp-review-2026-08.md``.

The quantity itself is still wanted: ``interest_coverage`` needs *some* upper
bound on debt to decide whether its debt-free cap is safe, and non-current
liabilities is a tighter bound than total liabilities. So it is kept -- under
``LiabilitiesNoncurrent``, a name that describes it honestly.

The first test fails on the pre-fix normalizer (which emitted a ``LongTermDebt``
fact of 5,000); the rest pin the surrounding behaviour that must not move.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Any, Optional

from pyvalue.normalization.eodhd import EODHDFactsNormalizer


def _payload(**balance_sheet_fields: Any) -> dict[str, Any]:
    """Build a single-FY payload carrying only the named balance-sheet keys."""

    return {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "currency_symbol": "USD",
                        **balance_sheet_fields,
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }


def _value(records: list[Any], concept: str) -> Optional[float]:
    matches = [r for r in records if r.concept == concept]
    return matches[0].value if matches else None


def test_null_debt_fields_do_not_fabricate_long_term_debt() -> None:
    """The headline regression: no debt reported means no debt fact."""

    records = EODHDFactsNormalizer().normalize(
        _payload(totalLiab=8000.0, totalCurrentLiabilities=3000.0),
        symbol="TEST.US",
    )

    # Pre-fix this asserted 5000.0 -- non-current liabilities wearing the name
    # of borrowed money.
    assert _value(records, "LongTermDebt") is None
    # The bound is not discarded, only renamed.
    assert _value(records, "LiabilitiesNoncurrent") == 5000.0


def test_reported_long_term_debt_is_preserved() -> None:
    """A real vendor figure still wins -- the fix removes only the substitute."""

    records = EODHDFactsNormalizer().normalize(
        _payload(
            longTermDebt=1200.0,
            totalLiab=8000.0,
            totalCurrentLiabilities=3000.0,
        ),
        symbol="TEST.US",
    )

    assert _value(records, "LongTermDebt") == 1200.0
    # Both facts coexist: one is borrowings, the other the bound above it.
    assert _value(records, "LiabilitiesNoncurrent") == 5000.0


def test_vendor_noncurrent_total_is_preferred_over_the_derivation() -> None:
    """``nonCurrentLiabilitiesTotal`` is reported; the subtraction is a fallback."""

    records = EODHDFactsNormalizer().normalize(
        _payload(
            nonCurrentLiabilitiesTotal=4200.0,
            totalLiab=8000.0,
            totalCurrentLiabilities=3000.0,
        ),
        symbol="TEST.US",
    )

    assert _value(records, "LiabilitiesNoncurrent") == 4200.0


def test_negative_derivation_is_rejected() -> None:
    """Current liabilities above total liabilities is garbage, not a bound."""

    records = EODHDFactsNormalizer().normalize(
        _payload(totalLiab=3000.0, totalCurrentLiabilities=8000.0),
        symbol="TEST.US",
    )

    assert _value(records, "LiabilitiesNoncurrent") is None
    assert _value(records, "LongTermDebt") is None


def test_no_liability_fields_emit_neither_concept() -> None:
    """Nothing to derive from means nothing stored -- unknown, not zero."""

    records = EODHDFactsNormalizer().normalize(
        _payload(totalAssets=9000.0),
        symbol="TEST.US",
    )

    assert _value(records, "LiabilitiesNoncurrent") is None
    assert _value(records, "LongTermDebt") is None
