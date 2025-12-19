"""Long-term debt metric fallbacks.

Author: Emre Tezel
"""

from datetime import date

from pyvalue.metrics.long_term_debt import LongTermDebtMetric
from pyvalue.storage import FactRecord, FinancialFactsRepository


def _make_fact(concept: str, value: float, end_date: str) -> FactRecord:
    return FactRecord(
        symbol="AAA.US",
        concept=concept,
        fiscal_period="FY",
        end_date=end_date,
        unit="USD",
        value=value,
    )


def test_long_term_debt_uses_noncurrent_plus_current(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    end_date = date.today().isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            _make_fact("LongTermDebtNoncurrent", 100.0, end_date),
            _make_fact("LongTermDebtCurrent", 20.0, end_date),
            _make_fact("SecuredLongTermDebt", 50.0, end_date),
        ],
    )

    metric = LongTermDebtMetric()
    result = metric.compute("AAA.US", repo)

    assert result is not None
    assert result.value == 120.0
    assert result.as_of == end_date


def test_long_term_debt_sums_components_when_noncurrent_missing(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "components.db")
    repo.initialize_schema()
    end_date = date.today().isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            _make_fact("SecuredLongTermDebt", 60.0, end_date),
            _make_fact("UnsecuredLongTermDebt", 40.0, end_date),
            _make_fact("LongTermDebtCurrent", 10.0, end_date),
        ],
    )

    metric = LongTermDebtMetric()
    result = metric.compute("AAA.US", repo)

    assert result is not None
    assert result.value == 110.0
    assert result.as_of == end_date


def test_long_term_debt_uses_notes_fallback(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "notes.db")
    repo.initialize_schema()
    end_date = date.today().isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            _make_fact("LongTermNotesPayable", 75.0, end_date),
            _make_fact("LongTermDebtCurrent", 20.0, end_date),
        ],
    )

    metric = LongTermDebtMetric()
    result = metric.compute("AAA.US", repo)

    assert result is not None
    assert result.value == 75.0
    assert result.as_of == end_date


def test_long_term_debt_uses_debt_and_lease_rollup(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "leases.db")
    repo.initialize_schema()
    end_date = date.today().isoformat()
    repo.replace_facts(
        "AAA.US",
        [
            _make_fact("LongTermDebtAndCapitalLeaseObligationsNoncurrent", 90.0, end_date),
            _make_fact("LongTermDebtAndCapitalLeaseObligationsCurrent", 15.0, end_date),
        ],
    )

    metric = LongTermDebtMetric()
    result = metric.compute("AAA.US", repo)

    assert result is not None
    assert result.value == 105.0
    assert result.as_of == end_date
