"""Tests for LongTermDebt metric fallbacks via normalization.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics.long_term_debt import LongTermDebtMetric
from pyvalue.normalization import SECFactsNormalizer
from pyvalue.storage import FinancialFactsRepository


def _recent_date() -> str:
    return (date.today() - timedelta(days=10)).isoformat()


def test_long_term_debt_metric_uses_other_long_term_debt_fallback(tmp_path):
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "OtherLongTermDebt": {
                    "units": {
                        "USD": [
                            {
                                "val": 300.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 40.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["OtherLongTermDebt", "LongTermDebtCurrent"]
    )
    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    repo.replace_facts("TEST.US", records)

    metric = LongTermDebtMetric()
    result = metric.compute("TEST.US", repo)

    assert result is not None
    assert result.value == 340.0
    assert result.as_of == recent


def test_long_term_debt_metric_uses_lease_including_current_fallback(tmp_path):
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities": {
                    "units": {
                        "USD": [
                            {
                                "val": 510.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities"]
    )
    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    repo.replace_facts("TEST.US", records)

    metric = LongTermDebtMetric()
    result = metric.compute("TEST.US", repo)

    assert result is not None
    assert result.value == 510.0
    assert result.as_of == recent


def test_long_term_debt_metric_uses_operating_lease_noncurrent_fallback(tmp_path):
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "OperatingLeaseLiabilityNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 60.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 4.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["OperatingLeaseLiabilityNoncurrent", "LongTermDebtCurrent"]
    )
    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    repo.replace_facts("TEST.US", records)

    metric = LongTermDebtMetric()
    result = metric.compute("TEST.US", repo)

    assert result is not None
    assert result.value == 64.0
    assert result.as_of == recent
