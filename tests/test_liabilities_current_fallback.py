"""LiabilitiesCurrent derivation via SEC normalization."""

from datetime import date, timedelta

from pyvalue.normalization import SECFactsNormalizer


def _recent_date(days: int = 10) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _quarter_entry(value: float, end_date: str) -> dict:
    return {
        "val": value,
        "fp": "Q1",
        "end": end_date,
        "form": "10-Q",
        "filed": end_date,
    }


def test_normalizer_derives_liabilities_current_from_components():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "AccountsPayableCurrent": {
                    "units": {"USD": [_quarter_entry(15.0, recent)]}
                },
                "AccruedLiabilitiesCurrent": {
                    "units": {"USD": [_quarter_entry(5.0, recent)]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 20.0


def test_normalizer_skips_liabilities_current_derivation_when_total_present():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "LiabilitiesCurrent": {
                    "units": {"USD": [_quarter_entry(40.0, recent)]}
                },
                "AccountsPayableCurrent": {
                    "units": {"USD": [_quarter_entry(15.0, recent)]}
                },
                "AccruedLiabilitiesCurrent": {
                    "units": {"USD": [_quarter_entry(5.0, recent)]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    liabilities = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(liabilities) == 1
    assert liabilities[0].value == 40.0


def test_normalizer_uses_combined_payables_when_components_missing():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent": {
                    "units": {"USD": [_quarter_entry(55.0, recent)]}
                }
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 55.0


def test_normalizer_prefers_components_over_combined_payables():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "AccountsPayableCurrent": {
                    "units": {"USD": [_quarter_entry(15.0, recent)]}
                },
                "AccruedLiabilitiesCurrent": {
                    "units": {"USD": [_quarter_entry(5.0, recent)]}
                },
                "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent": {
                    "units": {"USD": [_quarter_entry(120.0, recent)]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 20.0


def test_normalizer_uses_employee_liabilities_fallback():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "EmployeeRelatedLiabilitiesCurrentAndNoncurrent": {
                    "units": {"USD": [_quarter_entry(12.0, recent)]}
                }
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 12.0


def test_normalizer_prefers_employee_current_over_combined():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "EmployeeRelatedLiabilitiesCurrent": {
                    "units": {"USD": [_quarter_entry(8.0, recent)]}
                },
                "EmployeeRelatedLiabilitiesCurrentAndNoncurrent": {
                    "units": {"USD": [_quarter_entry(40.0, recent)]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 8.0
